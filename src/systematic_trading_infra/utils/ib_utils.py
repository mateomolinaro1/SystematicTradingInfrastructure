from ib_insync import *
import logging

logger = logging.getLogger(__name__)

class IbUtils:
    def __init__(self,
                 host:str,
                 port:int,
                 client_id:int):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib = None

    def connect_ib(self)->None:
        if self.ib is None:
            self.ib = IB()
            self.ib.connect(host=self.host,
                       port=self.port,
                       clientId=self.client_id)

    def log_out_ib(self):
        if self.ib is not None and self.ib.isConnected():
            self.ib.disconnect()
            self.ib = None


    def get_portfolio_value(self)->float|int:
        values = self.ib.accountValues()
        portfolio_value = float(next(v.value for v in values if v.tag == "NetLiquidation"))
        return portfolio_value

    def get_available_funds(self)->float|int:
        values = self.ib.accountValues()
        portfolio_value = float(next(v.value for v in values if v.tag == "AvailableFunds"))
        return portfolio_value

    def place_order(self,
                    ticker:str,
                    side:str,
                    quantity:float|int,
                    exchange:str,
                    currency:str,
                    order_type:str="MKT",
                    tif:str="DAY",
                    outside_rth:bool=True):
        """
        Places an order via Interactive Brokers.
        :parameter:
        - ticker: ib ticker
        - side: 'BUY' or 'SELL'
        - quantity: quantity
        - exchange: exchange
        - currency: currency
        - order_type: 'MKT', 'LMT',... see IB for more details
        - tif: Time in Force. It tells Interactive Brokers how long the order is
            allowed to remain active if it is not immediately filled.
            'DAY','GTC','IOC','FOC','OPG','CLS'... see IB for details.
        - outside_rth: outside regular trading hours. If set to True, it allows the order to
            survive until RTH opens

        Raises:
            Exception if order is rejected or not acknowledged
        """

        if quantity <= 0:
            logger.error(f"Invalid quantity: {quantity}")
            raise ValueError(f"Invalid quantity: {quantity}")

        side = side.upper()
        if side not in {"BUY", "SELL"}:
            logger.error(f"Invalid side: {side}")
            raise ValueError(f"Invalid side: {side}")

        # 1. Build contract
        contract = Stock(
            symbol=ticker,
            exchange=exchange,
            currency=currency
        )

        # Ensure contract is valid
        try:
            self.ib.qualifyContracts(contract)
        except Exception as e:
            raise Exception(f"Contract qualification failed for {ticker}: {e}")


        # 2. Build order
        order = Order(
            action=side,
            totalQuantity=quantity,
            orderType=order_type,
            tif=tif,
            outsideRth=outside_rth
        )

        # 3. Place order
        trade = self.ib.placeOrder(contract, order)

        # 4. Wait for acknowledgement
        self.ib.sleep(0.2)

        if trade.orderStatus.status in {"Cancelled", "Inactive"}:
            raise Exception(f"Order rejected: {trade.orderStatus}")

        return trade

    def print_all_orders(self)->None:
        if self.ib is None:
            logger.error("connect IB first.")

        self.ib.reqAllOpenOrders()
        for trade in self.ib.trades():
            print(
                trade.contract.symbol,
                trade.order.action,
                trade.order.totalQuantity,
                trade.orderStatus.status
            )

    def cancel_presubmitted_orders(self)->None:
        if self.ib is None:
            logger.error("connect IB first.")
        for trade in self.ib.trades():
            if trade.orderStatus.status in {"PreSubmitted", "PendingSubmit"}:
                self.ib.cancelOrder(trade.order)
                print(
                    f"Cancel sent | {trade.contract.symbol} | "
                    f"{trade.order.action} | status={trade.orderStatus.status}"
                )

