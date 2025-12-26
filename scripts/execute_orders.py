from ib_insync import IB, Stock, MarketOrder

ib = IB()
ib.connect('127.0.0.1', 4002, clientId=1)

contract = Stock('AAPL', 'SMART', 'USD')
ib.qualifyContracts(contract)

order = MarketOrder('SELL', 1)

trade = ib.placeOrder(contract, order)

trade.filledEvent += lambda trade: print("Order filled")
trade.cancelledEvent += lambda trade: print("Order cancelled")

ib.cancelOrder(trade.order)

open_trades = ib.reqOpenOrders()
for trade in open_trades:
    print(trade.contract.symbol, trade.order.action, trade.order.totalQuantity, trade.orderStatus.status)
