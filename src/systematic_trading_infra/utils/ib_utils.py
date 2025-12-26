from ib_insync import *

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
        portfolio_value = float(next(v.value for v in values if v.tag == 'NetLiquidation'))
        return portfolio_value


