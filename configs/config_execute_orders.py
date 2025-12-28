ORDERS_S3_PATH = "systematic-trading-infra-storage/paper_trading/orders.parquet"
IB_PRICES_S3 = "systematic-trading-infra-storage/data/ib_historical_prices.parquet"
ORDER_TYPE = "MKT"
TIF = "DAY"
OUTSIDE_RTH = True
PLACE_ORDERS_FIRST_TIME = False

CONFIG = {
    "orders_s3_path": ORDERS_S3_PATH,
    "ib_prices_s3": IB_PRICES_S3,
    "order_type": ORDER_TYPE,
    "tif": TIF,
    "outside_rth": OUTSIDE_RTH,
    "place_orders_first_time": PLACE_ORDERS_FIRST_TIME
}