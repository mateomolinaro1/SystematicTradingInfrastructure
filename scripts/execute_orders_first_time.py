from configs.config_execute_orders_first_time import *
from systematic_trading_infra.trading.orders_management import OrdersManagement
from dotenv import load_dotenv
import os
import logging
import sys
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

OrdersManagement.execute_orders_for_today(
    orders_s3_path=ORDERS_S3_PATH,
    ib_prices_s3=IB_PRICES_S3,
    host=os.getenv("IB_HOST"),
    port=int(os.getenv("IB_PORT")),
    client_id=int(os.getenv("IB_CLIENT_ID")),
    order_type=ORDER_TYPE,
    tif=TIF,
    outside_rth=OUTSIDE_RTH,
    place_orders_first_time=PLACE_ORDERS_FIRST_TIME
)

