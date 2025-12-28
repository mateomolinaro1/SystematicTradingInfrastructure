from systematic_trading_infra.trading.orders_management import OrdersManagement
from systematic_trading_infra.utils.alerts import PushoverAlters
from dotenv import load_dotenv
import os
import logging
import sys
load_dotenv()


def main(
        orders_s3_path: str,
        ib_prices_s3: str,
        order_type: str,
        tif: str,
        outside_rth: bool,
        place_orders_first_time: bool,
):
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger(__name__)
    logger.info("Starting order execution process.")

    PushoverAlters.send_pushover(
        pushover_user=os.getenv("PUSHOVER_USER_KEY"),
        pushover_token=os.getenv("PUSHOVER_APP_TOKEN"),
        message="Starting order execution process.",
        title="Systematic Trading Infra",
    )

    OrdersManagement.execute_orders_for_today(
        orders_s3_path=orders_s3_path,
        ib_prices_s3=ib_prices_s3,
        host=os.getenv("IB_HOST"),
        port=int(os.getenv("IB_PORT")),
        client_id=int(os.getenv("IB_CLIENT_ID")),
        order_type=order_type,
        tif=tif,
        outside_rth=outside_rth,
        place_orders_first_time=place_orders_first_time
    )

    logger.info("Order execution process completed.")
    PushoverAlters.send_pushover(
        pushover_user=os.getenv("PUSHOVER_USER_KEY"),
        pushover_token=os.getenv("PUSHOVER_APP_TOKEN"),
        message="Ending order execution process.",
        title="Systematic Trading Infra",
    )

if __name__ == "__main__":
    from configs.config_execute_orders import CONFIG
    main(**CONFIG)


