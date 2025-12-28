from systematic_trading_infra.data.data_handler import DataHandler
from systematic_trading_infra.utils.alerts import PushoverAlters
from dotenv import load_dotenv
import os
import logging
import sys


def main(
    data_path,
    bucket_name,
    wrds_request,
    date_cols,
    saving_config,
    return_bool,
):
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger(__name__)

    PushoverAlters.send_pushover(
        pushover_user=os.getenv("PUSHOVER_USER_KEY"),
        pushover_token=os.getenv("PUSHOVER_APP_TOKEN"),
        message="Starting Data Update",
        title="Systematic Trading Infra",
    )

    dh = DataHandler(
        data_path=data_path,
        wrds_username=os.getenv("WRDS_USERNAME"),
        wrds_password=os.getenv("WRDS_PASSWORD"),
        ib_host=os.getenv("IB_HOST"),
        ib_port=int(os.getenv("IB_PORT")),
        ib_client_id=int(os.getenv("IB_CLIENT_ID")),
        bucket_name=bucket_name,
    )

    dh.update_data(
        wrds_request=wrds_request,
        date_cols=date_cols,
        saving_config=saving_config,
        return_bool=return_bool,
        pushover_user=os.getenv("PUSHOVER_USER_KEY"),
        pushover_token=os.getenv("PUSHOVER_APP_TOKEN"),
    )

    PushoverAlters.send_pushover(
        pushover_user=os.getenv("PUSHOVER_USER_KEY"),
        pushover_token=os.getenv("PUSHOVER_APP_TOKEN"),
        message="Ending Data Update",
        title="Systematic Trading Infra",
    )

    logger.info("Data update completed.")

if __name__ == "__main__":
    from configs.config_update_data import CONFIG
    main(**CONFIG)