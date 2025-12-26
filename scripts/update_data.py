from configs.config_update_data  import *
from src.systematic_trading_infra.data.data_handler import DataHandler
from src.systematic_trading_infra.utils.alerts import PushoverAlters
from dotenv import load_dotenv
load_dotenv()
import os
import sys
import logging
import signal

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

def shutdown_handler(signum, frame):
    logger.warning(f"Received signal {signum}. Shutting down cleanly.")
    logging.shutdown()
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

PushoverAlters.send_pushover(pushover_user=os.getenv("PUSHOVER_USER_KEY"),
                             pushover_token=os.getenv("PUSHOVER_APP_TOKEN"),
                             message="Starting Data Update",
                             title="Systematic Trading Infra")

dh = DataHandler(data_path=DATA_PATH,
                 wrds_username=os.getenv("WRDS_USERNAME"),
                 wrds_password=os.getenv("WRDS_PASSWORD"),
                 ib_host=os.getenv("IB_HOST"),
                 ib_port=int(os.getenv("IB_PORT")),
                 ib_client_id=int(os.getenv("IB_CLIENT_ID")),
                 bucket_name=BUCKET_NAME
                 )

dh.update_data(wrds_request=WRDS_REQUEST,
               date_cols=DATE_COLS,
               saving_config=SAVING_CONFIG_UNIVERSE,
               return_bool=RETURN_BOOL_UNIVERSE,
               pushover_user=os.getenv("PUSHOVER_USER_KEY"),
               pushover_token=os.getenv("PUSHOVER_APP_TOKEN"))

PushoverAlters.send_pushover(pushover_user=os.getenv("PUSHOVER_USER_KEY"),
                             pushover_token=os.getenv("PUSHOVER_APP_TOKEN"),
                             message="Ending Data Update",
                             title="Systematic Trading Infra")

logging.info("Data update completed.")

# from src.systematic_trading_infra.utils.s3_utils import s3Utils
# res = s3Utils.pull_parquet_file_from_s3(
#     path="s3://systematic-trading-infra-storage/paper_trading/orders.parquet"
# )

# import pandas as pd
# pv = pd.read_parquet(r"C:\Users\mateo\Code\ENSAE\InfraSyst\Project\SystematicTradingInfra\data\portfolio_value_historical.parquet")
# df = pd.DataFrame(index=[pd.to_datetime("2025-12-24"),pd.to_datetime("2025-12-25")],
#                   data=[1054223.5,1054223.5],
#                   columns=["portfolio_value"])
# pv = pd.concat([df,pv],axis=0)
# from src.systematic_trading_infra.utils.s3_utils import s3Utils
# s3Utils.push_object_to_s3_parquet(pv,"s3://systematic-trading-infra-storage/paper_trading/portfolio_value_historical.parquet")

# pv = s3Utils.pull_parquet_file_from_s3("s3://systematic-trading-infra-storage/paper_trading/portfolio_value_historical.parquet")
# pv = pv.iloc[:-1,:].copy()
# s3Utils.push_object_to_s3_parquet(res,"s3://systematic-trading-infra-storage/data/ib_historical_prices.parquet")


