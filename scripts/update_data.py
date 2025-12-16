from configs.config_update_data  import *
from src.SystematicTradingInfra.data.data_handler import DataHandler
from pathlib import Path
import logging

LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    filename=LOG_PATH,
    filemode="a",
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

dh = DataHandler(data_path=DATA_PATH,
                 wrds_username=WRDS_USERNAME,
                 ib_host=IB_HOST,
                 ib_port=IB_PORT,
                 ib_client_id=IB_CLIENT_ID)

dh.update_data(wrds_request=WRDS_REQUEST,
               date_cols=DATE_COLS,
               saving_config=SAVING_CONFIG_UNIVERSE,
               return_bool=RETURN_BOOL_UNIVERSE)

logging.info("Data update completed.")

