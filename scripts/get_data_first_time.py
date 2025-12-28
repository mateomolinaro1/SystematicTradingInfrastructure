from configs.config_get_data_first_time  import *
from systematic_trading_infra.data.data_handler import DataHandler
from systematic_trading_infra.utils.files_utils import FileUtils
from dotenv import load_dotenv
load_dotenv()
import os
import logging

LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    filename=LOG_PATH,
    filemode="a",
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

dh = DataHandler(data_path=DATA_PATH,
                 wrds_username=os.getenv("WRDS_USERNAME"),
                 wrds_password=os.getenv("WRDS_PASSWORD"),
                 bucket_name=BUCKET_NAME,
                 ib_host=os.getenv("IB_HOST"),
                 ib_port=int(os.getenv("IB_PORT")),
                 ib_client_id=int(os.getenv("IB_CLIENT_ID"))
                 )

dh.connect_wrds()
dh.connect_ib()
FileUtils.delete_all_files(path=DATA_PATH, except_git_keep=True)
dh.fetch_wrds_historical_universe(wrds_request=WRDS_REQUEST,
                                  starting_date=STARTING_DATE,
                                  date_cols=DATE_COLS,
                                  saving_config=SAVING_CONFIG_UNIVERSE,
                                  save_tickers_across_dates=True,
                                  save_dates=True,
                                  return_bool=RETURN_BOOL_UNIVERSE)
dh.get_wrds_historical_prices(saving_config=SAVING_CONFIG_PRICES)
dh.get_wrds_returns()
dh.fetch_ib_historical_prices(end_date=END_DATE,
                              past_period=PAST_PERIOD,
                              frequency=FREQUENCY,
                              data_prices=DATA_PRICES,
                              use_rth=USE_RTH,
                              format_date= FORMAT_DATE,
                              save_prices=SAVE_PRICES,
                              return_bool=RETURN_BOOL_IB_PRICES
                              )
dh.fetch_portfolio_value(saving_path_pv=SAVING_PATH_PV,
                         return_bool=RET_BOOL_PV)
dh.compute_coverage()
dh.logout_wrds()
dh.logout_ib()
logging.info("Data fetching and processing completed.")

if PUSH_TO_CLOUD:
    dh.connect_aws_s3()
    dh.upload_file_to_s3(file_paths_and_s3_object_names=dh.file_paths_and_s3_object_names,
                         bucket_name=dh.bucket_name)
