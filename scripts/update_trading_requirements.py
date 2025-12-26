from configs.config_update_trading_requirements import *
from src.systematic_trading_infra.trading.orders_management import OrdersManagement
import sys
import os
import logging
from src.systematic_trading_infra.utils.alerts import PushoverAlters
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Production script to update the trading requirements files (the ones at
# "s3://systematic-trading-infra-storage/paper_trading/").
PushoverAlters.send_pushover(pushover_user=os.getenv("PUSHOVER_USER_KEY"),
                             pushover_token=os.getenv("PUSHOVER_APP_TOKEN"),
                             message="Starting Trading Requirements Update",
                             title="Systematic Trading Infra")

obj_to_pull = [
    "s3://systematic-trading-infra-storage/data/ib_historical_prices.parquet",
    "s3://systematic-trading-infra-storage/paper_trading/signals_values.parquet",
    "s3://systematic-trading-infra-storage/paper_trading/signals.parquet",
    "s3://systematic-trading-infra-storage/paper_trading/bench_signals.parquet",
    "s3://systematic-trading-infra-storage/paper_trading/bench_signals_values.parquet",
    "s3://systematic-trading-infra-storage/paper_trading/weights.parquet",
    "s3://systematic-trading-infra-storage/paper_trading/bench_weights.parquet",
    "s3://systematic-trading-infra-storage/paper_trading/orders.parquet"
]

# from src.systematic_trading_infra.utils.s3_utils import s3Utils
# dfs = s3Utils.pull_parquet_files_from_s3(obj_to_pull)
#
# dfs["orders"] = dfs["orders"].iloc[0:-1,:].copy()
#
# s3Utils.push_object_to_s3_parquet(dfs["orders"],
#                                   path="s3://systematic-trading-infra-storage/paper_trading/orders.parquet"
#                                   )

OrdersManagement.update_trading_requirements(
    obj_to_pull=obj_to_pull,
    bucket_name=BUCKET_NAME,
    s3_ib_hist_prices_name=S3_IB_HIST_PRICES_PATH,
    max_consecutive_nan=MAX_CONSECUTIVE_NAN,
    rebase_prices=REBASE_PRICES,
    n_implementation_lags=N_IMPLEMENTATION_LAGS,
    format_date=FORMAT_DATE,
    lookback_period_first_time=LOOK_BACK_FIRST_TIME,
    nb_period_mom=NB_PERIOD_MOM,
    nb_period_to_exclude_mom=NB_PERIOD_TO_EXCLUDE_MOM,
    exclude_last_period_mom=EXCLUDE_LAST_PERIOD_MOM,
    percentiles_winsorization=PCT_WINSO,
    percentiles_portfolios=PCT_PTF,
    industry_segmentation=INDUSTRY_SEG,
    rebal_periods=REBAL_PERIODS,
    portfolio_type=PTF_TYPE,
    rebal_periods_bench=REBAL_PERIODS_BENCH,
    portfolio_type_bench=PTF_TYPE_BENCH,
    transaction_costs=TC,
    strategy_name=STRAT_NAME,
    transaction_costs_bench=TC_BENCH,
    strategy_name_bench=STRAT_NAME_BENCH,
    performance_analysis=PERF_ANALYSIS,
    freq_data=FREQ_DATA,
    saving_path_plot=SAVING_PATH_PLOT
)

PushoverAlters.send_pushover(pushover_user=os.getenv("PUSHOVER_USER_KEY"),
                             pushover_token=os.getenv("PUSHOVER_APP_TOKEN"),
                             message="Ending Trading Requirements Update",
                             title="Systematic Trading Infra")

# from src.systematic_trading_infra.utils.s3_utils import s3Utils
# sw = s3Utils.pull_parquet_file_from_s3(
#             path="s3://systematic-trading-infra-storage/paper_trading/weights.parquet"
#         )
# wu = s3Utils.pull_parquet_file_from_s3(
#             path="s3://systematic-trading-infra-storage/data/wrds_universe.parquet"
#         )
# o = s3Utils.pull_parquet_file_from_s3(
#             path="s3://systematic-trading-infra-storage/paper_trading/orders.parquet"
#         )