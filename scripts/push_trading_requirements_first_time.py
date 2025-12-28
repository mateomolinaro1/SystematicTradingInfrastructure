from configs.config_push_trading_requirements_first_time import *
from systematic_trading_infra.trading.orders_management import OrdersManagement
from systematic_trading_infra.backtester import backtester_orchestrator
from systematic_trading_infra.utils.s3_utils import s3Utils
import sys
import logging
from dotenv import load_dotenv
load_dotenv()


logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# 1/ Backtest
backtest_orchestrator = backtester_orchestrator.BacktesterOrchestrator(
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
    freq_data=FREQ_DATA
)
res = backtest_orchestrator.run_backtest()


# 2/ push to S3 what we want to track
obj_to_push = {"systematic-trading-infra-storage/paper_trading/signals_values.parquet":res["signals_values"],
               "systematic-trading-infra-storage/paper_trading/signals.parquet":res["signals"],
               "systematic-trading-infra-storage/paper_trading/bench_signals_values.parquet":res["bench_signals_values"],
               "systematic-trading-infra-storage/paper_trading/bench_signals.parquet":res["bench_signals"],
               "systematic-trading-infra-storage/paper_trading/weights.parquet":res["weights"],
               "systematic-trading-infra-storage/paper_trading/bench_weights.parquet":res["bench_weights"]
               }
s3Utils.push_objects_to_s3_parquet(objects_dct=obj_to_push)

# 3/ create/initialize the orders df
orders_mngt = OrdersManagement(
    orders_s3_path="systematic-trading-infra-storage/paper_trading/orders.parquet",
    strat_weights_s3_path="systematic-trading-infra-storage/paper_trading/weights.parquet",
    strat_net_returns_from_backtester=res["strategy_net_returns"],
    strat_start_date_from_backtester=res["strategy_start_date"],
    prices_from_data_manager_cleaned_data=res["prices_cleaned_from_dm"],
    wrds_universe_s3_path="systematic-trading-infra-storage/data/wrds_universe.parquet",
    initial_invested_amount_backtest=INITIAL_INVESTED_AMOUNT,
    buffer=BUFFER
)
orders_mngt.build_orders_first_time()
