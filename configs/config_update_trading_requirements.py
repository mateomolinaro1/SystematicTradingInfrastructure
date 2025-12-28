OBJ_TO_PULL = [
    "systematic-trading-infra-storage/data/ib_historical_prices.parquet",
    "systematic-trading-infra-storage/paper_trading/signals_values.parquet",
    "systematic-trading-infra-storage/paper_trading/signals.parquet",
    "systematic-trading-infra-storage/paper_trading/bench_signals.parquet",
    "systematic-trading-infra-storage/paper_trading/bench_signals_values.parquet",
    "systematic-trading-infra-storage/paper_trading/weights.parquet",
    "systematic-trading-infra-storage/paper_trading/bench_weights.parquet",
    "systematic-trading-infra-storage/paper_trading/orders.parquet"
]

BUCKET_NAME = "systematic-trading-infra-storage"
S3_IB_HIST_PRICES_PATH = "data/ib_historical_prices.parquet"
S3_SIGNALS_LOC = "paper_trading/signals.parquet"

MAX_CONSECUTIVE_NAN = 5
REBASE_PRICES = False
N_IMPLEMENTATION_LAGS = 1
FORMAT_DATE = "%Y-%m-%d"
LOOK_BACK_FIRST_TIME = 0

NB_PERIOD_MOM = 22*12
NB_PERIOD_TO_EXCLUDE_MOM = 22*1
EXCLUDE_LAST_PERIOD_MOM = True

PCT_WINSO = (2,98)
PCT_PTF = (5,90)

INDUSTRY_SEG = None
REBAL_PERIODS = 1
PTF_TYPE = "long_only"

REBAL_PERIODS_BENCH = 22
PTF_TYPE_BENCH = "long_only"

TC = 10
TC_BENCH = 10

STRAT_NAME = "LO CSMOM"
STRAT_NAME_BENCH = "Bench Buy-and-Hold EW"

PERF_ANALYSIS = False
FREQ_DATA = None
SAVING_PATH_PLOT = None

S3_ORDERS_PATH = "systematic-trading-infra-storage/paper_trading/orders.parquet"
S3_STRAT_W_PATH = "systematic-trading-infra-storage/paper_trading/weights.parquet"
S3_WRDS_UNIVERSE_PATH = "systematic-trading-infra-storage/data/wrds_universe.parquet"

BUFFER = 0.02

CONFIG = {
    "obj_to_pull": OBJ_TO_PULL,
    "bucket_name": BUCKET_NAME,
    "s3_ib_hist_prices_name": S3_IB_HIST_PRICES_PATH,
    "max_consecutive_nan": MAX_CONSECUTIVE_NAN,
    "rebase_prices": REBASE_PRICES,
    "n_implementation_lags": N_IMPLEMENTATION_LAGS,
    "format_date": FORMAT_DATE,
    "lookback_period_first_time": LOOK_BACK_FIRST_TIME,
    "nb_period_mom": NB_PERIOD_MOM,
    "nb_period_to_exclude_mom": NB_PERIOD_TO_EXCLUDE_MOM,
    "exclude_last_period_mom": EXCLUDE_LAST_PERIOD_MOM,
    "percentiles_winsorization": PCT_WINSO,
    "percentiles_portfolios": PCT_PTF,
    "industry_segmentation": INDUSTRY_SEG,
    "rebal_periods": REBAL_PERIODS,
    "portfolio_type": PTF_TYPE,
    "rebal_periods_bench": REBAL_PERIODS_BENCH,
    "portfolio_type_bench": PTF_TYPE_BENCH,
    "transaction_costs": TC,
    "strategy_name": STRAT_NAME,
    "transaction_costs_bench": TC_BENCH,
    "strategy_name_bench": STRAT_NAME_BENCH,
    "performance_analysis": PERF_ANALYSIS,
    "freq_data": FREQ_DATA
}