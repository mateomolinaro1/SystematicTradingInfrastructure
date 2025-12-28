from configs.config_run_backtest import *
from systematic_trading_infra.backtester.data import DataManager
from dotenv import load_dotenv
load_dotenv()
from systematic_trading_infra.backtester.data import AmazonS3
from systematic_trading_infra.backtester import strategies, signal_utilities, portfolio, backtest, analysis, visualization
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

for S3_OBJECT_NAME, SAVING_PATH in zip(S3_OBJECT_NAMES, SAVING_PATHS):
    # Data retrieval
    ds = AmazonS3(
        bucket_name=BUCKET_NAME,
        s3_object_name=S3_OBJECT_NAME
    )
    dm = DataManager(
        data_source=ds,
        max_consecutive_nan=MAX_CONSECUTIVE_NAN,
        rebase_prices=REBASE_PRICES,
        n_implementation_lags=N_IMPLEMENTATION_LAGS
    )
    dm.get_data(format_date=FORMAT_DATE)

    # Strategy setup and benchmark
    strategy = strategies.CrossSectionalPercentiles(
        prices=dm.cleaned_data,
        returns=dm.returns,
        signal_function=signal_utilities.Momentum.rolling_momentum,
        signal_function_inputs={"df":dm.cleaned_data,
                                "nb_period":NB_PERIOD_MOM,
                                "nb_period_to_exclude":NB_PERIOD_TO_EXCLUDE_MOM,
                                "exclude_last_period":EXCLUDE_LAST_PERIOD_MOM
                                },
        percentiles_winsorization=PCT_WINSO
    )

    strategy.compute_signals_values()
    strategy.compute_signals(
        percentiles_portfolios=PCT_PTF,
        industry_segmentation=INDUSTRY_SEG
    )

    bench = strategies.BuyAndHold(
        prices=dm.cleaned_data,
        returns=dm.returns
    )

    bench.compute_signals_values()
    bench.compute_signals()

    # Portfolio level
    ptf = portfolio.EqualWeightingScheme(
        returns=dm.returns,
        aligned_returns=dm.aligned_returns,
        signals=strategy.signals,
        rebal_periods=REBAL_PERIODS,
        portfolio_type=PTF_TYPE
    )
    ptf.compute_weights()
    ptf.rebalance_portfolio()

    ptf_bench = portfolio.EqualWeightingScheme(
        returns=dm.returns,
        aligned_returns=dm.aligned_returns,
        signals=bench.signals,
        rebal_periods=REBAL_PERIODS_BENCH,
        portfolio_type=PTF_TYPE_BENCH
    )
    ptf_bench.compute_weights()
    ptf_bench.rebalance_portfolio()

    # Backtesting
    backtester = backtest.Backtest(
        returns=dm.aligned_returns,
        weights=ptf.rebalanced_weights,
        turnover=ptf.turnover,
        transaction_costs=TC,
        strategy_name=STRAT_NAME
    )
    backtester.run_backtest()

    backtester_bench = backtest.Backtest(
        returns=dm.aligned_returns,
        weights=ptf_bench.rebalanced_weights,
        turnover=ptf_bench.turnover,
        transaction_costs=TC_BENCH,
        strategy_name=STRAT_NAME_BENCH
    )
    backtester_bench.run_backtest()

    # Performance Analysis
    analyzer = analysis.PerformanceAnalyser(
        portfolio_returns=backtester.cropped_portfolio_net_returns,
        bench_returns=backtester_bench.portfolio_net_returns.loc[backtester.start_date:,:],
        freq=FREQ_DATA,
        percentiles=str(PCT_PTF),
        industries="Cross Industries" if INDUSTRY_SEG is None else "Intra Industries",
        rebal_freq=f"{REBAL_PERIODS}"+FREQ_DATA
    )
    metrics = analyzer.compute_metrics()

    analyzer.plot_cumulative_performance(saving_path=SAVING_PATH,
                                         show=False,
                                         blocking=False)
