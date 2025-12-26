import pandas as pd
from src.systematic_trading_infra.backtester.data import AmazonS3, DataManager
from src.systematic_trading_infra.backtester import strategies, signal_utilities, portfolio, backtest, analysis
from typing import Tuple
import logging

logger = logging.getLogger(__name__)

class BacktesterOrchestrator:
    def __init__(self,
                 bucket_name:str='systematic-trading-infra-storage',
                 s3_ib_hist_prices_name:str="data/ib_historical_prices.parquet",
                 max_consecutive_nan:int=5,
                 rebase_prices:bool=False,
                 n_implementation_lags:int=1,
                 format_date:str="%Y-%m-%d",
                 lookback_period_first_time:int=0,
                 nb_period_mom:int=22*12,
                 nb_period_to_exclude_mom:int=22*1,
                 exclude_last_period_mom:bool=True,
                 percentiles_winsorization:Tuple[int,int]=(2,98),
                 percentiles_portfolios:Tuple[int,int]=(10,90),
                 industry_segmentation:pd.DataFrame|None=None,
                 rebal_periods:int=22,
                 portfolio_type:str="long_only",
                 rebal_periods_bench:int=22,
                 portfolio_type_bench:str="long_only",
                 transaction_costs:int|float=10,
                 strategy_name:str="LO CSMOM",
                 transaction_costs_bench:int|float=10,
                 strategy_name_bench:str="Bench Buy-and-Hold EW",
                 performance_analysis:bool=False,
                 freq_data:str|None=None,
                 saving_path_plot:str|None=None
                 ):
        # --- Data / IO ---
        self.bucket_name = bucket_name
        self.s3_ib_hist_prices_name = s3_ib_hist_prices_name

        # --- Data quality ---
        self.max_consecutive_nan = max_consecutive_nan
        self.rebase_prices = rebase_prices

        # --- Timing / implementation ---
        self.n_implementation_lags = n_implementation_lags
        self.format_date = format_date
        self.lookback_period_first_time = lookback_period_first_time

        # --- Momentum construction ---
        self.nb_period_mom = nb_period_mom
        self.nb_period_to_exclude_mom = nb_period_to_exclude_mom
        self.exclude_last_period_mom = exclude_last_period_mom

        # --- Cross-section processing ---
        self.percentiles_winsorization = percentiles_winsorization
        self.percentiles_portfolios = percentiles_portfolios
        self.industry_segmentation = industry_segmentation

        # --- Portfolio / rebalancing ---
        self.rebal_periods = rebal_periods
        self.portfolio_type = portfolio_type
        self.transaction_costs = transaction_costs
        self.strategy_name = strategy_name

        # --- Benchmark ---
        self.rebal_periods_bench = rebal_periods_bench
        self.portfolio_type_bench = portfolio_type_bench
        self.transaction_costs_bench = transaction_costs_bench
        self.strategy_name_bench = strategy_name_bench

        # --- Analysis and plotting ---
        self.performance_analysis = performance_analysis
        self.freq_data = freq_data
        self.saving_path_plot = saving_path_plot

        # Check
        if self.performance_analysis:
            if self.freq_data is None or self.saving_path_plot is None:
                logger.error(
                    "If performance analysis is not None, both freq_data and saving_path_plot must be provided.")
                raise ValueError(
                    "If performance analysis is not None, both freq_data and saving_path_plot must be provided.")

    def run_backtest(self)->dict:
        ds = AmazonS3(
            bucket_name=self.bucket_name,
            s3_object_name=self.s3_ib_hist_prices_name
        )
        dm = DataManager(
            data_source=ds,
            max_consecutive_nan=self.max_consecutive_nan,
            rebase_prices=self.rebase_prices,
            n_implementation_lags=self.n_implementation_lags
        )
        dm.get_data(format_date=self.format_date,
                    crop_lookback_period=self.lookback_period_first_time)

        # Strategy setup and benchmark
        strategy = strategies.CrossSectionalPercentiles(
            prices=dm.cleaned_data,
            returns=dm.returns,
            signal_function=signal_utilities.Momentum.rolling_momentum,
            signal_function_inputs={"df": dm.cleaned_data,
                                    "nb_period": self.nb_period_mom,
                                    "nb_period_to_exclude": self.nb_period_to_exclude_mom,
                                    "exclude_last_period": self.exclude_last_period_mom
                                    },
            percentiles_winsorization=self.percentiles_winsorization
        )

        strategy.compute_signals_values()
        strategy.compute_signals(
            percentiles_portfolios=self.percentiles_portfolios,
            industry_segmentation=self.industry_segmentation
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
            rebal_periods=self.rebal_periods,
            portfolio_type=self.portfolio_type
        )
        ptf.compute_weights()
        ptf.rebalance_portfolio()

        ptf_bench = portfolio.EqualWeightingScheme(
            returns=dm.returns,
            aligned_returns=dm.aligned_returns,
            signals=bench.signals,
            rebal_periods=self.rebal_periods_bench,
            portfolio_type=self.portfolio_type_bench
        )
        ptf_bench.compute_weights()
        ptf_bench.rebalance_portfolio()

        # Backtesting
        backtester = backtest.Backtest(
            returns=dm.aligned_returns,
            weights=ptf.rebalanced_weights,
            turnover=ptf.turnover,
            transaction_costs=self.transaction_costs,
            strategy_name=self.strategy_name
        )
        backtester.run_backtest()

        backtester_bench = backtest.Backtest(
            returns=dm.aligned_returns,
            weights=ptf_bench.rebalanced_weights,
            turnover=ptf_bench.turnover,
            transaction_costs=self.transaction_costs_bench,
            strategy_name=self.strategy_name_bench
        )
        backtester_bench.run_backtest()

        if self.performance_analysis:
            # Performance Analysis
            analyzer = analysis.PerformanceAnalyser(
                portfolio_returns=backtester.cropped_portfolio_net_returns,
                bench_returns=backtester_bench.portfolio_net_returns.loc[backtester.start_date:, :],
                freq=self.freq_data,
                percentiles=str(self.percentiles_portfolios),
                industries="Cross Industries" if self.industry_segmentation is None else "Intra Industries",
                rebal_freq=f"{self.rebal_periods} {self.freq_data}"
            )
            analyzer.compute_metrics()

            analyzer.plot_cumulative_performance(saving_path=self.saving_path_plot,
                                                 show=False,
                                                 blocking=False)

        dct = {"signals_values":strategy.signals_values,
               "signals":strategy.signals,
               "bench_signals_values":bench.signals_values,
               "bench_signals":bench.signals,
               "weights":ptf.rebalanced_weights,
               "bench_weights":ptf_bench.rebalanced_weights,
               "strategy_net_returns":backtester.portfolio_net_returns,
               "strategy_start_date":backtester.start_date,
               "prices_cleaned_from_dm":dm.cleaned_data
               }
        return dct