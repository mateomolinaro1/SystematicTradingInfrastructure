from systematic_trading_infra.dashboards.backtesting.config import BacktestConfig
from systematic_trading_infra.backtester.backtester_orchestrator import BacktesterOrchestrator


class BacktestController:
    def __init__(self, config: BacktestConfig):
        self.config = config

    def run(self) -> dict:
        orchestrator = BacktesterOrchestrator(
            bucket_name=self.config.bucket_name,
            s3_ib_hist_prices_name=self.config.s3_ib_hist_prices_name,
            max_consecutive_nan=self.config.max_consecutive_nan,
            rebase_prices=self.config.rebase_prices,
            n_implementation_lags=self.config.n_implementation_lags,
            format_date=self.config.format_date,
            lookback_period_first_time=self.config.lookback_period_first_time,
            nb_period_mom=self.config.nb_period_mom,
            nb_period_to_exclude_mom=self.config.nb_period_to_exclude_mom,
            exclude_last_period_mom=self.config.exclude_last_period_mom,
            percentiles_winsorization=self.config.percentiles_winsorization,
            percentiles_portfolios=self.config.percentiles_portfolios,
            industry_segmentation=self.config.industry_segmentation,
            rebal_periods=self.config.rebal_periods,
            portfolio_type=self.config.portfolio_type,
            transaction_costs=self.config.transaction_costs,
            rebal_periods_bench=self.config.rebal_periods_bench,
            portfolio_type_bench=self.config.portfolio_type_bench,
            transaction_costs_bench=self.config.transaction_costs_bench,
            strategy_name=self.config.strategy_name,
            strategy_name_bench=self.config.strategy_name_bench,
        )

        return orchestrator.run_backtest()
