from systematic_trading_infra.dashboards.backtesting.config import BacktestConfig
from systematic_trading_infra.dashboards.backtesting.controller import BacktestController
from dotenv import load_dotenv
load_dotenv()

if __name__ == "__main__":
    config = BacktestConfig(
        nb_period_mom=252,
        percentiles_portfolios=(20, 80),
        rebal_periods=22,
    )

    controller = BacktestController(config)
    results = controller.run()

    print(results.keys())
    print(results["strategy_net_returns"].head())
