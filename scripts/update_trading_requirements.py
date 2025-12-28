from systematic_trading_infra.trading.orders_management import OrdersManagement
from systematic_trading_infra.utils.alerts import PushoverAlters
from dotenv import load_dotenv
import os
import sys
import logging


def main(
    obj_to_pull,
    bucket_name,
    s3_ib_hist_prices_name,
    max_consecutive_nan,
    rebase_prices,
    n_implementation_lags,
    format_date,
    lookback_period_first_time,
    nb_period_mom,
    nb_period_to_exclude_mom,
    exclude_last_period_mom,
    percentiles_winsorization,
    percentiles_portfolios,
    industry_segmentation,
    rebal_periods,
    portfolio_type,
    rebal_periods_bench,
    portfolio_type_bench,
    transaction_costs,
    strategy_name,
    transaction_costs_bench,
    strategy_name_bench,
    performance_analysis,
    freq_data
):
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger(__name__)

    PushoverAlters.send_pushover(
        pushover_user=os.getenv("PUSHOVER_USER_KEY"),
        pushover_token=os.getenv("PUSHOVER_APP_TOKEN"),
        message="Starting Trading Requirements Update",
        title="Systematic Trading Infra",
    )

    OrdersManagement.update_trading_requirements(
        obj_to_pull=obj_to_pull,
        bucket_name=bucket_name,
        s3_ib_hist_prices_name=s3_ib_hist_prices_name,
        max_consecutive_nan=max_consecutive_nan,
        rebase_prices=rebase_prices,
        n_implementation_lags=n_implementation_lags,
        format_date=format_date,
        lookback_period_first_time=lookback_period_first_time,
        nb_period_mom=nb_period_mom,
        nb_period_to_exclude_mom=nb_period_to_exclude_mom,
        exclude_last_period_mom=exclude_last_period_mom,
        percentiles_winsorization=percentiles_winsorization,
        percentiles_portfolios=percentiles_portfolios,
        industry_segmentation=industry_segmentation,
        rebal_periods=rebal_periods,
        portfolio_type=portfolio_type,
        rebal_periods_bench=rebal_periods_bench,
        portfolio_type_bench=portfolio_type_bench,
        transaction_costs=transaction_costs,
        strategy_name=strategy_name,
        transaction_costs_bench=transaction_costs_bench,
        strategy_name_bench=strategy_name_bench,
        performance_analysis=performance_analysis,
        freq_data=freq_data
    )

    PushoverAlters.send_pushover(
        pushover_user=os.getenv("PUSHOVER_USER_KEY"),
        pushover_token=os.getenv("PUSHOVER_APP_TOKEN"),
        message="Ending Trading Requirements Update",
        title="Systematic Trading Infra",
    )

    logger.info("Trading requirements update completed.")

if __name__ == "__main__":
    from configs.config_update_trading_requirements import CONFIG
    main(**CONFIG)
