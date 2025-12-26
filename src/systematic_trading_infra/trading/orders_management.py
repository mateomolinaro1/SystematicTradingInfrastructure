import time
from datetime import date

import numpy as np
import pandas as pd
from src.systematic_trading_infra.utils.s3_utils import s3Utils
from src.systematic_trading_infra.utils.ib_utils import IbUtils
from src.systematic_trading_infra.backtester.backtester_orchestrator import BacktesterOrchestrator
from src.systematic_trading_infra.utils.alerts import PushoverAlters
from typing import List, Tuple
import logging
import os

logger = logging.getLogger(__name__)

class OrdersManagement:

    def __init__(self,
                 orders_s3_path:str,
                 strat_weights_s3_path:str,
                 strat_net_returns_from_backtester:pd.DataFrame,
                 strat_start_date_from_backtester:pd.Timestamp,
                 prices_from_data_manager_cleaned_data:pd.DataFrame,
                 wrds_universe_s3_path:str,
                 initial_invested_amount_backtest:float|int=1000000,
                 buffer:float=0.02):

        self.orders_s3_path = orders_s3_path
        self.strat_weights_s3_path = strat_weights_s3_path
        self.strat_net_returns_from_backtester = strat_net_returns_from_backtester
        self.strat_start_date_from_backtester = strat_start_date_from_backtester
        self.prices_from_data_manager_cleaned_data = prices_from_data_manager_cleaned_data
        self.wrds_universe_s3_path = wrds_universe_s3_path
        self.initial_invested_amount_backtest = initial_invested_amount_backtest
        self.buffer = buffer

        self.strat_weights = None
        self.rebalancing_dates_index = None
        self.orders = None
        self.delta_weights = None
        self.portfolio_value = None
        self.portfolio_value_at_rebal = None
        self.quantities = None
        self.delta_weights_at_rebal = None
        self.prices_at_rebal = None
        self.wrds_universe = None
        self.wrds_universe_at_rebal = None

    @staticmethod
    def create_new_object(new_orders_s3_path: str,
                          new_strat_weights_s3_path: str,
                          new_strat_net_returns_from_backtester: pd.DataFrame,
                          new_strat_start_date_from_backtester: pd.Timestamp,
                          new_prices_from_data_manager_cleaned_data: pd.DataFrame,
                          new_wrds_universe_s3_path,
                          new_initial_invested_amount_backtest: float | int = 1000000,
                          new_buffer: float = 0.02):

        new_obj = OrdersManagement(
            orders_s3_path=new_orders_s3_path,
            strat_weights_s3_path=new_strat_weights_s3_path,
            strat_net_returns_from_backtester=new_strat_net_returns_from_backtester,
            strat_start_date_from_backtester=new_strat_start_date_from_backtester,
            prices_from_data_manager_cleaned_data=new_prices_from_data_manager_cleaned_data,
            wrds_universe_s3_path=new_wrds_universe_s3_path,
            initial_invested_amount_backtest=new_initial_invested_amount_backtest,
            buffer=new_buffer
        )
        return new_obj

    def build_orders_first_time(self)->None:
        if self.strat_weights is None:
            self.strat_weights = s3Utils.pull_parquet_file_from_s3(path=self.strat_weights_s3_path)

        # Filling the dates column
        flg = self.strat_weights.notna().any(axis=1)
        sub_w = self.strat_weights[flg]
        flg_rebal = sub_w.apply(lambda row: row.dropna().nunique() <= 1, axis=1)

        if self.rebalancing_dates_index is None:
            self.rebalancing_dates_index = sub_w.index[flg_rebal]

        # Filling the ib_ticker column
        notna_ib_tickers_at_rebal = {}
        for rebal_date in list(self.rebalancing_dates_index):
            flg_tickers = self.strat_weights.loc[rebal_date, :].notna()
            notna_ib_tickers_at_rebal[rebal_date] = sorted(list(self.strat_weights.columns[flg_tickers]))

        if self.orders is None:
            self.orders = (
                pd.DataFrame.from_dict(notna_ib_tickers_at_rebal, orient="index")
                .stack()
                .reset_index(level=1, drop=True)
                .rename("ib_ticker")
                .reset_index()
                .rename(columns={"index": "date"})
            )

        # Filling order_type column: at the inception date it is all BUY
        # Then at other rebal dates we have to compute the delta weights and take the sign. If + BUY else SELL
        self.orders["order_type"] = pd.Series(pd.NA, index=self.orders.index, dtype="string")
        mask = self.orders["date"] == self.orders["date"][0]
        self.orders.loc[mask, "order_type"] = "BUY"

        # For the other rebal dates
        if self.delta_weights is None:
            self.delta_weights = self.strat_weights - self.strat_weights.shift(1).fillna(0.0)

        order_type = pd.DataFrame(pd.NA, index=self.delta_weights.index, columns=self.delta_weights.columns)
        order_type[self.delta_weights > 0] = "BUY"
        order_type[self.delta_weights < 0] = "SELL"
        order_type = order_type.astype("string")
        order_type_long = (
            order_type
            .stack()
            .rename("order_type")
            .reset_index()
            .rename(columns={"level_1": "ib_ticker"})
        )
        self.orders = self.orders.drop(columns="order_type", errors="ignore")
        self.orders = self.orders.merge(
            order_type_long,
            on=["date", "ib_ticker"],
            how="left"
        )

        # Column quantity_i= portfolio_value*(1-buffer)*delta_weights_i/market_price_i
        # to fill history we'll use the cumulative perf * 1 000 000 as the portfolio_value
        if self.portfolio_value is None:
                self.portfolio_value = (1 + self.strat_net_returns_from_backtester.loc[
                                   self.strat_start_date_from_backtester:,:]).cumprod() * self.initial_invested_amount_backtest

        self.portfolio_value.rename(columns={self.portfolio_value.columns[0]: "portfolio_value"}, inplace=True)

        if self.portfolio_value_at_rebal is None:
            self.portfolio_value_at_rebal = self.portfolio_value.loc[self.portfolio_value.index.isin(self.rebalancing_dates_index), :]
        self.portfolio_value_at_rebal = self.portfolio_value_at_rebal.copy()
        self.portfolio_value_at_rebal.loc[self.portfolio_value_at_rebal.index[0], :] = self.initial_invested_amount_backtest

        if self.quantities is None:
            self.quantities = pd.DataFrame(index=self.rebalancing_dates_index, columns=self.strat_weights.columns, dtype=float)
        if self.delta_weights_at_rebal is None:
            self.delta_weights_at_rebal = self.delta_weights.loc[self.delta_weights.index.isin(self.rebalancing_dates_index), :]
        if self.prices_at_rebal is None:
            self.prices_at_rebal = self.prices_from_data_manager_cleaned_data.loc[self.prices_from_data_manager_cleaned_data.index.isin(self.rebalancing_dates_index), :]

        self.quantities.loc[:, :] = abs(
            (self.portfolio_value_at_rebal.values * (1 - self.buffer) * self.delta_weights_at_rebal.values) / self.prices_at_rebal.values
        )

        quantities_long = (
            self.quantities
            .stack()
            .rename("quantity")
            .reset_index()
            .rename(columns={"level_1": "ib_ticker"})
        )
        self.orders = self.orders.merge(
            quantities_long,
            on=["date", "ib_ticker"],
            how="left"
        )

        # Filling exchange and currency column
        if self.wrds_universe is None:
            self.wrds_universe = s3Utils.pull_parquet_file_from_s3(path=self.wrds_universe_s3_path)

        self.wrds_universe = self.wrds_universe.reset_index()

        if self.wrds_universe_at_rebal is None:
            self.wrds_universe_at_rebal = pd.DataFrame(data={"date": self.orders["date"],
                                                             "ticker_ib": self.orders["ib_ticker"]
                                                             },
                                                       index=self.orders.index
                                                       )
        self.wrds_universe_at_rebal = self.wrds_universe_at_rebal.sort_values("date")
        self.wrds_universe = self.wrds_universe.sort_values("date")

        self.wrds_universe_at_rebal = pd.merge_asof(
            left=self.wrds_universe_at_rebal,
            right=self.wrds_universe,
            on="date",
            by="ticker_ib",
            direction="backward",
        )
        self.orders = pd.merge(
            left=self.orders,
            right=self.wrds_universe_at_rebal.loc[:, ["date", "ticker_ib", "currency", "exchange_ib"]],
            left_on=["date", "ib_ticker"],
            right_on=["date", "ticker_ib"]
        )
        self.orders.drop(columns="ticker_ib", inplace=True)

        # Penultimate step: delete the rows where order type is na because it means it's not an order
        self.orders = self.orders.loc[self.orders["order_type"].notna(),:].copy()

        # Finally, push the orders df to s3
        s3Utils.push_object_to_s3_parquet(object_to_push=self.orders,
                                          path=self.orders_s3_path
                                          )
        return

    @staticmethod
    def update_orders(
            buffer:float,
            prices_cleaned_from_dm: pd.DataFrame
    ) -> dict | None:
        """
        Incrementally compute new orders and append them to historical ones.
        Orders are immutable.
        """

        # --------------------------------------------------
        # 1. Load historical orders
        # --------------------------------------------------
        old_orders = s3Utils.pull_parquet_file_from_s3(
            path="s3://systematic-trading-infra-storage/paper_trading/orders.parquet"
        )
        old_orders["date"] = pd.to_datetime(old_orders["date"])
        last_order_date = old_orders["date"].max()

        # --------------------------------------------------
        # 2. Identify rebalancing dates
        # --------------------------------------------------
        strat_weights = s3Utils.pull_parquet_file_from_s3(
            path="s3://systematic-trading-infra-storage/paper_trading/weights.parquet"
        )
        flg = strat_weights.notna().any(axis=1)
        sub_w = strat_weights.loc[flg]

        flg_rebal = sub_w.apply(
            lambda row: row.dropna().nunique() <= 1,
            axis=1
        )
        rebalancing_dates = sub_w.index[flg_rebal]

        # --------------------------------------------------
        # 3. Check if today is a NEW rebalancing date
        # --------------------------------------------------
        today = pd.Timestamp(date.today())

        if today not in rebalancing_dates:
            logger.info("Today is not a rebalancing date")
            return None

        if today <= last_order_date:
            logger.info("Rebalancing date already processed")
            return None

        new_rebal_dates = rebalancing_dates[
            rebalancing_dates > last_order_date
            ]

        # --------------------------------------------------
        # 4. Pull portfolio value from S3
        # --------------------------------------------------
        portfolio_value = s3Utils.pull_parquet_file_from_s3(
            path="s3://systematic-trading-infra-storage/paper_trading/portfolio_value_historical.parquet"
        )
        portfolio_value = portfolio_value.sort_index()

        portfolio_value_at_rebal = portfolio_value.loc[
            portfolio_value.index.isin(new_rebal_dates)
        ]

        # --------------------------------------------------
        # 5. Compute delta weights (only what is needed)
        # --------------------------------------------------
        delta_weights = strat_weights - strat_weights.shift(1).fillna(0.0)
        delta_weights_at_rebal = delta_weights.loc[new_rebal_dates]

        # --------------------------------------------------
        # 6. Prices at rebalancing
        # --------------------------------------------------
        prices_at_rebal = prices_cleaned_from_dm.loc[
            prices_cleaned_from_dm.index.isin(new_rebal_dates)
        ]

        # --------------------------------------------------
        # 7. Build orders for new dates
        # --------------------------------------------------
        new_orders = []

        for rebal_date in new_rebal_dates:
            active_tickers = strat_weights.loc[rebal_date].dropna().index

            # Check
            if rebal_date not in portfolio_value_at_rebal.index:
                logger.warning(f"Missing portfolio value for {rebal_date}")
                print(f"Missing portfolio value for {rebal_date}")

            for ticker in active_tickers:
                dw = delta_weights_at_rebal.loc[rebal_date, ticker]
                if pd.isna(dw) or dw == 0:
                    continue

                order_type = "BUY" if dw > 0 else "SELL"

                # Check
                price = prices_at_rebal.loc[rebal_date, ticker]
                if pd.isna(price) or price <= 0:
                    logger.warning(f"Invalid price for {ticker} on {rebal_date}")
                    print(f"Invalid price for {ticker} on {rebal_date}")

                quantity = abs(
                    portfolio_value_at_rebal.loc[rebal_date].values[0]
                    * (1 - buffer)
                    * dw
                    / prices_at_rebal.loc[rebal_date, ticker]
                )

                new_orders.append({
                    "date": rebal_date,
                    "ib_ticker": ticker,
                    "order_type": order_type,
                    "quantity": quantity
                })

        new_orders = pd.DataFrame(new_orders)

        # --------------------------------------------------
        # 8. Safety checks
        # --------------------------------------------------
        assert not new_orders.empty, "No new orders computed"
        if new_orders.empty:
            logger.error("No new orders computed")
        assert not new_orders.duplicated(
            subset=["date", "ib_ticker"]
        ).any(), "Duplicate orders detected"
        if new_orders.duplicated(subset=["date", "ib_ticker"]).any():
            logger.error("Duplicate orders detected")
        assert (new_orders["quantity"] > 0).all(), "Non-positive quantities"
        if not (new_orders["quantity"] > 0).all():
            logger.error("Non-positive quantities")

        # --------------------------------------------------
        # 10. Enrich new orders with currency & exchange
        # --------------------------------------------------
        wrds_universe = s3Utils.pull_parquet_file_from_s3(
            path="s3://systematic-trading-infra-storage/data/wrds_universe.parquet"
        )

        wrds_universe = wrds_universe.sort_index()

        wrds_universe_at_rebal = (
            pd.DataFrame(
                {
                    "date": new_orders["date"],
                    "ticker_ib": new_orders["ib_ticker"]
                },
                index=new_orders.index
            )
            .sort_values("date")
        )

        wrds_universe_at_rebal = pd.merge_asof(
            left=wrds_universe_at_rebal,
            right=wrds_universe,
            on="date",
            by="ticker_ib",
            direction="backward"
        )

        new_orders = pd.merge(
            left=new_orders,
            right=wrds_universe_at_rebal.loc[
                  :, ["date", "ticker_ib", "currency", "exchange_ib"]
                  ],
            left_on=["date", "ib_ticker"],
            right_on=["date", "ticker_ib"],
            how="left"
        )

        new_orders.drop(columns="ticker_ib", inplace=True)

        # --------------------------------------------------
        # 11. Concatenate & return
        # --------------------------------------------------
        updated_orders = (
            pd.concat([old_orders, new_orders], axis=0)
            .sort_values("date")
            .reset_index(drop=True)
        )

        return {"paper_trading/orders.parquet": updated_orders}

    @staticmethod
    def update_df(df_name:str,
                  old_signals_values:pd.DataFrame,
                  new_signals_values:pd.DataFrame,
                  )->None|pd.DataFrame:

        old_signals_values = old_signals_values.sort_index(ascending=True)
        last_date_old = old_signals_values.index[-1]
        new_signals_values = new_signals_values.sort_index(ascending=True)
        first_date_new = new_signals_values.index[-1]

        # Send pushover alter to monitor on phone if away from computer
        msg = "last date old " + df_name + " " + str(last_date_old)
        PushoverAlters.send_pushover(pushover_user=os.getenv("PUSHOVER_USER_KEY"),
                                     pushover_token=os.getenv("PUSHOVER_APP_TOKEN"),
                                     message=msg,
                                     title="Systematic Trading Infra")

        msg = "first date new " + df_name + " " + str(first_date_new)
        PushoverAlters.send_pushover(pushover_user=os.getenv("PUSHOVER_USER_KEY"),
                                     pushover_token=os.getenv("PUSHOVER_APP_TOKEN"),
                                     message=msg,
                                     title="Systematic Trading Infra")

        if not first_date_new > last_date_old:
            print("No new " + df_name + " during update.")
            logger.info("No new signals_values during update.")
            return None
        if not isinstance(old_signals_values.index, pd.DatetimeIndex):
            logger.error("old_signals_values index must be DatetimeIndex")
            raise TypeError("old_signals_values index must be DatetimeIndex")
        if not isinstance(new_signals_values.index, pd.DatetimeIndex):
            logger.error("new_signals_values index must be DatetimeIndex")
            raise TypeError("new_signals_values index must be DatetimeIndex")
        if new_signals_values.index.duplicated().any():
            logger.error("new_signals_values contains duplicate dates")
            raise ValueError("new_signals_values contains duplicate dates")

        # Warning: we can just vertically concatenate because if between the last date and the date
        # of the update, one or more stocks might have entered the universe and so, we must add a
        # new column for them and back-fill with nan and take care of the ordering of the columns
        # between the 2 dfs but pd.concat already handle that!!
        flg_new_data = new_signals_values.index > last_date_old
        new_data = new_signals_values.loc[flg_new_data,:].copy()
        updated_data = pd.concat([old_signals_values, new_data], axis=0)

        return updated_data

    @staticmethod
    def update_trading_requirements(
                                    obj_to_pull:List[str],
                                    bucket_name: str = 'systematic-trading-infra-storage',
                                    s3_ib_hist_prices_name: str = "data/ib_historical_prices.parquet",
                                    buffer:float=0.02,
                                    max_consecutive_nan: int = 5,
                                    rebase_prices: bool = False,
                                    n_implementation_lags: int = 1,
                                    format_date: str = "%Y-%m-%d",
                                    lookback_period_first_time: int = 0,
                                    nb_period_mom: int = 22 * 12,
                                    nb_period_to_exclude_mom: int = 22 * 1,
                                    exclude_last_period_mom: bool = True,
                                    percentiles_winsorization: Tuple[int, int] = (2, 98),
                                    percentiles_portfolios: Tuple[int, int] = (10, 90),
                                    industry_segmentation: pd.DataFrame | None = None,
                                    rebal_periods: int = 22,
                                    portfolio_type: str = "long_only",
                                    rebal_periods_bench: int = 22,
                                    portfolio_type_bench: str = "long_only",
                                    transaction_costs: int | float = 10,
                                    strategy_name: str = "LO CSMOM",
                                    transaction_costs_bench: int | float = 10,
                                    strategy_name_bench: str = "Bench Buy-and-Hold EW",
                                    performance_analysis: bool = False,
                                    freq_data: str | None = None,
                                    saving_path_plot: str | None = None
                                    )->None:

        # Step 0: Check if the current files are on s3
        # Unnecessary because if not the below function will fail

        # Step 1: load s3 files to update
        old_trading_requirements = s3Utils.pull_parquet_files_from_s3(paths=obj_to_pull)
        # Step 2: update each file:
        # Step 2.1: update signals_values: run the new backtest with the latest data available
        # then add the new rows from the last available date in the previous backtest data.
        backtester_orchestrator = BacktesterOrchestrator(
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
            freq_data=freq_data,
            saving_path_plot=saving_path_plot
        )
        new_trading_requirements = backtester_orchestrator.run_backtest()
        # Update

        to_update = ["signals_values",
                     "signals",
                     "weights",
                     "bench_signals_values",
                     "bench_signals",
                     "bench_weights"]

        for df_name in to_update:
            updated_df = OrdersManagement.update_df(df_name=df_name,
                old_signals_values=old_trading_requirements[df_name],
                new_signals_values=new_trading_requirements[df_name]
            )
            if updated_df is None:
                continue
            else:
                s = "paper_trading/" + df_name + ".parquet"
                files_dct = {
                    s:updated_df
                }
                creds = s3Utils.get_credentials(return_bool=True)
                s3 = s3Utils.connect_aws_s3(creds=creds)
                s3Utils.replace_existing_files_in_s3(s3=s3,
                                                     bucket_name=bucket_name,
                                                     files_dct=files_dct
                                                     )

        # Finally, we'll update separately orders because it is more complex
        time.sleep(10) # we wait to be sure that the files on s3 are well replaced

        orders_dct = OrdersManagement.update_orders(
            buffer=buffer,
            prices_cleaned_from_dm=new_trading_requirements["prices_cleaned_from_dm"]
        )

        if orders_dct is None:
            pass
        else:
            creds = s3Utils.get_credentials(return_bool=True)
            s3 = s3Utils.connect_aws_s3(creds=creds)
            s3Utils.replace_existing_files_in_s3(s3=s3,
                                                 bucket_name=bucket_name,
                                                 files_dct=orders_dct
                                                 )

        return

