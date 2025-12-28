from dataclasses import dataclass
from typing import Tuple
import pandas as pd


@dataclass
class BacktestConfig:
    # --- Data ---
    bucket_name: str = "systematic-trading-infra-storage"
    s3_ib_hist_prices_name: str = "backtest/wrds_universe_prices.parquet"
    max_consecutive_nan: int = 5
    rebase_prices: bool = False

    # --- Timing ---
    n_implementation_lags: int = 1
    format_date: str = "%Y-%m-%d"
    lookback_period_first_time: int = 0

    # --- Momentum ---
    nb_period_mom: int = 22 * 12
    nb_period_to_exclude_mom: int = 22
    exclude_last_period_mom: bool = True

    # --- Cross-section ---
    percentiles_winsorization: Tuple[int, int] = (2, 98)
    percentiles_portfolios: Tuple[int, int] = (10, 90)
    industry_segmentation: pd.DataFrame | None = None

    # --- Portfolio ---
    rebal_periods: int = 22
    portfolio_type: str = "long_only"
    transaction_costs: float = 10

    # --- Benchmark ---
    rebal_periods_bench: int = 22
    portfolio_type_bench: str = "long_only"
    transaction_costs_bench: float = 10

    # --- Meta ---
    strategy_name: str = "LO CSMOM"
    strategy_name_bench: str = "Bench Buy-and-Hold EW"
