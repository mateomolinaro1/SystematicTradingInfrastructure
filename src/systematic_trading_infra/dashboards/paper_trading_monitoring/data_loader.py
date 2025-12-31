import pandas as pd
from systematic_trading_infra.utils.s3_utils import s3Utils
from dotenv import load_dotenv
load_dotenv()


# Constants
PATH_PRICES = "systematic-trading-infra-storage/data/ib_historical_prices.parquet"
PATH_ORDERS = "systematic-trading-infra-storage/paper_trading/orders.parquet"
PATH_PORTFOLIO_VALUE = "systematic-trading-infra-storage/paper_trading/portfolio_value_historical.parquet"
PATH_WEIGHTS = "systematic-trading-infra-storage/paper_trading/weights.parquet"

# helper

def _load_parquet_from_s3(path: str) -> pd.DataFrame:
    """
    Load a parquet file from S3 and return a pandas DataFrame.
    """
    df = s3Utils.pull_parquet_file_from_s3(
        path=path
    )

    if df is None or df.empty:
        return pd.DataFrame()

    return df


# utils functions

def load_prices() -> pd.DataFrame:
    """
    Load historical prices used by the strategy.
    """
    df = _load_parquet_from_s3(PATH_PRICES)

    if not df.empty:
        df.index = pd.to_datetime(df.index)
        df = df.sort_index(ascending=False)

    return df


def load_orders() -> pd.DataFrame:
    """
    Load all paper trading orders.
    """
    df = _load_parquet_from_s3(PATH_ORDERS)

    if not df.empty:
        df = df.sort_values("date", ascending=False)
        df = df.reset_index(drop=True)

    return df


def load_portfolio_value() -> pd.DataFrame:
    """
    Load historical portfolio value (PnL / NAV).
    """
    df = _load_parquet_from_s3(PATH_PORTFOLIO_VALUE)

    if not df.empty:
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()

    return df


def load_weights() -> pd.DataFrame:
    """
    Load portfolio weights or positions.
    """
    df = _load_parquet_from_s3(PATH_WEIGHTS)

    if not df.empty:
        df.index = pd.to_datetime(df.index)
        df = df.sort_index(ascending=False)

    return df
