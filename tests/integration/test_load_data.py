import pickle
import pandas as pd
import pytest

from SystematicTradingInfra.data.data_handler import DataHandler


def test_load_data_success(tmp_path):
    # fake data directory
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    # WRDS universe
    wrds_df = pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "exchcd": [3, 3],
            "cusip": ["03783310", "59491810"],
            "ncusip": ["03783310", "59491810"],
            "comnam": ["APPLE INC", "MICROSOFT CORP"],
            "permno": [10001, 10002],
            "permco": [1, 2],
            "namedt": pd.to_datetime(["2010-01-01", "2010-01-01"]),
            "nameendt": pd.to_datetime(["2030-01-01", "2030-01-01"]),
            "date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
        }
    ).set_index("date")
    wrds_df.to_parquet(data_dir / "wrds_universe.parquet")

    # IB prices
    prices_df = pd.DataFrame(
        {"price": [100.0, 101.0]},
        index=pd.to_datetime(["2020-01-02", "2020-01-03"])
    )
    prices_df.to_parquet(data_dir / "ib_historical_prices.parquet")

    # Mapping
    mapping = {"AAPL": "AAPL", "MSFT": "MSFT"}
    with open(data_dir / "crsp_to_ib_mapping_tickers.pkl", "wb") as f:
        pickle.dump(mapping, f)

    # Handler
    handler = DataHandler(data_path=data_dir)
    handler.load_data()

    # assert
    assert handler.wrds_universe is not None
    assert handler.universe_prices_ib is not None
    assert handler.crsp_to_ib_mapping_tickers is not None
    assert handler.valid_tickers_per_ib_date is not None
    assert handler.valid_permnos_per_ib_date is not None
