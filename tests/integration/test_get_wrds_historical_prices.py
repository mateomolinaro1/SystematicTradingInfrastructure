import pandas as pd
import pytest

from SystematicTradingInfra.data.data_handler import DataHandler


def test_get_wrds_historical_prices_success(tmp_path):
    # create tmp dir
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    wrds_gross = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2020-01-01", "2020-01-01", "2020-01-02", "2020-01-02"]
            ),
            "permno": [10001, 10002, 10001, 10002],
            "prc": [10.0, 20.0, 11.0, 21.0],
        }
    )

    wrds_gross.to_parquet(data_dir / "wrds_gross_query.parquet")

    handler = DataHandler(data_path=data_dir)

    save_path = data_dir / "prices.parquet"

    saving_config = {
        "prices": {
            "path": save_path,
            "extension": "parquet",
        }
    }

    prices = handler.get_wrds_historical_prices(
        saving_config=saving_config,
        return_bool=True,
    )

    # assert
    assert prices is not None
    assert handler.universe_prices_wrds is not None
    assert prices.equals(handler.universe_prices_wrds)

    assert prices.shape == (2, 2)
    assert save_path.exists()
