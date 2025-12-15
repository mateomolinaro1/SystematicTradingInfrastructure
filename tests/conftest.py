import pytest
import pandas as pd
import numpy as np
from datetime import date
from pathlib import Path

@pytest.fixture(autouse=True)
def fixed_random_seed():
    np.random.seed(42)

@pytest.fixture
def simple_returns_df():
    dates = pd.date_range("2020-01-01", periods=5)
    return pd.DataFrame(
        np.random.normal(0, 0.01, (5, 3)),
        index=dates,
        columns=["A", "B", "C"],
    )
