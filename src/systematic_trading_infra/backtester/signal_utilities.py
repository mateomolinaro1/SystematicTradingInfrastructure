import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Union, Tuple
from sklearn.linear_model import LinearRegression
from src.systematic_trading_infra.backtester import utilities

class Momentum:

    @staticmethod
    def rolling_momentum(
            df: pd.DataFrame,
            nb_period: int,
            nb_period_to_exclude: int | None = None,
            exclude_last_period: bool = False,
    ) -> pd.DataFrame:

        if exclude_last_period:
            if nb_period_to_exclude is None:
                raise ValueError("nb_period_to_exclude must be provided")
            end_shift = nb_period_to_exclude
        else:
            end_shift = 0

        start_shift = nb_period + end_shift

        mom = df.shift(end_shift) / df.shift(start_shift) - 1
        return mom

