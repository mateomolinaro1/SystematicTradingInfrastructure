import numpy as np
import pandas as pd
from typing import List, Optional
from datetime import datetime


class PriceGenerator:
    """
    Simple multi-asset price generator using a geometric random walk.
    """

    def __init__(
        self,
        assets: List[str],
        init_price: float = 100.0,
        sigma: float = 0.001,
        seed: Optional[int] = None,
    ):
        self.assets = assets
        self.sigma = sigma
        self.rng = np.random.default_rng(seed)

        self.prices = pd.Series(
            init_price,
            index=assets,
            name="price",
            dtype=float,
        )

        self.timestamp = datetime.now()

    def step(self) -> pd.Series:
        """
        Advance prices by one time step.
        """
        returns = self.rng.normal(
            loc=0.0,
            scale=self.sigma,
            size=len(self.assets),
        )

        self.prices *= (1.0 + returns)
        self.timestamp = datetime.now()

        return self.prices.copy()

    def snapshot(self) -> pd.DataFrame:
        """
        Return prices as a DataFrame with timestamp.
        """
        return pd.DataFrame(
            {
                "price": self.prices,
                "timestamp": self.timestamp,
            }
        )
