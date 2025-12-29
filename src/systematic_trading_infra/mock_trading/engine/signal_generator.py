import numpy as np
import pandas as pd
from typing import List, Optional


class SignalGenerator:
    """
    Random long-or-flat signal generator.
    """

    def __init__(
        self,
        assets: List[str],
        long_fraction: float = 0.10,
        seed: Optional[int] = None,
    ):
        if not 0.0 < long_fraction <= 1.0:
            raise ValueError("long_fraction must be in (0, 1].")

        self.assets = assets
        self.long_fraction = long_fraction
        self.rng = np.random.default_rng(seed)

    def generate(self) -> pd.Series:
        """
        Generate a random signal vector (1 = long, 0 = flat).
        """
        n_assets = len(self.assets)
        n_long = int(n_assets * self.long_fraction)

        selected = self.rng.choice(
            self.assets,
            size=n_long,
            replace=False,
        )

        signals = pd.Series(
            0,
            index=self.assets,
            dtype=int,
            name="signal",
        )

        signals.loc[selected] = 1

        return signals
