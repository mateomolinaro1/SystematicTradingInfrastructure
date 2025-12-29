import pandas as pd
from typing import List


class Portfolio:
    """
    Minimal portfolio accounting:
    - Long / flat
    - Market execution
    - No transaction costs
    """

    def __init__(
        self,
        assets: List[str],
        initial_cash: float = 1_000_000.0,
    ):
        self.assets = assets
        self.cash = float(initial_cash)

        self.positions = pd.Series(
            0,
            index=assets,
            dtype=int,
            name="position",
        )

        self.history = []

    def apply_orders(
        self,
        orders: pd.DataFrame,
        prices: pd.Series,
        timestamp: pd.Timestamp,
    ):
        """
        Execute orders at given prices.
        """
        if not prices.index.equals(self.positions.index):
            raise ValueError("Prices index must match assets.")

        for _, order in orders.iterrows():
            asset = order["asset"]
            side = order["side"]
            qty = order["quantity"]
            price = prices.loc[asset]

            if side == "BUY":
                cost = qty * price
                if cost > self.cash:
                    raise RuntimeError("Insufficient cash.")
                self.cash -= cost
                self.positions.loc[asset] += qty

            elif side == "SELL":
                self.cash += qty * price
                self.positions.loc[asset] -= qty

            else:
                raise ValueError(f"Unknown side: {side}")

        self._record_state(prices, timestamp)

    def _record_state(
        self,
        prices: pd.Series,
        timestamp: pd.Timestamp,
    ):
        """
        Store portfolio snapshot.
        """
        nav = self.cash + (self.positions * prices).sum()

        self.history.append(
            {
                "timestamp": timestamp,
                "cash": self.cash,
                "nav": nav,
            }
        )

    def get_history(self) -> pd.DataFrame:
        return pd.DataFrame(self.history).set_index("timestamp")
