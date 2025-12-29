import pandas as pd
from typing import List, Dict


class OrderGenerator:
    """
    Generate orders by comparing desired signals with current positions.
    """

    def __init__(self, assets: List[str]):
        self.assets = assets

        # Current position per asset (0 or 1)
        self.positions = pd.Series(
            0,
            index=assets,
            dtype=int,
            name="position",
        )

    def generate_orders(
        self,
        signals: pd.Series,
        timestamp: pd.Timestamp,
    ) -> pd.DataFrame:
        """
        Generate BUY / SELL orders based on signal changes.
        """
        if not signals.index.equals(self.positions.index):
            raise ValueError("Signals index must match assets.")

        orders = []

        for asset in self.assets:
            prev_pos = self.positions.loc[asset]
            new_pos = signals.loc[asset]

            if prev_pos == 0 and new_pos == 1:
                orders.append(
                    {
                        "timestamp": timestamp,
                        "asset": asset,
                        "side": "BUY",
                        "quantity": 1,
                    }
                )

            elif prev_pos == 1 and new_pos == 0:
                orders.append(
                    {
                        "timestamp": timestamp,
                        "asset": asset,
                        "side": "SELL",
                        "quantity": 1,
                    }
                )

        # Update positions AFTER generating orders
        self.positions = signals.copy()
        self.positions.name = "position"

        return pd.DataFrame(orders)
