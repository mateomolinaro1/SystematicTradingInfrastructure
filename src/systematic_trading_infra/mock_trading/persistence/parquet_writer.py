from pathlib import Path
import pandas as pd
import shutil
from datetime import datetime


class ParquetWriter:
    """
    Handle persistence of mock trading outputs.
    One parquet per object, full history, auto-cleanup.
    """

    def __init__(self, base_dir: str = "runs"):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.run_dir = Path(base_dir) / f"run_{timestamp}"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        print("CWD:", Path.cwd())
        print("Run dir:", self.run_dir.resolve())

        # In-memory storage
        self._data = {
            "prices": pd.DataFrame(),
            "signals": pd.DataFrame(),
            "orders": pd.DataFrame(),
            "portfolio_value": pd.DataFrame(),
            "weights": pd.DataFrame(),
        }


    # Generic writer
    def _write(self, key: str, df: pd.DataFrame):
        if df.empty:
            return

        self._data[key] = pd.concat(
            [self._data[key], df],
            axis=0
        )

        path = self.run_dir / f"{key}.parquet"
        self._data[key].to_parquet(path)

    # Public "API"
    def write_prices(self, df: pd.DataFrame):
        self._write("prices", df)

    def write_signals(self, df: pd.DataFrame):
        self._write("signals", df)

    def write_orders(self, df: pd.DataFrame):
        self._write("orders", df)

    def write_portfolio_value(self, df: pd.DataFrame):
        self._write("portfolio_value", df)

    def write_weights(self, df: pd.DataFrame):
        self._write("weights", df)

    # Cleanup
    def cleanup(self):
        if self.run_dir.exists():
            shutil.rmtree(self.run_dir)
