import time
import signal
import sys
import pandas as pd

from systematic_trading_infra.mock_trading.engine.universe import generate_universe
from systematic_trading_infra.mock_trading.engine.price_generator import PriceGenerator
from systematic_trading_infra.mock_trading.engine.signal_generator import SignalGenerator
from systematic_trading_infra.mock_trading.engine.order_generator import OrderGenerator
from systematic_trading_infra.mock_trading.engine.portfolio_engine import Portfolio
from systematic_trading_infra.mock_trading.persistence.parquet_writer import ParquetWriter
from systematic_trading_infra.mock_trading.config import DATA_PATH

# ------------------------------------------------------------------
# shutdown flag
STOP_REQUESTED = False


def _handle_shutdown(signum, frame):
    global STOP_REQUESTED
    print(f"Received signal {signum}, shutting down runner...")
    STOP_REQUESTED = True


# Register signals (Docker + local)
signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


def run(
    n_assets: int = 1000,
    selection_pct: float = 0.10,
    interval_sec: int = 30,
):
    assets = generate_universe(n_assets)

    price_gen = PriceGenerator(assets)
    signal_gen = SignalGenerator(assets=assets, long_fraction=selection_pct)
    order_gen = OrderGenerator(assets)
    portfolio = Portfolio(assets)

    writer = ParquetWriter(base_dir=DATA_PATH)

    print("Mock trading started")
    print(f"Writing parquets to: {writer.run_dir}")

    try:
        step = 0

        while not STOP_REQUESTED:
            timestamp = pd.Timestamp.utcnow()

            prices = price_gen.step()
            signals = signal_gen.generate()
            orders = order_gen.generate_orders(signals, timestamp)

            portfolio.apply_orders(orders, prices, timestamp)

            # Prices
            writer.write_prices(
                prices.rename("price")
                .to_frame()
                .assign(timestamp=timestamp)
                .reset_index()
                .rename(columns={"index": "asset"})
                .set_index("timestamp")
            )

            # Signals
            writer.write_signals(
                signals.rename("signal")
                .to_frame()
                .assign(timestamp=timestamp)
                .reset_index()
                .rename(columns={"index": "asset"})
                .set_index("timestamp")
            )

            if not orders.empty:
                writer.write_orders(orders)

            pv_df = portfolio.get_history().iloc[[-1]]
            writer.write_portfolio_value(pv_df)

            writer.write_weights(
                portfolio.positions.rename("weight")
                .to_frame()
                .assign(timestamp=timestamp)
                .reset_index()
                .rename(columns={"index": "asset"})
                .set_index("timestamp")
            )

            print(f"[{timestamp}] Step={step} NAV={pv_df.iloc[0]['nav']:.2f}")
            step += 1

            time.sleep(interval_sec)

    finally:
        print("Cleaning up parquet files...")
        writer.cleanup()
        print("Cleanup completed")


if __name__ == "__main__":
    run(
        n_assets=50,
        selection_pct=0.2,
        interval_sec=10,
    )
