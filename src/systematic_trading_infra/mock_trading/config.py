from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = PROJECT_ROOT / "mock_trading"/ "runs"


@dataclass(frozen=True)
class MockTradingConfig:
    """
    Global configuration for the mock trading service.
    """

    # Universe
    n_assets: int = 1000
    selection_frac: float = 0.10  # 10% selected each tick

    # Timing
    tick_seconds: int = 60  # data generation frequency (60s)

    # Price process
    init_price: float = 100.0
    daily_vol: float = 0.20  # 20% annualized vol

    # Portfolio
    init_portfolio_value: float = 100.0

    # Randomness
    random_seed: int = 42
