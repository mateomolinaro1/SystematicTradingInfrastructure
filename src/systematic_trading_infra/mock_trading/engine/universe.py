from typing import List


def generate_universe(n_assets: int) -> List[str]:
    """
    Generate a stable universe of asset identifiers.

    Example:
    ASSET_0001, ASSET_0002, ..., ASSET_1000
    """
    if n_assets <= 0:
        raise ValueError("n_assets must be strictly positive")

    width = len(str(n_assets))

    return [
        f"ASSET_{i:0{width}d}"
        for i in range(1, n_assets + 1)
    ]
