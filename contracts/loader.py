"""
Loader module for my contract abis.
"""

import json
from pathlib import Path
from functools import lru_cache

ABI_DIR = Path(__file__).parent / "abis"


@lru_cache(maxsize=32)
def load_abi(contract_name: str) -> list:
    """Load ABI from JSON file with caching."""
    abi_path = ABI_DIR / f"{contract_name}.json"

    if not abi_path.exists():
        raise FileNotFoundError(f"ABI not found: {contract_name}")

    with open(abi_path, "r", encoding="UTF-8") as f:
        return json.load(f)
