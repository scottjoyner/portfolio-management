"""Deterministic helpers for generated event-market tests."""

import os
from pathlib import Path
import sys
import types

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

# The historical graph-alpha checkout uses a hyphenated directory name. Expose
# a normal import package for tests that import graph_alpha_bot.app.*.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_LEGACY_GRAPH_ROOT = _REPO_ROOT / "graph-alpha-bot"
graph_package = sys.modules.setdefault(
    "graph_alpha_bot",
    types.ModuleType("graph_alpha_bot"),
)
graph_package.__path__ = [str(_LEGACY_GRAPH_ROOT)]

_PM_ENV_VARS = [
    "KALSHI_API_BASE_URL", "KALSHI_API_ENV", "KALSHI_API_KEY_ID", "KALSHI_EMAIL",
    "KALSHI_ENV", "KALSHI_FEE_RATE", "KALSHI_PASSWORD", "KALSHI_PRIVATE_KEY_PATH",
    "POLYMARKET_CHAIN_ID", "POLYMARKET_CLOB_HOST", "POLYMARKET_FUNDER",
    "POLYMARKET_PRIVATE_KEY", "POLYMARKET_RELAYER_CREDENTIALS_PATH",
    "POLYMARKET_RELAYER_URL", "POLYMARKET_SIGNATURE_TYPE",
    "RELAYER_API_KEY", "RELAYER_API_KEY_ADDRESS",
    "ARBITRAGE_LIVE_ENABLED", "ARBITRAGE_MAX_NOTIONAL_USD", "ARBITRAGE_MAX_SLIPPAGE",
    "ARBITRAGE_MIN_CONFIDENCE", "ARBITRAGE_MIN_EDGE_PCT", "ARBITRAGE_REQUIRE_NET_PROFIT",
]


@pytest.fixture(autouse=True)
def _clear_pm_env():
    saved = {key: os.environ.pop(key) for key in _PM_ENV_VARS if key in os.environ}
    try:
        yield
    finally:
        os.environ.update(saved)
