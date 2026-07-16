"""Make the event_markets tests directory importable for ``from em_helpers import``."""

import os
import sys

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

# Credential / config env vars read by event_markets clients. Earlier tests in
# the suite set some of these (e.g. via monkeypatch) and leakage between tests
# caused "no-creds" assertions to fail intermittently. Clear them before every
# test so each test starts from a clean, deterministic environment.
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
    saved = {k: os.environ.pop(k) for k in _PM_ENV_VARS if k in os.environ}
    try:
        yield
    finally:
        os.environ.update(saved)
