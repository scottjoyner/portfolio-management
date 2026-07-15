"""Shared fixtures/helpers for optimizer coverage tests."""

import os
import tempfile
from unittest import mock

import pytest

import portfolio_optimizer as P


@pytest.fixture
def opt():
    """Build a PortfolioOptimizer with all network I/O mocked."""
    with mock.patch("subprocess.run", return_value=mock.MagicMock(returncode=0)), \
         mock.patch("fcntl.flock", return_value=0):
        db = os.path.join(tempfile.mkdtemp(), "opt.db")
        o = P.PortfolioOptimizer(dry_run=True, db_path=db, interval=5)
    # Stop the background smart-feed thread and discard the manager.
    mgr = getattr(o, "_feed_mgr", None)
    if mgr is not None and hasattr(mgr, "stop"):
        try:
            mgr.stop()
        except Exception:
            pass
    o._feed_mgr = None
    # Replace the real CLI with a mock returning sane defaults.
    o.cli = mock.MagicMock()
    o.cli.environment = "live"
    o.cli.best_product.side_effect = lambda c, s: f"{c}-USD"
    o.cli.get_price.return_value = {"price": 100.0}
    o.cli.get_candles.return_value = []
    o.cli.get_products.return_value = {}
    return o


def make_state(holdings, total_value=100000.0, usdc=80000.0,
               fee_volume_30d=0.0, volume_to_next_tier=0.0):
    return P.PortfolioState(
        holdings=holdings,
        total_value=total_value,
        usdc_balance=usdc,
        fee_volume_30d=fee_volume_30d,
        fee_tier=(0, 0.006, 0.012),
        volume_to_next_tier=volume_to_next_tier,
        timestamp="2024-01-01T00:00:00Z",
    )


def holding(currency, value, classification, **kw):
    price = kw.get("price", 100.0)
    return {
        "currency": currency,
        "value": value,
        "classification": classification,
        "price": price,
        "total": kw.get("total", (value / price) if price else 0.0),
        "product_id": kw.get("product_id", f"{currency}-USD"),
        "unrealized_pnl_pct": kw.get("pnl", 0.0),
        "volume_24h": kw.get("volume_24h", 1_000_000.0),
        "change_24h": kw.get("change_24h", 1.0),
        "allocation_pct": kw.get("allocation_pct", value / 100000.0 * 100),
        "liquidity_score": kw.get("liquidity_score", 0.8),
        "spread": kw.get("spread", 0.001),
    }
