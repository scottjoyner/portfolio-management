"""Coverage tests for PortfolioOptimizer __init__ optional wiring (Neo4j /
graph store / email notifier) and the single-tick orchestration path."""
from __future__ import annotations

import os
import tempfile
from unittest import mock

import pytest

import portfolio_optimizer as P
from tests.coverage.optimizer.conftest import holding, make_state


def mkopp(opp_type, side, currency, **kw):
    return P.Opportunity(
        opp_type=opp_type, currency=currency, side=side,
        size_usd=kw.get("size_usd", 1000.0), reason=kw.get("reason", "t"),
        priority=kw.get("priority", 0.5),
        product_id=kw.get("product_id", f"{currency}-USD"),
        meta=kw.get("meta", {}),
    )


def _build(**kw):
    db = os.path.join(tempfile.mkdtemp(), "opt.db")
    with mock.patch("subprocess.run", return_value=mock.MagicMock(returncode=0)), \
         mock.patch("fcntl.flock", return_value=0):
        o = P.PortfolioOptimizer(dry_run=True, db_path=db, interval=5, **kw)
    mgr = getattr(o, "_feed_mgr", None)
    if mgr is not None and hasattr(mgr, "stop"):
        try:
            mgr.stop()
        except Exception:
            pass
    o._feed_mgr = None
    return o


def test_init_with_neo4j_and_smtp():
    # Force the optional-store / notifier setup paths (both success + except).
    with mock.patch.object(P, "Neo4jStore", side_effect=RuntimeError("no neo4j")), \
         mock.patch.object(P, "CryptoGraphStore", side_effect=RuntimeError("no graph")), \
         mock.patch.object(P, "TradeNotifier", side_effect=RuntimeError("no smtp")):
        o = _build(neo4j_uri="bolt://127.0.0.1:7687", neo4j_user="u", neo4j_password="p",
                   require_approval=True, smtp_user="u", smtp_password="p",
                   to_addr="a@b.c", from_addr="f@b.c")
    o.stop()


def test_init_success_wiring():
    with mock.patch.object(P, "Neo4jStore", return_value=mock.MagicMock()), \
         mock.patch.object(P, "CryptoGraphStore", return_value=mock.MagicMock()), \
         mock.patch.object(P, "TradeNotifier", return_value=mock.MagicMock()):
        o = _build(neo4j_uri="bolt://x", neo4j_user="u", neo4j_password="p",
                   require_approval=True, smtp_user="u", smtp_password="p",
                   to_addr="a@b.c", from_addr="f@b.c")
    o.stop()


def test_init_with_kalshi():
    o = _build(kalshi_email="k@e.c", kalshi_password="pw")
    assert o._pm_client is not None
    o.stop()


def test_tick_orchestrates():
    o = _build()
    o._feed_mgr = None
    o.state = make_state({"XRP": holding("XRP", 5000, "growth")}, total_value=100000.0, usdc=40000.0)
    ops = [mkopp(P.OpportunityType.TLH, "SELL", "XRP")]
    with mock.patch.object(o, "_refresh_capital_policy"), \
         mock.patch.object(o, "_check_pending_approvals"), \
         mock.patch.object(o, "_fetch_state"), \
         mock.patch.object(o, "_apply_bear_market_policy"), \
         mock.patch.object(o, "_detect_opportunities", return_value=ops), \
         mock.patch.object(o, "_apply_cross_asset_risk_filter", side_effect=lambda x: x), \
         mock.patch.object(o, "_write_trade_plans"), \
         mock.patch.object(o, "_write_signal_cache"), \
         mock.patch.object(o, "_write_enhanced_state"), \
         mock.patch.object(o, "_process_opportunity") as proc, \
         mock.patch.object(o, "_save_state"):
        o._tick()
        proc.assert_called()
