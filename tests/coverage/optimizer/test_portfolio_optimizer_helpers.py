"""Direct-call coverage for PortfolioOptimizer standalone helper methods and
module-level SR/ATR helpers that don't require network access."""
from __future__ import annotations

import os
import tempfile
from unittest import mock

import portfolio_optimizer as P
from tests.coverage.optimizer.conftest import holding, make_state


def test_sr_atr_helpers():
    # Build synthetic OHLC with clear swings
    closes = [10, 12, 14, 13, 11, 12, 15, 14, 13, 16, 15, 14, 13, 12, 11,
              12, 14, 16, 15, 14, 13, 12, 14, 16, 15] * 3
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    atr = P._estimate_atr(closes, highs, lows)
    assert atr >= 0


def test_fee_and_clamp_helpers():
    assert P._clamp(5, 0, 3) == 3
    assert P._clamp(-1, 0, 3) == 0
    assert P.classify_asset("BTC") == "safe"
    assert P.classify_asset("XRP") in ("growth", "speculative")
    assert P.current_fee_tier(5_000_000) is not None
    assert P.volume_to_next(100.0) >= 0


def test_normalize_capital_policy(opt):
    p = opt._normalize_capital_policy({"max_deployable_usd": 5000})
    assert "max_deployable_usd" in p
    opt._save_capital_policy()
    p2 = opt._refresh_capital_policy()
    assert "max_deployable_usd" in p2


def test_graph_helpers(opt):
    # No graph store -> safe defaults
    assert opt._graph_score_for_product("XRP-USD") == 0.5
    assert 0.0 <= opt._graph_multiplier_for_product("XRP-USD") <= 1.0
    assert opt._graph_signal_for_product("XRP-USD") is None


def test_cluster_helpers(opt):
    opt.state = make_state({"XRP": holding("XRP", 5000, "growth")},
                           total_value=100000.0, usdc=40000.0)
    cluster = opt._get_cluster_for_currency("XRP")
    assert cluster in (None, "growth", "speculative", "safe")
    pct = opt._cluster_exposure_pct(cluster or "growth")
    assert pct >= 0
    assert isinstance(opt._check_cluster_limit("XRP", 100.0), bool)


def test_record_trade_and_state(opt):
    opt.state = make_state({"XRP": holding("XRP", 5000, "growth")},
                           total_value=100000.0, usdc=40000.0)
    op = P.Opportunity(opp_type=P.OpportunityType.TLH, currency="XRP", side="SELL",
                       size_usd=1000, reason="r", priority=0.5, product_id="XRP-USD")
    opt._record_trade(op, 1.0)
    opt._save_state()
    opt._load_from_store()


def test_route_amount_for_source(opt):
    assert opt._route_amount_for_source("USDC", 1000.0) == 1000.0
    assert opt._route_amount_for_source("BTC", 1000.0) > 0


def test_round_quote_base(opt):
    assert P._fmt_base(1.23456789) == "1.23456789"
    assert isinstance(P._fmt_quote(1.2), str)
