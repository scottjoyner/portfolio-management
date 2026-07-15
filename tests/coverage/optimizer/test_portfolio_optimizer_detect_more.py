"""Coverage tests for the detection-dimension methods of PortfolioOptimizer.

These exercise the per-dimension opportunity detectors (TLH, fee-tier volume,
rebalance, rebalance-bot, stair-step, volume-cycle, holding lookup) directly so
their branches are exercised without depending on the full tick pipeline.
"""
from __future__ import annotations

import time
from unittest import mock

import pytest

import portfolio_optimizer as P
from tests.coverage.optimizer.conftest import holding, make_state


@pytest.fixture
def o(opt):
    o = opt
    o.last_execution = {}
    o.position_ages = {}
    return o


# --------------------------------------------------------------------------- TLH
def test_tlh_no_state(o):
    o.state = None
    assert o._detect_tlh() == []


def test_tlh_cooldown(o):
    o.last_execution["tlh"] = time.time()
    o.state = make_state({"BTC": holding("BTC", 1000, "safe", pnl=-20.0)})
    assert o._detect_tlh() == []


def test_tlh_skip_static_and_stable(o):
    o.state = make_state({
        "BTC": holding("BTC", 1000, "safe", pnl=-1.0),
        "USDC": holding("USDC", 1000, "safe"),
    })
    assert o._detect_tlh() == []


def test_tlh_emits_for_loss(o):
    o.state = make_state({"XRP": holding("XRP", 1000, "speculative", pnl=-12.0, price=0.5)})
    ops = o._detect_tlh()
    assert len(ops) == 1
    assert ops[0].opp_type == P.OpportunityType.TLH
    assert ops[0].side == "SELL"


def test_tlh_no_product_skipped(o):
    o.state = make_state({"XRP": holding("XRP", 1000, "speculative", pnl=-12.0, price=0.5)})
    o.cli.best_product.side_effect = lambda c, s: None
    assert o._detect_tlh() == []


# --------------------------------------------------------------- fee tier volume
def test_fee_tier_no_state(o):
    o.state = None
    assert o._detect_fee_tier_volume() == []


def test_fee_tier_cooldown(o):
    o.last_execution["fee_tier"] = time.time()
    o.state = make_state({"BTC": holding("BTC", 1000, "safe")}, volume_to_next_tier=5000.0)
    assert o._detect_fee_tier_volume() == []


def test_fee_tier_no_needed(o):
    o.state = make_state({"BTC": holding("BTC", 1000, "safe")}, volume_to_next_tier=0.0)
    assert o._detect_fee_tier_volume() == []


def test_fee_tier_no_candidates(o):
    o.state = make_state({"USDC": holding("USDC", 1000, "safe")}, volume_to_next_tier=5000.0)
    assert o._detect_fee_tier_volume() == []


def test_fee_tier_high_vol_skip(o):
    o.state = make_state(
        {"BTC": holding("BTC", 1000, "safe", change_24h=25.0)},
        volume_to_next_tier=5000.0,
    )
    assert o._detect_fee_tier_volume() == []


def test_fee_tier_emits(o):
    o.state = make_state(
        {"XRP": holding("XRP", 1000, "speculative", change_24h=1.0)},
        volume_to_next_tier=5000.0,
    )
    ops = o._detect_fee_tier_volume()
    assert len(ops) == 1
    assert ops[0].opp_type == P.OpportunityType.FEE_TIER_VOLUME
    assert ops[0].side == "BUY"


def test_fee_tier_no_product(o):
    o.state = make_state(
        {"XRP": holding("XRP", 1000, "speculative", change_24h=1.0)},
        volume_to_next_tier=5000.0,
    )
    o.cli.best_product.side_effect = lambda c, s: None
    assert o._detect_fee_tier_volume() == []


# ------------------------------------------------------------------ rebalance
def test_rebalance_no_state(o):
    o.state = None
    assert o._detect_rebalance() == []


def test_rebalance_cooldown(o):
    o.last_execution["rebalance"] = time.time()
    o.state = make_state({"BTC": holding("BTC", 1000, "safe")})
    assert o._detect_rebalance() == []


def test_rebalance_overweight_sell(o):
    # safe target 0.75 but XRP (safe, non-static) is far overweight -> sell
    o.state = make_state({
        "XRP": holding("XRP", 90000.0, "safe", price=0.5, product_id="XRP-USD"),
        "ETH": holding("ETH", 1000.0, "growth", price=2000.0, product_id="ETH-USD"),
        "SOL": holding("SOL", 1000.0, "speculative", price=100.0, product_id="SOL-USD"),
    }, total_value=100000.0)
    ops = o._detect_rebalance()
    assert any(x.side == "SELL" and x.opp_type == P.OpportunityType.REBALANCE for x in ops)


def test_rebalance_underweight_buy_existing(o):
    # growth target 0.20 but XRP (growth) far underweight -> buy
    o.state = make_state({
        "BTC": holding("BTC", 70000.0, "safe", price=40000.0, product_id="BTC-USD"),
        "XRP": holding("XRP", 5000.0, "growth", price=0.5, product_id="XRP-USD"),
        "SOL": holding("SOL", 1000.0, "speculative", price=100.0, product_id="SOL-USD"),
    }, total_value=100000.0)
    ops = o._detect_rebalance()
    assert any(x.side == "BUY" and x.opp_type == P.OpportunityType.REBALANCE for x in ops)


def test_rebalance_skip_small_diff(o):
    o.state = make_state({
        "BTC": holding("BTC", 75000.0, "safe", price=40000.0, product_id="BTC-USD"),
        "ETH": holding("ETH", 20000.0, "growth", price=2000.0, product_id="ETH-USD"),
        "SOL": holding("SOL", 5000.0, "speculative", price=100.0, product_id="SOL-USD"),
    }, total_value=100000.0)
    assert o._detect_rebalance() == []


# ----------------------------------------------------------- holding lookup
def test_holding_for_product_by_pid(o):
    h = holding("BTC", 1000, "safe", product_id="BTC-USD")
    o.state = make_state({"BTC": h})
    assert o._holding_for_product("BTC-USD") is h


def test_holding_for_product_by_currency(o):
    h = holding("BTC", 1000, "safe", product_id="BTC-USD")
    o.state = make_state({"BTC": h})
    assert o._holding_for_product("BTC-USD") is h


def test_holding_for_product_missing(o):
    o.state = make_state({"BTC": holding("BTC", 1000, "safe", product_id="BTC-USD")})
    assert o._holding_for_product("ETH-USD") is None


def test_holding_for_product_no_state(o):
    o.state = None
    assert o._holding_for_product("BTC-USD") is None


# ------------------------------------------------------------ volume cycles
def test_volume_cycle_cooldown(o):
    o.last_execution["cycle"] = time.time()
    o.state = make_state({"BTC": holding("BTC", 1000, "safe")})
    assert o._detect_volume_cycles() == []


def test_volume_cycle_fresh_position(o):
    o.state = make_state({"XRP": holding("XRP", 1000, "speculative", product_id="XRP-USD")})
    o.position_ages = {}
    assert o._detect_volume_cycles() == []
    assert "XRP" in o.position_ages


def test_volume_cycle_stale_close(o):
    o.state = make_state({"XRP": holding("XRP", 1000, "speculative", product_id="XRP-USD")})
    o.position_ages["XRP"] = time.time() - (P.CYCLE_MAX_HOLD_HOURS + 1) * 3600
    o.cli.best_product.side_effect = lambda c, s: f"{c}-USD"
    ops = o._detect_volume_cycles()
    assert len(ops) == 1
    assert ops[0].opp_type == P.OpportunityType.VOLUME_CYCLE


def test_volume_cycle_no_product(o):
    o.state = make_state({"XRP": holding("XRP", 1000, "speculative", product_id="XRP-USD")})
    o.position_ages["XRP"] = time.time() - (P.CYCLE_MAX_HOLD_HOURS + 1) * 3600
    o.cli.best_product.side_effect = lambda c, s: None
    assert o._detect_volume_cycles() == []


# ------------------------------------------------------------ rebalance bot
def test_rebalance_bot_cooldown(o):
    o.last_execution["rebalance_bot"] = time.time()
    o.state = make_state({"BTC": holding("BTC", 1000, "safe", product_id="BTC-USD")})
    assert o._detect_rebalance_bot() == []


def test_rebalance_bot_no_state(o):
    o.state = None
    assert o._detect_rebalance_bot() == []


def test_rebalance_bot_zero_total(o):
    o.state = make_state({}, total_value=0.0)
    assert o._detect_rebalance_bot() == []


def test_rebalance_bot_no_drift(o):
    # balanced book but threshold raised so max_drift is below it -> no orders
    o.rebalance_drift_threshold = 1.0
    o.state = make_state({
        "BTC": holding("BTC", 75000.0, "safe", price=40000.0, product_id="BTC-USD"),
        "ETH": holding("ETH", 20000.0, "growth", price=2000.0, product_id="ETH-USD"),
        "SOL": holding("SOL", 5000.0, "speculative", price=100.0, product_id="SOL-USD"),
    }, total_value=100000.0)
    assert o._detect_rebalance_bot() == []


def test_rebalance_bot_emits(o):
    # heavily skewed book -> drift exceeds threshold -> orders emitted
    o.state = make_state({
        "BTC": holding("BTC", 95000.0, "safe", price=40000.0, product_id="BTC-USD"),
        "ETH": holding("ETH", 4000.0, "growth", price=2000.0, product_id="ETH-USD"),
        "SOL": holding("SOL", 1000.0, "speculative", price=100.0, product_id="SOL-USD"),
    }, total_value=100000.0)
    ops = o._detect_rebalance_bot()
    assert ops
    assert all(x.opp_type == P.OpportunityType.REBALANCE_BOT for x in ops)


def test_rebalance_bot_import_unavailable(o, monkeypatch):
    o.state = make_state({
        "BTC": holding("BTC", 95000.0, "safe", price=40000.0, product_id="BTC-USD"),
        "ETH": holding("ETH", 4000.0, "growth", price=2000.0, product_id="ETH-USD"),
        "SOL": holding("SOL", 1000.0, "speculative", price=100.0, product_id="SOL-USD"),
    }, total_value=100000.0)
    o._rebalance_bot = None
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *a, **k):
        if name == "coinbase.src.rebalance_engine":
            raise ImportError("blocked")
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert o._detect_rebalance_bot() == []


# ------------------------------------------------------------ stair-step
def test_stairstep_disabled(o):
    o.stairstep_enabled = False
    o.state = make_state({"BTC": holding("BTC", 1000, "safe", price=40000.0, product_id="BTC-USD")})
    assert o._detect_stairstep() == []


def test_stairstep_no_state(o):
    o.state = None
    assert o._detect_stairstep() == []


def test_stairstep_emits(o):
    o._stairstep_symbols = ["BTC-USD"]
    o.state = make_state({"BTC": holding("BTC", 1000, "safe", price=40000.0, product_id="BTC-USD")})
    ops = o._detect_stairstep()
    assert isinstance(ops, list)


def test_stairstep_no_holding(o):
    o._stairstep_symbols = ["DOGE-USD"]
    o.state = make_state({"BTC": holding("BTC", 1000, "safe", price=40000.0, product_id="BTC-USD")})
    assert o._detect_stairstep() == []


def test_stairstep_import_unavailable(o, monkeypatch):
    o._stairstep_symbols = ["BTC-USD"]
    o.state = make_state({"BTC": holding("BTC", 1000, "safe", price=40000.0, product_id="BTC-USD")})
    o._stairstep_engine = None
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *a, **k):
        if name == "coinbase.src.rebalance_engine":
            raise ImportError("blocked")
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert o._detect_stairstep() == []
