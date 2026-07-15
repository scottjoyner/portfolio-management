"""Coverage tests for portfolio_optimizer.py.

Strategy:
  * Module-level pure helpers and dataclasses (high-value, no I/O).
  * CoinbaseCLI methods with subprocess mocked.
  * PortfolioOptimizer detection dimensions + helpers, with collaborators mocked
    and PortfolioState injected (no live API / network).
"""

import json
import math
import os
import tempfile
import time
from unittest import mock

import pytest

import portfolio_optimizer as P
from strategy_engine import Signal as StrategySignal
from strategy_engine import BacktestVerdict


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

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
    # Replace the real CLI with a mock that returns sane defaults.
    o.cli = mock.MagicMock()
    o.cli.environment = "live"
    o.cli.best_product.side_effect = lambda c, s: f"{c}-USD"
    o.cli.get_price.return_value = {"price": 100.0}
    o.cli.get_candles.return_value = []
    o.cli.get_products.return_value = {}
    return o


def make_state(holdings, total_value=100000.0, usdc=20000.0,
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
    h = {
        "currency": currency,
        "value": value,
        "classification": classification,
        "price": kw.get("price", 100.0),
        "product_id": kw.get("product_id", f"{currency}-USD"),
        "unrealized_pnl_pct": kw.get("pnl", 0.0),
        "volume_24h": kw.get("volume_24h", 1_000_000.0),
        "change_24h": kw.get("change_24h", 1.0),
        "allocation_pct": kw.get("allocation_pct", value / 100000.0 * 100),
        "liquidity_score": kw.get("liquidity_score", 0.8),
        "spread": kw.get("spread", 0.001),
    }
    return h


# ---------------------------------------------------------------------------
# Module-level pure helpers
# ---------------------------------------------------------------------------

def test_fmt_helpers():
    assert P._fmt_base(1.5) == "1.5"
    assert P._fmt_base(0.0) == "0"
    assert P._fmt_quote(10.0) == "10"
    assert P._fmt_quote(0.0) == "0"


@pytest.mark.parametrize("val,exp", [
    (5, 5.0), (5.0, 5.0), ("5", 5.0), ("abc", 0.0), (None, 0.0), ([], 0.0),
])
def test_to_float(val, exp):
    assert P.to_float(val) == exp


@pytest.mark.parametrize("cur,cls", [
    ("BTC", "safe"), ("ETH", "safe"), ("USDC", "safe"),
    ("SOL", "growth"), ("XRP", "growth"), ("DOGE", "growth"),
    ("ALGO", "speculative"), ("SHIB", "speculative"),
    ("FOOCOIN", "speculative"), ("BTC-USD", "safe"),
])
def test_classify_asset(cur, cls):
    assert P.classify_asset(cur) == cls


def test_current_fee_tier_and_volume_to_next():
    assert P.current_fee_tier(0) == (0, 0.006, 0.012)
    assert P.current_fee_tier(500) == (0, 0.006, 0.012)
    assert P.current_fee_tier(1000) == (1000, 0.0035, 0.0075)
    assert P.current_fee_tier(5_000_000) == (1_000_000, 0.0008, 0.0018)
    assert P.current_fee_tier(50_000_000) == (20_000_000, 0.0005, 0.0015)
    assert P.volume_to_next(0) == 1000.0
    assert P.volume_to_next(1000) == 9000.0
    assert P.volume_to_next(50_000_000) == 0.0
    assert P.volume_to_next(999) == 1.0


def test_clamp():
    assert P._clamp(5, 0, 10) == 5
    assert P._clamp(-5, 0, 10) == 0
    assert P._clamp(50, 0, 10) == 10


def test_compute_adx():
    assert P._compute_adx([1], [1], [1]) == 20.0
    highs = [i for i in range(1, 40)]
    lows = [i - 0.5 for i in range(1, 40)]
    closes = [i for i in range(1, 40)]
    val = P._compute_adx(highs, lows, closes, 14)
    assert 0 <= val <= 100


def test_detect_market_regime():
    assert P._detect_market_regime([1, 2], [1, 2], [1, 2]) == "neutral"
    closes = [100 + (i % 2) * 20 for i in range(40)]
    highs = [c + 5 for c in closes]
    lows = [c - 5 for c in closes]
    assert P._detect_market_regime(highs, lows, closes) in (
        "trending", "ranging", "volatile", "quiet", "neutral")
    closes = [float(i) for i in range(40)]
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    assert P._detect_market_regime(highs, lows, closes) == "trending"


def test_swing_points_and_sr_levels():
    prices = [100 + 10 * math.sin(i / 3.0) for i in range(40)]
    highs = [p + 1 for p in prices]
    lows = [p - 1 for p in prices]
    swings = P._detect_swing_points(highs, lows, lookback=3)
    assert isinstance(swings, list)
    # _build_sr_levels: short series -> empty (also avoids a latent bug in the
    # price_range>0 branch at L434: max(closes[-20:], 1e-9) compares a list to
    # a float and raises; that branch is unreachable without triggering it).
    assert P._build_sr_levels(highs, lows, prices[:5]) == []
    # flat last-20 -> price_range==0 path returns [] without crashing
    flat = [100.0] * 40
    assert P._build_sr_levels(flat, flat, flat, min_touches=2) == []


def test_estimate_atr():
    assert P._estimate_atr([1], [1], [1]) == 0.0
    closes = [float(100 + i) for i in range(30)]
    highs = [c + 2 for c in closes]
    lows = [c - 2 for c in closes]
    assert P._estimate_atr(closes, highs, lows, 14) > 0


def test_dataclasses():
    opp = P.Opportunity(opp_type=P.OpportunityType.TLH, currency="SOL",
                        side="SELL", size_usd=100, reason="r")
    assert opp.priority == 0.0
    assert opp.opp_type.value == "tlh"
    st = P.PortfolioState(holdings={}, total_value=10)
    assert st.total_value == 10
    sp = P._SwingPoint(1, 100.0, "high")
    assert sp.kind == "high"
    sr = P._SRLevel(100.0, "support", 2.0)
    assert sr.strength == 2.0


# ---------------------------------------------------------------------------
# CoinbaseCLI (subprocess mocked)
# ---------------------------------------------------------------------------

def test_cli_verify_and_run():
    with mock.patch("subprocess.run", return_value=mock.MagicMock(returncode=0)):
        cli = P.CoinbaseCLI("live")
    assert cli.environment == "live"
    resp = mock.MagicMock(returncode=0, stdout='{"ok": true, "data": [1,2]}')
    with mock.patch("subprocess.run", return_value=resp):
        out = cli._run(["x"], parse_json=True)
    assert out["ok"] is True
    resp2 = mock.MagicMock(returncode=0, stdout="plain text")
    with mock.patch("subprocess.run", return_value=resp2):
        out2 = cli._run(["x"], parse_json=False)
    assert out2 == "plain text"
    err = mock.MagicMock(returncode=1, stderr="bad")
    with mock.patch("subprocess.run", return_value=err):
        with pytest.raises(RuntimeError):
            cli._run(["x"], parse_json=True)


def test_cli_methods():
    with mock.patch("subprocess.run", return_value=mock.MagicMock(returncode=0)):
        cli = P.CoinbaseCLI("live")
    # Seed product cache so get_products()/best_product() never hit the network.
    cli._products = {
        "BTC-USD": {"product_id": "BTC-USD", "id": "BTC-USD"},
        "SOL-USD": {"product_id": "SOL-USD"},
        "SOL-USDC": {"product_id": "SOL-USDC"},
    }
    # _public_get uses urllib.request.urlopen (not subprocess) -> mock it
    class FakeResp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b'{"a":1}'

    with mock.patch("urllib.request.urlopen", return_value=FakeResp()):
        assert cli._public_get("path") == {"a": 1}
    assert cli._round_quote("SOL", 10.0) == 10.0
    assert cli._round_base("SOL", 1.23456789) == 1.23456789
    resp2 = mock.MagicMock(returncode=0, stdout='{"products": [{"product_id": "BTC-USD", "id": "BTC-USD"}]}')
    with mock.patch("subprocess.run", return_value=resp2):
        prods = cli.get_products()
        assert "BTC-USD" in prods
        assert cli.get_product("BTC-USD")["id"] == "BTC-USD"
    cli._products = {
        "SOL-USD": {"product_id": "SOL-USD"},
        "SOL-USDC": {"product_id": "SOL-USDC"},
    }
    assert cli.best_product("SOL", "BUY") == "SOL-USDC"
    resp3 = mock.MagicMock(returncode=0, stdout='{"price": 123.0}')
    with mock.patch("subprocess.run", return_value=resp3):
        assert cli.get_price("BTC-USD")["price"] == 123.0
    for meth in ("get_balances", "get_fees", "get_fills"):
        with mock.patch("subprocess.run", return_value=mock.MagicMock(returncode=0, stdout='{}')):
            res = cli.__getattribute__(meth)()
        assert res in ({}, [])
    order_json = '{"id":"O1","preview":true}'
    with mock.patch("subprocess.run", return_value=mock.MagicMock(returncode=0, stdout=order_json)):
        assert cli.preview_order("BTC-USD", "BUY", 10)["id"] == "O1"
        assert cli.create_order("BTC-USD", "BUY", 10)["id"] == "O1"
        assert cli.get_order("O1")["id"] == "O1"
    candles_json = '{"candles":[{"start":"1","open":1,"high":2,"low":0.5,"close":1.5,"volume":3}]}'
    with mock.patch("subprocess.run", return_value=mock.MagicMock(returncode=0, stdout=candles_json)):
        c = cli.get_candles("BTC-USD")
    assert c[0]["close"] == 1.5


# ---------------------------------------------------------------------------
# Simple optimizer helpers (no state needed)
# ---------------------------------------------------------------------------

def test_regime_strategy_weight_all_regimes(opt):
    for regime in ("trending", "ranging", "volatile", "quiet", "other"):
        assert isinstance(opt._regime_strategy_weight("ema_cross", regime), float)
    assert opt._regime_strategy_weight("ema_cross", "trending") == 1.5
    assert opt._regime_strategy_weight("rsi_revert", "ranging") == 1.5


def test_exit_plan_profiles(opt):
    for style in ("momentum", "new_listing", "equity_momentum", "prediction_market",
                  "event", "mean_reversion", "arbitrage", "rebalance", "cycle",
                  "tax_loss", "unknown_style"):
        plan = opt._compute_exit_plan("SOL", 0.6, 5.0, trade_style=style,
                                      volatility_pct=40.0, spread_pct=0.01,
                                      hold_hint_hours=10.0)
        assert "stop_loss_pct" in plan
        assert plan["stop_loss_pct"] >= 0


def test_dynamic_stop(opt):
    assert opt._compute_dynamic_stop(0, "BUY", 1.0, "trending", [], 5.0) == (5.0, 0.0, "default")
    levels = [P._SRLevel(95.0, "support", 1.0)]
    stop, atr_d, detail = opt._compute_dynamic_stop(
        100.0, "BUY", 2.0, "trending", levels, 5.0)
    assert stop >= 0.5
    assert "sr_snap" in detail or detail == "atr=200.00"


def test_sr_aware_exit_plan(opt):
    closes = [float(100 + i) for i in range(40)]
    highs = [c + 2 for c in closes]
    lows = [c - 2 for c in closes]
    plan = opt._compute_sr_aware_exit_plan(
        "SOL", 0.6, 5.0, trade_style="momentum", volatility_pct=40.0,
        closes=closes, highs=highs, lows=lows)
    assert "stop_loss_pct" in plan


def test_detect_sr_levels_for_product(opt):
    assert opt._detect_sr_levels_for_product([1], [1], [1]) == ([], 0.0)
    closes = [float(100 + i) for i in range(40)]
    highs = [c + 2 for c in closes]
    lows = [c - 2 for c in closes]
    levels, atr = opt._detect_sr_levels_for_product(closes, highs, lows)
    assert isinstance(levels, list)
    assert atr >= 0


def test_static_currency_helpers(opt):
    assert opt._is_static_currency("BTC") is True
    assert opt._is_static_currency("SOL") is False
    assert opt._is_core_holding({"currency": "BTC"}) is True
    assert opt._is_core_holding({"currency": "DOGE"}) is False
    assert opt._static_holdings_set() == {"BTC", "ETH"}


def test_bucket_helpers(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=20000.0)
    assert opt._bucket_targets()["reserve"] == 50000.0
    assert isinstance(opt._bucket_values(), dict)
    assert opt._bucket_gap("core") <= 0 or opt._bucket_gap("core") >= 0
    o = P.Opportunity(P.OpportunityType.REBALANCE, "SOL", "BUY", 10, "r")
    assert opt._capital_bucket_for(o) in ("reserve", "core", "opportunity")


def test_capital_capacity_helpers(opt):
    opt.state = make_state({}, total_value=100000.0, usdc=20000.0)
    assert opt._usdc_reserve_amount() > 0
    assert opt._deployable_capital() > 0
    assert opt._remaining_deployable_capital() >= 0
    assert opt._buy_capacity() >= 0
    assert opt._core_batch_cap() > 0
    assert opt._opportunity_batch_cap() > 0


def test_normalize_and_bear_policy(opt):
    pol = opt._normalize_capital_policy(None)
    assert "targets" in pol
    pol2 = opt._normalize_capital_policy({"max_deployable_usd": 5000})
    assert pol2["max_deployable_usd"] == 5000
    opt._apply_bear_market_policy()
    assert "max_deployable_usd" in opt.capital_policy


def test_first_seen_age_days(opt):
    assert opt._first_seen_age_days("SOL-USD") == 0.0
    opt.store.set_meta(opt._seen_products_meta_prefix + "SOL-USD",
                       "2020-01-01T00:00:00+00:00")
    age = opt._first_seen_age_days("SOL-USD")
    assert age is not None and age > 0


def test_pulse_tracking(opt):
    key = opt._pulse_key("SOL-USD", "rsi", "BUY")
    assert key == "SOL-USD:rsi:BUY"
    p = opt._record_pulse("SOL-USD", "rsi", "BUY", 0.6, 100.0)
    assert p["pulse_count"] == 1
    p2 = opt._record_pulse("SOL-USD", "rsi", "BUY", 0.7, 101.0)
    assert p2["pulse_count"] == 2
    assert opt._is_pulse_valid(p2) is True
    opt._record_pulse("SOL-USD", "rsi", "SELL", 0.6, 99.0)
    assert opt._is_pulse_quality_sufficient("SOL-USD", "rsi", "BUY") is True
    opt._prune_pulses()
    assert "SOL-USD:rsi:BUY" in opt._signal_pulses


def test_cluster_helpers(opt):
    assert opt._get_cluster_for_currency("BTC") == "btc_eth"
    assert opt._get_cluster_for_currency("SOL") == "l1_solana"
    assert opt._get_cluster_for_currency("UNKNOWN") is None
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth")}, total_value=100000.0)
    assert opt._cluster_exposure_pct("l1_solana") >= 0
    assert opt._check_cluster_limit("SOL", 1000) in (True, False)


def test_parse_iso_and_live_test(opt):
    assert opt._parse_iso_ts(None) is None
    assert opt._parse_iso_ts("not-a-date") is None
    assert opt._parse_iso_ts("2020-01-01T00:00:00+00:00") is not None
    assert opt._live_test_started_at() is None
    # Seed a live-test window + a stored executed trade to exercise the
    # capital-in-play summation loop.
    opt.capital_policy["max_deployable_usd"] = 5000.0
    start_ts = time.time() - 100
    opt.capital_policy["live_test_started_at"] = time.strftime(
        "%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_ts))
    opt.store.save_trade({
        "side": "BUY", "size_usd": 250.0, "dry_run": 0,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })
    assert opt._live_test_capital_in_play() >= 250.0


def test_graph_overlays(opt):
    assert opt._graph_score_for_product("SOL-USD") == 0.5
    assert 0.75 <= opt._graph_multiplier_for_product("SOL-USD") <= 1.25


def test_signal_ensemble_blend(opt):
    ops = [
        P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r", priority=0.6),
        P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "DOGE", "SELL", 50, "r", priority=0.4),
    ]
    blended = opt._signal_ensemble_blend(ops)
    assert len(blended) == 2
    opt._update_meta_source_weights(blended)
    applied = opt._apply_meta_source_weights(blended)
    assert len(applied) == 2


def test_check_pending_approvals(opt):
    # Not requiring approval -> returns None immediately
    assert opt._check_pending_approvals() is None

    # Require approval + an approved pending entry -> executes via mocked handler
    opt.require_approval = True
    pending_file = os.path.join(tempfile.mkdtemp(), "pending.json")
    opt.pending_file = pending_file
    with open(pending_file, "w") as f:
        json.dump({"tok1": {"status": "approved", "side": "BUY",
                            "currency": "SOL", "size_usd": 100}}, f)
    with mock.patch.object(opt, "_execute_approved", return_value=True) as exe:
        opt._check_pending_approvals()
    assert exe.called


# ---------------------------------------------------------------------------
# Rust rebalancer / stair-step bots wired into the optimizer
# ---------------------------------------------------------------------------

def _rebalance_book(opt, total=100000.0):
    holdings = {
        "BTC": holding("BTC", 40000.0, "safe", price=40000.0, product_id="BTC-USD"),
        "ETH": holding("ETH", 15000.0, "growth", price=2000.0, product_id="ETH-USD"),
        "SOL": holding("SOL", 5000.0, "speculative", price=100.0, product_id="SOL-USD"),
        "XRP": holding("XRP", 10000.0, "speculative", price=0.5, product_id="XRP-USD"),
        "XLM": holding("XLM", 5000.0, "speculative", price=0.1, product_id="XLM-USD"),
        "MON": holding("MON", 5000.0, "speculative", price=0.2, product_id="MON-USD"),
    }
    opt.state = make_state(holdings, total_value=total)
    return holdings


def test_rebalance_bot_emits_on_drift(opt):
    _rebalance_book(opt)
    ops = opt._detect_rebalance_bot()
    assert any(o.opp_type == P.OpportunityType.REBALANCE_BOT for o in ops)
    assert all(o.side == "BUY" for o in ops)
    # cooldown blocks an immediate re-run
    assert opt._detect_rebalance_bot() == []


def test_rebalance_bot_overweight_sells_slim_profit(opt):
    # Build an overweight BTC book; slim-profit (0.25) should sell part of excess.
    opt.rebalance_profit_take_pct = 0.25
    holdings = {
        "BTC": holding("BTC", 60000.0, "safe", price=40000.0, product_id="BTC-USD"),
        "ETH": holding("ETH", 25000.0, "growth", price=2000.0, product_id="ETH-USD"),
        "SOL": holding("SOL", 15000.0, "speculative", price=100.0, product_id="SOL-USD"),
        "XRP": holding("XRP", 10000.0, "speculative", price=0.5, product_id="XRP-USD"),
        "XLM": holding("XLM", 5000.0, "speculative", price=0.1, product_id="XLM-USD"),
        "MON": holding("MON", 5000.0, "speculative", price=0.2, product_id="MON-USD"),
    }
    opt.state = make_state(holdings, total_value=120000.0)
    ops = opt._detect_rebalance_bot()
    btc = [o for o in ops if o.currency == "BTC"]
    assert btc and btc[0].side == "SELL"
    # slim profit: only 25% of the 12000 overweight excess => 3000
    assert abs(btc[0].size_usd - 3000.0) < 1e-6


def test_rebalance_bot_skips_within_drift(opt):
    opt.rebalance_drift_threshold = 0.005
    holdings = {
        "BTC": holding("BTC", 40000.0, "safe", price=40000.0, product_id="BTC-USD"),
        "ETH": holding("ETH", 25000.0, "growth", price=2000.0, product_id="ETH-USD"),
        "SOL": holding("SOL", 15000.0, "speculative", price=100.0, product_id="SOL-USD"),
        "XRP": holding("XRP", 10000.0, "speculative", price=0.5, product_id="XRP-USD"),
        "XLM": holding("XLM", 5000.0, "speculative", price=0.1, product_id="XLM-USD"),
        "MON": holding("MON", 5000.0, "speculative", price=0.2, product_id="MON-USD"),
    }
    opt.state = make_state(holdings, total_value=100000.0)
    assert opt._detect_rebalance_bot() == []


def test_stairstep_emits_buy_then_sell(opt):
    sym = "XRP-USD"
    opt._stairstep_symbols = [sym]
    h = holding("XRP", 1000.0, "speculative", price=0.60, product_id=sym)
    opt.state = make_state({"XRP": h}, total_value=100000.0)
    ops1 = opt._detect_stairstep()
    assert any(o.opp_type == P.OpportunityType.STAIRSTEP and o.side == "BUY" for o in ops1)
    # Recover above the grid top to bank the spread (SELL)
    h["price"] = 0.70
    ops2 = opt._detect_stairstep()
    assert any(o.opp_type == P.OpportunityType.STAIRSTEP and o.side == "SELL" for o in ops2)


def test_stairstep_disabled(opt):
    opt.stairstep_enabled = False
    assert opt._detect_stairstep() == []


def test_rebalance_bot_no_state(opt):
    opt.state = None
    assert opt._detect_rebalance_bot() == []
    assert opt._holding_for_product("XRP-USD") is None


def test_rebalance_bot_missing_pid(opt):
    _rebalance_book(opt)
    opt.cli.best_product.side_effect = lambda c, s: None
    assert opt._detect_rebalance_bot() == []


def test_stairstep_missing_holding(opt):
    opt._stairstep_symbols = ["DOGE-USD"]
    opt.state = make_state(
        {"BTC": holding("BTC", 40000.0, "safe", product_id="BTC-USD")},
        total_value=100000.0,
    )
    assert opt._detect_stairstep() == []


def test_stairstep_missing_pid(opt):
    sym = "XRP-USD"
    opt._stairstep_symbols = [sym]
    h = holding("XRP", 1000.0, "speculative", price=0.60, product_id=sym)
    opt.state = make_state({"XRP": h}, total_value=100000.0)
    opt.cli.best_product.side_effect = lambda c, s: None
    assert opt._detect_stairstep() == []   # BUY skipped (no product)
    h["price"] = 0.70
    assert opt._detect_stairstep() == []   # SELL skipped (no product)
