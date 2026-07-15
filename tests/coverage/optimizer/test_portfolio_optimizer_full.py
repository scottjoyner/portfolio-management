"""Coverage tests for portfolio_optimizer.py (PortfolioOptimizer daemon).

Targets: module-level helpers, CoinbaseCLI, detection dimensions (TLH,
fee-tier, rebalance, strategy signals, accumulator, event markets,
aggregator, order-flow, funding/onchain, stock, volume cycles, coinbase
universe), opportunity ranking/priority, confidence multipliers, execution
dry-run paths, state persistence, and CLI entrypoint.

All external I/O (Coinbase CLI, subprocess, network, DB, strategy engine
backends, prediction-market clients) is mocked with unittest.mock.
"""

import json
import time
from types import SimpleNamespace
from unittest import mock

import pytest

import portfolio_optimizer as P
from conftest import make_state, holding


def mk_state(holdings, total_value=100000.0, usdc=50000.0, fee_volume_30d=0.0, volume_to_next_tier=0.0):
    """Wrap conftest.make_state, accepting a list of holding dicts."""
    if isinstance(holdings, list):
        holdings = {h["currency"]: h for h in holdings}
    return make_state(holdings, total_value=total_value, usdc=usdc,
                       fee_volume_30d=fee_volume_30d, volume_to_next_tier=volume_to_next_tier)


# ---------------------------------------------------------------------------
# Fake collaborators
# ---------------------------------------------------------------------------

def _verdict(passed=True, win_rate=0.7, sharpe=1.2, pf=1.6, dd=8.0,
             total_return=20.0, total_trades=30, reason="ok"):
    wt = int(total_trades * win_rate)
    return P.BacktestVerdict(
        strategy="ema_cross", currency="BTC", total_trades=total_trades,
        winning_trades=wt, losing_trades=total_trades - wt,
        win_rate=win_rate, total_return_pct=total_return,
        sharpe_ratio=sharpe, profit_factor=pf, max_drawdown_pct=dd,
        regime="neutral", passed=passed, reason=reason,
    )


class FakeMarket:
    """Minimal prediction-market object used by _detect_event_markets."""

    def __init__(self, **kw):
        self.__dict__.update(kw)


class FakeKnowledgeGap:
    def __init__(self, significant=False, direction="fair", mid_price=0.5,
                 gap=0.0, gap_pct=0.0, evidence_score=0.5, evidence_count=2,
                 sentiment_label="neutral", confidence=0.4, sources_used=None):
        self.is_significant = significant
        self.direction = direction
        self.mid_price = mid_price
        self.gap = gap
        self.gap_pct = gap_pct
        self.evidence_score = evidence_score
        self.evidence_count = evidence_count
        self.sentiment_label = sentiment_label
        self.confidence = confidence
        self.sources_used = sources_used or []


def _configured_cli():
    cli = mock.MagicMock()
    cli.environment = "live"
    cli.best_product.side_effect = lambda c, s: f"{c}-USD"
    cli.get_price.return_value = {
        "price": 100.0, "price_percentage_change_24h": 1.0, "volume_24h": 5_000_000,
    }
    cli.get_products.return_value = {}
    cli.get_candles.return_value = []
    cli.get_balances.return_value = []
    cli.get_fees.return_value = {"advanced_trade_only_volume": 0}
    cli.get_fills.return_value = []
    cli.preview_order.return_value = {"total_fee": 1.0, "total_cost": 100.0}
    cli.create_order.return_value = {"id": "ord1"}
    return cli


@pytest.fixture(autouse=True)
def _disable_smart_feed_thread(monkeypatch):
    # Prevent the real constructor from spawning a background SmartFeed thread
    # that logs after pytest tears down its log handlers.
    monkeypatch.setattr(P, "_HAS_SMART_FEED", False)


@pytest.fixture
def o(opt):
    """Optimizer with mocked CLI and a populated portfolio state."""
    opt = opt
    opt.cli = _configured_cli()
    opt._feed_mgr = None
    opt._lock_fd = None
    for _attr in ("_funding_contrarian", "_onchain_flow", "_order_flow_engine",
                 "_smart_money_flow", "_ensemble_blender", "_cross_asset_regime",
                 "_macro_risk", "_arb_scanner", "_knowledge_gap", "_pm_client"):
        setattr(opt, _attr, mock.MagicMock())
    opt.confidence_engine = mock.MagicMock()
    opt.confidence_engine.apply_modifiers.return_value = SimpleNamespace(modified_confidence=0.6)
    opt.state = mk_state(
        holdings=[
            holding("BTC", 20000, "safe", pnl=-2, volume_24h=10_000_000, change_24h=1.0, allocation_pct=20),
            holding("ETH", 15000, "safe", pnl=-8, volume_24h=8_000_000, change_24h=-2.0, allocation_pct=15),
            holding("SOL", 8000, "growth", pnl=5, volume_24h=4_000_000, change_24h=3.0, allocation_pct=8),
            holding("ADA", 3000, "growth", pnl=-10, volume_24h=2_000_000, change_24h=-1.0, allocation_pct=3),
            holding("DOGE", 1000, "speculative", pnl=-20, volume_24h=1_000_000, change_24h=-5.0, allocation_pct=1),
        ],
        total_value=100000.0, usdc=50000.0,
        fee_volume_30d=0.0, volume_to_next_tier=0.0,
    )
    return opt


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def test_module_helpers():
    assert P.to_float(5) == 5.0
    assert P.to_float("5.5") == 5.5
    assert P.to_float(None) == 0.0
    assert P.to_float("abc") == 0.0
    assert P._fmt_base(1.0) == "1"
    assert P._fmt_base(1.5) == "1.5"
    assert P._fmt_quote(1.0) == "1"
    assert P._clamp(5, 0, 3) == 3
    assert P._clamp(-1, 0, 3) == 0
    assert P._clamp(2, 0, 3) == 2
    assert P.classify_asset("BTC") == "safe"
    assert P.classify_asset("SOL") == "growth"
    assert P.classify_asset("DOGE") == "growth"
    assert P.classify_asset("SHIB") == "speculative"
    assert P.classify_asset("XYZ") == "speculative"
    assert P.current_fee_tier(0) == P.COINBASE_FEE_TIERS[0]
    top = P.current_fee_tier(1e9)
    assert top[0] > 0
    assert P.volume_to_next(0) == max(0.0, P.COINBASE_FEE_TIERS[1][0])
    assert P.volume_to_next(1e9) == 0.0


def test_estimate_atr_and_regime():
    closes = [100 + i for i in range(60)]
    highs = [c + 2 for c in closes]
    lows = [c - 2 for c in closes]
    assert P._estimate_atr(closes, highs, lows, 14) > 0
    assert P._estimate_atr([1, 2], highs, lows, 14) == 0.0
    reg = P._detect_market_regime(highs, lows, closes)
    assert reg in ("trending", "ranging", "volatile", "quiet", "neutral")
    sp = P._detect_swing_points(highs, lows)
    assert isinstance(sp, list)
    levels = P._build_sr_levels(highs, lows, closes, min_touches=2)
    assert isinstance(levels, list)
    adx = P._compute_adx(highs, lows, closes, 14)
    assert isinstance(adx, float)


def test_latency_tuned_priority_module():
    assert P._latency_tuned_priority(0.5) == 0.5


# ---------------------------------------------------------------------------
# CoinbaseCLI
# ---------------------------------------------------------------------------

def test_cli_methods():
    with mock.patch("subprocess.run") as mrun, \
         mock.patch("portfolio_optimizer.urllib.request.urlopen") as murl:
        mrun.return_value = mock.Mock(returncode=0, stdout="")
        murl.return_value.__enter__.return_value.read.return_value = b'[]'
        murl.return_value.__enter__.return_value.decode.return_value = '[]'
        cli = P.CoinbaseCLI("live")
        cli._products = {
            "BTC-USDC": {"product_id": "BTC-USDC"},
            "BTC-USD": {"product_id": "BTC-USD"},
        }
        assert cli.best_product("BTC", "BUY") == "BTC-USDC"
        assert cli.best_product("BTC", "SELL") == "BTC-USD"
        cli._products = {}
        assert cli.best_product("BTC", "BUY") is None
        q = cli._round_quote("BTC-USD", 100.123456)
        assert isinstance(q, float)
        mrun.return_value = mock.Mock(returncode=0, stdout=json.dumps(
            {"commission_total": "1.5", "order_total": "100"}))
        pv = cli.preview_order("BTC-USD", "BUY", 100, is_quote=True)
        assert pv["total_fee"] == 1.5
        co = cli.create_order("BTC-USD", "BUY", 100, is_quote=True)
        # create_order returns the raw CLI response unmodified.
        assert co["commission_total"] == "1.5"
        mrun.side_effect = RuntimeError("boom")
        assert cli.preview_order("BTC-USD", "BUY", 100) is None
        assert cli.create_order("BTC-USD", "BUY", 100) is None
        assert cli.get_price("BTC-USD") == {}


# ---------------------------------------------------------------------------
# Pure optimizer helpers
# ---------------------------------------------------------------------------

def test_pulse_tracking(o):
    key = o._pulse_key("BTC-USD", "ema_cross", "BUY")
    assert key == "BTC-USD:ema_cross:BUY"
    p = o._record_pulse("BTC-USD", "ema_cross", "BUY", 0.6, 100.0)
    assert p["pulse_count"] == 1
    p2 = o._record_pulse("BTC-USD", "ema_cross", "BUY", 0.7, 105.0)
    assert p2["pulse_count"] == 2
    assert o._is_pulse_valid(p2) is True
    # Recording the opposite direction registers a separate pulse; then a
    # subsequent same-direction pulse increments flip_count on the BUY pulse.
    o._record_pulse("BTC-USD", "ema_cross", "SELL", 0.6, 100.0)
    p3 = o._record_pulse("BTC-USD", "ema_cross", "BUY", 0.6, 100.0)
    assert p3["flip_count"] >= 1
    # Direct branch coverage of _is_pulse_valid
    assert o._is_pulse_valid({"pulse_count": 1, "flip_count": 0, "avg_confidence": 0.5}) is False
    assert o._is_pulse_valid({"pulse_count": 2, "flip_count": 3, "avg_confidence": 0.5}) is False
    assert o._is_pulse_valid({"pulse_count": 2, "flip_count": 0, "avg_confidence": 0.1}) is False
    assert o._is_pulse_valid({"pulse_count": 2, "flip_count": 0, "avg_confidence": 0.5}) is True
    o._prune_pulses()


def test_cluster_helpers(o):
    assert o._get_cluster_for_currency("SOL") == "l1_solana"
    assert o._get_cluster_for_currency("XYZ") is None
    assert o._cluster_exposure_pct("l1_solana") > 0
    assert o._check_cluster_limit("SOL", 1000) is True
    o.state = None
    assert o._cluster_exposure_pct("l1_solana") == 0.0
    assert o._check_cluster_limit("SOL", 1000) is True


def test_capital_policy(o):
    norm = o._normalize_capital_policy()
    assert "targets" in norm
    assert o._normalize_capital_policy({"core_allowlist": "BTC, ETH"})["core_allowlist"] == ["BTC", "ETH"]
    assert o._normalize_capital_policy({"static_holdings": "BTC, ETH"})["static_holdings"] == ["BTC", "ETH"]
    assert o._normalize_capital_policy({"targets": {"reserve": -5}})["targets"]["reserve"] >= 0
    o.capital_policy = o._normalize_capital_policy({"max_deployable_usd": 5000})
    assert o.capital_policy["max_deployable_usd"] == 5000
    o._save_capital_policy()
    o._refresh_capital_policy()
    assert o.update_capital_policy({"core_min_allocation_pct": 12.0})["core_min_allocation_pct"] == 12.0


def test_bear_market_policy(o):
    o.state = None
    o._apply_bear_market_policy()
    o.state = mk_state(holdings=[holding("BTC", 50000, "safe", change_24h=-3.0)], total_value=100000.0)
    o._portfolio_peak_value = 200000.0
    o._apply_bear_market_policy()
    o._portfolio_peak_value = 105000.0
    o._apply_bear_market_policy()


def test_first_seen_age(o):
    age = o._first_seen_age_days("NEWCOIN-USD")
    assert age == 0.0
    age2 = o._first_seen_age_days("NEWCOIN-USD")
    # Second call reads the stored timestamp; age is a tiny positive elapsed time.
    assert age2 >= 0.0


def test_kelly_and_regime_weight(o):
    o.state = None
    assert o._kelly_size(0.7, 2.0, 1.5, 0.6) == 50.0  # min_notional default
    o.state = mk_state(holdings=[holding("BTC", 1000, "safe")], total_value=100000.0, usdc=50000.0)
    k = o._kelly_size(0.7, 2.0, 1.5, 0.6, max_notional=5000.0)
    assert 50.0 <= k <= 5000.0
    k2 = o._kelly_size(1.5, 2.0, 1.5, 0.6)
    assert k2 >= o.min_value
    assert o._regime_strategy_weight("ema_cross", "trending") == 1.5
    assert o._regime_strategy_weight("rsi_revert", "trending") == 0.5
    assert o._regime_strategy_weight("rsi_revert", "ranging") == 1.5
    assert o._regime_strategy_weight("ema_cross", "ranging") == 0.5
    assert o._regime_strategy_weight("boll_break", "volatile") == 1.4
    assert o._regime_strategy_weight("x", "quiet") == 0.7
    assert o._regime_strategy_weight("x", "unknown") == 1.0


def test_risk_reward_size(o):
    o.state = None
    assert o._risk_reward_size(5.0, 3.0, 0.6, 0.7) == 50.0  # min_notional default
    o.state = mk_state(holdings=[holding("BTC", 1000, "safe")], total_value=100000.0, usdc=50000.0)
    s = o._risk_reward_size(5.0, 3.0, 0.6, 0.7, max_notional=2000.0)
    assert 50.0 <= s <= 2000.0


def test_estimate_volatility(o):
    assert o._estimate_trade_volatility_pct([]) == 30.0
    closes = [100 + i for i in range(40)]
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    v = o._estimate_trade_volatility_pct(closes, highs, lows)
    assert 1.0 <= v <= 20.0


def test_current_price_for_symbol(o):
    assert o._current_price_for_symbol("") == 0.0
    assert o._current_price_for_symbol("BTC") == 100.0
    o.state = None
    assert o._current_price_for_symbol("BTC") == 100.0
    o.state = mk_state(holdings=[holding("BTC", 1000, "safe", price=250.0)], total_value=100000.0, usdc=50000.0)
    o.cli.get_price.return_value = {}
    assert o._current_price_for_symbol("BTC") == 250.0


def test_capital_buckets(o):
    o._forced_max_deployable_usd = 0.0
    o.capital_policy["max_deployable_usd"] = 0.0
    # usdc must exceed the reserve (50% of total) for buy capacity to be positive.
    o.state.usdc_balance = 90000.0
    assert o._usdc_reserve_amount() > 0
    assert o._deployable_capital() > 0
    assert o._buy_capacity() > 0
    assert o._core_batch_cap() > 0
    assert o._opportunity_batch_cap() > 0
    assert o._bucket_targets()["reserve"] > 0
    bt = o._is_core_holding({"currency": "BTC", "classification": "safe", "allocation_pct": 20})
    assert bt is True
    assert o._is_static_currency("BTC") is True  # BTC/ETH are static by default
    assert o._is_static_currency("SOL") is False
    assert o._static_holdings_set() is not None
    assert o._bucket_values()["reserve"] > 0
    assert o._bucket_gap("reserve") >= 0
    opp = P.Opportunity(opp_type=P.OpportunityType.STRATEGY_SIGNAL, currency="BTC", side="BUY", size_usd=10, reason="r")
    assert o._capital_bucket_for(opp) == "core"
    opp2 = P.Opportunity(opp_type=P.OpportunityType.STOCK_SIGNAL, currency="AAPL", side="BUY", size_usd=10, reason="r")
    assert o._capital_bucket_for(opp2) == "opportunity"


def test_parse_iso(o):
    assert o._parse_iso_ts(None) is None
    assert o._parse_iso_ts("2024-01-01T00:00:00Z") is not None
    assert o._parse_iso_ts("not-a-date") is None
    o.capital_policy["live_test_started_at"] = "2024-01-01T00:00:00Z"
    assert o._live_test_started_at() is not None
    o.capital_policy["max_deployable_usd"] = 0.0
    assert o._live_test_capital_in_play() == 0.0
    assert o._remaining_deployable_capital() == 0.0


def test_iso_tail(o):
    o.capital_policy["max_deployable_usd"] = 1000.0
    o.capital_policy["live_test_started_at"] = "2024-01-01T00:00:00Z"
    o.store.save_trade({"timestamp": "2024-02-01T00:00:00Z", "side": "BUY", "size_usd": 200, "dry_run": 0})
    assert o._live_test_capital_in_play() >= 0.0
    assert o._remaining_deployable_capital() >= 0.0


# ---------------------------------------------------------------------------
# Route helpers
# ---------------------------------------------------------------------------

def test_route_helpers(o):
    o._last_detected_opportunities = []
    assert o._route_market_products() == []
    o.cli.get_products.return_value = {"BTC-USD": {"product_id": "BTC-USD"}}
    assert o._route_market_products() == [{"product_id": "BTC-USD"}]
    opp = P.Opportunity(opp_type=P.OpportunityType.STRATEGY_SIGNAL, currency="BTC", side="BUY", size_usd=100, reason="r", product_id="BTC-USD")
    # With multi-hop routing disabled these return None quickly.
    with mock.patch.object(P, "_HAS_MULTI_HOP", False):
        assert o._route_context_for_opportunity(opp) is None
        assert o._best_route_decision_for_opportunity(opp) is None
    assert o._route_amount_for_source("USD", 100) == 100.0
    assert o._route_amount_for_source("BTC", 100) == 1.0


def test_route_from_payload_no_multihop(o):
    with mock.patch.object(P, "_HAS_MULTI_HOP", False):
        assert o._route_decision_from_payload({"steps": []}) is None


# ---------------------------------------------------------------------------
# S/R + exit plan helpers
# ---------------------------------------------------------------------------

def test_sr_and_exit_plan(o):
    closes = [100 + i for i in range(60)]
    highs = [c + 2 for c in closes]
    lows = [c - 2 for c in closes]
    levels, atr = o._detect_sr_levels_for_product(closes, highs, lows)
    assert isinstance(levels, list)
    sp, atrd, detail = o._compute_dynamic_stop(100.0, "BUY", 2.0, "trending", levels, 5.0)
    assert sp > 0
    sp2, _, _ = o._compute_dynamic_stop(100.0, "BUY", 0.0, "trending", levels, 5.0)
    assert sp2 == 5.0
    plan = o._compute_exit_plan("BTC", 0.6, expected_return_pct=5.0, sr_levels=levels, regime="trending", atr_value=2.0, entry_price=100.0)
    assert plan["stop_loss_pct"] >= 0
    plan2 = o._compute_exit_plan("BTC", 0.6, trade_style="equity_momentum")
    assert plan2["stop_loss_pct"] >= 0
    wrapped = o._compute_sr_aware_exit_plan("BTC", 0.6, closes=closes, highs=highs, lows=lows)
    assert "stop_loss_pct" in wrapped
    assert 0.0 <= o._latency_adjusted_priority(0.5) <= 1.0


# ---------------------------------------------------------------------------
# Graph helpers
# ---------------------------------------------------------------------------

def test_graph_helpers(o):
    assert o._graph_signal_for_product("") is None
    assert o._graph_score_for_product("BTC-USD") == 0.5
    assert o._graph_multiplier_for_product("BTC-USD") == 1.0
    o._graph_signals["BTC-USD"] = SimpleNamespace(product_id="BTC-USD", graph_score=0.9)
    assert o._graph_score_for_product("BTC-USD") == 0.9
    assert abs(o._graph_multiplier_for_product("BTC-USD") - 1.2) < 1e-6


# ---------------------------------------------------------------------------
# Detection: TLH
# ---------------------------------------------------------------------------

def test_detect_tlh(o):
    o.last_execution.clear()
    ops = o._detect_tlh()
    assert ops and ops[0].opp_type == P.OpportunityType.TLH
    o.last_execution["tlh"] = time.time()
    assert o._detect_tlh() == []
    o.last_execution.clear()
    o.state = None
    assert o._detect_tlh() == []


def test_detect_enhanced_tlh(o):
    o.last_execution.clear()
    o._wash_sale_cooldown.clear()
    ops = o._detect_enhanced_tlh()
    assert ops and ops[0].opp_type == P.OpportunityType.TLH
    o._wash_sale_cooldown["DOGE"] = time.time()
    ops2 = o._detect_enhanced_tlh()
    assert all(x.currency != "DOGE" for x in ops2)
    o.state = None
    assert o._detect_enhanced_tlh() == []


def test_wash_sale_and_replacement(o):
    assert o._check_wash_sale("SOL") is False
    o._wash_sale_cooldown["SOL"] = time.time()
    assert o._check_wash_sale("SOL") is True
    assert o._get_tlh_replacement("BTC") == "ETH-USD"
    o._wash_sale_cooldown["ETH"] = time.time()
    assert o._get_tlh_replacement("BTC") is None


# ---------------------------------------------------------------------------
# Detection: fee tier / rebalance / volume cycles
# ---------------------------------------------------------------------------

def test_detect_fee_tier(o):
    o.last_execution.clear()
    o.state = mk_state(holdings=[holding("SOL", 50000, "growth")], total_value=100000.0, usdc=90000.0, volume_to_next_tier=0.0)
    assert o._detect_fee_tier_volume() == []
    o.state = mk_state(holdings=[holding("USDC", 50000, "safe")], total_value=100000.0, usdc=90000.0, volume_to_next_tier=10000.0)
    assert o._detect_fee_tier_volume() == []
    o.last_execution.clear()
    o.state = mk_state(
        holdings=[holding("SOL", 50000, "growth", change_24h=1.0, volume_24h=5_000_000)],
        total_value=100000.0, usdc=90000.0, volume_to_next_tier=10000.0,
    )
    ops = o._detect_fee_tier_volume()
    assert ops and ops[0].opp_type == P.OpportunityType.FEE_TIER_VOLUME
    o.state = mk_state(
        holdings=[holding("SOL", 50000, "growth", change_24h=25.0, volume_24h=5_000_000)],
        total_value=100000.0, usdc=90000.0, volume_to_next_tier=10000.0,
    )
    assert o._detect_fee_tier_volume() == []


def test_detect_rebalance(o):
    o.last_execution.clear()
    o.state = mk_state(
        holdings=[
            holding("BTC", 1000, "safe", allocation_pct=1),
            holding("SOL", 20000, "growth", allocation_pct=20),
            holding("DOGE", 5000, "speculative", allocation_pct=5),
        ],
        total_value=100000.0, usdc=50000.0,
    )
    ops2 = o._detect_rebalance()
    assert isinstance(ops2, list)
    o.last_execution["rebalance"] = time.time()
    assert o._detect_rebalance() == []
    o.last_execution.clear()
    o.state = None
    assert o._detect_rebalance() == []


def test_detect_volume_cycles(o):
    o.last_execution.clear()
    o.position_ages.clear()
    assert o._detect_volume_cycles() == []
    for cur in list(o.state.holdings):
        o.position_ages[cur] = time.time() - 10000 * 3600
    ops2 = o._detect_volume_cycles()
    assert ops2 and all(x.opp_type == P.OpportunityType.VOLUME_CYCLE for x in ops2)


# ---------------------------------------------------------------------------
# Detection: strategy signals
# ---------------------------------------------------------------------------

def _setup_strategy_signals(o, monkeypatch):
    o.last_execution.clear()
    o._bt_cache.clear()
    candles = [{
        "time": str(i), "open": 100 + i, "high": 103 + i, "low": 97 + i,
        "close": 100 + i, "volume": 1000.0,
    } for i in range(60)]
    o.cli.get_candles.return_value = candles
    monkeypatch.setattr(P, "_batch_signals_fast",
                        lambda products, closes, volumes, highs, lows: {
                            "BTC-USD": {"ema_cross": "BUY", "rsi_revert": "SELL"},
                            "ETH-USD": {"macd": "BUY"},
                        })
    for cur in ("BTC", "ETH"):
        for s in ("ema_cross", "rsi_revert", "macd"):
            o._bt_cache[f"{s}/{cur}"] = _verdict(passed=True)


def test_detect_strategy_signals(o, monkeypatch):
    _setup_strategy_signals(o, monkeypatch)
    ops = o._detect_strategy_signals()
    assert isinstance(ops, list)
    o.last_execution["strategy"] = time.time()
    assert o._detect_strategy_signals() == []
    o.last_execution.clear()
    o.state = None
    assert o._detect_strategy_signals() == []


def test_batch_uncached_backtests(o, monkeypatch):
    o._bt_cache.clear()
    h = {"currency": "BTC", "classification": "safe", "price": 100.0, "value": 1000, "product_id": "BTC-USD"}
    sig = P.StrategySignal(strategy="ema_cross", action="BUY", confidence=0.6, reason="r")
    candidates = [(h, "BTC-USD", [1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 2, 3], [sig])]

    def fake_batch(items):
        return {"ema_cross/BTC": _verdict(passed=True)}

    monkeypatch.setattr(P, "_batch_backtest_rust", fake_batch)
    o._batch_uncached_backtests(candidates)
    assert "ema_cross/BTC" in o._bt_cache


# ---------------------------------------------------------------------------
# Detection: funding / onchain
# ---------------------------------------------------------------------------

def test_detect_funding_onchain(o, monkeypatch):
    o.last_execution.clear()
    o._funding_contrarian.on_bar.return_value = SimpleNamespace(action="BUY", confidence=0.6, reason="funding-buy")
    o._onchain_flow.get_signals.return_value = [{
        "action": "BUY", "product_id": "BTC-USD", "currency": "BTC",
        "confidence": 0.6, "price": 100.0, "volume_anomaly": 2.0, "price_trend": 0.1,
        "reason": "onchain-buy",
    }]
    ops = o._detect_funding_and_onchain_signals()
    assert isinstance(ops, list)
    o.last_execution.clear()
    o._funding_contrarian.on_bar.return_value = SimpleNamespace(action="SELL", confidence=0.6, reason="funding-sell")
    o._onchain_flow.get_signals.return_value = []
    ops2 = o._detect_funding_and_onchain_signals()
    assert isinstance(ops2, list)
    o.last_execution.clear()
    o.state = None
    assert o._detect_funding_and_onchain_signals() == []
    o.state = mk_state(holdings=[holding("BTC", 5000, "safe")], total_value=100000.0, usdc=50000.0)
    o.last_execution["funding_onchain"] = time.time()
    assert o._detect_funding_and_onchain_signals() == []


# ---------------------------------------------------------------------------
# Detection: order flow
# ---------------------------------------------------------------------------

def test_detect_order_flow(o, monkeypatch):
    o.last_execution.clear()
    o._order_flow_engine.evaluate.return_value = SimpleNamespace(
        confidence=0.5, action="BUY", spread_z=2.0, spread_tight=True, volume_24h=1000.0, spread_bps=5.0,
    )
    o._smart_money_flow.on_bar.return_value = SimpleNamespace(
        confidence=0.5, direction=SimpleNamespace(value="long"), reason="smart-buy", entry_price=100.0,
    )
    ops = o._detect_order_flow_signals()
    assert isinstance(ops, list)
    o.last_execution.clear()
    o._order_flow_engine.evaluate.return_value = SimpleNamespace(
        confidence=0.5, action="SELL", spread_z=-2.0, spread_tight=True, volume_24h=1000.0, spread_bps=5.0,
    )
    o._smart_money_flow.on_bar.return_value = None
    ops2 = o._detect_order_flow_signals()
    assert isinstance(ops2, list)
    o.last_execution.clear()
    o.state = None
    assert o._detect_order_flow_signals() == []
    o.state = mk_state(holdings=[holding("BTC", 5000, "safe")], total_value=100000.0, usdc=50000.0)
    o.last_execution["order_flow"] = time.time()
    assert o._detect_order_flow_signals() == []


# ---------------------------------------------------------------------------
# Detection: coinbase universe
# ---------------------------------------------------------------------------

def test_detect_coinbase_universe(o, monkeypatch):
    o.last_execution.clear()
    prods = {}
    for c in ("BTC", "ETH", "SOL", "ADA", "DOGE", "XRP", "LINK", "AVAX", "DOT", "MATIC",
             "LTC", "BCH", "NEAR", "APT", "SUI", "ARB", "OP", "FIL", "INJ", "SEI",
             "ATOM", "UNI", "POL", "ALGO", "XLM", "STX", "HBAR", "ICP", "GRT", "SHIB",
             "PEPE", "BONK", "TRUMP", "FLOKI"):
        prods[f"{c}-USD"] = {"product_id": f"{c}-USD", "volume_24h": 200_000_000, "trading_disabled": False}
    o.cli.get_products.return_value = prods
    candles = [{
        "time": str(i), "open": 100 + i, "high": 103 + i, "low": 97 + i,
        "close": 100 + i, "volume": 1000.0,
    } for i in range(60)]
    o.cli.get_candles.return_value = candles
    monkeypatch.setattr(P, "_batch_signals_fast", lambda products, closes, volumes, highs, lows: {})
    ops = o._detect_coinbase_universe_signals()
    assert isinstance(ops, list)
    o.last_execution.clear()
    o.state = None
    assert o._detect_coinbase_universe_signals() == []


# ---------------------------------------------------------------------------
# Detection: stock opportunities (gated)
# ---------------------------------------------------------------------------

def test_detect_stock_opportunities(o, monkeypatch):
    class FakeStock:
        def fetch_historical_data(self, symbol, start, end):
            return [{"close": 100 + i, "volume": 1000 + i} for i in range(80)]

        class yfinance:
            def get_stock_info(self, symbol):
                return {"market_capital": 50_000_000_000}

        yfinance = yfinance()

    monkeypatch.setattr(P, "UnifiedMarketDataAdapter", FakeStock)
    ops = o._detect_stock_opportunities()
    assert isinstance(ops, list)


# ---------------------------------------------------------------------------
# Detection: accumulator (gated)
# ---------------------------------------------------------------------------

def test_detect_accumulator(o, monkeypatch):
    class FakeSig:
        def __init__(self, action, conf):
            self.action = action
            self.final_confidence = conf
            self.base_confidence = conf
            self.symbol = "ADA-USD"  # growth, non-static, non-core -> tradeable
            self.strategy_name = "NewsSentiment:foo"
            self.signal_reason = "news up"
            self.opportunity_score = 0.5
            self.market_data = {"price": 100.0, "change_pct": 1.0}

    class FakeAcc:
        def __init__(self, *a, **k):
            pass

        def accumulate(self):
            return [FakeSig("BUY", 0.5), FakeSig("SELL", 0.1), FakeSig("HOLD", 0.5)]

    monkeypatch.setattr(P, "_HAS_ACCUMULATOR", True)
    monkeypatch.setattr(P, "UnifiedSignalAccumulator", FakeAcc)
    o.last_execution.clear()
    o.state = mk_state(holdings=[holding("BTC", 5000, "safe")], total_value=100000.0, usdc=90000.0)
    ops = o._detect_accumulator_signals()
    assert isinstance(ops, list)
    assert any(x.opp_type == P.OpportunityType.ACCUMULATOR_SIGNAL for x in ops)


# ---------------------------------------------------------------------------
# Detection: aggregator (gated)
# ---------------------------------------------------------------------------

def test_detect_aggregator(o, monkeypatch):
    class FakeResult:
        def __init__(self, base):
            self.direction = "BUY"
            self.base = base
            self.product_id = f"{base}-USD"
            self.unified_score = 0.8
            self.priority = 0.7
            self.conviction = 0.7
            self.backtest_quality = 0.6
            self.trend_score = 0.5
            self.top_strategies = ["ema_cross"]
            self.price = 100.0
            self.details = {"buy_strategies": ["ema_cross"], "sell_strategies": []}

    class FakeAgg:
        def scan_universe(self, products, closes, volumes, highs, lows, min_candles=60):
            return [FakeResult(base) for _, base in products]

    pairs = [("BTC-USD", "BTC"), ("ETH-USD", "ETH"), ("SOL-USD", "SOL"), ("ADA-USD", "ADA")]
    candles = {pid: [[i, 100 + i, 103 + i, 97 + i, 100 + i, 1000.0] for i in range(80)]
               for pid, _ in pairs}

    monkeypatch.setattr(P, "_HAS_AGGREGATOR", True)
    monkeypatch.setattr(P, "SignalAggregator", FakeAgg)
    monkeypatch.setattr(P, "_HAS_MULTI_HOP", False)
    monkeypatch.setattr("coinbase.src.pair_discovery.top_coinbase_pairs",
                        lambda n=50, min_volume_usd=500_000: pairs)
    monkeypatch.setattr("coinbase.src.rest_feed.fetch_candles_batch",
                        lambda *a, **k: candles)
    o.last_execution.clear()
    o._feed_mgr = None
    o.state = mk_state(holdings=[holding("BTC", 5000, "safe")], total_value=100000.0, usdc=90000.0)
    ops = o._detect_aggregator_signals()
    assert isinstance(ops, list)
    assert any(x.opp_type == P.OpportunityType.STRATEGY_SIGNAL for x in ops)


# ---------------------------------------------------------------------------
# Detection: event markets
# ---------------------------------------------------------------------------

def _fake_market(category, question, mid=0.7, vol=5000, extremity=0.6, liq=0.8, spread=0.01, platform="kalshi", mid_price=None):
    return FakeMarket(
        category=category, question=question, mid_price=mid_price if mid_price is not None else mid,
        volume=vol, probability_extremity=extremity, liquidity_score=liq, spread=spread,
        platform=platform, market_id="m1", is_open=True,
    )


def test_detect_event_markets(o, monkeypatch):
    o.last_execution.clear()
    m1 = _fake_market("crypto", "Will bitcoin exceed 100k?")
    m2 = _fake_market("sports", "Who wins super bowl", mid=0.9)
    o._pm_client.search_all_categories.return_value = {"crypto": [m1], "sports": [m2]}
    o._knowledge_gap.analyze.return_value = FakeKnowledgeGap(
        significant=True, direction="overvalued", mid_price=0.7, gap=0.25, gap_pct=25.0,
    )
    o._arb_scanner.scan.return_value = []
    ops = o._detect_event_markets()
    assert isinstance(ops, list)
    o.last_execution.clear()
    o._arb_scanner.scan.return_value = [SimpleNamespace(
        edge_pct=0.02, edge=0.02, confidence=0.6, reason="arb", event_key="k", category="crypto",
        platform_buy="kalshi", platform_hedge="polymarket",
        leg_buy=SimpleNamespace(platform="kalshi", market_id="b", question="q", outcome="yes", side="BUY", price=0.5),
        leg_hedge=SimpleNamespace(platform="polymarket", market_id="h", question="q", outcome="no", side="SELL", price=0.5),
    )]
    o._pm_client.search_all_categories.return_value = {"crypto": [m1]}
    o._knowledge_gap.analyze.return_value = None
    o.state = mk_state(holdings=[holding("BTC", 5000, "safe")], total_value=100000.0, usdc=90000.0)
    ops2 = o._detect_event_markets()
    assert any(x.opp_type == P.OpportunityType.EVENT_ARBITRAGE for x in ops2)
    o.last_execution.clear()
    o._pm_client = None
    o.event_engine = None
    assert o._detect_event_markets() == []


def test_event_signals_to_ops(o):
    sig = SimpleNamespace(
        outcome="BUY BTC", reason="r", confidence=0.6, position_size=100,
        platform="kalshi", market_ticker="BTC", probability=0.6, signal_type="x", market_question="q",
    )
    ops = o._event_signals_to_ops([sig])
    assert ops and ops[0].opp_type == P.OpportunityType.EVENT_MARKET


# ---------------------------------------------------------------------------
# Cross-asset risk filter + ensemble
# ---------------------------------------------------------------------------

def test_cross_asset_filter(o, monkeypatch):
    opp = P.Opportunity(opp_type=P.OpportunityType.STRATEGY_SIGNAL, currency="BTC", side="BUY", size_usd=100, reason="r", priority=0.5)
    opps = [opp]
    # No regime state and no macro signal -> opportunities pass through unchanged.
    o._cross_asset_regime.get_state.return_value = None
    o._macro_risk.get_signal.return_value = None
    assert o._apply_cross_asset_risk_filter(opps) == opps
    o._cross_asset_regime.get_state.return_value = SimpleNamespace(
        allows_new_longs=False, risk_multiplier=0.5, regime="crash", trend_bias=0.0)
    assert o._apply_cross_asset_risk_filter(opps) == []
    o._cross_asset_regime.get_state.return_value = SimpleNamespace(
        allows_new_longs=True, risk_multiplier=0.5, regime="risk_off", trend_bias=0.0)
    out2 = o._apply_cross_asset_risk_filter(opps)
    assert out2 and out2[0].priority < 0.5
    o._cross_asset_regime.get_state.side_effect = Exception("x")
    o._macro_risk.get_signal.return_value = SimpleNamespace(macro_score=2.0)
    out3 = o._apply_cross_asset_risk_filter(opps)
    assert out3 and out3[0].priority < 0.5


def test_ensemble_blend(o, monkeypatch):
    assert o._signal_ensemble_blend([]) == []
    opp = P.Opportunity(opp_type=P.OpportunityType.STRATEGY_SIGNAL, currency="BTC", side="BUY", size_usd=100, reason="r", priority=0.5, meta={"strategy": "x"})
    # No blended results -> opportunities returned unchanged
    o._ensemble_blender.blend_signals.return_value = []
    assert o._signal_ensemble_blend([opp]) == [opp]
    # Blended results are mapped back onto the original opportunities
    o._ensemble_blender.blend_signals.return_value = [SimpleNamespace(
        reason="r", meta={"bayesian_weight": 1.2, "bayesian_win_rate": 0.7}, score=0.8)]
    out = o._signal_ensemble_blend([opp])
    # VERIFIES FIX (portfolio_optimizer.py:2737): the EnsembleOpp is now built with
    # all required fields, so the blend actually runs and writes ensemble metadata.
    assert out[0].meta.get("ensemble_weight") == 1.2
    assert out[0].meta.get("ensemble_win_rate") == 0.7
    assert out[0].priority == 0.8
    assert isinstance(o._apply_meta_source_weights(out), list)


# ---------------------------------------------------------------------------
# _detect_opportunities orchestrator
# ---------------------------------------------------------------------------

def test_detect_opportunities(o, monkeypatch):
    o.last_execution.clear()
    o._bt_cache.clear()
    candles = [{"time": str(i), "open": 100 + i, "high": 103 + i, "low": 97 + i, "close": 100 + i, "volume": 1000.0} for i in range(60)]
    o.cli.get_candles.return_value = candles
    o.cli.get_products.return_value = {f"{c}-USD": {"product_id": f"{c}-USD", "volume_24h": 200_000_000} for c in ("BTC", "ETH", "SOL", "ADA", "DOGE")}
    monkeypatch.setattr(P, "_batch_signals_fast", lambda *a, **k: {"BTC-USD": {"ema_cross": "BUY"}})
    o._bt_cache["ema_cross/BTC"] = _verdict(passed=True)
    ops = o._detect_opportunities()
    assert isinstance(ops, list)


# ---------------------------------------------------------------------------
# Execution: record / normalize / process (dry-run)
# ---------------------------------------------------------------------------

def test_record_trade(o):
    opp = P.Opportunity(opp_type=P.OpportunityType.TLH, currency="DOGE", side="SELL", size_usd=100, reason="r", entry_price_est=10)
    before = len(o.trade_log)
    o._record_trade(opp, 1.0)
    assert len(o.trade_log) == before + 1
    o.state = mk_state(holdings=[holding("BTC", 5000, "safe")], total_value=100000.0, usdc=50000.0)
    bop = P.Opportunity(opp_type=P.OpportunityType.REBALANCE, currency="BTC", side="BUY", size_usd=100, reason="r", entry_price_est=100)
    o._record_trade(bop, 0.5)
    vop = P.Opportunity(opp_type=P.OpportunityType.VOLUME_CYCLE, currency="SOL", side="SELL", size_usd=100, reason="r")
    o._record_trade(vop, 0.0)


def test_normalize_product_id(o):
    o.cli.best_product.return_value = "BTC-USDC"
    assert o._normalize_product_id("BTC", "BUY") == "BTC-USD"
    o.cli.best_product.side_effect = Exception("x")
    assert o._normalize_product_id("BTC", "BUY", "BTC-USD") == "BTC-USD"


def test_process_opportunity_dryrun(o, monkeypatch):
    o.dry_run = True
    o.require_approval = False
    # Force the direct (non-route) execution path deterministically.
    o._best_route_decision_for_opportunity = lambda *a, **k: None
    sop = P.Opportunity(opp_type=P.OpportunityType.STRATEGY_SIGNAL, currency="BTC", side="BUY", size_usd=100, reason="r", entry_price_est=100, stop_loss_pct=5, meta={})
    o.capital_policy["static_holdings"] = ["BTC"]
    o._process_opportunity(sop)
    o.capital_policy["static_holdings"] = []
    eop = P.Opportunity(opp_type=P.OpportunityType.EVENT_MARKET, currency="?", side="NONE", size_usd=0, reason="r", meta={"platform": "k", "market_question": "q", "signal_type": "x", "confidence": 0.5})
    o._process_opportunity(eop)
    o.state = mk_state(holdings=[holding("BTC", 5000, "safe")], total_value=100000.0, usdc=50.0)
    bop = P.Opportunity(opp_type=P.OpportunityType.STRATEGY_SIGNAL, currency="BTC", side="BUY", size_usd=100000, reason="r", entry_price_est=100, meta={})
    o._process_opportunity(bop)
    o.state = mk_state(holdings=[holding("BTC", 5000, "safe")], total_value=100000.0, usdc=90000.0)
    o._bracket_mgr = None
    o._exec_engine = None
    bop2 = P.Opportunity(opp_type=P.OpportunityType.STRATEGY_SIGNAL, currency="SOL", side="BUY", size_usd=100, reason="r", entry_price_est=100, stop_loss_pct=5, meta={})
    o._process_opportunity(bop2)
    assert bop2.executed


def test_execute_with_bracket_dryrun(o, monkeypatch):
    o.dry_run = True
    o.require_approval = False
    o._exec_engine = None
    opp = P.Opportunity(opp_type=P.OpportunityType.REBALANCE, currency="BTC", side="BUY", size_usd=100, reason="r", entry_price_est=100, stop_loss_pct=5, take_profit_pct=10)
    o._execute_with_bracket(opp, 1.0, is_quote=True)
    assert opp.executed
    o._execute_with_bracket(opp, 0.0, is_quote=True)


# ---------------------------------------------------------------------------
# Pending approvals / execute approved
# ---------------------------------------------------------------------------

def test_check_pending_approvals_noop(o):
    o.require_approval = False
    assert o._check_pending_approvals() is None
    o.require_approval = True
    o.pending_file = "/tmp/does_not_exist_pend.json"
    assert o._check_pending_approvals() is None


def test_execute_approved(o, tmp_path):
    o.dry_run = True
    o.require_approval = False
    o.pending_file = str(tmp_path / "pend.json")
    o._HAS_MULTI_HOP = False
    entry = {
        "side": "BUY", "currency": "BTC", "size_usd": 100, "product_id": "BTC-USD",
        "reason": "approved", "priority": 0.5, "type": "strategy", "capital_bucket": "opportunity",
    }
    o._execute_approved(entry)
    o._execute_approved({"side": "BUY", "currency": "BTC", "size_usd": 0})
    entry2 = dict(entry)
    entry2["route_decision"] = {"steps": [{"product_id": "BTC-USD", "from_currency": "USDC", "to_currency": "BTC", "direction": "BUY", "price": 100, "effective_rate": 1.0}], "source": "USDC", "target": "BTC", "effective_rate": 1.0, "fee_bps": 5.0, "spread_bps": 2.0, "score": 0.5}
    o._execute_approved(entry2)


# ---------------------------------------------------------------------------
# State persistence / write helpers
# ---------------------------------------------------------------------------

def test_load_and_save_state(o):
    o.neo4j_store = None
    o._load_from_store()
    o._save_state()


def test_write_helpers(o):
    ops = [P.Opportunity(opp_type=P.OpportunityType.TLH, currency="DOGE", side="SELL", size_usd=100, reason="r", meta={"trade_style": "tax_loss"})]
    o._write_trade_plans(ops)
    o._write_signal_cache(ops)
    o._meta_source_weights = {"x": 0.5}
    o._param_opt_results = {"atr": {"best_params": {"a": 1}}}
    o._wash_sale_cooldown = {"DOGE": time.time()}
    o._order_flow_engine.get_signal.return_value = SimpleNamespace(action="BUY", confidence=0.5, spread_bps=5, spread_z=2.0, spread_tight=True)
    o._cross_asset_regime.get_state.return_value = SimpleNamespace(to_dict=lambda: {"r": 1})
    o._ensemble_blender.to_dict.return_value = {}
    o._ensemble_blender.top_strategies.return_value = []
    o._tick_count = 20
    o._feed_mgr = None
    o._write_enhanced_state()


# ---------------------------------------------------------------------------
# summary + main
# ---------------------------------------------------------------------------

def test_summary(o):
    o.trade_log.append({"type": "tlh", "size_usd": 100, "fee": 1.0})
    s = o.summary()
    assert s["total_trades"] == 1
    assert s["total_volume"] == 100.0


def test_main_summary_and_once(monkeypatch, tmp_path):
    db = str(tmp_path / "opt.db")
    monkeypatch.setattr("sys.argv", ["portfolio_optimizer.py", "--summary", "--db", db, "--reset-db"])

    class FakeOpt:
        def __init__(self, **kw):
            self.trade_log = []

        def summary(self):
            return {"total_trades": 0}

        def _tick(self):
            return None

    monkeypatch.setattr(P, "PortfolioOptimizer", FakeOpt)
    P.main()
    monkeypatch.setattr("sys.argv", ["portfolio_optimizer.py", "--once", "--db", db])
    P.main()
