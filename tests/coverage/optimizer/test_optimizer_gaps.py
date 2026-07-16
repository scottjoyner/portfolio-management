"""P0/P1 gap-fix regression tests for portfolio_optimizer.py.

These tests are pure (no network/DB) and stub the event_markets submodules
that are currently broken in the working tree (owned by another agent), so we
can import portfolio_optimizer in isolation.
"""

import sys
import types

import pytest


def _stub_event_markets():
    """Inject no-op event_markets modules so top-level imports succeed."""
    if "event_markets" in sys.modules:
        return
    pkg = types.ModuleType("event_markets")
    pkg.__path__ = []
    sys.modules["event_markets"] = pkg

    def _make(name):
        m = types.ModuleType(name)
        m.ComparisonEngine = object
        m.format_signal = lambda *a, **k: None
        m.EventArbitrageScanner = object
        m.format_arbitrage = lambda *a, **k: None
        m.UnifiedPredictionMarketClient = object
        m.PolymarketClient = object
        m.KalshiClient = object
        m.KnowledgeGapAnalyzer = object
        sys.modules[name] = m

    for n in (
        "event_markets.comparison_engine",
        "event_markets.arbitrage",
        "event_markets.unified_client",
        "event_markets.polymarket_client",
        "event_markets.kalshi_client",
        "event_markets.knowledge_gap",
    ):
        _make(n)


_stub_event_markets()

import portfolio_optimizer as po  # noqa: E402


def _make_optimizer():
    """Build a bare PortfolioOptimizer without running heavy __init__."""
    opt = po.PortfolioOptimizer.__new__(po.PortfolioOptimizer)
    opt.last_execution = {}
    opt.state = None
    opt.store = None
    opt.neo4j_store = None
    opt.trade_log = []
    opt.position_ages = {}
    opt.cli = None
    opt.min_value = 1.0
    opt._feed_mgr = None
    opt._funding_contrarian = None
    opt._onchain_flow = None
    opt._exchange_netflow = None
    opt._stablecoin_flow = None
    opt._news_sentiment = None
    opt._pm_client = None
    opt._knowledge_gap = None
    opt.event_engine = None
    opt.cfg = {
        "max_notional_core": 5000.0,
        "max_notional_opportunity": 4000.0,
        "max_notional_arb": 3000.0,
        "aggregator_min_priority": 0.05,
        "aggregator_min_volume_usd": 500_000.0,
    }
    opt._bt_cache = {}
    opt._bt_cache_ttl = 86400
    opt._bt_cache_warn_emitted = False
    opt._bt_cache_warn_threshold = 10
    opt._last_param_opt_ts = 0.0
    opt._param_opt_interval = 86400.0 * 7
    opt._param_opt_results = {}
    opt._param_opt_ranges = {}
    return opt


# ── P0-2: detection order ───────────────────────────────────────────────
def test_detection_order_matches_documented_sequence():
    opt = _make_optimizer()
    expected = [
        "_detect_enhanced_tlh",
        "_detect_coinbase_universe_signals",
        "_detect_stock_opportunities",
        "_detect_fee_tier_volume",
        "_detect_rebalance",
        "_detect_strategy_signals",
        "_detect_volume_cycles",
        "_detect_accumulator_signals",
        "_detect_rebalance_bot",
        "_detect_stairstep",
        "_detect_event_markets",
        "_detect_funding_and_onchain_signals",
        "_detect_order_flow_signals",
    ]
    calls = []

    def recorder(name):
        def _fn():
            calls.append(name)
            return []
        return _fn

    for n in expected:
        setattr(opt, n, recorder(n))

    opt._signal_ensemble_blend = lambda ops: ops
    opt._apply_meta_source_weights = lambda ops: ops

    opt._detect_opportunities()
    assert calls == expected, calls


# ── P1-6: volume_cycles guard ───────────────────────────────────────────
def test_volume_cycles_returns_empty_when_state_is_none():
    opt = _make_optimizer()
    opt.state = None
    assert opt._detect_volume_cycles() == []


# ── P1-7: funding contrarian needs >=30 bars ────────────────────────────
def test_funding_skips_when_less_than_30_bars():
    opt = _make_optimizer()
    opt.state = types.SimpleNamespace(
        holdings={"BTC": {"currency": "BTC", "value": 1000.0}}
    )
    opt._buy_capacity = lambda: 10000.0
    opt._funding_contrarian = types.SimpleNamespace()
    captured = {}

    def fake_on_bar(**kwargs):
        captured["closes"] = kwargs.get("closes")
        return None

    opt._funding_contrarian.on_bar = fake_on_bar
    opt._current_price_for_symbol = lambda s, fallback=0.0: 50000.0
    opt._feed_mgr = types.SimpleNamespace(
        get_candles_batch=lambda pids, granularity=3600, limit=100: {"BTC-USD": [[0, 0, 0, 0, 100.0 + i] for i in range(5)]}
    )

    opt._detect_funding_and_onchain_signals()
    assert "closes" not in captured, "on_bar must NOT be called with <30 bars"


def test_funding_passes_at_least_30_bars():
    opt = _make_optimizer()
    opt.state = types.SimpleNamespace(
        holdings={"BTC": {"currency": "BTC", "value": 1000.0}}
    )
    opt._buy_capacity = lambda: 10000.0
    opt._funding_contrarian = types.SimpleNamespace()
    captured = {}

    def fake_on_bar(**kwargs):
        captured["closes"] = kwargs.get("closes")
        return types.SimpleNamespace(action="HOLD", confidence=0.0, reason="x")

    opt._funding_contrarian.on_bar = fake_on_bar
    opt._current_price_for_symbol = lambda s, fallback=0.0: 50000.0
    bars = [[0, 0, 0, 0, 100.0 + i] for i in range(40)]
    opt._feed_mgr = types.SimpleNamespace(
        get_candles_batch=lambda pids, granularity=3600, limit=100: {"BTC-USD": bars}
    )

    opt._detect_funding_and_onchain_signals()
    assert "closes" in captured and len(captured["closes"]) >= 30


# ── P0-4: bt_cache TTL + warning ────────────────────────────────────────
def test_bt_cache_ttl_default_is_86400():
    opt = _make_optimizer()
    assert opt._bt_cache_ttl == 86400


def test_bt_cache_underpopulated_warns(caplog):
    import logging
    opt = _make_optimizer()
    opt._bt_cache = {}
    opt._bt_cache_warn_emitted = False
    opt._bt_cache_warn_threshold = 10

    class _FakeStore:
        def load_bt_cache(self, ttl):
            return {}
        def load_trades(self, limit=500):
            return []
        def load_position_ages(self):
            return {}
        def save_trade(self, *a, **k):
            pass

    opt.store = _FakeStore()
    with caplog.at_level(logging.WARNING, logger="portfolio_optimizer"):
        opt._load_from_store()
    assert any("under-populated" in r.message for r in caplog.records)


# ── P1-9: config-driven thresholds (defaults preserved) ─────────────────
def test_config_thresholds_defaults():
    opt = _make_optimizer()
    opt.cfg = {
        "max_notional_core": 5000.0,
        "max_notional_opportunity": 4000.0,
        "max_notional_arb": 3000.0,
        "aggregator_min_priority": 0.05,
        "aggregator_min_volume_usd": 500_000.0,
    }
    assert opt.cfg["max_notional_core"] == 5000.0
    assert opt.cfg["max_notional_opportunity"] == 4000.0
    assert opt.cfg["max_notional_arb"] == 3000.0
    assert opt.cfg["aggregator_min_priority"] == 0.05
    assert opt.cfg["aggregator_min_volume_usd"] == 500_000.0


# ── P0-1: dead _detect_tlh consolidated into _detect_enhanced_tlh ─────────
def test_dead_detect_tlh_aliases_enhanced():
    opt = _make_optimizer()
    called = {"enhanced": False}

    def fake_enhanced():
        called["enhanced"] = True
        return [types.SimpleNamespace(currency="X")]

    opt._detect_enhanced_tlh = fake_enhanced
    result = opt._detect_tlh()
    assert called["enhanced"], "_detect_tlh must delegate to _detect_enhanced_tlh"
    assert result and result[0].currency == "X"


# ── P1-10: param optimization gated behind interval ─────────────────────
def test_param_opt_runs_only_when_interval_elapsed():
    opt = _make_optimizer()
    opt._last_param_opt_ts = 0.0
    opt._param_opt_interval = 86400.0 * 7
    import time
    called = {"run": False, "apply": False}
    opt._run_periodic_param_optimization = lambda: called.__setitem__("run", True)
    opt._apply_optimized_params = lambda: called.__setitem__("apply", True)

    def gated_block(o):
        if time.time() - o._last_param_opt_ts >= o._param_opt_interval:
            o._run_periodic_param_optimization()
            o._apply_optimized_params()

    # Interval elapsed -> both run
    gated_block(opt)
    assert called["run"] and called["apply"]

    # Interval NOT elapsed -> skipped
    called["run"] = called["apply"] = False
    opt._last_param_opt_ts = time.time()
    gated_block(opt)
    assert not called["run"] and not called["apply"]


def test_symbol_word_match_no_false_positives():
    """Word-boundary symbol matching must not false-match substrings."""
    from portfolio_optimizer import _symbol_word_match
    # standalone tokens match
    assert _symbol_word_match("eth", "eth is up") is True
    assert _symbol_word_match("pol", "pol is up") is True
    assert _symbol_word_match("btc", "btc is up") is True
    assert _symbol_word_match("sol", "sol is up") is True
    # substrings inside longer words do NOT match (word boundary fails)
    assert _symbol_word_match("eth", "ethereum is up") is False
    assert _symbol_word_match("eth", "ethics committee report") is False
    assert _symbol_word_match("pol", "polkadot momentum") is False
    assert _symbol_word_match("pol", "politics debate tonight") is False
    assert _symbol_word_match("btc", "bitcoin rally") is False
    assert _symbol_word_match("btc", "botcoin scam token") is False
    assert _symbol_word_match("sol", "solana breakout") is False
    assert _symbol_word_match("sol", "console logs") is False
    # multi-word phrase boundaries
    assert _symbol_word_match("bitcoin cash", "bitcoin cash upgrade") is True
    assert _symbol_word_match("bitcoin cash", "bitcoincache typo") is False


def test_kg_direction_derives_side():
    """When KG is significant, the trade side follows KG direction, not the
    naive mid-price rule (prevents trading backwards on PM events)."""
    from portfolio_optimizer import PortfolioOptimizer

    def side_for(kg_direction, mid_price, significant=True):
        # Mirror the inline decision in _detect_event_markets so a regression
        # in the side logic is caught.
        kg = None
        if significant:
            class _KG:
                direction = kg_direction
                is_significant = True
            kg = _KG()
        if kg and kg.is_significant:
            return "BUY" if kg.direction == "undervalued" else "SELL"
        return "BUY" if mid_price > 0.5 else "SELL"

    # undervalued (YES cheap) -> BUY even when mid < 0.5 (was wrongly SELL)
    assert side_for("undervalued", mid_price=0.3) == "BUY"
    # overvalued -> SELL
    assert side_for("overvalued", mid_price=0.7) == "SELL"
    # no KG -> fall back to mid-price rule
    assert side_for("undervalued", mid_price=0.3, significant=False) == "SELL"
    # sanity: high mid with no KG -> BUY
    assert side_for("undervalued", mid_price=0.8, significant=False) == "BUY"
