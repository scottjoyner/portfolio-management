"""Coverage tests for the remaining large detection-dimension methods of
PortfolioOptimizer (event markets, accumulator, aggregator, funding/onchain,
coinbase universe, stock, enhanced TLH, param optimization).

External data sources are mocked so each method's full decision path is
exercised.
"""
from __future__ import annotations

import time
from types import SimpleNamespace
from unittest import mock

import pytest

import portfolio_optimizer as P
import strategy_engine as SE
from tests.coverage.optimizer.conftest import holding, make_state


# --------------------------------------------------------------- fake helpers
class _AccSignal:
    def __init__(self, symbol, action, final_confidence=0.6, strategy_name="NewsSentiment",
                 signal_reason="news bullish", market_data=None, base_confidence=0.5,
                 opportunity_score=0.4):
        self.symbol = symbol
        self.action = action
        self.final_confidence = final_confidence
        self.base_confidence = base_confidence
        self.opportunity_score = opportunity_score
        self.strategy_name = strategy_name
        self.signal_reason = signal_reason
        self.market_data = market_data or {"price": 1.0, "change_pct": 0.0}


class _AggSignal:
    def __init__(self, product_id, base, direction, unified_score=0.6, priority=0.2,
                 conviction=0.7, backtest_quality=0.8, trend_score=0.3,
                 top_strategies=("ema_cross",), details=None, price=1.0):
        self.product_id = product_id
        self.base = base
        self.direction = direction
        self.unified_score = unified_score
        self.priority = priority
        self.conviction = conviction
        self.backtest_quality = backtest_quality
        self.trend_score = trend_score
        self.top_strategies = list(top_strategies)
        self.details = details or {"buy_strategies": [], "sell_strategies": []}
        self.price = price


class _FakeMarket:
    def __init__(self, **kw):
        self.category = kw.get("category", "crypto")
        self.volume = kw.get("volume", 5000.0)
        self.market_id = kw.get("market_id", "m1")
        self.is_open = kw.get("is_open", True)
        self.probability_extremity = kw.get("probability_extremity", 0.5)
        self.liquidity_score = kw.get("liquidity_score", 0.8)
        self.mid_price = kw.get("mid_price", 0.7)
        self.spread = kw.get("spread", 0.01)
        self.question = kw.get("question", "Will BTC reach 100k?")
        self.platform = kw.get("platform", "kalshi")


@pytest.fixture
def of(opt):
    o = opt
    o.last_execution = {}
    o.position_ages = {}
    o._wash_sale_cooldown = {}
    o._knowledge_gap = None
    o._arb_scanner = None
    o.event_engine = None
    o._pm_client = None
    return o


# --------------------------------------------------------------- enhanced TLH
def test_enhanced_tlh_emits(of):
    of.state = make_state({"SOL": holding("SOL", 1000, "speculative", pnl=-12.0, price=100.0)})
    ops = of._detect_enhanced_tlh()
    assert len(ops) == 1
    assert ops[0].opp_type == P.OpportunityType.TLH
    assert "replace with" in ops[0].reason


def test_enhanced_tlh_wash_sale_skip(of):
    of._wash_sale_cooldown["XRP"] = time.time()
    of.state = make_state({"XRP": holding("XRP", 1000, "speculative", pnl=-12.0, price=0.5)})
    assert of._detect_enhanced_tlh() == []


def test_enhanced_tlh_no_state(of):
    of.state = None
    assert of._detect_enhanced_tlh() == []


def test_enhanced_tlh_cooldown(of):
    of.last_execution["tlh"] = time.time()
    of.state = make_state({"XRP": holding("XRP", 1000, "speculative", pnl=-12.0, price=0.5)})
    assert of._detect_enhanced_tlh() == []


# -------------------------------------------------------- param optimization
def test_param_opt_walk_forward_unavailable(of):
    with mock.patch.object(P, "_HAS_WALK_FORWARD", False):
        assert of._run_periodic_param_optimization() == {}


def test_param_opt_early_return_interval(of):
    with mock.patch.object(P, "_HAS_WALK_FORWARD", False):
        of._last_param_opt_ts = time.time()
        assert of._run_periodic_param_optimization() == {}


# --------------------------------------------------------------- stock screen
def test_stock_opportunities(of):
    adapter = mock.MagicMock()
    bars = [{"close": 100.0 + i, "volume": 1000.0} for i in range(120)]
    adapter.fetch_historical_data.return_value = bars
    adapter.yfinance.get_stock_info.return_value = {"market_capital": 1e12}
    with mock.patch.object(P, "UnifiedMarketDataAdapter", return_value=adapter):
        of.state = make_state({"BTC": holding("BTC", 1000, "safe", price=40000.0)})
        ops = of._detect_stock_opportunities()
    assert isinstance(ops, list)


def test_stock_opportunities_no_adapter(of):
    with mock.patch.object(P, "UnifiedMarketDataAdapter", None):
        assert of._detect_stock_opportunities() == []


# ------------------------------------------------------------ accumulator
def test_accumulator_signals(of):
    fake_acc = mock.MagicMock()
    fake_acc.return_value = fake_acc
    fake_acc.accumulate.return_value = [
        _AccSignal("XRP-USD", "BUY", final_confidence=0.6, strategy_name="NewsSentiment"),
    ]
    with mock.patch.object(P, "_HAS_ACCUMULATOR", True), \
         mock.patch.object(P, "UnifiedSignalAccumulator", fake_acc), \
         mock.patch("portfolio_optimizer.classify_asset", return_value="growth"):
        of.state = make_state({"XRP": holding("XRP", 5000, "growth", price=0.5, product_id="XRP-USD")},
                               total_value=100000.0)
        ops = of._detect_accumulator_signals()
    assert isinstance(ops, list)


def test_accumulator_unavailable(of):
    with mock.patch.object(P, "_HAS_ACCUMULATOR", False):
        assert of._detect_accumulator_signals() == []


def test_accumulator_cooldown(of):
    of.last_execution["accumulator"] = time.time()
    with mock.patch.object(P, "_HAS_ACCUMULATOR", True):
        assert of._detect_accumulator_signals() == []


# ----------------------------------------------------- funding / onchain
def test_funding_onchain_signals(of):
    funding = mock.MagicMock()
    funding.on_bar.return_value = SE.Signal(strategy="funding", action="BUY", confidence=0.6, reason="funding")
    onchain = mock.MagicMock()
    onchain.get_signals.return_value = [
        {"action": "BUY", "product_id": "XRP-USD", "currency": "XRP", "confidence": 0.6,
         "price": 0.5, "volume_anomaly": 2.0, "price_trend": 0.1, "reason": "flow"},
    ]
    with mock.patch.object(of, "_funding_contrarian", funding), \
         mock.patch.object(of, "_onchain_flow", onchain), \
         mock.patch.object(of, "_current_price_for_symbol", return_value=40000.0):
        of.state = make_state({
            "BTC": holding("BTC", 1000, "safe", price=40000.0, product_id="BTC-USD"),
            "XRP": holding("XRP", 5000, "growth", price=0.5, product_id="XRP-USD", volume_24h=2e6),
        }, total_value=100000.0)
        ops = of._detect_funding_and_onchain_signals()
    assert isinstance(ops, list)


def test_funding_onchain_no_state(of):
    of.state = None
    assert of._detect_funding_and_onchain_signals() == []


def test_funding_onchain_cooldown(of):
    of.last_execution["funding_onchain"] = time.time()
    of.state = make_state({"BTC": holding("BTC", 1000, "safe", price=40000.0)})
    assert of._detect_funding_and_onchain_signals() == []


# ------------------------------------------------------------ event markets
def test_event_markets_signals(of):
    pm = mock.MagicMock()
    mkt = _FakeMarket(category="crypto", volume=5000.0, mid_price=0.7, question="Will BTC reach 100k?")
    pm.search_all_categories.return_value = {"crypto": [mkt]}
    with mock.patch.object(of, "_pm_client", pm), \
         mock.patch.object(of, "_current_price_for_symbol", return_value=100.0):
        of.state = make_state({"BTC": holding("BTC", 1000, "safe", price=40000.0, product_id="BTC-USD")},
                               total_value=100000.0)
        ops = of._detect_event_markets()
    assert isinstance(ops, list)
    assert any(o.opp_type == P.OpportunityType.STRATEGY_SIGNAL for o in ops)


def test_event_markets_no_client(of):
    of._pm_client = None
    of.event_engine = None
    assert of._detect_event_markets() == []


class _FakeKG:
    def __init__(self, direction, mid_over_half, significant=True):
        self.direction = direction
        self.gap = 0.25
        self.gap_pct = 25.0
        self.evidence_score = 0.4
        self.evidence_count = 3
        self.sentiment_label = "bearish"
        self.confidence = 0.5
        self.sources_used = ["wikipedia", "news"]
        self.is_significant = significant
        self.mid_over_half = mid_over_half


class _FakeArb:
    def __init__(self):
        self.edge = 0.02
        self.edge_pct = 0.02
        self.confidence = 0.7
        self.category = "crypto"
        self.event_key = "evt1"
        self.platform_buy = "kalshi"
        self.platform_hedge = "polymarket"
        self.reason = "arb reason"
        self.leg_buy = SimpleNamespace(platform="kalshi", market_id="m1", question="q",
                                        outcome="yes", side="BUY", price=0.5)
        self.leg_hedge = SimpleNamespace(platform="polymarket", market_id="m2", question="q",
                                         outcome="yes", side="SELL", price=0.52)


def test_event_markets_full(of):
    def _kg(m):
        # overvalued when mid > 0.5, undervalued otherwise
        return _FakeKG("overvalued" if m.mid_price > 0.5 else "undervalued",
                       m.mid_price > 0.5)

    pm = mock.MagicMock()
    markets = [
        _FakeMarket(category="crypto", volume=5000.0, mid_price=0.7,
                    question="Will Bitcoin reach 100k?"),
        _FakeMarket(category="crypto", volume=5000.0, mid_price=0.3,
                    question="Will Ethereum merge soon?"),
        _FakeMarket(category="economics", volume=2000.0, mid_price=0.7,
                    question="Will the fed raise rates?"),
        _FakeMarket(category="sports", volume=2000.0, mid_price=0.7,
                    question="Who wins the super bowl?"),
        _FakeMarket(category="crypto", volume=5000.0, mid_price=0.7,
                    question="Will XRP outperform?"),
        _FakeMarket(category="crypto", volume=100.0, mid_price=0.7,  # below min vol
                    question="Will Litecoin rise?"),
        _FakeMarket(category="crypto", volume=5000.0, mid_price=0.7,
                    question="tiny extremity", probability_extremity=0.1),
        _FakeMarket(category="crypto", volume=5000.0, mid_price=0.7,
                    question="closed market", is_open=False),
    ]
    pm.search_all_categories.return_value = {"crypto": markets,
                                             "economics": [markets[2]],
                                             "sports": [markets[3]]}
    of._knowledge_gap = mock.MagicMock(analyze=_kg)
    of._arb_scanner = mock.MagicMock()
    of._arb_scanner.scan.return_value = [_FakeArb()]
    with mock.patch.object(of, "_pm_client", pm), \
         mock.patch.object(of, "_current_price_for_symbol", return_value=100.0), \
         mock.patch.object(of, "_usdc_reserve_amount", lambda: 0.0):
        of.state = make_state({"XRP": holding("XRP", 5000, "growth")},
                               total_value=100000.0, usdc=40000.0)
        of.capital_policy = {"max_deployable_usd": 1_000_000.0,
                             "targets": {"reserve": 0.1, "core": 0.1, "opportunity": 0.8}}
        ops = of._detect_event_markets()
    assert any(o.opp_type == P.OpportunityType.STRATEGY_SIGNAL for o in ops)
    assert any(o.opp_type == P.OpportunityType.EVENT_MARKET for o in ops)
    assert any(o.opp_type == P.OpportunityType.EVENT_ARBITRAGE for o in ops)


def test_event_markets_engine_path(of):
    eng = mock.MagicMock()
    eng.find_opportunities.return_value = [SimpleNamespace(
        opp_type="EVENT_MARKET", currency="?", side="NONE", size_usd=0,
        reason="r", priority=0.1, product_id="k:m", meta={})]
    of._pm_client = None
    of.event_engine = eng
    assert isinstance(of._detect_event_markets(), list)


# ------------------------------------------------------------ aggregator
def test_aggregator_signals(of):
    candles = [[i, 100.0, 100.0, 100.0, 100.0 + (i % 2) * 0.01, 100.0] for i in range(80)]
    agg = mock.MagicMock()
    agg.scan_universe.return_value = [
        _AggSignal("XRP-USD", "XRP", "BUY", unified_score=0.6, trend_score=0.3, price=0.5),
    ]
    with mock.patch.object(P, "_HAS_AGGREGATOR", True), \
         mock.patch("coinbase.src.pair_discovery.top_coinbase_pairs", return_value=[("XRP-USD", "XRP"), ("ETH-USD", "ETH"), ("SOL-USD", "SOL")]), \
         mock.patch.object(of, "_feed_mgr", mock.MagicMock(get_candles_batch=lambda pids, **k: {p: candles for p in pids})), \
         mock.patch.object(P, "SignalAggregator", return_value=agg):
        of.state = make_state({"XRP": holding("XRP", 5000, "growth", price=0.5, product_id="XRP-USD")},
                               total_value=100000.0)
        of._usdc_reserve_amount = lambda: 0.0
        of.capital_policy = {"max_deployable_usd": 1_000_000.0,
                             "targets": {"reserve": 0.1, "core": 0.1, "opportunity": 0.8}}
        ops = of._detect_aggregator_signals()
    assert isinstance(ops, list)
    assert ops and ops[0].side == "BUY"


def test_aggregator_unavailable(of):
    with mock.patch.object(P, "_HAS_AGGREGATOR", False):
        assert of._detect_aggregator_signals() == []


# --------------------------------------------------------- coinbase universe
def test_coinbase_universe_signals(of):
    closes = [float(100 + i) for i in range(60)]
    products = {
        "XRP-USD": {
            "trading_disabled": False, "volume_24h": 200_000_000.0,
            "close": closes[-1],
        }
    }
    candles = [{"close": c, "high": c + 1, "low": c - 1, "volume": 1000.0} for c in closes]
    with mock.patch.object(of, "_feed_mgr", mock.MagicMock(get_candles_batch=lambda pids, **k: {"XRP-USD": candles})), \
         mock.patch.object(of, "_first_seen_age_days", return_value=100), \
         mock.patch("trading_system.core.compute_backend.get_compute_backend", side_effect=RuntimeError("no gpu")):
        of.cli.get_products.return_value = products
        of.state = make_state({"XRP": holding("XRP", 5000, "growth", price=closes[-1], product_id="XRP-USD")},
                               total_value=100000.0)
        ops = of._detect_coinbase_universe_signals()
    assert isinstance(ops, list)
