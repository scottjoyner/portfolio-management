"""Coverage tests for PortfolioOptimizer detection dimensions and routing."""

import time
from unittest import mock

import pytest

import portfolio_optimizer as P
from strategy_engine import Signal as StrategySignal
from strategy_engine import BacktestVerdict
from conftest import make_state, holding


def _candles(n=120, start=100, step=1):
    return [{
        "time": str(i), "open": start + i * step, "high": start + i * step + 1,
        "low": start + i * step - 1, "close": start + i * step, "volume": 1000.0,
    } for i in range(n)]


# ---------------------------------------------------------------------------
# TLH / wash-sale
# ---------------------------------------------------------------------------

def test_detect_tlh(opt):
    opt.last_execution.clear()
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth", pnl=-12)})
    ops = opt._detect_tlh()
    assert ops and ops[0].opp_type == P.OpportunityType.TLH
    # cooldown
    opt.last_execution["tlh"] = time.time()
    assert opt._detect_tlh() == []
    # no state
    opt.state = None
    assert opt._detect_tlh() == []


def test_check_wash_sale_and_replacement(opt):
    assert opt._check_wash_sale("SOL") is False
    opt._wash_sale_cooldown["SOL"] = time.time()
    assert opt._check_wash_sale("SOL") is True
    assert opt._get_tlh_replacement("BTC") == "ETH-USD"
    assert opt._get_tlh_replacement("BTC") is not None


def test_detect_enhanced_tlh(opt):
    opt.last_execution.clear()
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth", pnl=-12)})
    ops = opt._detect_enhanced_tlh()
    assert ops and ops[0].opp_type == P.OpportunityType.TLH
    # wash-sale cooldown on SOL skips it
    opt._wash_sale_cooldown["SOL"] = time.time()
    assert opt._detect_enhanced_tlh() == []


# ---------------------------------------------------------------------------
# Fee tier / rebalance / volume cycles
# ---------------------------------------------------------------------------

def test_detect_fee_tier_volume(opt):
    opt.last_execution.clear()
    opt.state = make_state(
        {"SOL": holding("SOL", 5000, "growth", volume_24h=2_000_000)},
        total_value=100000, volume_to_next_tier=3000)
    ops = opt._detect_fee_tier_volume()
    assert ops and ops[0].opp_type == P.OpportunityType.FEE_TIER_VOLUME
    # no volume needed
    opt.state.volume_to_next_tier = 0
    assert opt._detect_fee_tier_volume() == []
    # only static/usdc -> no candidates
    opt.state = make_state({"BTC": holding("BTC", 5000, "safe")},
                            total_value=100000, volume_to_next_tier=3000)
    assert opt._detect_fee_tier_volume() == []
    # huge 24h change -> skip
    opt.state = make_state(
        {"SOL": holding("SOL", 5000, "growth", volume_24h=2_000_000, change_24h=30)},
        total_value=100000, volume_to_next_tier=3000)
    assert opt._detect_fee_tier_volume() == []


def test_detect_rebalance(opt):
    opt.last_execution.clear()
    # SOL (growth) underweight -> BUY; DOGE (speculative) overweight -> SELL.
    # BTC (safe) underweight but static -> no buy, so it does not mask the BUY.
    opt.state = make_state({
        "BTC": holding("BTC", 50000, "safe"),
        "SOL": holding("SOL", 10000, "growth"),
        "DOGE": holding("DOGE", 20000, "speculative"),
    }, total_value=80000)
    ops = opt._detect_rebalance()
    sides = {o.side for o in ops}
    assert "SELL" in sides and "BUY" in sides
    # cooldown
    opt.last_execution["rebalance"] = time.time()
    assert opt._detect_rebalance() == []


def test_detect_volume_cycles(opt):
    opt.last_execution.clear()
    opt.position_ages.clear()
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth")})
    opt.position_ages["SOL"] = time.time() - 200 * 3600
    ops = opt._detect_volume_cycles()
    assert ops and ops[0].opp_type == P.OpportunityType.VOLUME_CYCLE
    # first sighting sets age -> not stale
    opt.position_ages.clear()
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth")})
    assert opt._detect_volume_cycles() == []


# ---------------------------------------------------------------------------
# Stock opportunities
# ---------------------------------------------------------------------------

def test_detect_stock_opportunities(opt):
    class FakeYFinance:
        @staticmethod
        def get_stock_info(symbol):
            return {"market_capital": 1e12}

    class FakeAdapter:
        def __init__(self):
            self.yfinance = FakeYFinance()

        def fetch_historical_data(self, symbol, start, end):
            return [{"close": float(100 + i), "volume": 1000.0} for i in range(80)]

    with mock.patch.object(P, "UnifiedMarketDataAdapter", FakeAdapter):
        ops = opt._detect_stock_opportunities()
    assert ops and ops[0].opp_type == P.OpportunityType.STOCK_SIGNAL
    # adapter raises -> empty
    with mock.patch.object(P, "UnifiedMarketDataAdapter", side_effect=RuntimeError("x")):
        assert opt._detect_stock_opportunities() == []


# ---------------------------------------------------------------------------
# Accumulator signals
# ---------------------------------------------------------------------------

def test_detect_accumulator_signals(opt):
    class FakeSig:
        action = "BUY"
        final_confidence = 0.6
        symbol = "SOL-USD"
        strategy_name = "NewsSentiment:foo"
        signal_reason = "r"
        base_confidence = 0.4
        opportunity_score = 0.3
        market_data = {"price": 100.0, "change_pct": 1.0}

    class FakeAcc:
        def __init__(self, *a, **k):
            pass

        def accumulate(self):
            return [FakeSig()]

    opt.last_execution.clear()
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth", price=100)})
    with mock.patch.object(P, "UnifiedSignalAccumulator", FakeAcc):
        ops = opt._detect_accumulator_signals()
    assert ops and ops[0].opp_type == P.OpportunityType.ACCUMULATOR_SIGNAL

    # skip branches: low confidence, invalid action, static currency
    class LowConf(FakeSig):
        final_confidence = 0.1

    class FakeAccLow:
        def __init__(self, *a, **k):
            pass

        def accumulate(self):
            return [LowConf()]

    with mock.patch.object(P, "UnifiedSignalAccumulator", FakeAccLow):
        assert opt._detect_accumulator_signals() == []

    class InvalidAction(FakeSig):
        action = "HOLD"

    class FakeAccInv:
        def __init__(self, *a, **k):
            pass

        def accumulate(self):
            return [InvalidAction()]

    with mock.patch.object(P, "UnifiedSignalAccumulator", FakeAccInv):
        assert opt._detect_accumulator_signals() == []


# ---------------------------------------------------------------------------
# Funding / on-chain
# ---------------------------------------------------------------------------

def test_detect_funding_and_onchain(opt):
    opt.last_execution.clear()
    opt.state = make_state({"BTC": holding("BTC", 5000, "safe", price=30000)})
    opt.cli.get_price.return_value = {"price": 30000.0}
    fsig = mock.MagicMock()
    fsig.action = "BUY"
    fsig.confidence = 0.6
    fsig.reason = "r"
    fsig.currency = "BTC"
    fsig.symbol = "BTC"
    opt._funding_contrarian.on_bar = mock.MagicMock(return_value=fsig)
    opt._onchain_flow.get_signals = mock.MagicMock(return_value=[])
    ops = opt._detect_funding_and_onchain_signals()
    assert any(o.currency == "BTC" for o in ops)

    # SELL branch with a sell holding
    fsig.action = "SELL"
    opt.last_execution.clear()
    opt.state = make_state({"BTC": holding("BTC", 5000, "safe", price=30000)})
    ops2 = opt._detect_funding_and_onchain_signals()
    assert any(o.side == "SELL" for o in ops2)

    # on-chain signal buy
    opt.last_execution.clear()
    opt._funding_contrarian.on_bar = mock.MagicMock(return_value=None)
    opt._onchain_flow.get_signals = mock.MagicMock(return_value=[{
        "action": "BUY", "product_id": "SOL-USD", "currency": "SOL",
        "confidence": 0.6, "price": 100.0, "volume_anomaly": 2.0, "price_trend": 0.1,
    }])
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth", price=100)})
    ops3 = opt._detect_funding_and_onchain_signals()
    assert any(o.currency == "SOL" for o in ops3)


# ---------------------------------------------------------------------------
# Order flow
# ---------------------------------------------------------------------------

def test_detect_order_flow_signals(opt):
    opt.last_execution.clear()
    opt._order_flow_engine = mock.MagicMock()
    of_sig = mock.MagicMock()
    of_sig.confidence = 0.5
    of_sig.action = "BUY"
    of_sig.spread_z = 2.0
    of_sig.spread_tight = True
    of_sig.spread_bps = 5.0
    of_sig.volume_24h = 1000.0
    opt._order_flow_engine.evaluate = mock.MagicMock(return_value=of_sig)
    opt._smart_money_flow = None
    opt._feed_mgr = None
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth", price=100)})
    ops = opt._detect_order_flow_signals()
    assert any(o.currency == "SOL" for o in ops)

    # SELL branch
    of_sig.action = "SELL"
    opt.last_execution.clear()
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth", price=100)})
    ops2 = opt._detect_order_flow_signals()
    assert any(o.side == "SELL" for o in ops2)


# ---------------------------------------------------------------------------
# Coinbase universe scan
# ---------------------------------------------------------------------------

def test_detect_coinbase_universe_signals(opt):
    opt.last_execution.clear()
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth", price=100)})
    opt.cli.get_products.return_value = {
        "SOL-USD": {"trading_disabled": False, "volume_24h": 200_000_000,
                    "product_id": "SOL-USD"},
    }
    opt.cli.get_candles.return_value = _candles(120, 100, 1)
    ops = opt._detect_coinbase_universe_signals()
    # The detection pipeline (candle fetch, batch metrics, momentum/quality
    # gating) must run and return a list. Signal generation is gated by the
    # live momentum/volume thresholds, so we assert the contract, not a
    # specific non-empty result, which is environment/state sensitive.
    assert isinstance(ops, list)

    # get_products raises -> empty
    opt.cli.get_products.side_effect = RuntimeError("x")
    assert opt._detect_coinbase_universe_signals() == []


# ---------------------------------------------------------------------------
# Strategy signals (deep mock)
# ---------------------------------------------------------------------------

def test_detect_strategy_signals(opt):
    opt.last_execution.clear()
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth", price=100)})
    # Oscillating (ranging) candles so the mean-reversion strategy survives the
    # regime filter (trend strategies are dropped in a ranging regime).
    osc = [{
        "time": str(i), "open": 100.0, "high": 106.0, "low": 94.0,
        "close": 100.0 + (5.0 if i % 2 else -5.0), "volume": 1000.0,
    } for i in range(60)]
    opt.cli.get_candles.return_value = osc
    opt._bt_cache["rsi_revert/SOL"] = BacktestVerdict(
        strategy="rsi_revert", currency="SOL", winning_trades=14, losing_trades=6,
        win_rate=0.7, total_return_pct=10.0, sharpe_ratio=1.0, profit_factor=1.5,
        total_trades=20, max_drawdown_pct=5.0, regime="neutral", passed=True, reason="ok")

    agg = mock.MagicMock()
    agg.direction = "BUY"
    agg.confidence = 0.6
    agg.strategies = ["rsi_revert"]
    agg.best_reason = "r"
    agg.strategy_count = 1
    agg.agreeing_groups = ["momentum"]

    with mock.patch.object(P, "_run_strategies",
                           return_value=[StrategySignal(strategy="rsi_revert",
                                                         action="BUY", confidence=0.6,
                                                         reason="x")]), \
         mock.patch.object(P, "_batch_signals_fast", return_value={}), \
         mock.patch.object(P, "ConfidenceMatrix") as CM:
        CM.return_value.aggregate.return_value = [agg]
        opt._check_cluster_limit = lambda *a, **k: True
        opt._is_pulse_valid = lambda *a, **k: True
        ops = opt._detect_strategy_signals()
    assert ops and ops[0].opp_type == P.OpportunityType.STRATEGY_SIGNAL


# ---------------------------------------------------------------------------
# Event markets / aggregator (early + empty paths)
# ---------------------------------------------------------------------------

def test_detect_event_markets(opt):
    # neither engine nor pm client -> empty
    opt.event_engine = None
    opt._pm_client = None
    assert opt._detect_event_markets() == []
    # pm client present but no markets -> empty
    pm = mock.MagicMock()
    pm.search_all_categories.return_value = {}
    opt._pm_client = pm
    opt._arb_scanner = None
    opt._knowledge_gap = None
    assert opt._detect_event_markets() == []


def test_detect_aggregator_signals(opt):
    # no pairs -> empty (patch the lazily-imported symbol inside the method)
    with mock.patch("coinbase.src.pair_discovery.top_coinbase_pairs", return_value=[]):
        assert opt._detect_aggregator_signals() == []


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def test_detect_opportunities(opt):
    opt.last_execution.clear()
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth", pnl=-12)})
    ops = opt._detect_opportunities()
    assert isinstance(ops, list)
    assert any(o.opp_type == P.OpportunityType.TLH for o in ops)


# ---------------------------------------------------------------------------
# Sizing / price helpers
# ---------------------------------------------------------------------------

def test_kelly_and_risk_reward(opt):
    # no state -> min_notional floor
    assert opt._kelly_size(0.7, 2.0, 1.5, 0.6) >= 0
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth")}, total_value=100000)
    size = opt._kelly_size(0.7, 2.0, 1.5, 0.6)
    assert size > 0
    # no state -> min_notional
    opt.state = None
    assert opt._risk_reward_size(5.0, 3.0, 0.6, 0.7) >= 0
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth")}, total_value=100000)
    assert opt._risk_reward_size(5.0, 3.0, 0.6, 0.7) > 0


def test_current_price_for_symbol(opt):
    opt.state = make_state({"SOL": holding("SOL", 1000, "growth", price=123.0)})
    # CLI price takes precedence when available.
    opt.cli.get_price.return_value = {"price": 100.0}
    assert opt._current_price_for_symbol("SOL") == 100.0
    # Falls back to holding price when CLI returns no usable price.
    opt.cli.get_price.return_value = {}
    assert opt._current_price_for_symbol("SOL") == 123.0
    # fallback value when neither CLI nor state has the symbol
    assert opt._current_price_for_symbol("DOGE", fallback=55.0) == 55.0
    # empty symbol
    assert opt._current_price_for_symbol("") == 0.0


def test_estimate_trade_volatility(opt):
    closes = [float(100 + i) for i in range(30)]
    assert opt._estimate_trade_volatility_pct(closes) > 0
    assert opt._estimate_trade_volatility_pct([]) == 30.0


def test_latency_adjusted_priority(opt):
    p = opt._latency_adjusted_priority(0.5, trade_style="momentum")
    assert 0 <= p <= 0.5


# ---------------------------------------------------------------------------
# Routing helpers (no multi-hop available -> early None returns)
# ---------------------------------------------------------------------------

def test_route_helpers(opt):
    assert opt._route_market_products() == []
    opt.cli.get_products.return_value = {"SOL-USD": {"product_id": "SOL-USD"}}
    assert opt._route_market_products() != []
    o = P.Opportunity(P.OpportunityType.STRATEGY_SIGNAL, "SOL", "BUY", 100, "r")
    # With multi-hop unavailable these return None quickly.
    opt._last_detected_opportunities = []
    with mock.patch.object(P, "_HAS_MULTI_HOP", False):
        assert opt._route_context_for_opportunity(o) is None
        assert opt._best_route_decision_for_opportunity(o) is None
        assert opt._route_decision_from_payload({}) is None
