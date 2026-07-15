"""Extra coverage for large PortfolioOptimizer detection/persistence methods:
order-flow signals, coinbase universe scan, accumulator signals, enhanced
state persistence. External engines / data sources are faked.
"""
from __future__ import annotations

import math
from types import SimpleNamespace
from unittest import mock

import pytest

import portfolio_optimizer as P
from tests.coverage.optimizer.conftest import holding, make_state


# --------------------------------------------------------------- fake engines
class _OFEngine:
    """Stand-in for OrderFlowEngine (spread z-score) with a configurable action."""

    def __init__(self, action="BUY"):
        self._action = action

    def evaluate(self, pid, bid, ask, price, volume):
        return SimpleNamespace(
            action=self._action, confidence=0.5, spread_z=2.0,
            spread_tight=True, spread_bps=5.0, volume_24h=volume,
        )

    def get_signal(self, pid):
        return SimpleNamespace(
            action=self._action, confidence=0.5, spread_bps=5.0,
            spread_z=2.0, spread_tight=True,
        )


class _SmartMoney:
    """Stand-in for SmartMoneyFlowStrategy.on_bar -> setup."""

    def on_bar(self, bar, prev_bars):
        return SimpleNamespace(
            confidence=0.5, direction=SimpleNamespace(value="long"),
            reason="cvd bullish divergence", entry_price=100.0,
        )


def _candles(n=50, drift=1):
    # Linear drift with a mild sinusoidal pullback so RSI stays in a realistic
    # (non-saturated) band — strong trend, but not an exhaustion extreme.
    return [
        {"open": 100 + drift * i + 5.0 * math.sin(2 * math.pi * i / 5.0) - drift,
         "high": 100 + drift * i + 5.0 * math.sin(2 * math.pi * i / 5.0) + 1.0,
         "low": 100 + drift * i + 5.0 * math.sin(2 * math.pi * i / 5.0) - 1.0,
         "close": 100 + drift * i + 5.0 * math.sin(2 * math.pi * i / 5.0),
         "volume": 1000, "time": i}
        for i in range(n)
    ]


# ----------------------------------------------------------- order-flow signals
def test_order_flow_buy(opt):
    opt.last_execution = {}
    opt.state = make_state({"SOL": holding("SOL", 1000, "speculative")})
    opt._order_flow_engine = _OFEngine("BUY")
    opt._smart_money_flow = None
    ops = opt._detect_order_flow_signals()
    assert any(o.side == "BUY" for o in ops)


def test_order_flow_sell(opt):
    opt.last_execution = {}
    opt.state = make_state({"SOL": holding("SOL", 1000, "speculative")})
    opt._order_flow_engine = _OFEngine("SELL")
    opt._smart_money_flow = None
    ops = opt._detect_order_flow_signals()
    assert any(o.side == "SELL" for o in ops)


def test_order_flow_smart_money(opt):
    opt.last_execution = {}
    opt.state = make_state({"SOL": holding("SOL", 1000, "speculative")})
    opt._order_flow_engine = None
    fm = mock.MagicMock()
    fm.get_candles_batch.return_value = {"SOL-USD": _candles(50)}
    opt._feed_mgr = fm
    opt._smart_money_flow = _SmartMoney()
    # Bar() in coinbase.src.protocols does not accept instrument_type; the
    # method swallows that, so substitute a tolerant stand-in.
    with mock.patch("coinbase.src.protocols.Bar", SimpleNamespace):
        ops = opt._detect_order_flow_signals()
    assert any("smartflow" in o.reason for o in ops)


# -------------------------------------------------------- coinbase universe scan
def test_coinbase_universe_buy_and_sell(opt):
    opt.last_execution = {}
    opt.state = make_state({})
    # Old listing (listing_bonus=0) so a genuine downtrend yields SELL.
    opt._first_seen_age_days = lambda pid: 200.0
    opt.cli.get_products.return_value = {
        "SOL-USD": {"volume_24h": 5e8, "trading_disabled": False},
        "XRP-USD": {"volume_24h": 5e8, "trading_disabled": False},
    }
    # NOTE: the method reverses candles (newest-first), so a price that
    # *increases* over time yields a SELL signal and vice-versa.
    fm = mock.MagicMock()
    fm.get_candles_batch.return_value = {
        "SOL-USD": _candles(50, drift=-1),
        "XRP-USD": _candles(50, drift=1),
    }
    opt._feed_mgr = fm
    ops = opt._detect_coinbase_universe_signals()
    sides = {o.side for o in ops}
    assert "BUY" in sides
    assert "SELL" in sides


def test_coinbase_universe_new_listing(opt):
    opt.last_execution = {}
    # A new listing dumping (downtrend) -> SELL momentum; the asset must be
    # held to be sold, so seed a WIF position.
    opt.state = make_state({"WIF": holding("WIF", 1000, "speculative", price=50.0)})
    opt._first_seen_age_days = lambda pid: 5.0
    opt.cli.get_products.return_value = {
        "WIF-USD": {"volume_24h": 5e8, "trading_disabled": False},
    }
    fm = mock.MagicMock()
    fm.get_candles_batch.return_value = {"WIF-USD": _candles(50, drift=-1)}
    opt._feed_mgr = fm
    ops = opt._detect_coinbase_universe_signals()
    assert any(o.opp_type == P.OpportunityType.NEW_LISTING_MOMENTUM for o in ops)


def test_coinbase_universe_no_products(opt):
    opt.last_execution = {}
    opt.state = make_state({})
    opt.cli.get_products.side_effect = RuntimeError("boom")
    assert opt._detect_coinbase_universe_signals() == []


# --------------------------------------------------------- accumulator signals
def test_accumulator_sell_path(opt):
    opt.last_execution = {}
    opt.state = make_state({"SOL": holding("SOL", 1000, "speculative")})

    class _Sig:
        symbol = "SOL-USD"
        action = "SELL"
        final_confidence = 0.6
        base_confidence = 0.5
        opportunity_score = 0.4
        strategy_name = "NewsSentiment"
        signal_reason = "bearish"
        market_data = {"price": 100.0, "change_pct": -2.0}

    fake = mock.MagicMock()
    fake.accumulate.return_value = [_Sig()]
    with mock.patch.object(P, "UnifiedSignalAccumulator", return_value=fake):
        ops = opt._detect_accumulator_signals()
    assert any(o.side == "SELL" for o in ops)


def test_accumulator_buy_path(opt):
    opt.last_execution = {}
    opt.state = make_state({})

    class _Sig:
        symbol = "SOL-USD"
        action = "BUY"
        final_confidence = 0.6
        base_confidence = 0.5
        opportunity_score = 0.4
        strategy_name = "NewsMomentum"
        signal_reason = "bullish"
        market_data = {"price": 100.0, "change_pct": 2.0}

    fake = mock.MagicMock()
    fake.accumulate.return_value = [_Sig()]
    with mock.patch.object(P, "UnifiedSignalAccumulator", return_value=fake):
        ops = opt._detect_accumulator_signals()
    assert any(o.side == "BUY" for o in ops)


# ----------------------------------------------------- enhanced state persistence
def test_write_enhanced_state(opt):
    opt.last_execution = {}
    opt._tick_count = 20
    opt._meta_source_weights = {"a": 0.5}
    opt._cross_asset_regime = mock.MagicMock()
    opt._cross_asset_regime.get_state.return_value = SimpleNamespace(
        to_dict=lambda: {"regime": "bull"})
    opt._ensemble_blender = mock.MagicMock()
    opt._ensemble_blender.to_dict.return_value = {"x": 1}
    opt._ensemble_blender.top_strategies.return_value = ["s1"]
    opt._param_opt_results = {"k": "v"}
    opt._wash_sale_cooldown = {"BTC": 1.0}
    opt._order_flow_engine = _OFEngine("BUY")
    opt.state = make_state({"SOL": holding("SOL", 1000, "speculative")})
    fm = mock.MagicMock()
    fm.get_candles_batch.return_value = {"SOL-USD": _candles(50)}
    opt._feed_mgr = fm
    opt._detect_sr_levels_for_product = staticmethod(
        lambda c, h, l: ([SimpleNamespace(price=100, kind="support", strength=0.5)], 1.0))
    opt._write_enhanced_state()
    assert True


def test_write_enhanced_state_empty(opt):
    opt.last_execution = {}
    opt._tick_count = 0
    opt._meta_source_weights = {}
    opt._cross_asset_regime = None
    opt._ensemble_blender = None
    opt._param_opt_results = {}
    opt._wash_sale_cooldown = {}
    opt._order_flow_engine = None
    opt._feed_mgr = None
    opt.state = make_state({})
    opt._write_enhanced_state()
    assert True
