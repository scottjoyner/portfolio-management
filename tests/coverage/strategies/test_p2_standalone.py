"""Coverage tests for small subclass + standalone strategy modules."""
from __future__ import annotations

import math

from trading_system.strategies.base import OHLCVBar


def bars(n, start=100.0, step=1.0, lo=None, hi=None):
    out = []
    p = start
    for i in range(n):
        c = p + step * i
        out.append(OHLCVBar(timestamp=i, open=c, high=(hi or c * 1.01),
                            low=(lo or c * 0.99), close=c, volume=1000 + i))
    return out


# ---------------------------------------------------------------------------
# subclass modules (just need instantiation; methods inherited from base.simple)
# ---------------------------------------------------------------------------

def test_ensemble_regime_allocator():
    from trading_system.strategies.ensemble.regime_allocator import RegimeSwitchingEnsembleAllocator
    s = RegimeSwitchingEnsembleAllocator()
    assert s.strategy_id == "RegimeSwitchingEnsembleAllocator"
    assert s.metadata()["strategy_type"] == "ensemble"


def test_ensemble_rotation():
    from trading_system.strategies.ensemble.rotation import CrossSectionalRelativeStrengthStrategy
    s = CrossSectionalRelativeStrengthStrategy()
    assert s.strategy_id == "CrossSectionalRelativeStrengthStrategy"
    sig = s.generate_signal({"product_id": "BTC-USD", "score": 0.9,
                              "universe_rank": 1, "warmup_complete": True})
    assert sig is not None


def test_execution_algos_vwap_twap():
    from trading_system.strategies.execution_algos.vwap_twap import VwapTwapExecutionStrategy
    s = VwapTwapExecutionStrategy()
    assert s.strategy_id == "VwapTwapExecutionStrategy"
    sig = s.generate_signal({"product_id": "BTC-USD", "score": 0.9,
                              "arrival_price": 1, "participation": 1, "warmup_complete": True})
    assert sig is not None


def test_microstructure_orderbook_imbalance():
    from trading_system.strategies.microstructure.orderbook_imbalance import OrderBookImbalanceStrategy
    s = OrderBookImbalanceStrategy()
    assert s.strategy_id == "OrderBookImbalanceStrategy"
    sig = s.generate_signal({"product_id": "BTC-USD", "score": 0.9, "imbalance": 1,
                             "best_bid": 1, "best_ask": 1, "warmup_complete": True})
    assert sig is not None


def test_stat_arb_pairs():
    from trading_system.strategies.stat_arb.pairs import PairsTradingStrategy
    s = PairsTradingStrategy()
    assert s.strategy_id == "PairsTradingStrategy"
    sig = s.generate_signal({"product_id": "BTC-USD", "score": 0.9, "spread": 1,
                             "hedge_ratio": 1, "warmup_complete": True})
    assert sig is not None


# ---------------------------------------------------------------------------
# factory.py
# ---------------------------------------------------------------------------

def test_factory_dataclasses():
    from trading_system.strategies.factory import StrategyConfig, Signal, BacktestResult
    cfg = StrategyConfig(name="x")
    assert cfg.risk_limit_pct == 0.05
    sig = Signal(action="BUY", price=1.0)
    assert sig.confidence == 1.0
    br = BacktestResult(strategy_name="s", start_date="a", end_date="b",
                        total_return_pct=1, sharpe_ratio=1, max_drawdown_pct=1,
                        win_rate=1, profit_factor=1, num_trades=1, realized_pnl=1)
    assert br.strategy_name == "s"


def _reg_names():
    from trading_system.strategies.factory import _strategy_registry
    return set(_strategy_registry.keys())


def test_register_and_create():
    from trading_system.strategies.factory import (
        register_strategy, create_strategy_instance, StrategyBase, StrategyConfig,
    )

    class MyStrat(StrategyBase):
        def init(self, data):
            self._d = data

        def on_bar(self, bar):
            return None

    # register_strategy returns a wrapper; invoke it to populate the registry.
    wrapper = register_strategy(MyStrat)
    registered = wrapper(MyStrat)
    assert registered is MyStrat
    assert "MyStrat" in _reg_names()
    inst = create_strategy_instance(MyStrat)
    assert inst._registered_name == "MyStrat"
    inst2 = create_strategy_instance(MyStrat, StrategyConfig(name="z"))
    assert inst2.config.name == "z"


def test_strategy_base_defaults():
    from trading_system.strategies.factory import StrategyBase, StrategyConfig

    class _C(StrategyBase):
        def init(self, data):
            pass

        def on_bar(self, bar):
            return None

    s = _C(StrategyConfig())
    assert s.position is None
    assert s.signals == []
    assert s.on_bar({"close": 1}) is None
    s.on_order_fills([{}])
    assert s.finalize() == {}
    assert s.get_name() == "StrategyBase"


def test_factory_builtins():
    from trading_system.strategies.factory import (
        TrendBreakoutStrategy, ZScoreMeanReversionStrategy, EMA_CrossoverStrategy,
        AVAILABLE_STRATEGIES,
    )
    t = TrendBreakoutStrategy()
    t.init({})
    assert t.on_bar({}) is None
    z = ZScoreMeanReversionStrategy()
    z.init({})
    assert z.on_bar({}) is None
    e = EMA_CrossoverStrategy()
    e.init({})
    assert e.on_bar({}) is None
    assert AVAILABLE_STRATEGIES["trend_breakout"] is TrendBreakoutStrategy


# ---------------------------------------------------------------------------
# zscore_strategy.py
# ---------------------------------------------------------------------------

def test_zscore_strategy():
    from trading_system.strategies.zscore_strategy import ZScoreMeanReversionStrategy, Position

    z = ZScoreMeanReversionStrategy()
    try:
        z.setup(bars(5))
        assert False
    except ValueError:
        pass
    data = bars(80, start=100.0, step=0.5)
    z.setup(data)
    assert z.close_prices

    z2 = ZScoreMeanReversionStrategy()
    z2.close_prices = []
    assert z2.on_bar(OHLCVBar(timestamp=1, close=100.0)) == (None, None)
    assert z2.on_bar(OHLCVBar(timestamp=1, close=None)) == (None, None)

    z3 = ZScoreMeanReversionStrategy()
    z3.close_prices = [100.0] * 60 + [10.0]
    sig, price = z3.on_bar(OHLCVBar(timestamp=99, close=10.0))
    assert sig is True and price == 10.0

    z3.position = Position(entry_price=10.0)
    z3.close_prices = [100.0] * 60 + [1000.0]
    sig, price = z3.on_bar(OHLCVBar(timestamp=99, close=1000.0))
    assert sig is False

    # close None while a position exists (line 94 path)
    z3n = ZScoreMeanReversionStrategy()
    z3n.close_prices = [100.0] * 60
    z3n.position = Position(entry_price=10.0)
    assert z3n.on_bar(OHLCVBar(timestamp=1, close=None)) == (None, None)

    # trailing stop trigger: price rose (max pnl) then dropped below 90%
    z4 = ZScoreMeanReversionStrategy()
    z4.close_prices = [100.0] * 60
    z4.position = Position(entry_price=100.0, quantity=1)
    z4.max_unrealized_pnl_reached = 0.0
    # first level: price up to 120 -> max_unrealized = 20
    sig, _ = z4.on_bar(OHLCVBar(timestamp=99, close=120.0))
    assert sig is None
    # now drop: unrealized 0 < 20*0.9 -> sell signal (covers trailing branch)
    sig, px = z4.on_bar(OHLCVBar(timestamp=100, close=100.0))
    assert sig is False and px == 100.0

    # overbought z with NO position -> falls through to final return (line 110->122)
    z5 = ZScoreMeanReversionStrategy()
    z5.close_prices = [100.0] * 60 + [1000.0]
    z5.position = None
    assert z5.on_bar(OHLCVBar(timestamp=99, close=1000.0)) == (None, None)

    # _calculate_z_score with fewer than 10 prices returns 0.0 (line 128)
    z6 = ZScoreMeanReversionStrategy()
    z6.close_prices = [1.0, 2.0, 3.0]
    assert z6._calculate_z_score() == 0.0

    p = Position(entry_price=10.0, quantity=2)
    assert p.calculate_realized_pnl(15.0) == 10.0

    z4.close_prices = [5.0, 5.0, 5.0]
    assert z4._calculate_z_score() == 0.0
    z4.close_prices = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    assert abs(z4._calculate_z_score() - (10 - 5.5) / 3.027) < 0.1


def test_zscore_strategy_fallback_import():
    # Force the `except ImportError` fallback (lines 16-24) to execute by making
    # the canonical base import fail.
    import sys
    import importlib.util
    import os

    saved = sys.modules.get("trading_system.strategies.base")
    sys.modules["trading_system.strategies.base"] = None  # causes ImportError
    try:
        p = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..",
                         "trading_system", "strategies", "zscore_strategy.py"))
        spec = importlib.util.spec_from_file_location("zscore_fallback_mod", p)
        m = importlib.util.module_from_spec(spec)
        sys.modules["zscore_fallback_mod"] = m
        spec.loader.exec_module(m)
        z = m.ZScoreMeanReversionStrategy()
        z.close_prices = [100.0] * 60 + [10.0]
        sig, price = z.on_bar(m.OHLCVBar(timestamp=99, close=10.0))
        assert sig is True and price == 10.0
    finally:
        if saved is None:
            sys.modules.pop("trading_system.strategies.base", None)
        else:
            sys.modules["trading_system.strategies.base"] = saved


# ---------------------------------------------------------------------------
# emacrossor_strategy.py
# ---------------------------------------------------------------------------

def test_emacrossor():
    from trading_system.strategies.emacrossor_strategy import (
        EMACrossoverStrategy, compute_ema, Position, StrategyConfig,
    )
    assert compute_ema([], 5) == []
    assert compute_ema([1.0, 2.0], 5) == []
    ema = compute_ema([1.0, 2.0, 3.0, 4.0, 5.0], 2)
    assert len(ema) == 1 and ema[0] > 0

    pos = Position(entry_price=10.0, quantity=3)
    assert pos.mark_close(12.0) == 6.0
    assert pos.quantity == 0

    e = EMACrossoverStrategy()
    try:
        e.setup([1.0, 2.0])
        assert False
    except ValueError:
        pass
    e.setup(list(range(40)))
    assert e.ema_fast and e.ema_slow

    e2 = EMACrossoverStrategy()
    assert e2.on_bar(100.0) == (None, None)

    e3 = EMACrossoverStrategy()
    e3.ema_fast = [3.0]
    e3.ema_slow = [2.0]
    e3.last_crossed_above = False
    sig, px = e3.on_bar(100.0)
    assert sig is True and px == 100.0
    e3.ema_fast = [2.0]
    e3.ema_slow = [3.0]
    e3.last_crossed_above = True
    sig, px = e3.on_bar(100.0)
    assert sig is False and px == 100.0
    # no crossover (both False) -> line 113 path
    e3.ema_fast = [2.0]
    e3.ema_slow = [3.0]
    e3.last_crossed_above = False
    assert e3.on_bar(100.0) == (None, None)


# ---------------------------------------------------------------------------
# adaptive_stop_loss.py
# ---------------------------------------------------------------------------

def _dbar(n, start=100.0, step=0.1):
    out = []
    for i in range(n):
        c = start + step * i
        out.append({"timestamp": i, "open": c, "high": c * 1.01,
                    "low": c * 0.99, "close": c, "volume": 1000 + i})
    return out


def test_adaptive_stop_loss():
    from trading_system.strategies.adaptive_stop_loss import (
        AdaptiveStopLossSystem, AdaptiveStopLossConfig,
    )
    assert AdaptiveStopLossConfig is AdaptiveStopLossSystem.AdaptiveStopLossConfig
    sys_mod = __import__("trading_system.strategies.adaptive_stop_loss", fromlist=["x"])
    assert sys_mod.AdaptiveStopLossConfig is AdaptiveStopLossConfig

    a = AdaptiveStopLossSystem()
    try:
        a.init([])
        assert False
    except ValueError:
        pass
    try:
        a.init(_dbar(50))
        assert False
    except ValueError:
        pass

    data = _dbar(120, start=100.0, step=0.1)
    a.init(data)
    assert a.baseline_volatility > 0

    a.config.volatility_threshold = 0.0
    lvl, reason = a.get_adaptive_stop(100.0)
    assert reason == "high_volatility_wide_stop"
    a.config.volatility_threshold = 1e9
    a.config.trend_strength_threshold = 0.0
    lvl, reason = a.get_adaptive_stop(100.0)
    assert reason == "strong_trend_trailing_stop"
    a.config.trend_strength_threshold = 1e9
    lvl, reason = a.get_adaptive_stop(100.0)
    assert reason == "standard_atr_stop"

    a.recent_bars = [{"timestamp": 0, "close": 100.0}]
    assert a._calculate_trend_strength(100.0) == 0.0

    r1 = a.handle_exit(100.0, "high_volatility_wide_stop")
    assert r1["successful_exit"] is True
    r2 = a.handle_exit(50.0, "other_reason")
    assert r2["successful_exit"] is False

    assert AdaptiveStopLossSystem().get_performance_metrics()["total_exits"] == 0
    m = a.get_performance_metrics()
    assert m["total_exits"] == 2
