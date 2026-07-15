"""Coverage tests for the volatility strategy package.

Covers:
- volatility.atr_breakout (on_bar was incomplete -> never returned a signal)
- volatility.atrbreakout (complete sibling implementation)
- volatility.vol_breakout / trend.breakout (BaseSignalStrategy wrappers)
- volatility.__init__ factory
"""
import math

import pytest

from trading_system.strategies.volatility.atr_breakout import (
    ATBBreakoutConfig as CfgA,
    ATBBreakoutStrategy as StratA,
)
from trading_system.strategies.volatility.atrbreakout import (
    ATBBreakoutConfig as CfgB,
    ATBBreakoutStrategy as StratB,
)


def _hist(n=30):
    return [
        {"open": 100, "high": 101 + i, "low": 99, "close": 100 + i, "volume": 1000}
        for i in range(n)
    ]


@pytest.mark.parametrize("Strat, Cfg", [(StratA, CfgA), (StratB, CfgB)])
def test_atr_breakout_lifecycle(Strat, Cfg):
    s = Strat()
    with pytest.raises(ValueError):
        s.init([])
    with pytest.raises(ValueError):
        s.init(_hist(3))
    s.init(_hist(30))
    assert s.atr_values

    # invalid price -> None
    assert s.on_bar({"close": 0}) is None
    assert s.on_bar({"close": math.nan}) is None

    # bullish breakout: close far above high -> BUY
    buy = s.on_bar({"close": 100000, "high": 1.0, "low": 1.0})
    assert buy["action"] == "BUY" and buy["reason"] == "atr_breakout_above_resistance"

    # bearish breakout: close far below low -> SELL
    sell = s.on_bar({"close": 0.01, "high": 100000, "low": 100000})
    assert sell["action"] == "SELL" and sell["reason"] == "atr_breakout_below_support"

    # no breakout (close within band) -> None
    assert s.on_bar({"close": 100, "high": 100, "low": 100}) is None


@pytest.mark.parametrize("Strat", [StratA, StratB])
def test_handle_signal_and_metrics(Strat):
    s = Strat()
    assert s.get_performance_metrics()["total_signals"] == 0
    assert s.handle_signal({"action": "BUY", "entry_price": 100})["position_opened"]
    assert s.handle_signal({"action": "SELL"})["position_closed"]
    m = s.get_performance_metrics()
    assert m["successful_trades"] == 1


def test_atr_breakout_no_high_low_defaults():
    # bar without high/low -> defaults to close, no breakout
    s = StratA()
    s.init(_hist(30))
    assert s.on_bar({"close": 100}) is None


def test_base_signal_wrappers():
    from trading_system.strategies.volatility.vol_breakout import VolatilityBreakoutStrategy
    from trading_system.strategies.trend.breakout import TrendFollowingBreakoutStrategy

    for cls in (VolatilityBreakoutStrategy, TrendFollowingBreakoutStrategy):
        s = cls()
        md = s.metadata()
        assert "strategy_id" in md
        # a state with all required inputs and a score above threshold -> signal
        req = s.required_inputs()
        state = {k: 1.0 for k in req}
        state["product_id"] = "BTC-USD"
        state["score"] = 0.9
        state["warmup_complete"] = True
        sig = s.generate_signal(state)
        assert sig is not None
        assert s.explain_trade(sig)


def test_volatility_factory():
    from trading_system.strategies.volatility import VolatilityStrategyFactory as f
    assert f.get_all() == []
    assert f.get_all("missing") is None
