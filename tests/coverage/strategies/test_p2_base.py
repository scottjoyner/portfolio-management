"""Coverage tests for the legacy trading_system/strategies/base.py module loaded
by file location (it is shadowed by the ``base`` package), plus
base/interfaces.py and base/simple.py via normal imports.
"""
from __future__ import annotations

import importlib.util
import os
import sys

# --- load the shadowed base.py by file location under a unique name ----------
_BASE_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..",
                 "trading_system", "strategies", "base.py"))
_spec = importlib.util.spec_from_file_location("strat_base_file", _BASE_PATH)
BASE = importlib.util.module_from_spec(_spec)
sys.modules["strat_base_file"] = BASE
_spec.loader.exec_module(BASE)

from trading_system.strategies.base.interfaces import (
    Strategy,
    StrategySignal,
    StrategyMetadata,
    StrategyConfig,
)
from trading_system.strategies.base.simple import BaseSignalStrategy, SimpleSignalModel


# ---------------------------------------------------------------------------
# base.py  (loaded as strat_base_file)
# ---------------------------------------------------------------------------

def test_ohlcv_bar():
    b = BASE.OHLCVBar(timestamp=1, close=100.0)
    assert b.open is None
    b2 = BASE.OHLCVBar(timestamp=2, open=10, high=12, low=8, close=11)
    assert b2.high == 12
    try:
        BASE.OHLCVBar(timestamp=3, open=10, high=5, low=8, close=7)
        assert False, "expected AssertionError"
    except AssertionError:
        pass


def test_compute_sma():
    assert BASE.compute_sma([], 5) == []
    bars = [BASE.OHLCVBar(timestamp=i, close=float(i)) for i in range(3)]
    assert BASE.compute_sma(bars, 5) == []
    bars = [BASE.OHLCVBar(timestamp=i, close=float(i)) for i in range(10)]
    sma = BASE.compute_sma(bars, 3)
    assert len(sma) == 10
    assert sma[0] == 0.0
    assert sma[2] == 1.0
    # window where all closes are None -> append None
    nbars = [BASE.OHLCVBar(timestamp=i, close=None) for i in range(5)]
    nsma = BASE.compute_sma(nbars, 3)
    assert nsma[2] is None
    # None close handled via `bar.close or last_ema`
    assert BASE.compute_ema([1.0, 2.0, 3.0], 2)


def test_compute_ema():
    assert BASE.compute_ema([], 5) == []
    bars = [BASE.OHLCVBar(timestamp=i, close=float(i)) for i in range(3)]
    assert BASE.compute_ema(bars, 5) == []
    bars = [BASE.OHLCVBar(timestamp=i, close=float(10 + i)) for i in range(10)]
    ema = BASE.compute_ema(bars, 3)
    assert len(ema) == 8
    assert ema[-1] > 0


def test_compute_z_score():
    assert BASE.compute_z_score([]) == []
    assert BASE.compute_z_score([1.0]) == []
    data = [10.0, 10.0, 10.0, 10.0]
    assert BASE.compute_z_score(data) == [0.0, 0.0, 0.0, 0.0]
    z2 = BASE.compute_z_score([1.0, 2.0, 3.0, 4.0, 5.0])
    assert abs(z2[0] + 1.41) < 0.1


def test_base_strategy_defaults():
    s = BASE.BaseStrategy()
    s.setup([])
    sig, price = s.on_bar(BASE.OHLCVBar(timestamp=1, close=1.0))
    assert sig is None and price is None
    assert s.is_position_open() is False
    s._position_size = 5
    assert s.is_position_open() is True
    s.close_position()
    assert s.is_position_open() is False


# ---------------------------------------------------------------------------
# base/interfaces.py
# ---------------------------------------------------------------------------

class _Concrete(Strategy):
    def metadata(self):
        return {"live_supported": True, "paper_mode": False,
                "replay_supported": True, "backtest_supported": False}

    def generate_signal(self, market_state):
        return StrategySignal(strategy_id="x", product_id="BTC-USD", score=1.0)

    def explain_trade(self, signal):
        return "expl"


def test_strategy_signal_metadata_config():
    sig = StrategySignal(strategy_id="s", product_id="BTC-USD", score=0.5, reason="r")
    assert sig.confidence == 0.5
    assert sig.warmup_passed is True
    assert sig.tags == []
    assert sig.features == {}
    meta = StrategyMetadata(strategy_id="s", strategy_type="t")
    assert meta.status == "implemented"
    assert meta.products == ["BTC-USD"]
    cfg = StrategyConfig()
    assert cfg.threshold == 0.1
    try:
        StrategyConfig(max_abs_score=0.1)
        assert False
    except Exception as e:
        assert "unrealistically tight" in str(e)


def test_strategy_default_methods():
    c = _Concrete()
    assert c.sizing_hints({}) == {}
    assert c.order_intents(None, {}) == []
    assert c.risk_hints({}) == {}
    assert c.required_inputs() == {"product_id", "score"}
    assert c.supports_mode("live") is True
    assert c.supports_mode("paper") is False
    assert c.supports_mode("replay") is True
    assert c.supports_mode("backtest") is False
    assert c.supports_mode("unknown") is False
    assert c.in_cooldown() is False
    assert c.is_disabled({}) == (False, "enabled")
    assert c.approvals_required() is True
    assert c.on_paper_fill({}) is None
    assert c.replay_hooks() == {}
    assert c.analytics_tags() == {}
    assert c.serialize_state() == {}
    assert c.restore_state({}) is None


# ---------------------------------------------------------------------------
# base/simple.py
# ---------------------------------------------------------------------------

def _make_strat(**cfg_kwargs):
    meta = StrategyMetadata(strategy_id="s1", strategy_type="t",
                            live_supported=True, data_requirements=["product_id", "score"])
    cfg = StrategyConfig(**cfg_kwargs)
    return BaseSignalStrategy(metadata=meta, config=cfg)


def test_simple_metadata_required_inputs_supports():
    s = _make_strat(threshold=0.1)
    md = s.metadata()
    assert md["strategy_id"] == "s1"
    assert "config" in md
    assert s.required_inputs() == {"product_id", "score"}
    assert s.supports_mode("live") is True
    assert s.supports_mode("paper") is True
    assert s.supports_mode("replay") is True
    assert s.supports_mode("backtest") is True


def test_simple_is_disabled():
    s = _make_strat()
    assert s.is_disabled({"score": 1.0}) == (False, "enabled")
    s2 = _make_strat(enabled=False)
    assert s2.is_disabled({"score": 1.0}) == (True, "strategy disabled by config")
    s3 = _make_strat(max_abs_score=10.0)
    assert s3.is_disabled({"score": 50.0}) == (True, "input score breached safety ceiling")


def test_simple_generate_signal_paths():
    import time
    s = _make_strat(threshold=0.3)
    s.config.enabled = False
    assert s.generate_signal({"product_id": "BTC-USD", "score": 1.0}) is None
    s.config.enabled = True
    s.config.cooldown_seconds = 100
    s._last_emit_ts = time.monotonic()
    assert s.generate_signal({"product_id": "BTC-USD", "score": 1.0}) is None
    s.config.cooldown_seconds = 0
    assert s.generate_signal({"score": 1.0}) is None  # missing inputs
    assert s.generate_signal({"product_id": "BTC-USD", "score": 0.1}) is None  # below threshold
    sm = SimpleSignalModel(score_key="score")
    s.signal_model = sm
    sig = s.generate_signal({"product_id": "BTC-USD", "score": 0.9})
    assert sig is not None and sig.score == 0.9 and sig.confidence == 0.9
    s2 = _make_strat()
    sig2 = s2.generate_signal({"product_id": "X", "score": 0.9, "warmup_complete": False})
    assert sig2.warmup_passed is False
    assert s2.explain_trade(sig2)
    # direct in_cooldown branch coverage
    s3 = _make_strat(cooldown_seconds=10)
    assert s3.in_cooldown() is False  # last_emit_ts <= 0
    s3._last_emit_ts = 0.0
    assert s3.in_cooldown() is False  # explicit <= 0 branch
    s3._last_emit_ts = -5.0
    assert s3.in_cooldown() is False
    s3._last_emit_ts = 1e18
    assert s3.in_cooldown() is True  # within window
