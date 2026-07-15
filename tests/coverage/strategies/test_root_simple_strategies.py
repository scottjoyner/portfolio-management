"""Coverage tests for shadowed root modules and the simple standalone
strategy modules.

``stat_arb.py`` and ``registry.py`` are shadowed by same-named packages, so
they are loaded directly by file location here.  Also covers zscore_strategy,
emacrossor_strategy, factory and base.py.
"""
from __future__ import annotations

import importlib.util
import os
import sys

import pytest

STRAT_DIR = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "..", "..",
    "trading_system", "strategies"))


def _load(modname, filename):
    path = os.path.join(STRAT_DIR, filename)
    spec = importlib.util.spec_from_file_location(modname, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[modname] = mod
    spec.loader.exec_module(mod)
    return mod


def dict_bars(n, kind="rising", start=100.0):
    out = []
    p = start
    for i in range(n):
        if kind == "rising":
            p *= 1.01
        elif kind == "falling":
            p *= 0.99
        elif kind == "spike":
            p = start * (3.0 if i == n - 1 else 1.0)
        out.append({"open": p, "high": p * 1.01, "low": p * 0.99,
                    "close": p, "volume": 1000 + i, "timestamp": i})
    return out


# ---------------------------------------------------------------------------
# stat_arb.py  (shadowed module file)
# ---------------------------------------------------------------------------

def test_stat_arb_module_full():
    mod = _load("_statarb_file", "stat_arb.py")
    S = mod.StatisticalArbitrageStrategy
    assert mod.StatisticalArbitrageConfig is S.StatisticalArbitrageConfig

    s = S()
    with pytest.raises(ValueError):
        s.init([])
    with pytest.raises(ValueError):
        s.init(dict_bars(10))

    s.init(dict_bars(200, "rising"))
    assert s.moving_average_history

    # invalid close
    assert s.on_bar({"close": 0}) is None

    # not enough MA history -> None
    s2 = S()
    s2.moving_average_history = [1.0]
    s2.std_deviation_history = [1.0]
    assert s2.on_bar({"close": 100}) is None

    # entry (extreme z) BUY: price far above MA
    ma = s.moving_average_history[-1]
    std = s.std_deviation_history[-1]
    buy = s.on_bar({"close": ma + 5 * std})
    assert buy["action"] == "BUY"
    sell = s.on_bar({"close": ma - 5 * std})
    assert sell["action"] == "SELL"

    # exit (small z)
    ex = s.on_bar({"close": ma})
    assert ex["action"] == "EXIT"

    # no signal (mid z between exit and entry thresholds)
    mid = s.on_bar({"close": ma + 1.7 * std})
    assert mid is None

    assert s.handle_signal({"action": "BUY", "z_score": 3,
                            "reason": "x"})["position_opened"]
    assert s.handle_signal({"action": "SELL", "z_score": -3,
                            "reason": "y"})["position_opened"]
    assert s.handle_signal({"action": "EXIT", "z_score": 0,
                            "reason": "z"})["position_closed"]
    assert s.handle_signal({"action": "HOLD"}) is None

    assert s.get_performance_metrics()["total_signals"] == 3
    assert S().get_performance_metrics()["total_signals"] == 0


# ---------------------------------------------------------------------------
# registry.py  (shadowed module file)
# ---------------------------------------------------------------------------

def test_registry_module_full(tmp_path):
    mod = _load("_registry_file", "registry.py")

    # StrategyError str formatting
    e = mod.StrategyError("boom", key="k")
    assert "[k]" in str(e)
    assert str(mod.StrategyError("boom")) == "boom"
    assert issubclass(mod.ValidationError, mod.StrategyError)
    assert issubclass(mod.LoadError, mod.StrategyError)
    assert issubclass(mod.ExecutionError, mod.StrategyError)

    # metadata versioning
    meta = mod.StrategyMetadata(key="k")
    meta2 = meta.with_version(2, author="me")
    assert meta2.version == 2 and meta2.author == "me"

    reg = mod.StrategyRegistry()

    class DummyStrat:
        def setup(self, ohlcv):
            self.n = len(ohlcv)

        def on_bar(self, bar):
            return True, bar.get("close", 0)

    # register with explicit key + auto-generated key
    reg.register(DummyStrat(), key="dummy")
    assert "dummy" in reg.strategies
    reg.register(DummyStrat())  # auto key via _generate_key

    # execute_strategy: not found
    with pytest.raises(mod.LoadError):
        reg.execute_strategy("missing", {})

    # execute_strategy: found (empty ohlcv -> 0 signals)
    res = reg.execute_strategy("dummy", {"p": 1})
    assert res["status"] == "success"

    # _calculate_performance branches
    assert reg._calculate_performance([], [])["total_trades"] == 0
    signals = [
        {"signal_type": "buy", "entry_price": 100},
        {"signal_type": "buy", "entry_price": 110},
        {"signal_type": "sell", "entry_price": 90},
    ]
    perf = reg._calculate_performance([{}, {}, {}], signals)
    assert perf["total_trades"] == 3

    # _generate_key variants
    assert "unknown" in reg._generate_key({})
    assert reg._generate_key({"name": "n", "module": "m"}).startswith("n")

    # load_from_yaml / load_from_json: missing files -> warning + return
    reg.load_from_yaml(str(tmp_path / "nope.yml"))
    reg.load_from_json(str(tmp_path / "nope.json"))

    # load_from_json with a real json file (Strategy undefined -> caught/logged)
    jf = tmp_path / "defs.json"
    jf.write_text('{"a": {"name": "a"}}')
    reg.load_from_json(str(jf))

    # load_from_yaml with a real yaml file
    yf = tmp_path / "defs.yml"
    yf.write_text("- name: a\n- name: b\n")
    reg.load_from_yaml(str(yf))

    # StrategyManager (session-backed) - use a dummy session
    class DummySession:
        pass

    mgr = mod.StrategyManager(DummySession())
    df = tmp_path / "m.yml"
    df.write_text("name: x\n")
    mgr.load_definition_from_yaml(str(df))
    jf2 = tmp_path / "m.json"
    jf2.write_text('{"name": "x"}')
    mgr.load_definition_from_json(str(jf2))
    mgr.register(DummyStrat(), key="mk", author="a")
    mgr.record_result("mk", {"fast": 9}, {"sharpe": 1.5})


# ---------------------------------------------------------------------------
# zscore_strategy
# ---------------------------------------------------------------------------

def test_zscore_strategy_full():
    from trading_system.strategies.zscore_strategy import (
        ZScoreMeanReversionStrategy, StrategyConfig, Position, OHLCVBar)

    pos = Position(entry_price=100.0, quantity=2)
    assert pos.calculate_realized_pnl(110.0) == 20.0

    s = ZScoreMeanReversionStrategy()
    assert isinstance(s.config, StrategyConfig)

    # setup validation
    with pytest.raises(ValueError):
        s.setup([])

    def bars(prices):
        return [OHLCVBar(timestamp=i, close=p, high=p, low=p, open=p, volume=1)
                for i, p in enumerate(prices)]

    # on_bar before setup -> None
    assert s.on_bar(OHLCVBar(timestamp=0, close=100)) == (None, None)

    # buy branch: last setup price far below mean
    prices = [100.0] * 59 + [50.0]
    s.setup(bars(prices))
    sig, price = s.on_bar(OHLCVBar(timestamp=1, close=50.0))
    assert sig is True and price == 50.0

    # None close -> None
    assert s.on_bar(OHLCVBar(timestamp=2, close=None)) == (None, None)

    # sell branch: high z + existing position
    s2 = ZScoreMeanReversionStrategy()
    prices2 = [100.0] * 59 + [200.0]
    s2.setup(bars(prices2))
    s2.position = Position(entry_price=150.0, quantity=1)
    sig, _ = s2.on_bar(OHLCVBar(timestamp=1, close=200.0))
    assert sig is False

    # trailing stop branch: mid z-score, position present
    s3 = ZScoreMeanReversionStrategy()
    s3.setup(bars([100.0] * 60))          # std==0 -> z=0
    s3.position = Position(entry_price=100.0, quantity=1)
    s3.max_unrealized_pnl_reached = 50.0
    sig, _ = s3.on_bar(OHLCVBar(timestamp=1, close=90.0))  # pnl -10 << max
    assert sig is False

    # mid-z with position but no trailing trigger -> None
    s4 = ZScoreMeanReversionStrategy()
    s4.setup(bars([100.0] * 60))
    s4.position = Position(entry_price=100.0, quantity=1)
    assert s4.on_bar(OHLCVBar(timestamp=1, close=100.0)) == (None, None)

    # _calculate_z_score with < 10 prices -> 0.0
    s5 = ZScoreMeanReversionStrategy()
    s5.close_prices = [100.0] * 5
    assert s5._calculate_z_score() == 0.0


def test_zscore_import_fallback(monkeypatch):
    """Force the ``except ImportError`` fallback OHLCVBar definition."""
    import types

    fake_base = types.ModuleType("trading_system.strategies.base")
    # Deliberately omit OHLCVBar so ``from ... import OHLCVBar`` fails.
    monkeypatch.setitem(sys.modules, "trading_system.strategies.base", fake_base)

    mod = _load("_zscore_fallback", "zscore_strategy.py")
    b = mod.OHLCVBar(timestamp=0, close=100.0)
    assert b.close == 100.0


# ---------------------------------------------------------------------------
# emacrossor_strategy
# ---------------------------------------------------------------------------

def test_emacrossor_full():
    from trading_system.strategies.emacrossor_strategy import (
        EMACrossoverStrategy, StrategyConfig, Position, compute_ema)

    assert compute_ema([1, 2], 5) == []          # too short
    assert len(compute_ema([float(i) for i in range(50)], 9)) == 1

    pos = Position(entry_price=100.0, quantity=3)
    assert pos.mark_close(110.0) == 30.0
    assert pos.quantity == 0

    s = EMACrossoverStrategy()
    assert isinstance(s.config, StrategyConfig)

    with pytest.raises(ValueError):
        s.setup([])
    with pytest.raises(ValueError):
        s.setup([1, 2, 3])

    # on_bar before setup -> None (empty ema lists)
    assert s.on_bar(100.0) == (None, None)

    # rising prices: fast ema > slow ema -> golden cross signal
    s.setup([float(100 + i) for i in range(60)])
    sig, price = s.on_bar(160.0)
    assert sig is True and price == 160.0
    # no change -> None
    assert s.on_bar(161.0) == (None, None)

    # falling prices: death cross (start already crossed above)
    s2 = EMACrossoverStrategy()
    s2.setup([float(200 - i) for i in range(60)])
    s2.last_crossed_above = True
    sig2, _ = s2.on_bar(140.0)
    assert sig2 is False


# ---------------------------------------------------------------------------
# factory
# ---------------------------------------------------------------------------

def test_factory_full():
    from trading_system.strategies import factory as f

    cfg = f.StrategyConfig(name="X")
    assert cfg.name == "X"
    sig = f.Signal(action="BUY", price=1.0)
    assert sig.action == "BUY"
    br = f.BacktestResult("s", "a", "b", 1, 2, 3, 4, 5, 6, 7)
    assert br.strategy_name == "s"

    # register_strategy: it is a decorator-factory whose returned wrapper
    # performs the actual registration keyed by the wrapped class name.
    class MyStrat(f.StrategyBase):
        def init(self, data):
            pass

        def on_bar(self, bar):
            return None

    wrapper = f.register_strategy(None)
    cls = wrapper(MyStrat)
    assert cls is MyStrat
    assert "MyStrat" in f._strategy_registry

    # create_strategy_instance with and without config
    inst = f.create_strategy_instance(MyStrat)
    assert inst._registered_name == "MyStrat"
    inst2 = f.create_strategy_instance(MyStrat, f.StrategyConfig(name="Z"))
    assert inst2.config.name == "Z"

    # StrategyBase helpers
    inst.on_order_fills([{"x": 1}])
    assert inst.finalize() == {}
    assert inst.get_name() == "MyStrat"

    # get_name fallback to config.name when no _registered_name
    class Bare(f.StrategyBase):
        def init(self, data):
            pass

        def on_bar(self, bar):
            return None
    b = Bare(f.StrategyConfig(name="cfgname"))
    del b._registered_name
    assert b.get_name() == "cfgname"

    # pre-registered example strategies
    for cls in f.AVAILABLE_STRATEGIES.values():
        e = cls()
        e.init({})
        assert e.on_bar({}) is None


# ---------------------------------------------------------------------------
# base.py  (exercised via the base package re-exports)
# ---------------------------------------------------------------------------

def test_base_building_blocks():
    from trading_system.strategies.base import (
        BaseStrategy, OHLCVBar, compute_sma, compute_ema, compute_z_score)

    # OHLCVBar post-init assertion (high >= low ok)
    OHLCVBar(timestamp=0, open=1.0, high=2.0, low=1.0, close=1.5, volume=1)
    with pytest.raises(AssertionError):
        OHLCVBar(timestamp=0, open=1.0, high=1.0, low=2.0)

    data = [OHLCVBar(timestamp=i, close=float(i), high=float(i), low=float(i),
                     open=float(i), volume=1.0) for i in range(30)]

    assert compute_sma([], 5) == []
    assert compute_sma(data, 100) == []
    sma = compute_sma(data, 5)
    assert len(sma) == len(data)

    # SMA with all-None window -> None entry
    none_data = [OHLCVBar(timestamp=i, close=None) for i in range(10)]
    sma_none = compute_sma(none_data, 3)
    assert sma_none[0] is None

    assert compute_ema([], 5) == []
    ema = compute_ema(data, 5)
    assert len(ema) >= 1

    assert compute_z_score([]) == []
    assert compute_z_score([1.0]) == []
    assert compute_z_score([5.0, 5.0, 5.0]) == [0.0, 0.0, 0.0]  # std == 0
    zs = compute_z_score([1.0, 2.0, 3.0])
    assert len(zs) == 3

    # BaseStrategy defaults
    bs = BaseStrategy()
    assert bs.setup([]) is None
    assert bs.on_bar(OHLCVBar(timestamp=0)) == (None, None)
    assert bs.is_position_open() is False
    bs._position_size = 5
    assert bs.is_position_open() is True
    bs.close_position()
    assert bs.is_position_open() is False
