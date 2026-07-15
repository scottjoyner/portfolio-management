"""Tests for tests/coverage/strategies/strat_helpers.py."""
import importlib
import sys

import pytest

HELPERS = "tests.coverage.strategies.strat_helpers"
ROOT = "/home/scott/git/portfolio-management"


@pytest.fixture
def mod():
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    return importlib.import_module(HELPERS)


def test_make_bar_trend(mod):
    b = mod.make_bar(100.0, trend=1, i=0)
    assert b["rsi"] == 72.0
    assert b["atr_trend"] == 1
    b2 = mod.make_bar(100.0, trend=-1, i=0)
    assert b2["rsi"] == 28.0
    b3 = mod.make_bar(100.0, trend=0, i=0)
    assert b3["rsi"] == 50.0


def test_price_series_kinds(mod):
    assert len(mod.price_series(10, "rising")) == 10
    assert mod.price_series(10, "rising")[1] > mod.price_series(10, "rising")[0]
    assert mod.price_series(10, "falling")[1] < mod.price_series(10, "falling")[0]
    assert len(mod.price_series(6, "flat")) == 6
    assert mod.price_series(6, "flat")[0] == mod.price_series(6, "flat")[1]
    # flathigh / flatlow
    fh = mod.price_series(10, "flathigh")
    assert fh[0] == fh[4]
    assert fh[5] > fh[4]
    fl = mod.price_series(10, "flatlow")
    assert fl[5] < fl[4]


def test_bars(mod):
    bl = mod.bars(12, "volatile", 100.0, volume=500.0)
    assert len(bl) == 12
    # bars() derives close from price_series (spread only affects open/high/low).
    assert bl[0]["close"] == round(mod.price_series(12, "volatile", 100.0)[0], 8)
    assert bl[0]["volume"] == 500.0


def test_roundtrip_series(mod):
    rs = mod.roundtrip_series(20)
    assert len(rs) == 20
    # first half rising, second half falling
    assert rs[1]["close"] > rs[0]["close"]
    assert rs[11]["close"] < rs[10]["close"]


def test_discover_strategy_classes(mod):
    import types

    fake = types.ModuleType("fakemod_x")
    M = "fakemod_x"

    def mk(name, **kw):
        return type(name, (object,), {"__module__": M, **kw})

    MyStrategy = mk("MyStrategy", on_bar=lambda self, bar: None)
    NotStrategy = mk("NotStrategy")
    WithSuffixBot = mk("WithSuffixBot", on_bar=lambda self, bar: None)
    BaseOnly = mk("BaseOnly")
    Private = mk("Private", on_bar=lambda self, bar: None)

    fake.MyStrategy = MyStrategy
    fake.NotStrategy = NotStrategy
    fake.WithSuffixBot = WithSuffixBot
    fake.BaseOnly = BaseOnly
    fake._Private = Private

    found = mod.discover_strategy_classes(fake)
    names = {c.__name__ for c in found}
    assert "MyStrategy" in names
    assert "WithSuffixBot" in names
    # "NotStrategy" ends in the recognized "Strategy" suffix, so it IS found.
    assert "NotStrategy" in names
    assert "BaseOnly" not in names
    assert "_Private" not in names


def test_discover_excludes_non_strategy_and_abstract(mod):
    import types

    fake = types.ModuleType("fakemod_y")
    M = "fakemod_y"

    def mk(name, **kw):
        return type(name, (object,), {"__module__": M, **kw})

    Signal = mk("Signal")
    AbsBase = mk("AbsBase", __abstractmethods__=frozenset({"on_bar"}))

    fake.Signal = Signal
    fake.AbsBase = AbsBase

    found = mod.discover_strategy_classes(fake)
    names = {c.__name__ for c in found}
    assert "Signal" not in names
    assert "AbsBase" not in names


def test_instantiate_variants(mod):
    class NoArg:
        def __init__(self):
            self.ok = True

    class CfgArg:
        def __init__(self, config=None):
            self.cfg = config

    assert mod._instantiate(NoArg) is not None
    assert mod._instantiate(CfgArg) is not None

    class NeedsCfg:
        def __init__(self, config):
            self.config = config

    inst = mod._instantiate(NeedsCfg)
    assert inst is not None
    # fails for truly un-instantiable
    class Bang:
        def __init__(self):
            raise RuntimeError("boom")
    assert mod._instantiate(Bang) is None


def test_feed_init_variants(mod):
    class A:
        def init(self, bars):
            assert isinstance(bars, list)

    class B:
        def init(self, payload):
            assert "bars" in payload

    class C:
        def init(self, single):
            assert isinstance(single, dict)

    class D:
        pass

    assert mod._feed_init(A(), [{"close": 1}]) is True
    assert mod._feed_init(B(), [{"close": 1}]) is True
    assert mod._feed_init(C(), [{"close": 1}]) is True
    assert mod._feed_init(D(), [{"close": 1}]) is False


def test_drive_class_full(mod):
    class DemoStrategy:
        def __init__(self):
            self.called = 0

        def init(self, bars):
            pass

        def on_bar(self, bar):
            self.called += 1
            return ("BUY", 0.5) if bar.get("rsi", 50) > 60 else None

        def finalize(self):
            return "done"

        def metadata(self):
            return {"name": "demo"}

    res = mod.drive_class(DemoStrategy)
    assert res["instantiated"] is True
    assert res["init_ok"] is True
    assert res["signals"] >= 0
    assert "instantiation_failed" not in res["errors"]


def test_drive_class_no_on_bar(mod):
    class NoBar:
        def __init__(self):
            pass

    res = mod.drive_class(NoBar)
    assert res["instantiated"] is True
    assert "no_on_bar" in res["errors"]


def test_drive_class_generate_signal(mod):
    class GenStrat:
        def __init__(self):
            pass

        def on_bar(self, bar):
            return None

        def generate_signal(self, ms):
            return ("SELL", 0.3)

        def sizing_hints(self, ms):
            return {}

        def explain_trade(self, obj):
            return "x"

    res = mod.drive_class(GenStrat, scenarios=("rising",))
    assert res["instantiated"] is True


def test_drive_class_init_error(mod):
    class Bad:
        def __init__(self):
            pass

        def init(self, bars):
            raise ValueError("nope")

        def on_bar(self, bar):
            return None

    res = mod.drive_class(Bad)
    assert res["init_ok"] is False


def test_drive_class_instantiation_fails(mod):
    class Nope:
        def __init__(self):
            raise RuntimeError()

    res = mod.drive_class(Nope)
    assert res["instantiated"] is False
    assert "instantiation_failed" in res["errors"]
