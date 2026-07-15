import math

from trading_system.oracles.rsi_family import (
    OHLCVBar,
    RSIOracle,
    StochasticRSIOracle,
    WilliamsROracle,
    RSISignal,
)


def mk_bar(ts, close, high=None, low=None, open_=None, vol=100.0):
    return OHLCVBar(timestamp=ts, open=open_, high=high, low=low, close=close, volume=vol)


def test_ohlcvbar_fallback_and_signal():
    b = OHLCVBar(timestamp=1, close=10.0)
    assert b.close == 10.0
    sig = RSISignal(timestamp=1, signal_type="BUY", strength=0.5, rsi_value=30.0)
    assert sig.signal_type == "BUY"


# ----------------------------- RSIOracle -----------------------------

def test_rsi_oracle_none_close():
    oracle = RSIOracle({"lookback_periods": 5})
    assert oracle.on_bar(mk_bar(1, None)) is None


def test_rsi_oracle_buy_signal():
    oracle = RSIOracle({"lookback_periods": 5, "overbought_threshold": 70, "oversold_threshold": 30})
    oracle.rsi_values = [None, None, None, None, 25.0]
    oracle.previous_close = 100.0
    sig = oracle.on_bar(mk_bar(2, 100.0))
    assert sig is not None and sig.signal_type == "BUY"
    assert oracle.position_entry_price is not None


def test_rsi_oracle_sell_signal():
    oracle = RSIOracle({"lookback_periods": 5, "overbought_threshold": 70, "oversold_threshold": 30})
    oracle.rsi_values = [None, None, None, None, 80.0]
    oracle.previous_close = 100.0
    oracle.open_position(50.0)
    oracle.unrealized_pnl = 5.0
    sig = oracle.on_bar(mk_bar(2, 100.0))
    assert sig is not None and sig.signal_type == "SELL"
    assert oracle.position_entry_price is None


def test_rsi_oracle_warmup_append_none():
    oracle = RSIOracle({"lookback_periods": 5})
    oracle.rsi_values = [None, None]  # len 2 < lookback-1
    oracle.previous_close = 100.0
    assert oracle.on_bar(mk_bar(2, 100.0)) is None
    assert oracle.rsi_values[-1] is None


def test_rsi_oracle_compute_from_none():
    oracle = RSIOracle({"lookback_periods": 5})
    oracle.rsi_values = [None, None, None, None]  # last is None -> compute branch
    oracle.gains = [1.0, 2.0, 3.0, 4.0, 5.0]
    oracle.losses = [0.0, 0.0, 0.0, 0.0, 0.0]
    oracle.previous_close = 100.0
    r = oracle.on_bar(mk_bar(2, 100.0))
    assert r is None  # 100 not oversold, no position
    assert oracle.rsi_values[-1] == 100.0


def test_rsi_oracle_setup():
    oracle = RSIOracle({"lookback_periods": 5})
    bars = [mk_bar(i, c) for i, c in enumerate([100, 102, 98, 105, 95, 110, 90, 108, 92, 115])]
    oracle.setup(bars)
    assert len(oracle.rsi_values) == 10
    # none-close bars are skipped during setup
    oracle2 = RSIOracle({"lookback_periods": 5})
    oracle2.setup([mk_bar(1, None), mk_bar(2, None)])
    assert oracle2.gains == [] and oracle2.losses == []


def test_stochastic_rsi_setup_varying():
    oracle = StochasticRSIOracle({"rsi_lookback": 5, "stochastic_lookback": 3})
    bars = [mk_bar(i, c) for i, c in enumerate([100, 110, 90, 120, 80, 115, 85, 105, 95, 125, 75, 130])]
    oracle.setup(bars)
    assert len(oracle.stoch_values) == 12
    assert oracle.stoch_values[-1] != 50.0


def test_stochastic_rsi_setup_flat():
    oracle = StochasticRSIOracle({"rsi_lookback": 5, "stochastic_lookback": 3})
    bars = [mk_bar(i, 100.0) for i in range(12)]
    oracle.setup(bars)
    # all rsi equal -> adjusted min/max -> stoch = 100.0
    assert oracle.stoch_values[-1] == 100.0
    assert len(oracle.stoch_d_values) == 12


def test_williams_r_setup_varying():
    oracle = WilliamsROracle({"lookback_periods": 5})
    bars = [mk_bar(i, 75.0, high=100.0, low=50.0) for i in range(10)]
    oracle.setup(bars)
    assert len(oracle.williams_r_values) == 10


def test_williams_r_setup_flat():
    oracle = WilliamsROracle({"lookback_periods": 5})
    bars = [mk_bar(i, 100.0, high=100.0, low=100.0) for i in range(10)]
    oracle.setup(bars)
    # highest == lowest -> wr = 0.0 neutral
    assert oracle.williams_r_values[-1] == 0.0

    oracle = RSIOracle({"lookback_periods": 5})
    assert oracle.rsi_value is None
    oracle.rsi_values = [None, None, None, None, 42.0]
    assert oracle.rsi_value == 42.0


# -------------------------- StochasticRSIOracle --------------------------

def test_stochastic_rsi_none_close():
    oracle = StochasticRSIOracle({"rsi_lookback": 5, "stochastic_lookback": 3})
    assert oracle.on_bar(mk_bar(1, None)) is None


def test_stochastic_rsi_short_gains():
    oracle = StochasticRSIOracle({"rsi_lookback": 5, "stochastic_lookback": 3})
    oracle.gains = []
    assert oracle.on_bar(mk_bar(1, 100.0)) is None


def test_stochastic_rsi_buy_signal():
    oracle = StochasticRSIOracle({"rsi_lookback": 5, "stochastic_lookback": 3,
                                   "overbought_threshold": 80, "oversold_threshold": 20})
    oracle.stoch_values = [50.0, 50.0, 50.0, 50.0, 10.0]
    oracle.gains = [0.0, 0.0, 0.0, 0.0, 0.0]
    oracle.losses = [1.0, 1.0, 1.0, 1.0, 1.0]
    oracle.previous_close = 100.0
    sig = oracle.on_bar(mk_bar(2, 100.0))
    assert sig is not None and sig.signal_type == "BUY"


def test_stochastic_rsi_sell_signal():
    oracle = StochasticRSIOracle({"rsi_lookback": 5, "stochastic_lookback": 3,
                                   "overbought_threshold": 80, "oversold_threshold": 20})
    oracle.stoch_values = [50.0, 50.0, 50.0, 50.0, 90.0]
    oracle.gains = [1.0, 1.0, 1.0, 1.0, 1.0]
    oracle.losses = [0.0, 0.0, 0.0, 0.0, 0.0]
    oracle.previous_close = 100.0
    oracle.open_position(100.0)
    oracle.unrealized_pnl = 5.0
    sig = oracle.on_bar(mk_bar(2, 100.0))
    assert sig is not None and sig.signal_type == "SELL"


def test_stochastic_rsi_neutral_branch():
    oracle = StochasticRSIOracle({"rsi_lookback": 5, "stochastic_lookback": 3})
    # historical has a None -> stoch appended as 50.0 (neutral)
    oracle.stoch_values = [None]
    oracle.gains = [1.0, 1.0, 1.0, 1.0, 1.0]
    oracle.losses = [0.0, 0.0, 0.0, 0.0, 0.0]
    oracle.previous_close = 100.0
    r = oracle.on_bar(mk_bar(2, 100.0))
    assert r is None
    assert oracle.stoch_values[-1] == 50.0


# ---------------------------- WilliamsROracle ----------------------------

def test_williams_r_none_high_low():
    oracle = WilliamsROracle({"lookback_periods": 5})
    assert oracle.on_bar(mk_bar(1, 100.0, high=None, low=None)) is None


def test_williams_r_buy_signal():
    oracle = WilliamsROracle({"lookback_periods": 5, "overbought_threshold": -20, "oversold_threshold": -80})
    oracle.setup([mk_bar(i, 75.0, high=100.0, low=50.0) for i in range(5)])
    sig = oracle.on_bar(mk_bar(6, 55.0, high=100.0, low=50.0))
    assert sig is not None and sig.signal_type == "BUY"


def test_williams_r_sell_signal():
    oracle = WilliamsROracle({"lookback_periods": 5, "overbought_threshold": -20, "oversold_threshold": -80})
    oracle.setup([mk_bar(i, 75.0, high=100.0, low=50.0) for i in range(5)])
    oracle.open_position(55.0)
    oracle.unrealized_pnl = 5.0
    sig = oracle.on_bar(mk_bar(6, 95.0, high=100.0, low=50.0))
    assert sig is not None and sig.signal_type == "SELL"


def test_williams_r_warmup_append_none():
    oracle = WilliamsROracle({"lookback_periods": 5})
    oracle.high_values = [100.0, 100.0, 100.0]
    oracle.low_values = [50.0, 50.0, 50.0]
    oracle.williams_r_values = [None, None, None]  # len 3 < 4
    assert oracle.on_bar(mk_bar(4, 75.0, high=100.0, low=50.0)) is None
    assert oracle.williams_r_values[-1] is None


# ---- branch-only cases: no signal paths ----

def test_rsi_oracle_overbought_no_pnl():
    oracle = RSIOracle({"lookback_periods": 5, "overbought_threshold": 70, "oversold_threshold": 30})
    oracle.rsi_values = [None, None, None, None, 80.0]
    oracle.previous_close = 100.0
    oracle.open_position(50.0)
    oracle.unrealized_pnl = 0.0  # pnl not positive -> no exit
    assert oracle.on_bar(mk_bar(2, 100.0)) is None


def test_rsi_oracle_no_signal_mid():
    oracle = RSIOracle({"lookback_periods": 5, "overbought_threshold": 70, "oversold_threshold": 30})
    oracle.rsi_values = [None, None, None, None, 50.0]
    oracle.previous_close = 100.0
    assert oracle.on_bar(mk_bar(2, 100.0)) is None


def test_stochastic_rsi_overbought_no_pnl():
    oracle = StochasticRSIOracle({"rsi_lookback": 5, "stochastic_lookback": 3,
                                   "overbought_threshold": 80, "oversold_threshold": 20})
    oracle.stoch_values = [50.0, 50.0, 50.0, 50.0, 90.0]
    oracle.gains = [1.0, 1.0, 1.0, 1.0, 1.0]
    oracle.losses = [0.0, 0.0, 0.0, 0.0, 0.0]
    oracle.previous_close = 100.0
    oracle.open_position(100.0)
    oracle.unrealized_pnl = 0.0
    assert oracle.on_bar(mk_bar(2, 100.0)) is None


def test_stochastic_rsi_no_signal_mid():
    oracle = StochasticRSIOracle({"rsi_lookback": 5, "stochastic_lookback": 3,
                                   "overbought_threshold": 80, "oversold_threshold": 20})
    oracle.stoch_values = [50.0, 50.0, 50.0, 50.0, 50.0]
    oracle.gains = [1.0, 1.0, 1.0, 1.0, 1.0]
    oracle.losses = [0.0, 0.0, 0.0, 0.0, 0.0]
    oracle.previous_close = 100.0
    assert oracle.on_bar(mk_bar(2, 100.0)) is None


def test_williams_r_overbought_no_pnl():
    oracle = WilliamsROracle({"lookback_periods": 5, "overbought_threshold": -20, "oversold_threshold": -80})
    oracle.setup([mk_bar(i, 75.0, high=100.0, low=50.0) for i in range(5)])
    oracle.open_position(55.0)
    oracle.unrealized_pnl = 0.0
    assert oracle.on_bar(mk_bar(6, 95.0, high=100.0, low=50.0)) is None


def test_williams_r_no_signal_mid():
    oracle = WilliamsROracle({"lookback_periods": 5, "overbought_threshold": -20, "oversold_threshold": -80})
    oracle.setup([mk_bar(i, 75.0, high=100.0, low=50.0) for i in range(5)])
    assert oracle.on_bar(mk_bar(6, 75.0, high=100.0, low=50.0)) is None


def test_rsi_oracle_setup_all_gains():
    oracle = RSIOracle({"lookback_periods": 5})
    # monotonically increasing -> no losses -> avg_loss == 0 branch (line 111)
    oracle.setup([mk_bar(i, 100.0 + i * 5) for i in range(10)])
    assert oracle.rsi_values[-1] == 100.0


def test_rsi_oracle_compute_with_loss():
    oracle = RSIOracle({"lookback_periods": 5})
    oracle.rsi_values = [None, None, None, None]  # last None -> compute branch
    oracle.gains = [1.0, 2.0, 3.0, 4.0, 5.0]
    oracle.losses = [1.0, 1.0, 1.0, 1.0, 1.0]
    oracle.previous_close = 100.0
    r = oracle.on_bar(mk_bar(2, 100.0))
    assert r is None
    assert isinstance(oracle.rsi_values[-1], float)


def test_rsi_family_fallback_ohlcvbar_on_import():
    # Force the try/except import branch where trading_system.strategies.base
    # is unavailable, exercising the local OHLCVBar fallback definition.
    import sys
    import importlib

    real = sys.modules.get("trading_system.strategies.base")
    sys.modules["trading_system.strategies.base"] = None  # triggers ImportError on import
    try:
        spec = importlib.util.spec_from_file_location(
            "rsi_family_fallback",
            __import__("trading_system.oracles.rsi_family", fromlist=["x"]).__file__,
        )
        mod = importlib.util.module_from_spec(spec)
        sys.modules["rsi_family_fallback"] = mod
        spec.loader.exec_module(mod)
        bar = mod.OHLCVBar(timestamp=1, close=10.0)
        assert bar.close == 10.0
    finally:
        if real is None:
            sys.modules.pop("trading_system.strategies.base", None)
        else:
            sys.modules["trading_system.strategies.base"] = real
