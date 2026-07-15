import inspect
import math
import time
import urllib.request

import pytest

import strategy_engine as se
from strategy_engine import (
    Signal, run_strategies, backtest_strategy, batch_signals_fast,
    batch_backtest_rust, _classify_regime, _sma, _ema, _rsi, _bollinger,
    _zscore, _wma, _get_cache, _clear_cache, _cache_key,
)

STRATEGY_CLASSES = [
    se.EMA_Crossover, se.RSI_MeanReversion, se.BollingerBreakout, se.ZScoreReversion,
    se.VolumeMomentum, se.MACD, se.VWAP_Reversion, se.OBV_Divergence,
    se.ChandeMomentum, se.TRIX, se.ADX, se.KeltnerChannels, se.ChaikinMoneyFlow,
    se.WilliamsR, se.ParabolicSAR, se.HullMA, se.ForceIndex, se.VolumePriceTrend,
    se.DonchianChannels, se.Aroon, se.PriceEfficiencyRatio, se.SimplifiedCCI,
    se.RangeExpansionIndex, se.EMADeviation, se.SignalToNoiseRatio,
    se.FundingRateContrarian, se.ExchangeFlowSignal, se.BTCDXYCorrelation,
    se.KalshiSignal, se.PolymarketSignal,
]


def _make_data(n=80, kind="rise"):
    if kind == "rise":
        closes = [100.0 + i for i in range(n)]
        closes[-1] = closes[-2] + 200.0  # huge final spike up
    elif kind == "fall":
        closes = [200.0 - i for i in range(n)]
        closes[-1] = closes[-2] - 200.0  # huge final spike down
    else:  # oscillate (triggers trend crossovers)
        closes = [120.0 + 25.0 * math.sin(i * 0.5) for i in range(n)]
    closes = [max(c, 1.0) for c in closes]
    volumes = [100.0] * (n - 1) + [1000.0]
    highs = [c + 1.0 for c in closes]
    lows = [max(c - 1.0, 0.1) for c in closes]
    return closes, volumes, highs, lows


def _accepted_params(cls):
    sig = inspect.signature(cls.on_bar)
    return set(sig.parameters.keys()) - {"self", "close", "closes"}


def _seed_external(cls, inst, kind):
    if cls is se.FundingRateContrarian:
        inst._cache_ts = time.time()
        inst._cache = {"BTCUSDT": -0.001 if kind != "fall" else 0.001}
    elif cls is se.ExchangeFlowSignal:
        direction = 1 if kind != "fall" else -1
        prices = [[t, 100.0 + direction * t * 0.5] for t in range(60)]
        vols = [[t, 100.0] for t in range(59)] + [[59, 1000.0]]
        inst._cache = {"bitcoin": {"prices": prices, "total_volumes": vols}}
        inst._cache_ts = time.time()
    elif cls is se.BTCDXYCorrelation:
        n = 200
        btc = [100.0 + i for i in range(n)]
        dxy = [100.0 + i if kind != "fall" else 100.0 - i for i in range(n)]
        inst._cache = {"btc": btc, "dxy": dxy}
        inst._cache_ts = time.time()
    elif cls in (se.KalshiSignal, se.PolymarketSignal):
        inst._cache_ts = time.time()
        prob = 0.9 if kind != "fall" else 0.1
        inst._cache = [{
            "question": "Will X happen?", "prob": prob, "volume": 100000,
            "spread": 0.01, "liq": 1.0, "extremity": 0.5,
        }]


def _drive(cls):
    acc = _accepted_params(cls)
    total = 0
    errors = 0
    for kind in ("rise", "fall", "oscillate"):
        closes, volumes, highs, lows = _make_data(kind=kind)
        inst = cls()
        _seed_external(cls, inst, kind)
        for i in range(len(closes)):
            kwargs = {"close": closes[i], "closes": closes[: i + 1]}
            if "volumes" in acc:
                kwargs["volumes"] = volumes[: i + 1]
            if "highs" in acc:
                kwargs["highs"] = highs[: i + 1]
            if "lows" in acc:
                kwargs["lows"] = lows[: i + 1]
            if "currency" in acc:
                kwargs["currency"] = "BTC-USD"
            if "opens" in acc:
                kwargs["opens"] = closes[: i + 1]
            try:
                sig = inst.on_bar(**kwargs)
            except Exception as e:
                errors += 1
                continue
            if isinstance(sig, Signal):
                total += 1
    return total, errors


@pytest.mark.parametrize("cls", STRATEGY_CLASSES)
def test_strategy_class_executes(cls):
    total, errors = _drive(cls)
    assert errors == 0


# ---------------------------------------------------------------------------
# Individual helper functions
# ---------------------------------------------------------------------------

def test_sma_helper():
    assert _sma([1, 2, 3, 4], 2) == 3.5
    _sma([1, 2, 3, 4], 2)
    _clear_cache()


def test_ema_helper():
    v = _ema([1, 2, 3, 4, 5], 3)
    assert v > 0
    _ema([1, 2, 3, 4, 5], 3)


def test_rsi_helper():
    rsi_inc = _rsi([i for i in range(1, 16)], 14)
    rsi_dec = _rsi([16 - i for i in range(1, 16)], 14)
    assert 90 <= rsi_inc <= 100
    assert 0 <= rsi_dec <= 10


def test_bollinger_helper():
    mid, upper, lower, std = _bollinger(list(range(1, 22)), 20)
    assert upper >= mid >= lower


def test_zscore_helper():
    assert isinstance(_zscore(list(range(30)), 30), float)


def test_wma_helper():
    assert _wma([1, 2, 3, 4], 4) > 0


def test_cache_helpers():
    c = _get_cache()
    assert isinstance(c, dict)
    _cache_key("x", 5, 1)
    _clear_cache()


def test_classmethods():
    assert se.TRIX()._triple_ema([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], 5) > 0
    assert se.ADX()._wilder_smooth([1.0, 2.0, 3.0, 4.0, 5.0], 3) > 0
    plus_di, minus_di, adx = se.ADX()._calc_di_adx(
        [i + 2.0 for i in range(21)],
        [max(i * 0.5, 0.1) for i in range(21)],
        [i + 1 for i in range(21)],
        14,
    )
    assert adx >= 0
    assert se.HullMA._hma([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 5) > 0
    eff = se.PriceEfficiencyRatio._efficiency([1, 2, 3, 4, 5], [1, 1, 1, 1, 1])
    assert isinstance(eff, list)
    assert se.BTCDXYCorrelation._rolling_corr([1, 2, 3, 4, 5], [1, 2, 3, 4, 5], 3) == 1.0
    assert se.BTCDXYCorrelation._rolling_corr([1, 2, 3], [1, 2, 3], 5) == 0.0


def test_classify_regime():
    assert _classify_regime([1, 2]) == "UNKNOWN"
    trended = [100 + i for i in range(30)]
    assert _classify_regime(trended) == "TRENDED"
    flat = [100.0] * 30
    assert _classify_regime(flat) == "RANGING"
    volatile = [100.0 + (i % 3) * 5 for i in range(30)]
    assert _classify_regime(volatile) == "VOLATILE"


# ---------------------------------------------------------------------------
# run_strategies / backtest_strategy (force pure-Python path)
# ---------------------------------------------------------------------------

@pytest.fixture
def force_python(monkeypatch):
    monkeypatch.setattr(se, "_HAS_RUST", False)
    monkeypatch.setattr(urllib.request, "urlopen",
                        lambda *a, **k: (_ for _ in ()).throw(Exception("no net")))
    monkeypatch.setattr(se.KalshiSignal, "_make_client",
                        staticmethod(lambda: (_ for _ in ()).throw(Exception("no net"))))
    monkeypatch.setattr(se.PolymarketSignal, "_make_client",
                        staticmethod(lambda: (_ for _ in ()).throw(Exception("no net"))))
    return None


def _series(n=120, direction=1):
    closes = [100.0 + direction * i for i in range(n)]
    closes = [max(c, 1.0) for c in closes]
    volumes = [100.0] * (n - 1) + [1000.0]
    highs = [c + 1.0 for c in closes]
    lows = [max(c - 1.0, 0.1) for c in closes]
    return closes, volumes, highs, lows


def test_run_strategies_python(force_python):
    closes, volumes, highs, lows = _series()
    sigs = run_strategies("BTC-USD", "growth", closes, volumes, closes[-1], highs, lows)
    assert isinstance(sigs, list)


def test_run_strategies_default_class(force_python):
    closes, volumes, highs, lows = _series()
    sigs = run_strategies("ZZZ-USD", "unknown", closes, volumes, closes[-1], highs, lows)
    assert isinstance(sigs, list)


def test_run_strategies_rust_skipped(force_python):
    closes, volumes, highs, lows = _series()
    sigs = run_strategies("BTC-USD", "speculative", closes, volumes, closes[-1], highs, lows)
    assert isinstance(sigs, list)


def test_backtest_strategy_python(force_python):
    closes, volumes, highs, lows = _series()
    v = backtest_strategy("rsi_revert", "BTC-USD", closes, volumes, highs, lows)
    assert v.strategy == "rsi_revert"
    assert 0.0 <= v.win_rate <= 1.0


def test_backtest_strategy_insufficient(force_python):
    closes = [float(i) for i in range(10)]
    v = backtest_strategy("rsi_revert", "BTC-USD", closes, [1] * 10)
    assert v.passed is False
    assert "Insufficient" in v.reason


def test_backtest_strategy_unknown(force_python):
    closes, volumes, highs, lows = _series()
    v = backtest_strategy("not_a_real_strategy", "BTC-USD", closes, volumes, highs, lows)
    assert v.passed is False


def test_backtest_strategy_few_trades(force_python):
    closes = [100.0 + 0.01 * i for i in range(120)]
    v = backtest_strategy("ema_cross", "BTC-USD", closes, [1.0] * 120)
    assert v.total_trades < 3 or v.passed is False


# ---------------------------------------------------------------------------
# batch helpers
# ---------------------------------------------------------------------------

def test_batch_signals_fast_short():
    res = batch_signals_fast([("BTC-USD", "growth")], {"BTC-USD": [1, 2, 3]}, {}, {}, {})
    assert res == {"BTC-USD": {}}


def test_batch_signals_fast_empty():
    assert batch_signals_fast([], {}, {}, {}, {}) == {}


def test_batch_backtest_rust_empty():
    assert batch_backtest_rust([]) == {}


def test_batch_backtest_rust_non_rust():
    closes, volumes, highs, lows = _series()
    res = batch_backtest_rust([("made_up", "BTC-USD", closes, volumes, highs, lows)])
    assert res == {}


def test_run_strategies_rust_path():
    closes, volumes, highs, lows = _series()
    sigs = run_strategies("BTC-USD", "growth", closes, volumes, closes[-1], highs, lows)
    assert isinstance(sigs, list)


def test_run_strategies_no_hl_vol():
    # high_low/volume strategies without hl/vol -> rust skip branch (line 2236)
    closes = [100.0 + i for i in range(60)]
    sigs = run_strategies("BTC-USD", "growth", closes, None, closes[-1], None, None)
    assert isinstance(sigs, list)


def test_backtest_highlow_and_volume(force_python):
    # ADX is high_low -> covers 2383; VolumeMomentum is volume -> covers 2391
    closes, volumes, highs, lows = _series()
    v1 = backtest_strategy("adx", "BTC-USD", closes, volumes, highs, lows)
    assert v1.strategy == "adx"
    v2 = backtest_strategy("vol_mom", "BTC-USD", closes, volumes)
    assert v2.strategy == "vol_mom"
    # failing backtest -> reason branches (2513->2525) and override (2510)
    flat = [100.0 + 0.5 * (i % 2) for i in range(120)]
    v3 = backtest_strategy("rsi_revert", "BTC-USD", flat, [1.0] * 120)
    assert isinstance(v3.passed, bool)


def test_backtest_strategy_rust_path():
    closes, volumes, highs, lows = _series()
    v = backtest_strategy("rsi_revert", "BTC-USD", closes, volumes, highs, lows)
    assert v.strategy == "rsi_revert"


def test_backtest_strategy_python_trades(force_python):
    closes = []
    for _ in range(10):
        closes += [100.0 + 5.0 * i for i in range(20)]
        closes += [100.0 + 5.0 * (19 - i) for i in range(20)]
    volumes = [1.0] * len(closes)
    v = backtest_strategy("ema_cross", "BTC-USD", closes, volumes)
    assert v.total_trades >= 3


def test_batch_signals_fast_full():
    closes, volumes, highs, lows = _series(n=60)
    res = batch_signals_fast(
        [("BTC-USD", "growth")],
        {"BTC-USD": closes}, {"BTC-USD": volumes},
        {"BTC-USD": highs}, {"BTC-USD": lows},
    )
    assert "BTC-USD" in res


def test_batch_signals_fast_numpy(monkeypatch):
    monkeypatch.setattr(se, "_HAS_RUST", False)
    closes, volumes, highs, lows = _series(n=60)
    res = batch_signals_fast(
        [("BTC-USD", "growth")],
        {"BTC-USD": closes}, {"BTC-USD": volumes},
        {"BTC-USD": highs}, {"BTC-USD": lows},
    )
    assert "BTC-USD" in res


def test_batch_backtest_rust_real():
    closes, volumes, highs, lows = _series(n=60)
    res = batch_backtest_rust([("rsi_revert", "BTC-USD", closes, volumes, highs, lows)])
    assert "rsi_revert/BTC-USD" in res


def test_batch_backtest_rust_no_rust(monkeypatch):
    monkeypatch.setattr(se, "_HAS_RUST", False)
    closes, volumes, highs, lows = _series()
    assert batch_backtest_rust([("rsi_revert", "BTC-USD", closes, volumes, highs, lows)]) == {}


# ---------------------------------------------------------------------------
# External-data strategy edge branches (no network)
# ---------------------------------------------------------------------------

def test_funding_none_paths():
    f = se.FundingRateContrarian()
    f._cache_ts = time.time()
    f._cache = {}
    assert f.on_bar(1.0, [1, 2, 3], currency="BTC-USD") is None
    f2 = se.FundingRateContrarian()
    f2._cache_ts = time.time()
    f2._cache = {"BTCUSDT": 0.0000001}
    assert f2.on_bar(1.0, [1, 2, 3], currency="BTC-USD") is None


def test_exchange_edge_paths():
    e = se.ExchangeFlowSignal()
    assert e.on_bar(1.0, [1, 2, 3], currency=None) is None
    assert e.on_bar(1.0, [1, 2, 3], currency="ZZZ-USD") is None
    e2 = se.ExchangeFlowSignal()
    e2._cache_ts = time.time()
    e2._cache = {"bitcoin": {"prices": [[0, 1]], "total_volumes": [[0, 1]]}}
    assert e2.on_bar(1.0, [1, 2, 3], currency="BTC-USD") is None
    prices = [[t, 100.0 + t] for t in range(60)]
    vols = [[t, 100.0] for t in range(60)]
    e3 = se.ExchangeFlowSignal()
    e3._cache_ts = time.time()
    e3._cache = {"bitcoin": {"prices": prices, "total_volumes": vols}}
    assert e3.on_bar(1.0, [1, 2, 3], currency="BTC-USD") is None


def test_btcdxy_edge_paths():
    b = se.BTCDXYCorrelation()
    assert b.on_bar(1.0, [1, 2, 3], currency="ETH-USD") is None
    b2 = se.BTCDXYCorrelation()
    b2._cache_ts = time.time()
    b2._cache = {"btc": [1.0] * 10, "dxy": [1.0] * 10}
    assert b2.on_bar(1.0, [1, 2, 3], currency="BTC-USD") is None


def test_kalshi_polymarket_none(monkeypatch):
    for cls in (se.KalshiSignal, se.PolymarketSignal):
        monkeypatch.setattr(cls, "_make_client",
                            staticmethod(lambda: (_ for _ in ()).throw(Exception("no net"))))
        inst = cls()
        inst._cache_ts = 0  # force fetch attempt (will fail, swallowed)
        out = inst.on_bar(1.0, [1, 2, 3])
        assert out is None  # fetch failure swallowed
        inst._cache = []
        inst._cache_ts = time.time()
        assert inst.on_bar(1.0, [1, 2, 3]) is None
        inst._cache = [{"question": "q", "prob": 0.5, "volume": 1, "spread": 0.5, "liq": 0.01, "extremity": 0.01}]
        inst._cache_ts = time.time()
        assert inst.on_bar(1.0, [1, 2, 3]) is None


def test_signal_dataclass():
    s = Signal("BUY", 100.0, 0.5, "reason", "strat")
    assert s.action == "BUY"
    assert s.strategy == "strat"
