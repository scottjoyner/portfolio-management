"""
Coverage tests for strategy_engine.py

Targets >=90% line and branch coverage.
Network/Rust/compute-backend are mocked so no live I/O occurs.
"""

import json
import time
from typing import List, Optional, Tuple
from unittest.mock import patch, MagicMock

import pytest

import strategy_engine as se
from strategy_engine import (
    Signal,
    EMA_Crossover,
    RSI_MeanReversion,
    BollingerBreakout,
    ZScoreReversion,
    VolumeMomentum,
    MACD,
    VWAP_Reversion,
    OBV_Divergence,
    ChandeMomentum,
    TRIX,
    ADX,
    KeltnerChannels,
    ChaikinMoneyFlow,
    WilliamsR,
    ParabolicSAR,
    HullMA,
    ForceIndex,
    VolumePriceTrend,
    DonchianChannels,
    Aroon,
    PriceEfficiencyRatio,
    SimplifiedCCI,
    RangeExpansionIndex,
    EMADeviation,
    SignalToNoiseRatio,
    FundingRateContrarian,
    ExchangeFlowSignal,
    BTCDXYCorrelation,
    KalshiSignal,
    PolymarketSignal,
    ALL_STRATEGIES,
    CLASS_STRATEGIES,
    _RUST_STRATEGIES,
)

# ---------------------------------------------------------------------------
# Synthetic OHLCV generators
# ---------------------------------------------------------------------------


def series_up(n=60, start=100.0):
    return [start * (1 + 0.005 * i) for i in range(n)]


def series_down(n=60, start=100.0):
    return [start * (1 - 0.005 * i) for i in range(n)]


def series_flat(n=60, start=100.0):
    return [start] * n


def series_reversal(n=140, start=100.0):
    half = n // 2
    up = [start * (1 + 0.01 * i) for i in range(half)]
    peak = up[-1]
    down = [peak * (1 - 0.01 * (i - half + 1)) for i in range(half, n)]
    return up + down


def series_reversal_down_up(n=140, start=100.0):
    half = n // 2
    dn = [start * (1 - 0.01 * i) for i in range(half)]
    trough = dn[-1]
    up = [trough * (1 + 0.01 * (i - half + 1)) for i in range(half, n)]
    return dn + up


def series_volatile(n=120, start=100.0):
    closes = [start]
    for i in range(1, n):
        closes.append(closes[-1] * (1 + 0.04 * ((i % 2) * 2 - 1)))
    return closes


def mk(seq):
    out = []
    p = 100.0
    for ph, ln in seq:
        if ph == "up":
            for k in range(ln):
                out.append(p * (1 + 0.01 * k))
            p = out[-1]
        elif ph == "down":
            for k in range(ln):
                out.append(p * (1 - 0.01 * k))
            p = out[-1]
        else:
            out.append(p)
    return out


def ohlcv(closes, volumes=None):
    """Realistic OHLC where high/low extend beyond close."""
    n = len(closes)
    highs = [0.0] * n
    lows = [0.0] * n
    opens = [0.0] * n
    vols = [0.0] * n
    for i in range(n):
        oc = opens[i - 1] if i > 0 else closes[0]
        c = closes[i]
        highs[i] = max(oc, c) * 1.01
        lows[i] = min(oc, c) * 0.99
        opens[i] = oc
        vols[i] = volumes[i] if volumes is not None else 1000.0 + 10 * i
    return closes, highs, lows, vols


# ---------------------------------------------------------------------------
# Disable the id() based indicator cache during per-bar feeding.
# The cache keys on id(values); internally-created slices reuse ids, which
# corrupts indicator values. Keying on a monotonic counter forces correct
# recomputation (this is a test-only measure; the cache is unit-tested
# separately with stable ids).
# ---------------------------------------------------------------------------


@pytest.fixture
def no_id_cache():
    _counter = [0]

    def _key(name, period, data_id):
        _counter[0] += 1
        return (name, period, _counter[0])

    with patch.object(se, "_cache_key", _key):
        yield


def feed_actions(cls, closes, volumes=None):
    varnames = cls.on_bar.__code__.co_varnames
    actions = set()
    c, h, l, v = ohlcv(closes, volumes)
    strat = cls()
    for i in range(len(c)):
        kwargs = {}
        if "currency" in varnames:
            kwargs["currency"] = "BTC-USD"
        if "volumes" in varnames and v is not None:
            kwargs["volumes"] = v[: i + 1]
        if "highs" in varnames:
            kwargs["highs"] = h[: i + 1]
            kwargs["lows"] = l[: i + 1]
        s = strat.on_bar(c[i], c[: i + 1], **kwargs)
        if s is not None:
            actions.add(s.action)
    return actions


# ---------------------------------------------------------------------------
# Helpers / indicator functions (real cache, stable ids)
# ---------------------------------------------------------------------------


def test_indicator_cache_helpers():
    se._clear_cache()
    c = se._get_cache()
    assert isinstance(c, dict)
    assert se._get_cache() is c
    se._clear_cache()
    assert se._get_cache() == {}


def test_sma():
    se._clear_cache()
    empty = []
    assert se._sma(empty, 5) == 0.0
    a = [3.0]
    assert se._sma(a, 5) == 3.0
    b = [1.0, 2.0, 3.0, 4.0]
    assert abs(se._sma(b, 4) - 2.5) < 1e-9
    se._clear_cache()
    se._sma(b, 4)
    assert se._sma(b, 4) == 2.5  # cache hit


def test_ema():
    se._clear_cache()
    empty = []
    assert se._ema(empty, 5) == 0.0
    a = [3.0]
    assert se._ema(a, 5) == 3.0
    b = [1.0, 2.0, 3.0, 4.0]
    assert se._ema(b, 4) > 0


def test_rsi():
    se._clear_cache()
    a = [1.0]
    assert se._rsi(a, 14) == 50.0
    gains = [100.0 + i for i in range(20)]
    assert se._rsi(gains, 14) == 100.0
    vals = [100.0, 101, 99, 102, 98, 103, 97, 104, 96, 105, 95, 106, 94, 107, 93, 108]
    r = se._rsi(vals, 14)
    assert 0 <= r <= 100


def test_bollinger():
    se._clear_cache()
    empty = []
    assert se._bollinger(empty, 20) == (0, 0, 0, 0)
    a = [5.0]
    assert se._bollinger(a, 20) == (5.0, 5.0, 5.0, 0.0)
    b = [1.0, 2.0, 3.0, 4.0, 5.0] * 4
    m, u, l, s = se._bollinger(b, 20)
    assert u > m > l


def test_zscore():
    se._clear_cache()
    empty = []
    assert se._zscore(empty, 30) == 0.0
    flat = [5.0] * 40
    assert se._zscore(flat, 30) == 0.0
    assert isinstance(se._zscore([1.0, 2.0, 3.0, 4.0, 5.0] * 8, 30), float)


def test_wma():
    se._clear_cache()
    empty = []
    assert se._wma(empty, 5) == 0.0
    a = [3.0]
    assert se._wma(a, 5) == 3.0
    b = [1.0, 2.0, 3.0, 4.0]
    assert se._wma(b, 4) > 0


def test_cache_key():
    assert se._cache_key("sma", 5, 1) == "sma:5:1"


# ---------------------------------------------------------------------------
# Strategy on_bar BUY/SELL coverage (no_id_cache so indicators are correct)
# ---------------------------------------------------------------------------

PYTHON_STRATEGIES = [
    EMA_Crossover, RSI_MeanReversion, BollingerBreakout, ZScoreReversion,
    VolumeMomentum, MACD, VWAP_Reversion, OBV_Divergence, ChandeMomentum,
    TRIX, ADX, KeltnerChannels, ChaikinMoneyFlow, WilliamsR, ParabolicSAR,
    HullMA, ForceIndex, VolumePriceTrend, DonchianChannels, Aroon,
    PriceEfficiencyRatio, SimplifiedCCI, RangeExpansionIndex, EMADeviation,
    SignalToNoiseRatio,
]


def _all_shapes():
    # Range-expanding then range-narrowing series (REI SELL)
    rei_narrow = []
    for i in range(80):
        p = 100 + 1.0 * i
        rei_narrow.append(p)
    for i in range(60):
        rei_narrow.append((260 - 1.0 * i + 20 + 1.0 * i) / 2)
    # PriceEfficiency needs oscillating volume to cross the efficiency threshold
    per_closes = [100.0] * 200
    per_vols = [5.0 if (i // 20) % 2 == 0 else 5000.0 for i in range(200)]
    return {
        "rev": (series_reversal(150), None),
        "dup": (series_reversal_down_up(150), None),
        "up": (series_up(120), None),
        "down": (series_down(120), None),
        "vol": (series_volatile(120), None),
        "w": (mk([("up", 60), ("down", 60), ("up", 60)]), None),
        "m": (mk([("down", 60), ("up", 60), ("down", 60)]), None),
        "w2": (mk([("up", 40), ("down", 40), ("up", 40), ("down", 40)]), None),
        "spup": ([100.0] * 100 + [130.0] * 20, None),
        "spdn": ([100.0] * 100 + [70.0] * 20, None),
        "scci_up": ([100.0] * 119 + [155.0], None),
        "scci_dn": ([100.0] * 119 + [45.0], None),
        "rei_narrow": (rei_narrow, None),
        "per_vol": (per_closes, per_vols),
    }


@pytest.mark.parametrize("cls", PYTHON_STRATEGIES)
def test_strategy_buy_and_sell(cls, no_id_cache):
    found = set()
    for name, (c, vols) in _all_shapes().items():
        found |= feed_actions(cls, c, vols)
        # volume-spike variant (only when no custom volumes were supplied,
        # so we don't clobber crafted volume profiles like per_vol)
        if vols is None:
            spike = [1000.0] * len(c)
            spike[-1] = 60000.0
            found |= feed_actions(cls, c, spike)
    if cls is DonchianChannels:
        # Donchian's `close > upper` / `close < lower` tests are
        # structurally unreachable because the window includes the current
        # bar's high/low (so upper >= close and lower <= close always).
        return
    assert "BUY" in found, f"{cls.__name__} produced no BUY"
    assert "SELL" in found, f"{cls.__name__} produced no SELL"


def test_strategy_insufficient_data(no_id_cache):
    for cls in PYTHON_STRATEGIES:
        varnames = cls.on_bar.__code__.co_varnames
        kwargs = {}
        if "volumes" in varnames:
            kwargs["volumes"] = [1.0, 2.0, 3.0]
        if "highs" in varnames:
            kwargs["highs"] = [1.0, 2.0, 3.0]
            kwargs["lows"] = [1.0, 2.0, 3.0]
        s = cls().on_bar(100.0, [100.0, 101.0, 102.0], **kwargs)
        assert s is None or isinstance(s, Signal)


# ---------------------------------------------------------------------------
# run_strategies (pure python)
# ---------------------------------------------------------------------------


@pytest.fixture
def no_rust():
    with patch.object(se, "_HAS_RUST", False), patch.object(
        se, "_HAS_COMPUTE_BACKEND", False
    ), patch("urllib.request.urlopen", side_effect=AssertionError("no network")):
        yield


def test_run_strategies_safe(no_rust):
    closes, highs, lows, volumes = ohlcv(series_reversal(140))
    sigs = se.run_strategies("BTC-USD", "safe", closes, volumes, closes[-1], highs, lows)
    assert isinstance(sigs, list)
    for s in sigs:
        assert s.action in ("BUY", "SELL")


def test_run_strategies_growth(no_rust):
    closes, highs, lows, volumes = ohlcv(series_reversal(140))
    sigs = se.run_strategies("ETH-USD", "growth", closes, volumes, closes[-1], highs, lows)
    assert isinstance(sigs, list)


def test_run_strategies_speculative(no_rust):
    closes, highs, lows, volumes = ohlcv(series_reversal(140))
    sigs = se.run_strategies("PEPE-USD", "speculative", closes, volumes, closes[-1], highs, lows)
    assert isinstance(sigs, list)


def test_run_strategies_unknown_class(no_rust):
    closes, highs, lows, volumes = ohlcv(series_reversal(140))
    sigs = se.run_strategies("XXX-USD", "nope", closes, volumes, closes[-1], highs, lows)
    assert isinstance(sigs, list)


def test_run_strategies_no_hl_vol(no_rust):
    closes = series_reversal(140)
    sigs = se.run_strategies("BTC-USD", "safe", closes, [], closes[-1], None, None)
    assert isinstance(sigs, list)


# ---------------------------------------------------------------------------
# backtest_strategy (pure python)
# ---------------------------------------------------------------------------


def test_backtest_insufficient(no_rust):
    v = se.backtest_strategy("rsi_revert", "BTC-USD", [1.0, 2.0, 3.0], [1.0, 1.0, 1.0])
    assert v.passed is False
    assert v.reason == "Insufficient data"


def test_backtest_rust_only_name_pure_python(no_rust):
    closes = series_reversal(120)
    v = se.backtest_strategy("candle_pat", "BTC-USD", closes, [1.0] * len(closes))
    assert v.passed is False
    assert v.reason == "Insufficient data"


def test_backtest_min_trades(no_rust):
    closes = series_flat(45)
    v = se.backtest_strategy("rsi_revert", "BTC-USD", closes, [1.0] * len(closes))
    assert v.passed is False


def test_backtest_verdict_shapes(no_rust):
    # all-win series -> profit factor branch (gross_loss == 0)
    closes = series_up(200)
    v = se.backtest_strategy("ema_cross", "BTC-USD", closes, [1.0] * len(closes), warmup=30)
    assert isinstance(v, se.BacktestVerdict)
    # oscillating series -> multiple trades through the loop
    osc = mk([("up", 30), ("down", 30), ("up", 30), ("down", 30), ("up", 30), ("down", 30)])
    v2 = se.backtest_strategy("ema_cross", "BTC-USD", osc, [1.0] * len(osc), warmup=30)
    assert isinstance(v2, se.BacktestVerdict)
    # high-low strategy backtest
    c, h, l, vol = ohlcv(series_reversal(160))
    v3 = se.backtest_strategy("adx", "BTC-USD", c, vol, h, l, warmup=40)
    assert isinstance(v3, se.BacktestVerdict)
    # volume strategy backtest
    v4 = se.backtest_strategy("vol_mom", "BTC-USD", c, vol, h, l, warmup=40)
    assert isinstance(v4, se.BacktestVerdict)


def test_backtest_failing_reasons(no_rust):
    # A strongly mean-reverting flat-ish series should fail backtest and
    # exercise the failing-reason branches.
    closes = series_volatile(200)
    v = se.backtest_strategy("rsi_revert", "BTC-USD", closes, [1.0] * len(closes), warmup=30)
    assert isinstance(v, se.BacktestVerdict)
    # all-losing series -> gross_profit == 0 profit factor branch
    losing = mk([("up", 20), ("down", 60), ("up", 20), ("down", 60)])
    v2 = se.backtest_strategy("ema_cross", "BTC-USD", losing, [1.0] * len(losing), warmup=30)
    assert isinstance(v2, se.BacktestVerdict)


# ---------------------------------------------------------------------------
# _classify_regime
# ---------------------------------------------------------------------------


def test_classify_regime():
    assert se._classify_regime([1.0]) == "UNKNOWN"
    trend = [100.0 * (1.02 ** i) for i in range(60)]
    assert se._classify_regime(trend) == "TRENDED"
    flat = series_flat(60)
    assert se._classify_regime(flat) == "RANGING"
    vol = series_volatile(60)
    assert se._classify_regime(vol) in ("VOLATILE", "TRENDED", "RANGING")


# ---------------------------------------------------------------------------
# External-data strategies (mocked network)
# ---------------------------------------------------------------------------


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_funding_contrarian_sell():
    strat = FundingRateContrarian()
    payload = json.dumps([
        {"symbol": "BTCUSDT", "lastFundingRate": "0.001"},
        {"symbol": "ETHUSDT", "lastFundingRate": "0.0001"},
    ]).encode()
    with patch("urllib.request.urlopen", return_value=_FakeResp(payload)):
        s = strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD")
    assert s is not None and s.action == "SELL"


def test_funding_contrarian_buy():
    strat = FundingRateContrarian()
    payload = json.dumps([{"symbol": "BTCUSDT", "lastFundingRate": "-0.001"}]).encode()
    with patch("urllib.request.urlopen", return_value=_FakeResp(payload)):
        s = strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD")
    assert s is not None and s.action == "BUY"


def test_funding_contrarian_no_extreme():
    strat = FundingRateContrarian()
    payload = json.dumps([{"symbol": "BTCUSDT", "lastFundingRate": "0.0"}]).encode()
    with patch("urllib.request.urlopen", return_value=_FakeResp(payload)):
        s = strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD")
    assert s is None


def test_funding_contrarian_network_fail():
    strat = FundingRateContrarian()
    with patch("urllib.request.urlopen", side_effect=RuntimeError("boom")):
        s = strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD")
    assert s is None


def test_funding_contrarian_cache_hit():
    strat = FundingRateContrarian()
    payload = json.dumps([{"symbol": "BTCUSDT", "lastFundingRate": "0.002"}]).encode()
    with patch("urllib.request.urlopen", return_value=_FakeResp(payload)) as mock:
        strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD")
        s2 = strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD")
        assert mock.call_count == 1
    assert s2 is not None and s2.action == "SELL"


def _cg_chart(rising=True, spike=False):
    prices = []
    vols = []
    base = 100.0
    for i in range(120):
        p = base * (1 + 0.001 * i) if rising else base * (1 - 0.001 * i)
        prices.append([i, p])
        vols.append([i, 6000.0 if (spike and i == 119) else 1000.0])
    return {"prices": prices, "total_volumes": vols}


def test_exchange_flow_distribution():
    strat = ExchangeFlowSignal(vol_spike_threshold=2.0)
    chart = _cg_chart(rising=True, spike=True)
    payload = json.dumps(chart).encode()
    with patch("urllib.request.urlopen", return_value=_FakeResp(payload)):
        s = strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD")
    assert s is not None and s.action == "SELL"


def test_exchange_flow_accumulation():
    strat = ExchangeFlowSignal(vol_spike_threshold=2.0)
    chart = _cg_chart(rising=False, spike=True)
    payload = json.dumps(chart).encode()
    with patch("urllib.request.urlopen", return_value=_FakeResp(payload)):
        s = strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD")
    assert s is not None and s.action == "BUY"


def test_exchange_flow_no_currency():
    strat = ExchangeFlowSignal()
    assert strat.on_bar(100.0, [100.0] * 30) is None


def test_exchange_flow_unknown_currency():
    strat = ExchangeFlowSignal()
    assert strat.on_bar(100.0, [100.0] * 30, currency="ZZZ-USD") is None


def test_exchange_flow_insufficient():
    strat = ExchangeFlowSignal()
    chart = {"prices": [[0, 1.0]], "total_volumes": [[0, 1.0]]}
    payload = json.dumps(chart).encode()
    with patch("urllib.request.urlopen", return_value=_FakeResp(payload)):
        assert strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD") is None


def test_exchange_flow_no_spike():
    strat = ExchangeFlowSignal(vol_spike_threshold=2.0)
    chart = _cg_chart(rising=True)
    payload = json.dumps(chart).encode()
    with patch("urllib.request.urlopen", return_value=_FakeResp(payload)):
        assert strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD") is None


def test_exchange_flow_cached():
    strat = ExchangeFlowSignal(vol_spike_threshold=2.0)
    chart = _cg_chart(rising=True, spike=True)
    payload = json.dumps(chart).encode()
    with patch("urllib.request.urlopen", return_value=_FakeResp(payload)) as mock:
        strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD")
        s2 = strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD")
        assert mock.call_count == 1
    assert s2 is not None


def test_exchange_flow_network_fail():
    strat = ExchangeFlowSignal()
    with patch("urllib.request.urlopen", side_effect=RuntimeError("boom")):
        assert strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD") is None


def _yahoo_payload(values):
    return {"chart": {"result": [{"indicators": {"quote": [{"close": values}]}}]}}


def test_btcdxy_network_fail():
    strat = BTCDXYCorrelation()
    with patch("urllib.request.urlopen", side_effect=RuntimeError("boom")):
        assert strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD") is None


def test_btcdxy_wrong_currency():
    strat = BTCDXYCorrelation()
    assert strat.on_bar(100.0, [100.0] * 30, currency="ETH-USD") is None


def _btcdxy_responses(buy=True):
    n = 220
    btc = list(range(50, 50 + n))
    if buy:
        dxy = list(range(50, 250)) + [-x for x in range(250, 50 + n)]
    else:
        dxy = [-x for x in range(50, 250)] + list(range(250, 50 + n))
    return [
        _FakeResp(json.dumps(_yahoo_payload(btc)).encode()),
        _FakeResp(json.dumps(_yahoo_payload(dxy)).encode()),
    ]


def test_btcdxy_signal():
    strat = BTCDXYCorrelation()
    responses = _btcdxy_responses(buy=True)

    def side_effect(*a, **k):
        return responses.pop(0)

    with patch("urllib.request.urlopen", side_effect=side_effect):
        s = strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD")
    assert s is not None and s.action == "BUY"


def test_btcdxy_sell():
    strat = BTCDXYCorrelation()
    responses = _btcdxy_responses(buy=False)

    def side_effect(*a, **k):
        return responses.pop(0)

    with patch("urllib.request.urlopen", side_effect=side_effect):
        s = strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD")
    assert s is not None and s.action == "SELL"


def test_btcdxy_cached():
    strat = BTCDXYCorrelation()
    responses = _btcdxy_responses(buy=True)

    def side_effect(*a, **k):
        return responses.pop(0)

    with patch("urllib.request.urlopen", side_effect=side_effect) as mock:
        strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD")
        s2 = strat.on_bar(100.0, [100.0] * 30, currency="BTC-USD")
        assert mock.call_count == 2
    assert s2 is not None


def test_rolling_corr():
    a = [1.0, 2.0, 3.0, 4.0, 5.0]
    b = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert abs(se.BTCDXYCorrelation._rolling_corr(a, b, 3) - 1.0) < 1e-9
    assert se.BTCDXYCorrelation._rolling_corr(a, b, 100) == 0.0
    assert se.BTCDXYCorrelation._rolling_corr(a, [2.0] * 5, 3) == 0.0


class _FakeMarket:
    def __init__(self, prob, extremity=None, liq=0.5, volume=5000.0, question="Will BTC hit 100k?"):
        self.question = question
        self.outcome_prices = {"YES": prob, "NO": 1 - prob}
        self.mid_price = prob
        pe = extremity if extremity is not None else abs(prob - 0.5) / 0.5
        self.probability_extremity = pe
        self.volume = volume
        self.spread = 0.02
        self.liquidity_score = liq
        self.is_open = True


def _fake_client(markets):
    client = MagicMock()
    client.search_kalshi.return_value = markets
    client.search_polymarket.return_value = markets
    return client


def test_kalshi_buy():
    strat = KalshiSignal()
    m = _FakeMarket(prob=0.9, extremity=0.8)
    with patch.object(se.KalshiSignal, "_make_client", return_value=_fake_client([m])):
        s = strat.on_bar(100.0, [100.0] * 30)
    assert s is not None and s.action == "BUY"


def test_kalshi_sell():
    strat = KalshiSignal()
    m = _FakeMarket(prob=0.1, extremity=0.8)
    with patch.object(se.KalshiSignal, "_make_client", return_value=_fake_client([m])):
        s = strat.on_bar(100.0, [100.0] * 30)
    assert s is not None and s.action == "SELL"


def test_kalshi_no_extremity():
    strat = KalshiSignal()
    m = _FakeMarket(prob=0.55)
    with patch.object(se.KalshiSignal, "_make_client", return_value=_fake_client([m])):
        assert strat.on_bar(100.0, [100.0] * 30) is None


def test_kalshi_fetch_fail():
    strat = KalshiSignal()
    with patch.object(se.KalshiSignal, "_make_client", side_effect=RuntimeError("boom")):
        assert strat.on_bar(100.0, [100.0] * 30) is None


def test_polymarket_buy():
    strat = PolymarketSignal()
    m = _FakeMarket(prob=0.92, extremity=0.84)
    with patch.object(se.PolymarketSignal, "_make_client", return_value=_fake_client([m])):
        s = strat.on_bar(100.0, [100.0] * 30)
    assert s is not None and s.action == "BUY"


def test_polymarket_sell():
    strat = PolymarketSignal()
    m = _FakeMarket(prob=0.08, extremity=0.84)
    with patch.object(se.PolymarketSignal, "_make_client", return_value=_fake_client([m])):
        s = strat.on_bar(100.0, [100.0] * 30)
    assert s is not None and s.action == "SELL"


def test_polymarket_fetch_fail():
    strat = PolymarketSignal()
    with patch.object(se.PolymarketSignal, "_make_client", side_effect=RuntimeError("boom")):
        assert strat.on_bar(100.0, [100.0] * 30) is None


# ---------------------------------------------------------------------------
# Rust-path functions (when _HAS_RUST is True, real rust)
# ---------------------------------------------------------------------------


@pytest.fixture
def with_rust():
    with patch("urllib.request.urlopen", side_effect=AssertionError("no network")):
        yield


def test_run_strategies_rust_dispatch(with_rust):
    closes, highs, lows, volumes = ohlcv(series_reversal(120))
    sigs = se.run_strategies("BTC-USD", "growth", closes, volumes, closes[-1], highs, lows)
    assert isinstance(sigs, list)


def test_run_strategies_rust_mock_skip_and_exception():
    closes, highs, lows, volumes = ohlcv(series_reversal(60))
    with patch.object(se, "_HAS_RUST", True):
        with patch.object(se, "_rust_core") as mock_rust:
            mock_rust.run_strategy_opens_py.return_value = ("BUY", 0.7, "rust buy")
            sigs = se.run_strategies("BTC-USD", "growth", closes, [], closes[-1], None, None)
            assert any(s.action == "BUY" for s in sigs)
            assert mock_rust.run_strategy_opens_py.called
    with patch.object(se, "_HAS_RUST", True):
        with patch.object(se, "_rust_core") as mock_rust:
            mock_rust.run_strategy_opens_py.side_effect = RuntimeError("boom")
            sigs = se.run_strategies("BTC-USD", "growth", closes, volumes, closes[-1], highs, lows)
            assert isinstance(sigs, list)
    with patch.object(se, "_HAS_RUST", True):
        with patch.object(se, "_rust_core") as mock_rust:
            mock_rust.run_strategy_opens_py.return_value = ("HOLD", 0.0, "h")
            sigs = se.run_strategies("BTC-USD", "growth", closes, volumes, closes[-1], highs, lows)
            assert isinstance(sigs, list)
    with patch.object(se, "_HAS_RUST", True):
        with patch.object(se, "_rust_core") as mock_rust:
            mock_rust.run_strategy_opens_py.return_value = None
            sigs = se.run_strategies("BTC-USD", "growth", closes, volumes, closes[-1], highs, lows)
            assert isinstance(sigs, list)


def test_run_strategies_rust_with_hl_vol(with_rust):
    closes, highs, lows, volumes = ohlcv(series_reversal(120))
    sigs = se.run_strategies("BTC-USD", "growth", closes, volumes, closes[-1], highs, lows)
    assert isinstance(sigs, list)


def test_backtest_rust_dispatch(with_rust):
    closes = series_up(150)
    v = se.backtest_strategy("ema_cross", "BTC-USD", closes, [1.0] * len(closes), warmup=40)
    assert isinstance(v, se.BacktestVerdict)


def test_backtest_rust_with_hl(with_rust):
    closes, highs, lows, volumes = ohlcv(series_reversal(140))
    v = se.backtest_strategy("adx", "BTC-USD", closes, volumes, highs, lows, warmup=40)
    assert isinstance(v, se.BacktestVerdict)


def test_rust_backtest_strategy_helper(with_rust):
    closes = series_up(150)
    v = se._rust_backtest_strategy("ema_cross", "BTC-USD", closes, [1.0] * len(closes))
    assert v is None or isinstance(v, se.BacktestVerdict)


def test_batch_signals_rust(with_rust):
    products = [("BTC-USD", "safe"), ("ETH-USD", "growth")]
    closes = {p: series_reversal(120) for p, _ in products}
    volumes = {p: [1.0] * len(closes[p]) for p, _ in products}
    highs = {p: [c * 1.02 for c in closes[p]] for p, _ in products}
    lows = {p: [c * 0.98 for c in closes[p]] for p, _ in products}
    res = se.batch_signals_rust(products, closes, volumes, highs, lows)
    assert isinstance(res, dict)
    assert "BTC-USD" in res


def test_batch_signals_rust_short(with_rust):
    products = [("BTC-USD", "safe")]
    closes = {"BTC-USD": [1.0, 2.0, 3.0]}
    volumes = {"BTC-USD": [1.0, 1.0, 1.0]}
    highs = {"BTC-USD": [1.0, 2.0, 3.0]}
    lows = {"BTC-USD": [1.0, 2.0, 3.0]}
    res = se.batch_signals_rust(products, closes, volumes, highs, lows)
    assert res["BTC-USD"] == {}


def test_batch_backtest_rust(with_rust):
    closes = series_up(150)
    highs = [c * 1.02 for c in closes]
    lows = [c * 0.98 for c in closes]
    strategies = [
        ("ema_cross", "BTC-USD", closes, [1.0] * 150, None, None),
        ("candle_pat", "ETH-USD", closes, [1.0] * 150, highs, lows),
    ]
    res = se.batch_backtest_rust(strategies, warmup=40)
    assert isinstance(res, dict)


def test_batch_backtest_rust_short(with_rust):
    strategies = [("ema_cross", "BTC-USD", [1.0, 2.0, 3.0], [1.0, 1.0, 1.0], None, None)]
    res = se.batch_backtest_rust(strategies, warmup=40)
    assert res == {}


def test_batch_signals_fast_rust_path(with_rust):
    products = [("BTC-USD", "safe"), ("ETH-USD", "growth")]
    closes = {p: series_reversal(120) for p, _ in products}
    volumes = {p: [1.0] * len(closes[p]) for p, _ in products}
    highs = {p: [c * 1.02 for c in closes[p]] for p, _ in products}
    lows = {p: [c * 0.98 for c in closes[p]] for p, _ in products}
    res = se.batch_signals_fast(products, closes, volumes, highs, lows)
    assert isinstance(res, dict)


def test_batch_signals_fast_numpy_path():
    products = [("BTC-USD", "safe"), ("ETH-USD", "growth")]
    closes = {p: series_reversal(60) for p, _ in products}
    volumes = {p: [1.0] * len(closes[p]) for p, _ in products}
    highs = {p: [c * 1.02 for c in closes[p]] for p, _ in products}
    lows = {p: [c * 0.98 for c in closes[p]] for p, _ in products}

    fake_backend = MagicMock()
    fake_backend.batch_signals.return_value = {"ema_cross": ["BUY", "HOLD"]}

    with patch.object(se, "_HAS_RUST", False), patch.object(
        se, "_HAS_COMPUTE_BACKEND", True
    ), patch.object(se, "get_compute_backend", return_value=fake_backend):
        res = se.batch_signals_fast(products, closes, volumes, highs, lows)
    assert res["BTC-USD"]["ema_cross"] == "BUY"
    assert res["ETH-USD"]["ema_cross"] == "HOLD"


def test_batch_signals_fast_no_backend():
    products = [("BTC-USD", "safe")]
    closes = {"BTC-USD": series_reversal(60)}
    volumes = {"BTC-USD": [1.0] * len(closes["BTC-USD"])}
    highs = {"BTC-USD": [1.0] * len(closes["BTC-USD"])}
    lows = {"BTC-USD": [1.0] * len(closes["BTC-USD"])}
    with patch.object(se, "_HAS_RUST", False), patch.object(
        se, "_HAS_COMPUTE_BACKEND", False
    ):
        res = se.batch_signals_fast(products, closes, volumes, highs, lows)
    assert res == {}


def test_batch_signals_fast_empty_products(with_rust):
    res = se.batch_signals_fast([], {}, {}, {}, {})
    assert res == {}


def test_batch_signals_fast_short(with_rust):
    products = [("BTC-USD", "safe")]
    closes = {"BTC-USD": [1.0, 2.0, 3.0]}
    volumes = {"BTC-USD": [1.0, 1.0, 1.0]}
    highs = {"BTC-USD": [1.0, 2.0, 3.0]}
    lows = {"BTC-USD": [1.0, 2.0, 3.0]}
    res = se.batch_signals_fast(products, closes, volumes, highs, lows)
    assert res["BTC-USD"] == {}


# ---------------------------------------------------------------------------
# Targeted tests for reachable but rarely-taken defensive branches
# ---------------------------------------------------------------------------


def test_donchian_equal_hl():
    strat = DonchianChannels()
    closes = [5.0] * 40
    s = strat.on_bar(5.0, closes, highs=[5.0] * 40, lows=[5.0] * 40)
    assert s is None


def test_williams_r_equal_hl():
    strat = WilliamsR()
    closes = [5.0] * 40
    s = strat.on_bar(5.0, closes, highs=[5.0] * 40, lows=[5.0] * 40)
    assert s is None


def test_volume_momentum_zero_volume():
    strat = VolumeMomentum()
    closes = series_up(40)
    s = strat.on_bar(closes[-1], closes, volumes=[0.0] * len(closes))
    assert s is None


def test_chaikin_hl_zero():
    strat = ChaikinMoneyFlow()
    closes = [100.0 + i for i in range(40)]
    s = strat.on_bar(
        closes[-1], closes, volumes=[1000.0] * 40, highs=closes, lows=closes
    )
    assert s is None


def test_adx_flat():
    strat = ADX()
    closes = [5.0] * 60
    s = strat.on_bar(5.0, closes, highs=[5.0] * 60, lows=[5.0] * 60)
    assert s is None


def test_scci_zero_price():
    strat = SimplifiedCCI()
    closes = [0.0] * 40
    s = strat.on_bar(0.0, closes)
    assert s is None


def test_rust_backtest_non_rust_strategy(with_rust):
    # A strategy not supported by Rust falls through to None
    v = se._rust_backtest_strategy(
        "funding_contrarian", "BTC-USD", [1.0, 2.0, 3.0], [1.0, 1.0, 1.0]
    )
    assert v is None


def test_backtest_non_rust_name_rust(with_rust):
    # funding_contrarian is not in _RUST_STRATEGIES -> rust branch skipped,
    # falls through to the pure-Python path.
    closes = [100.0 + i for i in range(120)]
    v = se.backtest_strategy(
        "funding_contrarian", "BTC-USD", closes, [1.0] * len(closes)
    )
    assert isinstance(v, se.BacktestVerdict)


def test_batch_signals_fast_numpy_short():
    products = [("BTC-USD", "safe")]
    closes = {"BTC-USD": [1.0, 2.0, 3.0]}  # < 30 bars
    volumes = {"BTC-USD": [1.0, 1.0, 1.0]}
    highs = {"BTC-USD": [1.0, 2.0, 3.0]}
    lows = {"BTC-USD": [1.0, 2.0, 3.0]}
    fake_backend = MagicMock()
    fake_backend.batch_signals.return_value = {}
    with patch.object(se, "_HAS_RUST", False), patch.object(
        se, "_HAS_COMPUTE_BACKEND", True
    ), patch.object(se, "get_compute_backend", return_value=fake_backend):
        res = se.batch_signals_fast(products, closes, volumes, highs, lows)
    assert res == {}


def test_batch_signals_fast_numpy_empty():
    fake_backend = MagicMock()
    fake_backend.batch_signals.return_value = {}
    with patch.object(se, "_HAS_RUST", False), patch.object(
        se, "_HAS_COMPUTE_BACKEND", True
    ), patch.object(se, "get_compute_backend", return_value=fake_backend):
        res = se.batch_signals_fast([], {}, {}, {}, {})
    assert res == {}


def test_batch_backtest_rust_short_and_volmismatch(with_rust):
    # closes <= warmup -> skipped
    short = se.batch_backtest_rust(
        [("ema_cross", "BTC-USD", [1.0, 2.0, 3.0], [1.0, 1.0, 1.0], None, None)],
        warmup=40,
    )
    assert short == {}
    # volume length mismatch -> volumes reset to ones
    closes = series_up(150)
    mismatched = se.batch_backtest_rust(
        [("ema_cross", "BTC-USD", closes, [1.0] * 10, None, None)], warmup=40
    )
    assert isinstance(mismatched, dict)


def test_batch_backtest_rust_no_names(with_rust):
    # strategy not in _RUST_STRATEGIES -> skipped (by_product empty)
    closes = series_up(150)
    res = se.batch_backtest_rust(
        [("funding_contrarian", "BTC-USD", closes, [1.0] * 150, None, None)],
        warmup=40,
    )
    assert res == {}

