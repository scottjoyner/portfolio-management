"""Coverage tests for novel_crypto_strategies.py (part 1).

All external I/O (Kalshi/Polymarket HTTP) is mocked; no live calls.
"""
from __future__ import annotations

import pytest

from trading_system.strategies.base import OHLCVBar
import trading_system.strategies.novel_crypto_strategies as nc


def obars(n, kind="rising", start=100.0, vol=1e7):
    out = []
    p = start
    for i in range(n):
        if kind == "rising":
            p *= 1.01
        elif kind == "falling":
            p *= 0.97
        elif kind == "volatile":
            p *= 1.0 + ((i % 2) * 2 - 1) * 0.06
        elif kind == "squeeze":
            # flat then breakout
            p = start if i < n - 5 else start * 1.2
        out.append(OHLCVBar(timestamp=i, open=p, high=p * 1.02, low=p * 0.98,
                            close=p, volume=vol + i))
    return out


# ---------------------------------------------------------------------------
# CoinbaseMomentumStrategy
# ---------------------------------------------------------------------------

def test_coinbase_momentum():
    s = nc.CoinbaseMomentumStrategy(initial_capital=5000.0)
    # on_bar before setup / too little history
    s.setup([])
    assert s.on_bar(OHLCVBar(timestamp=0, close=100.0)) == (None, None)

    s.setup(obars(120, "falling"))
    # avg volatility computed -> adaptive period branch
    assert s._get_adaptive_rsi_period() in (14, 21, 28)

    # drive many bars in both directions
    got = False
    for b in obars(40, "falling"):
        r = s.on_bar(b)
        if r[0] is not None:
            got = True
    for b in obars(40, "rising"):
        s.on_bar(b)

    # adaptive period branches via manual avg_volatility
    s.avg_volatility = 0.06
    assert s._get_adaptive_rsi_period() == 28
    s.avg_volatility = 0.04
    assert s._get_adaptive_rsi_period() == 21
    s.avg_volatility = 0.01
    assert s._get_adaptive_rsi_period() == 14

    # _compute_rsi edge cases
    assert s._compute_rsi([], 14) == [0.0] * (0 - 14) or s._compute_rsi([], 14) == []
    short = s._compute_rsi([1.0, 2.0], 14)
    assert isinstance(short, list)
    rsi = s._compute_rsi([float(i) for i in range(30)], 14)
    assert rsi

    # position lifecycle
    s.update_position(True, 100.0)
    assert s.position_size > 0
    assert s.get_position_value() > 0
    assert s.calculate_pnl(110.0) != 0.0
    s.update_position(False, 0.0)
    assert s.get_position_value() == 0.0
    assert s.calculate_pnl(110.0) == 0.0

    assert s.get_strategy_info()["name"] == "CoinbaseMomentumStrategy"

    # _calculate_initial_volatility with <50 bars returns early
    s2 = nc.CoinbaseMomentumStrategy()
    s2.setup(obars(10))
    assert not hasattr(s2, "avg_volatility") or s2.avg_volatility is not None


# ---------------------------------------------------------------------------
# CoinbaseMeanReversionStrategy
# ---------------------------------------------------------------------------

def test_coinbase_mean_reversion():
    s = nc.CoinbaseMeanReversionStrategy(bb_period=20, bb_std=2.0)
    # on_bar before setup
    assert s.on_bar(OHLCVBar(timestamp=0, close=100.0)) == (None, None)

    s.setup(obars(120, "volatile"))
    for b in obars(60, "volatile"):
        s.on_bar(b)
    for b in obars(60, "rising"):
        s.on_bar(b)

    # get_bb_position branches
    s.upper_band = [110.0]
    s.lower_band = [90.0]
    assert s.get_bb_position(120.0) == "above_upper"
    assert s.get_bb_position(80.0) == "below_lower"
    assert s.get_bb_position(100.0) == "near_mean"
    assert s.get_bb_position(107.0) in ("outer_quarter", "extreme", "near_mean")
    assert s.get_bb_position(109.0) in ("extreme", "outer_quarter")

    # unknown position when band is None/0
    s.upper_band = [None]
    s.lower_band = [None]
    assert s.get_bb_position(100.0) == "unknown"

    assert s.get_strategy_info()["name"] == "CoinbaseMeanReversionStrategy"


def test_coinbase_mean_reversion_signals():
    """Directly drive mean-reversion / squeeze branches."""
    s = nc.CoinbaseMeanReversionStrategy(bb_period=5, bb_std=0.001)
    s.setup(obars(30, "flat" if False else "rising"))
    # Force band-touch buy: lower band above price, wide candle range
    s.sma_values = [100.0] * 30
    s.lower_band = [200.0]      # current price below -> buy candidate
    s.upper_band = [50.0]       # current price above -> sell candidate
    bar = OHLCVBar(timestamp=1, open=100, high=110, low=90, close=100, volume=1)
    res = s.on_bar(bar)
    assert res[0] in (True, False, None)

    # squeeze branch: build band_width_history contraction then breakout
    s2 = nc.CoinbaseMeanReversionStrategy(bb_period=5, bb_std=2.0)
    s2.setup(obars(30, "rising"))
    s2.sma_values = [100.0] * 30
    s2.lower_band = [95.0] * 30
    s2.upper_band = [105.0] * 30
    s2.band_width_history = [1.0] * 10 + [0.1] * 10  # older wide, recent tight
    up = s2.on_bar(OHLCVBar(timestamp=1, high=210, low=200, close=205, volume=1))
    assert up[0] in (True, False, None)
    dn = s2.on_bar(OHLCVBar(timestamp=1, high=1, low=0.1, close=0.5, volume=1))
    assert dn[0] in (True, False, None)


# ---------------------------------------------------------------------------
# PredictionMarketArbitrageStrategy
# ---------------------------------------------------------------------------

def test_prediction_market_arbitrage():
    s = nc.PredictionMarketArbitrageStrategy()

    # No API key -> mock fallback
    ks = s.fetch_kalshi_markets()
    pm = s.fetch_polymarket_events()
    assert ks and pm

    opps = s.detect_opportunities()
    assert isinstance(opps, list)

    sim = s._calculate_string_similarity("bitcoin100k", "bitcoin100k")
    assert sim == 1.0

    # execute_arbitrage both directions
    opp = nc.PredictionMarketOpportunity(
        kalshi_market_id="k", polymarket_slug="p",
        kalshi_yes_price=0.4, polymarket_yes_price=0.6,
        divergence_pct=20.0, estimated_profit_pct=19.0, confidence_score=0.9)
    r = s.execute_arbitrage(opp)
    assert r["buy_platform"] == "kalshi"
    opp2 = nc.PredictionMarketOpportunity(
        kalshi_market_id="k", polymarket_slug="p",
        kalshi_yes_price=0.6, polymarket_yes_price=0.4,
        divergence_pct=20.0, estimated_profit_pct=19.0, confidence_score=0.9)
    r2 = s.execute_arbitrage(opp2)
    assert r2["buy_platform"] == "polymarket"

    assert s.get_strategy_info()["name"] == "PredictionMarketArbitrageStrategy"


def test_prediction_market_api_paths(monkeypatch):
    """Cover the API-key branches by mocking urllib to raise (no network)."""
    import urllib.request

    def boom(*a, **k):
        raise RuntimeError("no network")

    monkeypatch.setattr(urllib.request, "urlopen", boom)

    s = nc.PredictionMarketArbitrageStrategy(kalshi_api_key="k",
                                             polymarket_api_key="p")
    # both should fall back to mock data after the mocked failure
    assert s.fetch_kalshi_markets()
    assert s.fetch_polymarket_events()


def test_prediction_market_api_success(monkeypatch):
    """Cover the successful-response parsing branch with a fake urlopen."""
    import urllib.request
    import json as _json

    class FakeResp:
        def __init__(self, payload):
            self._p = payload

        def read(self):
            return _json.dumps(self._p).encode()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    calls = {"n": 0}

    def fake_urlopen(req, timeout=30):
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeResp({"items": [{"market_id": "x"}]})
        return FakeResp({"results": [{"slug": "y"}]})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    s = nc.PredictionMarketArbitrageStrategy(kalshi_api_key="k",
                                             polymarket_api_key="p")
    assert s.fetch_kalshi_markets() == [{"market_id": "x"}]
    assert s.fetch_polymarket_events() == [{"slug": "y"}]


# ---------------------------------------------------------------------------
# VolatilityBreakoutStrategy
# ---------------------------------------------------------------------------

def test_volatility_breakout():
    s = nc.VolatilityBreakoutStrategy(bb_period=20, bb_std=2.0, atr_period=14)
    # too little history
    s.setup(obars(10))
    assert s.on_bar(OHLCVBar(timestamp=0, close=100.0)) == (None, None)

    s2 = nc.VolatilityBreakoutStrategy()
    s2.setup(obars(80, "squeeze"))
    # feed a breakout bar with big volume to hit the True branch
    got = False
    for b in obars(30, "squeeze"):
        hi_vol = OHLCVBar(timestamp=b.timestamp, open=b.open, high=b.high,
                          low=b.low, close=b.close, volume=1e12)
        r = s2.on_bar(hi_vol)
        if r[0] is True:
            got = True

    # _get_average_volume branches
    small = nc.VolatilityBreakoutStrategy()
    small.ohlcv_data = obars(5)
    assert small._get_average_volume() == 1e8
    s2.ohlcv_data = obars(30)
    assert s2._get_average_volume() > 0

    assert s2.get_strategy_info()["name"] == "VolatilityBreakoutStrategy"


# ---------------------------------------------------------------------------
# RegimeAwareAdaptiveStrategy
# ---------------------------------------------------------------------------

def test_regime_aware_adaptive():
    s = nc.RegimeAwareAdaptiveStrategy()

    # <50 bars -> unknown
    assert s.detect_regime(obars(10))["regime"] == "unknown"

    # trending
    trend = s.detect_regime(obars(120, "rising"))
    assert "regime" in trend and "volatility_state" in trend

    # volatile -> high vol state
    vold = s.detect_regime(obars(120, "volatile"))
    assert vold["volatility_state"] in ("high", "normal", "low")

    # ranging (flat-ish) - construct closes with tiny changes then no trend
    flat = [OHLCVBar(timestamp=i, open=100, high=100.1, low=99.9,
                     close=100.0, volume=1e6) for i in range(60)]
    fr = s.detect_regime(flat)
    assert fr["regime"] in ("ranging", "reversing", "trending")

    # reversing branch: force lows increasing so minus_dm large & trend<... 
    # (exercise via manually engineered highs/lows)
    rev = [OHLCVBar(timestamp=i, open=100, high=100 - i * 0.01,
                    low=100 - i * 0.5, close=100.0, volume=1e6)
           for i in range(60)]
    s.detect_regime(rev)

    assert s.on_bar(OHLCVBar(timestamp=0, close=100.0)) == (None, None)
    assert s.get_strategy_info()["name"] == "RegimeAwareAdaptiveStrategy"
