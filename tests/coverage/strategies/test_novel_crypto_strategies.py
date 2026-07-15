"""Coverage tests for trading_system.strategies.novel_crypto_strategies."""
from __future__ import annotations

import json
import urllib.request

from strategies.base import OHLCVBar

from trading_system.strategies.novel_crypto_strategies import (
    CoinbaseMomentumStrategy,
    CoinbaseMeanReversionStrategy,
    PredictionMarketArbitrageStrategy,
    VolatilityBreakoutStrategy,
    RegimeAwareAdaptiveStrategy,
)


def mk(close, high=None, low=None, volume=1000.0, ts=0):
    return OHLCVBar(
        open=close,
        high=high if high is not None else close,
        low=low if low is not None else close,
        close=close,
        volume=volume,
        timestamp=ts,
    )


def flat(n, price=100.0):
    return [mk(price) for _ in range(n)]


def rising(n=120, start=100.0):
    return [mk(round(start * (1.01 ** i), 2), volume=1000 + i) for i in range(n)]


# --------------------------------------------------------------------------
# CoinbaseMomentumStrategy
# --------------------------------------------------------------------------
def test_momentum_init_and_info():
    s = CoinbaseMomentumStrategy()
    assert s.initial_capital == 10000.0
    assert s.get_strategy_info()["name"] == "CoinbaseMomentumStrategy"


def test_momentum_setup_short_and_long():
    s = CoinbaseMomentumStrategy()
    s.setup([mk(100.0) for _ in range(10)])
    assert s.ohlcv is not None
    s.setup(rising(100))
    assert len(s.ohlcv) == 100


def test_momentum_adaptive_rsi_period():
    s = CoinbaseMomentumStrategy()
    s.avg_volatility = 0.06
    assert s._get_adaptive_rsi_period() == 28
    s.avg_volatility = 0.04
    assert s._get_adaptive_rsi_period() == 21
    s.avg_volatility = 0.01
    assert s._get_adaptive_rsi_period() == 14


def test_momentum_on_bar_short_history():
    s = CoinbaseMomentumStrategy()
    s.setup(rising(20))
    assert s.on_bar(mk(100.0)) == (None, None)


def test_momentum_on_bar_buy_signal():
    s = CoinbaseMomentumStrategy()
    s.setup(flat(100))  # neutral warmup so RSI can swing
    crash = [mk(round(100 * (0.9 ** i), 2)) for i in range(45)]
    sigs = [s.on_bar(b) for b in crash]
    assert any(sig == (True, b.close) for sig, b in zip(sigs, crash))


def test_momentum_on_bar_sell_with_position():
    s = CoinbaseMomentumStrategy()
    s.setup(flat(100))
    s.position_size = 1.0
    rally = [mk(round(100 * (1.1 ** i), 2)) for i in range(45)]
    sigs = [s.on_bar(b) for b in rally]
    assert any(sig == (False, b.close) for sig, b in zip(sigs, rally))


def test_momentum_position_helpers():
    s = CoinbaseMomentumStrategy()
    s.update_position(True, 100.0)
    assert s.position_size > 0
    assert s.get_position_value() > 0
    s.update_position(False, 100.0)
    assert s.position_size == 0
    assert s.calculate_pnl(110.0) == 0.0
    s.update_position(True, 100.0)
    assert s.calculate_pnl(110.0) != 0.0


# --------------------------------------------------------------------------
# CoinbaseMeanReversionStrategy
# --------------------------------------------------------------------------
def test_meanrev_init_and_info():
    s = CoinbaseMeanReversionStrategy()
    assert s.bb_period == 20
    assert s.get_strategy_info()["name"] == "CoinbaseMeanReversionStrategy"


def test_meanrev_setup_and_short():
    s = CoinbaseMeanReversionStrategy()
    s.setup(rising(60))
    assert s.on_bar(mk(100.0)) == (None, None)


def test_meanrev_signal_branches():
    s = CoinbaseMeanReversionStrategy()
    s.setup(rising(60))
    for b in rising(40):
        s.on_bar(b)
    n = len(s.lower_band) or 20
    s.lower_band = [50.0] * n
    s.upper_band = [150.0] * n
    # Neutralise squeeze history so mean-reversion branches are exercised.
    s.band_width_history = [1.0] * max(len(s.band_width_history), 20)
    buy = s.on_bar(mk(40.0, high=42.0, low=39.0, volume=2000))
    assert buy[0] is True
    sell = s.on_bar(mk(160.0, high=165.0, low=158.0, volume=2000))
    assert sell[0] is False
    for price in (40.0, 160.0, 100.0, 120.0, 80.0):
        assert s.get_bb_position(price) in (
            "above_upper", "below_lower", "near_mean", "outer_quarter", "extreme", "unknown",
        )


# --------------------------------------------------------------------------
# PredictionMarketArbitrageStrategy
# --------------------------------------------------------------------------
def test_prediction_init_and_info():
    s = PredictionMarketArbitrageStrategy()
    assert s.get_strategy_info()["name"] == "PredictionMarketArbitrageStrategy"
    assert s.similarity_threshold == 0.75


def test_prediction_fetch_mock():
    s = PredictionMarketArbitrageStrategy()
    assert len(s.fetch_kalshi_markets()) == 3
    assert len(s.fetch_polymarket_events()) == 3


def test_prediction_fetch_with_keys(monkeypatch):
    payload = json.dumps(
        {"items": [{"market_id": "X", "title": "BTC", "bid": 0.4, "ask": 0.5}],
         "results": [{"slug": "btc", "question": "BTC?", "bid": 0.4, "ask": 0.5}]}
    ).encode()

    class _Resp:
        def read(self):
            return payload

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(urllib.request, "urlopen", lambda req, timeout=30: _Resp())
    s = PredictionMarketArbitrageStrategy(kalshi_api_key="k", polymarket_api_key="p")
    assert s.fetch_kalshi_markets()[0]["market_id"] == "X"
    assert s.fetch_polymarket_events()  # parsed from mocked response


def test_prediction_similarity():
    s = PredictionMarketArbitrageStrategy()
    assert 0.0 <= s._calculate_string_similarity("Bitcoin 100k", "bitcoin 100k") <= 1.0


def test_prediction_detect_opportunities():
    s = PredictionMarketArbitrageStrategy()
    opps = s.detect_opportunities()
    assert isinstance(opps, list)
    if opps:
        ex = s.execute_arbitrage(opps[0], max_position_usd=1000)
        assert "buy_platform" in ex and "sell_platform" in ex


def test_prediction_execute_arbitrage_both_sides():
    s = PredictionMarketArbitrageStrategy()

    class _O:
        kalshi_yes_price = 0.40
        polymarket_yes_price = 0.60

    ex = s.execute_arbitrage(_O())
    assert ex["buy_platform"] == "kalshi"
    _O.kalshi_yes_price = 0.60
    _O.polymarket_yes_price = 0.40
    assert s.execute_arbitrage(_O())["buy_platform"] == "polymarket"


# --------------------------------------------------------------------------
# VolatilityBreakoutStrategy
# --------------------------------------------------------------------------
def test_volatility_init_and_info():
    s = VolatilityBreakoutStrategy()
    assert s.atr_period == 14
    assert s.get_strategy_info()["name"] == "VolatilityBreakoutStrategy"


def test_volatility_setup_and_squeeze():
    s = VolatilityBreakoutStrategy()
    hist = [mk(100.0, high=110.0, low=90.0, volume=1000) for _ in range(60)]
    s.setup(hist)
    assert s.on_bar(mk(100.0)) == (None, None)
    quiet = [mk(100.0, high=100.0, low=100.0, volume=5000.0) for _ in range(25)]
    sigs = [s.on_bar(b) for b in quiet]
    assert any(sig[0] is True for sig in sigs)


def test_volatility_average_volume():
    s = VolatilityBreakoutStrategy()
    s.setup(rising(60))
    assert s._get_average_volume() > 0
    s2 = VolatilityBreakoutStrategy()
    s2.setup([mk(100.0) for _ in range(5)])
    assert s2._get_average_volume() == 1e8


# --------------------------------------------------------------------------
# RegimeAwareAdaptiveStrategy
# --------------------------------------------------------------------------
def test_regime_init_and_info():
    s = RegimeAwareAdaptiveStrategy()
    assert s.get_strategy_info()["name"] == "RegimeAwareAdaptiveStrategy"


def test_regime_short():
    s = RegimeAwareAdaptiveStrategy()
    assert s.detect_regime([mk(100.0) for _ in range(10)])["regime"] == "unknown"


def test_regime_trending_normal():
    s = RegimeAwareAdaptiveStrategy()
    up = [mk(100.0 + i * 0.1) for i in range(60)]
    r = s.detect_regime(up)
    assert r["regime"] == "trending"
    assert r["volatility_state"] == "normal"
    assert r["position_multiplier"] == 1.2


def test_regime_ranging_low():
    # All-flat series -> zero baseline variance -> 'low' volatility, flat trend.
    data = [mk(100.0, high=100.0, low=100.0) for i in range(60)]
    s = RegimeAwareAdaptiveStrategy()
    r = s.detect_regime(data)
    assert r["regime"] == "ranging"
    assert r["volatility_state"] == "low"
    assert r["position_multiplier"] == 0.8


def test_regime_high_vol_else_branch():
    # First 50 bars calm, last 10 volatile -> high volatility, ~flat trend.
    data = [
        mk(100.0, high=100.0, low=100.0)
        if i < 50 else mk(100.0 + (30.0 if i % 2 else -30.0), high=130.0, low=70.0)
        for i in range(60)
    ]
    s = RegimeAwareAdaptiveStrategy()
    r = s.detect_regime(data)
    assert r["volatility_state"] == "high"
    assert r["position_multiplier"] == 1.0


def test_regime_on_bar():
    s = RegimeAwareAdaptiveStrategy()
    assert s.on_bar(mk(100.0)) == (None, None)
