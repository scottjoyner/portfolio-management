"""Coverage tests for novel_crypto_strategies_part3.py (regime/seasonal/derivatives)."""
from __future__ import annotations

import datetime as _dt

import pytest

from trading_system.strategies.base import OHLCVBar
import trading_system.strategies.novel_crypto_strategies_part3 as nc3


def obars(n, start=100.0, kind="rising"):
    out = []
    p = start
    for i in range(n):
        if kind == "rising":
            p *= 1.02
        elif kind == "falling":
            p *= 0.98
        elif kind == "flat":
            p = start + (0.01 if i % 2 else -0.01)
        out.append(OHLCVBar(timestamp=i, open=p * 0.99, high=p * 1.02,
                            low=p * 0.98, close=p, volume=1e6 + i))
    return out


class FakeNow:
    """Fake datetime providing a controllable ``now``."""
    def __init__(self, month=9, day=15, hour=16, weekday=2):
        self._month = month
        self._day = day
        self._hour = hour
        self._weekday = weekday

    def now(self):
        outer = self

        class _N:
            month = outer._month
            day = outer._day
            hour = outer._hour

            def weekday(self):
                return outer._weekday

            def timestamp(self):
                return 0.0
        return _N()


# ---------------------------------------------------------------------------
# AdaptiveRegimeTuner
# ---------------------------------------------------------------------------

def test_adaptive_regime_tuner():
    s = nc3.AdaptiveRegimeTuner()
    s.setup(obars(10))
    # <50 -> unknown regime & on_bar None
    assert s._classify_regime(obars(1)[0]) == "unknown"
    assert s.on_bar(OHLCVBar(timestamp=0, close=100.0)) == (None, None)

    # trending up
    s.setup(obars(60, kind="rising"))
    assert s._classify_regime(s.ohlcv[-1]) == "trending_up"
    r = s.on_bar(OHLCVBar(timestamp=0, open=100, high=999, low=99,
                          close=900, volume=1e6))
    assert r[0] in (True, None)

    # trending down
    sd = nc3.AdaptiveRegimeTuner()
    sd.setup(obars(60, kind="falling"))
    assert sd._classify_regime(sd.ohlcv[-1]) == "trending_down"
    sd.on_bar(OHLCVBar(timestamp=0, open=100, high=101, low=1, close=2, volume=1e6))

    # ranging: flat data (low trend). Build closes with alternating tiny moves.
    sr = nc3.AdaptiveRegimeTuner()
    sr.setup(obars(60, kind="flat"))
    reg = sr._classify_regime(sr.ohlcv[-1])
    assert reg in ("ranging_high_vol", "ranging_low_vol")
    sr.on_bar(OHLCVBar(timestamp=0, open=100, high=100.1, low=99.9,
                       close=103, volume=1e6))

    # ranging_high_vol: alternating large moves
    highvol = []
    p = 100.0
    for i in range(60):
        p = p * (1.05 if i % 2 else 0.95)
        highvol.append(OHLCVBar(timestamp=i, open=p, high=p * 1.05,
                                low=p * 0.95, close=p, volume=1e6))
    shv = nc3.AdaptiveRegimeTuner()
    shv.setup(highvol)
    shv._classify_regime(highvol[-1])

    # _get_interpolated_params: unknown -> default; with perf buffer
    assert s._get_interpolated_params("nonexistent")["rsi_period"] == 7
    for i in range(12):
        s.record_performance(0.01)
    p = s._get_interpolated_params("trending_up")
    assert "position_size_pct" in p

    # record_performance pop path
    s2 = nc3.AdaptiveRegimeTuner()
    for i in range(55):
        s2.record_performance(0.001)
    assert len(s2.performance_buffer) == 50

    # regime analysis
    assert "current_regime" in s.get_regime_analysis()
    assert "error" in nc3.AdaptiveRegimeTuner().get_regime_analysis()

    assert s.get_strategy_info()["name"] == "AdaptiveRegimeTuner"


# ---------------------------------------------------------------------------
# SeasonalCryptoPatternStrategy
# ---------------------------------------------------------------------------

def test_seasonal_pattern(monkeypatch):
    from trading_system.strategies.novel_crypto_strategies_part3 import SeasonalPattern

    s = nc3.SeasonalCryptoPatternStrategy()

    # time-of-day factor branches
    monkeypatch.setattr(nc3, "datetime", FakeNow(hour=16))
    assert s._get_time_of_day_factor() == 1.3
    monkeypatch.setattr(nc3, "datetime", FakeNow(hour=10))
    assert s._get_time_of_day_factor() == 1.1
    monkeypatch.setattr(nc3, "datetime", FakeNow(hour=3))
    assert s._get_time_of_day_factor() == 0.9

    # holiday detection: weekend
    s.is_weekend = True
    assert s._detect_holiday() is True
    s.is_weekend = False
    monkeypatch.setattr(nc3, "datetime", FakeNow(month=12, day=25, hour=16))
    assert s._detect_holiday() is True
    monkeypatch.setattr(nc3, "datetime", FakeNow(month=9, day=15, hour=16))
    assert s._detect_holiday() is False

    # on_bar buy branch: inject a huge positive seasonal pattern
    s.current_month = 9
    s._initialize_seasonal_data()
    s.seasonal_patterns[9] = SeasonalPattern(9, 50.0, 5.0, 0.9)
    monkeypatch.setattr(s, "_detect_holiday", lambda: False)
    r = s.on_bar(OHLCVBar(timestamp=0, open=100, high=101, low=99,
                          close=100, volume=1e6))
    assert r[0] is True

    # sell branch: huge negative pattern
    s.seasonal_patterns[9] = SeasonalPattern(9, -50.0, 5.0, 0.9)
    r = s.on_bar(OHLCVBar(timestamp=0, open=100, high=101, low=99,
                          close=100, volume=1e6))
    assert r[0] is False

    # no signal: neutral pattern
    s.seasonal_patterns[9] = SeasonalPattern(9, 0.0, 5.0, 0.9)
    assert s.on_bar(OHLCVBar(timestamp=0, close=100, open=100,
                             high=101, low=99, volume=1e6)) == (None, None)

    # current_pattern missing -> None
    s.current_month = 99
    assert s.on_bar(OHLCVBar(timestamp=0, close=100)) == (None, None)

    # seasonal analysis (accumulate + neutral)
    s.current_month = 9
    an = s.get_seasonal_analysis()
    assert "recommendation" in an
    s.current_month = 99
    an2 = s.get_seasonal_analysis()
    assert an2["recommendation"] == "neutral"

    # fresh instance -> analysis initializes data
    fresh = nc3.SeasonalCryptoPatternStrategy()
    assert "current_month" in fresh.get_seasonal_analysis()

    assert s.get_strategy_info()["name"] == "SeasonalCryptoPatternStrategy"


# ---------------------------------------------------------------------------
# FundingRateArbitrageStrategy
# ---------------------------------------------------------------------------

def test_funding_rate_arb():
    s = nc3.FundingRateArbitrageStrategy()
    s.setup(obars(30))
    rates = s._fetch_funding_rates()
    assert len(rates) == 3

    # carry profit (present/absent)
    assert s._calculate_carry_profit("coinbase") != 0.0
    assert s._calculate_carry_profit("nonexistent") == 0.0
    empty = nc3.FundingRateArbitrageStrategy()
    assert empty._calculate_carry_profit("x") == 0.0

    # cross-exchange arb: <2 -> None
    e2 = nc3.FundingRateArbitrageStrategy()
    e2.funding_rates = {"a": 0.05}
    assert e2._calculate_cross_exchange_arb() is None

    # arb opportunity present
    arb = nc3.FundingRateArbitrageStrategy(min_arb_profit_pct=0.05)
    arb.funding_rates = {"a": 0.01, "b": 0.09}
    assert arb._calculate_cross_exchange_arb() is not None

    # arb too small -> None
    arb.funding_rates = {"a": 0.05, "b": 0.051}
    assert arb._calculate_cross_exchange_arb() is None

    # on_bar: extreme high funding -> hedge (False)
    hi = nc3.FundingRateArbitrageStrategy(max_funding_rate_pct=0.0)
    hi.setup(obars(30))
    hi.funding_rates = {"a": 0.05, "b": 0.06}
    assert hi.on_bar(OHLCVBar(timestamp=0, close=100))[0] is False

    # on_bar: extreme low funding -> long (True)
    lo = nc3.FundingRateArbitrageStrategy(max_funding_rate_pct=0.1)
    lo.setup(obars(30))
    lo.funding_rates = {"a": -0.5, "b": -0.4}
    assert lo.on_bar(OHLCVBar(timestamp=0, close=100))[0] is True

    # on_bar: arb opportunity -> True
    ao = nc3.FundingRateArbitrageStrategy(max_funding_rate_pct=1.0,
                                          min_arb_profit_pct=0.05)
    ao.setup(obars(30))
    ao.funding_rates = {"a": 0.01, "b": 0.09}
    assert ao.on_bar(OHLCVBar(timestamp=0, close=100))[0] is True

    # on_bar: no signal
    ns = nc3.FundingRateArbitrageStrategy(max_funding_rate_pct=1.0,
                                          min_arb_profit_pct=0.5)
    ns.setup(obars(30))
    ns.funding_rates = {"a": 0.05, "b": 0.05}
    assert ns.on_bar(OHLCVBar(timestamp=0, close=100)) == (None, None)

    # on_bar fetches when empty
    fe = nc3.FundingRateArbitrageStrategy()
    fe.setup(obars(30))
    fe.on_bar(OHLCVBar(timestamp=0, close=100))

    # analysis sentiment branches
    a = nc3.FundingRateArbitrageStrategy()
    a.funding_rates = {"a": 0.2, "b": 0.05}
    assert a.get_funding_analysis()["sentiment"] == "strongly_bullish"
    a.funding_rates = {"a": -0.2, "b": -0.05}
    assert a.get_funding_analysis()["sentiment"] == "strongly_bearish"
    a.funding_rates = {"a": 0.05, "b": 0.05}
    assert a.get_funding_analysis()["sentiment"] == "neutral"
    # empty -> fetch
    fa = nc3.FundingRateArbitrageStrategy()
    fa.setup(obars(30))
    assert "sentiment" in fa.get_funding_analysis()

    assert s.get_strategy_info()["name"] == "FundingRateArbitrageStrategy"


# ---------------------------------------------------------------------------
# LiquidationCascadeDetector
# ---------------------------------------------------------------------------

def test_liquidation_cascade():
    s = nc3.LiquidationCascadeDetector()
    s.setup(obars(30))
    # no oi history -> None
    assert s.on_bar(OHLCVBar(timestamp=0, close=100)) == (None, None)

    # _simulate_open_interest short -> default
    short = nc3.LiquidationCascadeDetector()
    short.ohlcv = obars(5)
    assert short._simulate_open_interest() == 1e9
    s.ohlcv = obars(30)
    assert s._simulate_open_interest() > 0

    # risk score with <20 history -> 0
    assert s._calculate_cascade_risk_score() == 0.0

    # signal: force risk over threshold, upward momentum -> False
    su = nc3.LiquidationCascadeDetector(cascade_risk_threshold=-1.0)
    su.setup(obars(30))
    su.oi_history = [5e8] * 25
    assert su.on_bar(OHLCVBar(timestamp=0, open=100, high=110, low=99,
                              close=105, volume=1e6))[0] is False

    # downward momentum -> True
    sdn = nc3.LiquidationCascadeDetector(cascade_risk_threshold=-1.0)
    sdn.setup(obars(30))
    sdn.oi_history = [5e8] * 25
    assert sdn.on_bar(OHLCVBar(timestamp=0, open=100, high=101, low=90,
                               close=95, volume=1e6))[0] is True

    # risk high but flat momentum -> None
    sf = nc3.LiquidationCascadeDetector(cascade_risk_threshold=-1.0)
    sf.setup(obars(30))
    sf.oi_history = [5e8] * 25
    assert sf.on_bar(OHLCVBar(timestamp=0, open=100, high=100.5, low=99.5,
                              close=100.0, volume=1e6)) == (None, None)

    # analysis
    assert "cascade_risk_score" in su.get_cascade_analysis()
    assert "error" in nc3.LiquidationCascadeDetector().get_cascade_analysis()

    assert s.get_strategy_info()["name"] == "LiquidationCascadeDetector"


# ---------------------------------------------------------------------------
# OptionsImpliedVolatilitySkewStrategy
# ---------------------------------------------------------------------------

def test_options_iv_skew():
    s = nc3.OptionsImpliedVolatilitySkewStrategy()
    s.setup(obars(40))
    # no history -> None
    assert s.on_bar(OHLCVBar(timestamp=0, close=100)) == (None, None)

    # _simulate_iv_surface / _calculate_skew_magnitude
    surf = s._simulate_iv_surface()
    assert "atm_iv_pct" in surf
    assert s._calculate_skew_magnitude(surf) != 0.0
    assert s._calculate_skew_magnitude({}) == 0.0

    # historical avg default (<20) then computed
    assert s._get_historical_avg_skew() == 15.0
    s.iv_surface_history = [{"skew": 10.0} for _ in range(30)]
    assert s._get_historical_avg_skew() == 10.0

    # on_bar sell branch: current skew >> historical (small hist)
    ss = nc3.OptionsImpliedVolatilitySkewStrategy()
    ss.setup(obars(30))
    ss.iv_surface_history = [{"skew": 0.5} for _ in range(25)]
    r = ss.on_bar(OHLCVBar(timestamp=0, close=100, open=100,
                           high=101, low=99, volume=1e6))
    assert r[0] in (False, None)

    # on_bar buy branch: current skew << historical (large hist)
    sb = nc3.OptionsImpliedVolatilitySkewStrategy()
    sb.setup(obars(30))
    sb.iv_surface_history = [{"skew": 100.0} for _ in range(25)]
    r = sb.on_bar(OHLCVBar(timestamp=0, close=100, open=100,
                           high=101, low=99, volume=1e6))
    assert r[0] in (True, None)

    # get_iv_analysis skew_state branches
    an = nc3.OptionsImpliedVolatilitySkewStrategy()
    an.setup(obars(30))
    an.iv_surface_history = [{"skew": 5.0} for _ in range(25)]
    assert "skew_state" in an.get_iv_analysis()
    assert "error" in nc3.OptionsImpliedVolatilitySkewStrategy().get_iv_analysis()

    assert s.get_strategy_info()["name"] == "OptionsImpliedVolatilitySkewStrategy"
