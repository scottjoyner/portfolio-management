import time
import unittest
from unittest import mock

from coinbase.src.cross_asset_regime import (
    CrossAssetRegimeEngine, CrossAssetRegimeState,
)
from coinbase.src.resilience import SourceCircuitBreaker


def set_scores(eng, btc, spy, qqq, vix, dxy, tnx):
    eng._btc_trend_score = lambda: (btc, "btc_r")
    eng._yfinance_trend_score = lambda sym: (spy if sym == "SPY" else qqq, "eq_r")
    eng._yfinance_delta_score = lambda sym, higher_is_risk_off=False: (
        dxy if sym == "DX-Y.NYB" else tnx, "delta_r")
    eng._vix_risk_score = lambda: (vix, "vix_r")
    eng._fetch_close_series = lambda sym: [100.0] * 20


class TestState(unittest.TestCase):
    def test_to_dict(self):
        st = CrossAssetRegimeState(regime="risk_off", confidence=0.7)
        d = st.to_dict()
        self.assertEqual(d["regime"], "risk_off")
        self.assertIn("risk_multiplier", d)


class TestEngineBasic(unittest.TestCase):
    def test_update_btc_snapshot(self):
        eng = CrossAssetRegimeEngine()
        eng.update_btc_snapshot(50000.0, closes=[100.0, 200.0, -5.0], volumes=[1.0, 2.0, -1.0])
        self.assertEqual(eng._btc_price, 50000.0)
        self.assertEqual(eng._btc_closes, [100.0, 200.0, -5.0])
        self.assertEqual(eng._btc_volumes, [1.0, 2.0, -1.0])

    def test_update_btc_snapshot_price_zero(self):
        eng = CrossAssetRegimeEngine()
        eng.update_btc_snapshot(0.0, closes=[100.0], volumes=[1.0])
        self.assertEqual(eng._btc_price, 0.0)

    def test_update_btc_snapshot_no_closes_volumes(self):
        eng = CrossAssetRegimeEngine()
        eng.update_btc_snapshot(40000.0, closes=None, volumes=None)
        self.assertEqual(eng._btc_price, 40000.0)
        self.assertEqual(eng._btc_closes, [])
        self.assertEqual(eng._btc_volumes, [])

    def test_last_daily_close(self):
        eng = CrossAssetRegimeEngine()
        self.assertEqual(eng.last_daily_close(), 0.0)
        eng._btc_closes = [100.0, 200.0]
        self.assertEqual(eng.last_daily_close(), 200.0)

    def test_get_state_cache_hit(self):
        eng = CrossAssetRegimeEngine(cache_ttl_s=600)
        cached = CrossAssetRegimeState(regime="mixed", updated_at=time.time())
        eng._cached = cached
        st = eng.get_state()
        self.assertIs(st, cached)

    def test_get_state_refresh(self):
        eng = CrossAssetRegimeEngine()
        set_scores(eng, 0.1, 0.1, 0.1, 0.2, 0.0, 0.0)
        st = eng.get_state(refresh=True)
        self.assertIn(st.regime, {"mixed", "risk_off", "risk_on", "crash", "rebound"})

    def test_refresh_and_snapshot(self):
        eng = CrossAssetRegimeEngine()
        set_scores(eng, 0.1, 0.1, 0.1, 0.2, 0.0, 0.0)
        st = eng.refresh()
        self.assertIsInstance(st, CrossAssetRegimeState)
        self.assertIsInstance(eng.snapshot(), dict)


class TestComputeStateRegimes(unittest.TestCase):
    def _compute(self, **kw):
        eng = CrossAssetRegimeEngine()
        set_scores(eng, kw["btc"], kw["spy"], kw["qqq"], kw["vix"], kw["dxy"], kw["tnx"])
        eng._btc_closes = kw.get("closes", [100.0])
        eng._btc_price = kw.get("price", 100.0)
        return eng._compute_state(time.time())

    def test_live_override(self):
        st = self._compute(btc=0.0, spy=0.0, qqq=0.0, vix=0.0, dxy=0.0, tnx=0.0,
                           closes=[100.0], price=103.0)
        self.assertEqual(st.regime, "mixed")
        self.assertEqual(st.trend_bias, "bullish")
        self.assertTrue(st.allows_new_longs)

    def test_crash(self):
        st = self._compute(btc=-1.0, spy=-1.0, qqq=-1.0, vix=2.0, dxy=2.0, tnx=2.0)
        self.assertEqual(st.regime, "crash")
        self.assertFalse(st.allows_new_longs)

    def test_risk_off(self):
        st = self._compute(btc=-0.3, spy=-0.3, qqq=-0.3, vix=1.0, dxy=1.0, tnx=1.0)
        self.assertEqual(st.regime, "risk_off")

    def test_risk_on(self):
        st = self._compute(btc=1.0, spy=0.5, qqq=0.5, vix=0.0, dxy=0.0, tnx=0.0)
        self.assertEqual(st.regime, "risk_on")
        self.assertTrue(st.allows_new_longs)

    def test_rebound(self):
        st = self._compute(btc=-0.1, spy=0.1, qqq=0.1, vix=0.0, dxy=0.0, tnx=0.0)
        self.assertEqual(st.regime, "rebound")

    def test_mixed_else(self):
        st = self._compute(btc=0.1, spy=0.1, qqq=0.1, vix=0.2, dxy=0.0, tnx=0.0)
        self.assertEqual(st.regime, "mixed")


class TestComputeStateBreakerAndException(unittest.TestCase):
    def test_breaker_open_no_cache(self):
        eng = CrossAssetRegimeEngine()
        eng._breaker.failure_threshold = 1
        eng._breaker.on_failure(RuntimeError())
        st = eng._compute_state(time.time())
        self.assertEqual(st.reason, "breaker_open")

    def test_breaker_open_with_cache(self):
        eng = CrossAssetRegimeEngine()
        cached = CrossAssetRegimeState(regime="risk_off", updated_at=time.time() - 10)
        eng._cached = cached
        eng._breaker.failure_threshold = 1
        eng._breaker.on_failure(RuntimeError())
        st = eng._compute_state(time.time())
        self.assertIs(st, cached)

    def test_exception_path(self):
        eng = CrossAssetRegimeEngine()
        eng._breaker.failure_threshold = 1
        def boom():
            raise RuntimeError("kaboom")
        eng._btc_trend_score = boom
        st = eng._compute_state(time.time())
        self.assertEqual(st.reason, "kaboom")
        self.assertEqual(eng._breaker.state, "open")


class TestHelperMethods(unittest.TestCase):
    def test_btc_trend_score_enough_closes(self):
        eng = CrossAssetRegimeEngine()
        eng._btc_closes = [float(i) for i in range(30)]
        score, reason = eng._btc_trend_score()
        self.assertIsInstance(score, float)

    def test_btc_trend_score_fetch_success(self):
        eng = CrossAssetRegimeEngine()
        eng._btc_closes = [1.0]
        with mock.patch("coinbase.src.yahoo_chart.fetch_closes", return_value=[float(i) for i in range(30)]):
            score, reason = eng._btc_trend_score()
        self.assertIsInstance(score, float)

    def test_btc_trend_score_fetch_fail(self):
        eng = CrossAssetRegimeEngine()
        eng._btc_closes = [1.0]
        with mock.patch("coinbase.src.yahoo_chart.fetch_closes", side_effect=RuntimeError("x")):
            score, reason = eng._btc_trend_score()
        self.assertEqual(score, 0.0)
        self.assertIn("btc_unavailable", reason)

    def test_btc_trend_score_live_chg(self):
        eng = CrossAssetRegimeEngine()
        eng._btc_closes = [float(i) for i in range(30)]
        eng._btc_price = eng._btc_closes[-1] * 1.05  # > +2%
        score, reason = eng._btc_trend_score()
        self.assertIn("live_over_last", reason)
        eng._btc_price = eng._btc_closes[-1] * 0.95  # < -2%
        score, reason = eng._btc_trend_score()
        self.assertIn("live_under_last", reason)
        # within +/-2% -> neither live branch taken
        eng._btc_price = eng._btc_closes[-1] * 1.01
        score, reason = eng._btc_trend_score()
        self.assertNotIn("live_over_last", reason)
        self.assertNotIn("live_under_last", reason)

    def test_yfinance_trend_score(self):
        eng = CrossAssetRegimeEngine()
        eng._fetch_close_series = lambda sym: [float(i) for i in range(20)]
        score, reason = eng._yfinance_trend_score("SPY")
        self.assertIsInstance(score, float)
        eng._fetch_close_series = lambda sym: []
        score, reason = eng._yfinance_trend_score("SPY")
        self.assertEqual(score, 0.0)

    def test_yfinance_delta_score(self):
        eng = CrossAssetRegimeEngine()
        eng._fetch_close_series = lambda sym: [100.0, 110.0]
        score, reason = eng._yfinance_delta_score("DX-Y.NYB", higher_is_risk_off=True)
        self.assertGreater(score, 0)
        # higher_is_risk_off defaults False -> else branch
        eng._fetch_close_series = lambda sym: [100.0, 110.0]
        score, reason = eng._yfinance_delta_score("DX-Y.NYB")
        self.assertGreater(score, 0)
        eng._fetch_close_series = lambda sym: [100.0]
        score, reason = eng._yfinance_delta_score("DX-Y.NYB")
        self.assertEqual(score, 0.0)

    def test_vix_risk_score(self):
        eng = CrossAssetRegimeEngine()
        # last >= 20 and chg > 0 -> both `if` branches True
        eng._fetch_close_series = lambda sym: [30.0, 40.0]
        score, reason = eng._vix_risk_score()
        self.assertGreater(score, 0)
        # last < 20 (False) but chg > 0 (True)
        eng._fetch_close_series = lambda sym: [10.0, 15.0]
        score, reason = eng._vix_risk_score()
        self.assertGreater(score, 0)
        # last < 20 (False) and chg <= 0 (False) -> covers both False branches
        eng._fetch_close_series = lambda sym: [18.0, 15.0]
        score, reason = eng._vix_risk_score()
        self.assertEqual(score, 0.0)
        # last >= 20 (True) and chg <= 0 (False)
        eng._fetch_close_series = lambda sym: [25.0, 20.0]
        score, reason = eng._vix_risk_score()
        self.assertEqual(score, 0.0)
        # insufficient series
        eng._fetch_close_series = lambda sym: [100.0]
        score, reason = eng._vix_risk_score()
        self.assertEqual(score, 0.0)

    def test_trend_score_from_closes(self):
        eng = CrossAssetRegimeEngine()
        series = [float(i) for i in range(30)]
        score, reason = eng._trend_score_from_closes(series, "x")
        self.assertIsInstance(score, float)
        score, reason = eng._trend_score_from_closes([1.0, 2.0], "x")
        self.assertEqual(score, 0.0)

    def test_fetch_close_series(self):
        eng = CrossAssetRegimeEngine()
        eng._breaker.failure_threshold = 1
        eng._breaker.on_failure(RuntimeError())
        self.assertEqual(eng._fetch_close_series("SPY"), [])
        # restore breaker
        eng._breaker = SourceCircuitBreaker("t")
        with mock.patch("coinbase.src.yahoo_chart.fetch_closes", return_value=[1.0] * 30):
            self.assertEqual(len(eng._fetch_close_series("SPY")), 30)
        # exception path
        with mock.patch("coinbase.src.yahoo_chart.fetch_closes", side_effect=RuntimeError("x")):
            self.assertEqual(eng._fetch_close_series("SPY"), [])


if __name__ == "__main__":
    unittest.main()
