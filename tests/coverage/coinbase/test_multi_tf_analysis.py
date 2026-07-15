import unittest
from datetime import datetime, timezone
from unittest import mock

from coinbase.src import multi_tf_analysis as mta


class TestIndicators(unittest.TestCase):
    def test_sma_short(self):
        self.assertEqual(mta._sma([1, 2, 3], 5), [])

    def test_sma(self):
        self.assertEqual(mta._sma(list(range(1, 6)), 5), [3.0])
        self.assertEqual(mta._sma(list(range(1, 7)), 3), [2.0, 3.0, 4.0, 5.0])

    def test_ema_short(self):
        self.assertEqual(mta._ema([1, 2], 5), [])

    def test_ema(self):
        out = mta._ema([1, 2, 3, 4, 5], 3)
        self.assertEqual(len(out), 3)
        self.assertAlmostEqual(out[-1], 4.0, places=1)

    def test_atr_short(self):
        self.assertEqual(mta._atr([1, 2], [1, 2], [1, 2]), [])

    def test_atr(self):
        highs = [10, 11, 12, 13]
        lows = [9, 10, 11, 12]
        closes = [9.5, 10.5, 11.5, 12.5]
        out = mta._atr(highs, lows, closes, period=3)
        self.assertTrue(len(out) >= 1)

    def test_adx_short(self):
        self.assertEqual(mta._adx([1, 2], [1, 2], [1, 2]), 0.0)

    def test_adx_empty_tr(self):
        closes = list(range(1, 40))
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        dx = mta._adx(closes, highs, lows)
        self.assertIsInstance(dx, float)

    def test_adx_long(self):
        closes = list(range(1, 60))
        highs = [c + 2 for c in closes]
        lows = [c - 1 for c in closes]
        dx = mta._adx(closes, highs, lows)
        self.assertGreaterEqual(dx, 0.0)


class TestDetectBitcoinCycle(unittest.TestCase):
    def _now(self, dt):
        return mock.patch.object(mta, "datetime", MockDateTime(dt))

    def test_pre_halving_genesis(self):
        # a date before 2012-11-28
        now = datetime(2010, 1, 1, tzinfo=timezone.utc)
        with self._now(now):
            p = mta.detect_bitcoin_cycle()
        self.assertEqual(p.name, "pre_halving_genesis")

    def test_halving_day(self):
        now = datetime(2024, 4, 20, tzinfo=timezone.utc)
        with self._now(now):
            p = mta.detect_bitcoin_cycle()
        self.assertEqual(p.name, "halving_day")

    def test_accumulation(self):
        now = datetime(2024, 6, 1, tzinfo=timezone.utc)  # ~1.3 months
        with self._now(now):
            p = mta.detect_bitcoin_cycle()
        self.assertEqual(p.name, "accumulation")
        self.assertEqual(p.bias, "bullish")

    def test_expansion(self):
        now = datetime(2025, 6, 1, tzinfo=timezone.utc)  # ~13 months
        with self._now(now):
            p = mta.detect_bitcoin_cycle()
        self.assertEqual(p.name, "expansion")

    def test_mania(self):
        now = datetime(2026, 9, 1, tzinfo=timezone.utc)  # ~28 months
        with self._now(now):
            p = mta.detect_bitcoin_cycle()
        self.assertEqual(p.name, "mania")

    def test_distribution(self):
        now = datetime(2027, 9, 1, tzinfo=timezone.utc)  # ~40 months
        with self._now(now):
            p = mta.detect_bitcoin_cycle()
        self.assertEqual(p.name, "distribution")
        self.assertEqual(p.bias, "bearish")

    def test_capitulation(self):
        now = datetime(2028, 9, 1, tzinfo=timezone.utc)  # ~52 months
        with self._now(now):
            p = mta.detect_bitcoin_cycle()
        self.assertEqual(p.name, "capitulation")
        self.assertEqual(p.bias, "bearish")


class MockDateTime:
    def __init__(self, fixed):
        self._fixed = fixed

    def now(self, tz=None):
        return self._fixed

    def __getattr__(self, name):
        return getattr(datetime, name)


class TestMacroTrendAnalyzer(unittest.TestCase):
    def setUp(self):
        self.an = mta.MacroTrendAnalyzer(cache_ttl=0.0)

    def _candles(self, n=300, start=100.0, step=0.5):
        closes = [start + i * step for i in range(n)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        volumes = [1000.0] * n
        return {"closes": closes, "highs": highs, "lows": lows, "volumes": volumes}

    def test_check_set_cache(self):
        an = mta.MacroTrendAnalyzer(cache_ttl=100.0)
        self.assertIsNone(an._check_cache("k"))
        an._set_cache("k", "v")
        self.assertEqual(an._check_cache("k"), "v")

    def test_fetch_higher_tf_missing(self):
        with mock.patch.object(mta, "fetch_candles_batch_sync", return_value={}):
            self.assertEqual(self.an.fetch_higher_tf("BTC-USD"), ([], [], [], [], "daily"))

    def test_fetch_higher_tf_no_arrays(self):
        with mock.patch.object(mta, "fetch_candles_batch_sync", return_value={"BTC-USD": []}), \
                mock.patch.object(mta, "candle_arrays", return_value=None):
            self.assertEqual(self.an.fetch_higher_tf("BTC-USD"), ([], [], [], [], "daily"))

    def test_fetch_higher_tf_success(self):
        candles = {"BTC-USD": object()}
        arr = self._candles()
        with mock.patch.object(mta, "fetch_candles_batch_sync", return_value=candles), \
                mock.patch.object(mta, "candle_arrays", return_value=arr):
            closes, highs, lows, vols, tf = self.an.fetch_higher_tf("BTC-USD")
        self.assertEqual(len(closes), 300)
        self.assertEqual(tf, "daily")

    def test_compute_tf_stats_short(self):
        s = self.an.compute_tf_stats([1, 2, 3], [1, 2, 3], [1, 2, 3], "daily")
        self.assertEqual(s.tf_name, "daily")
        self.assertEqual(s.trend, "")

    def test_compute_tf_stats_full(self):
        arr = self._candles(n=250)
        s = self.an.compute_tf_stats(arr["closes"], arr["highs"], arr["lows"], "daily")
        self.assertGreater(s.sma50, 0)
        self.assertGreater(s.sma200, 0)
        self.assertGreater(s.adx, 0)
        self.assertIn(s.trend, ("bull", "bear", "neutral"))

    def test_compute_tf_stats_no_sma200(self):
        arr = self._candles(n=120)  # >=50 but <200 -> sma200 stays 0
        s = self.an.compute_tf_stats(arr["closes"], arr["highs"], arr["lows"], "daily")
        self.assertEqual(s.sma200, 0.0)
        # above_sma200 is None -> trend neutral path
        self.assertIn(s.trend, ("bull", "bear", "neutral"))

    def test_compute_tf_stats_no_atr(self):
        # highs/lows flat so ATR may be 0; just ensure no crash
        arr = self._candles(n=250)
        # make highs==lows so tr=0
        arr2 = dict(arr)
        arr2["highs"] = list(arr["closes"])
        arr2["lows"] = list(arr["closes"])
        s = self.an.compute_tf_stats(arr2["closes"], arr2["highs"], arr2["lows"], "daily")
        self.assertIsInstance(s.atr_pct, float)

    def test_compute_tf_stats_bear(self):
        arr = self._candles(n=250)
        # current below sma200 and ema12 < ema26 -> bear
        arr["closes"] = [300.0 - i * 0.1 for i in range(250)]
        arr["highs"] = [c + 1 for c in arr["closes"]]
        arr["lows"] = [c - 1 for c in arr["closes"]]
        s = self.an.compute_tf_stats(arr["closes"], arr["highs"], arr["lows"], "daily")
        self.assertEqual(s.trend, "bear")

    def test_analyze_cache_hit(self):
        an = mta.MacroTrendAnalyzer(cache_ttl=100.0)
        fake = mta.CompositeMacroSignal(bias="bullish", confidence=0.9,
                                        risk_multiplier=1.2, allows_new_longs=True,
                                        allows_new_shorts=False, btc_price=100.0)
        an._set_cache("macro_tf", fake)
        res = an.analyze()
        self.assertIs(res, fake)

    def test_analyze_insufficient_data(self):
        arr = self._candles(n=20)
        with mock.patch.object(mta, "fetch_candles_batch_sync", return_value={"BTC-USD": object()}), \
                mock.patch.object(mta, "candle_arrays", return_value=arr):
            res = self.an.analyze(btc_price=123.0)
        self.assertEqual(res.bias, "neutral")
        self.assertEqual(res.btc_price, 123.0)
        self.assertEqual(res.cycle_phase, "unknown")

    def _patch_fetch(self, arr):
        return mock.patch.object(mta, "fetch_candles_batch_sync", return_value={"BTC-USD": object()}), \
            mock.patch.object(mta, "candle_arrays", return_value=arr)

    def _stats(self, price_vs=10.0, ema12=200.0, ema26=100.0, adx=30.0,
               recent=15.0, atr_pct=1.0, trend="bull", current=200.0):
        s = mta.TFStats(tf_name="daily")
        s.sma50 = 100.0
        s.sma200 = 100.0
        s.ema12 = ema12
        s.ema26 = ema26
        s.adx = adx
        s.price_vs_sma200_pct = price_vs
        s.recent_return_pct = recent
        s.atr_pct = atr_pct
        s.trend = trend
        s.current = current
        return s

    def test_analyze_bullish(self):
        arr = self._candles(n=250)
        stats = mta.TFStats("daily")
        stats.sma50 = 100.0
        stats.sma200 = 100.0
        stats.ema12 = 200.0
        stats.ema26 = 100.0
        stats.adx = 30.0
        stats.price_vs_sma200_pct = 10.0
        stats.recent_return_pct = 15.0
        stats.atr_pct = 1.0
        stats.trend = "bull"
        cycle = mta.MacroCyclePhase("expansion", 12, "bullish", 1.25, "desc")
        with self._patch_fetch(arr)[0], self._patch_fetch(arr)[1], \
                mock.patch.object(self.an, "compute_tf_stats", return_value=stats), \
                mock.patch.object(mta, "detect_bitcoin_cycle", return_value=cycle):
            res = self.an.analyze(btc_price=arr["closes"][-1] * 1.005)
        self.assertEqual(res.bias, "bullish")
        self.assertTrue(res.allows_new_longs)
        self.assertFalse(res.allows_new_shorts)

    def test_analyze_bullish_with_live_momentum(self):
        arr = self._candles(n=250)
        stats = mta.TFStats("daily")
        stats.sma200 = 100.0
        stats.ema12 = 200.0
        stats.ema26 = 100.0
        stats.adx = 30.0
        stats.price_vs_sma200_pct = 10.0
        stats.recent_return_pct = 15.0
        stats.atr_pct = 1.0
        stats.trend = "bull"
        cycle = mta.MacroCyclePhase("expansion", 12, "bullish", 1.25, "desc")
        with self._patch_fetch(arr)[0], self._patch_fetch(arr)[1], \
                mock.patch.object(self.an, "compute_tf_stats", return_value=stats), \
                mock.patch.object(mta, "detect_bitcoin_cycle", return_value=cycle):
            # btc_price 3% above last close -> live momentum positive
            res = self.an.analyze(btc_price=arr["closes"][-1] * 1.03)
        self.assertEqual(res.bias, "bullish")
        self.assertGreater(res.risk_multiplier, 1.25)

    def test_analyze_bearish(self):
        arr = self._candles(n=250)
        stats = mta.TFStats("daily")
        stats.sma200 = 100.0
        stats.ema12 = 50.0
        stats.ema26 = 100.0
        stats.adx = 30.0
        stats.price_vs_sma200_pct = -10.0
        stats.recent_return_pct = -15.0
        stats.atr_pct = 1.0
        stats.trend = "bear"
        cycle = mta.MacroCyclePhase("distribution", 40, "bearish", 0.7, "desc")
        with self._patch_fetch(arr)[0], self._patch_fetch(arr)[1], \
                mock.patch.object(self.an, "compute_tf_stats", return_value=stats), \
                mock.patch.object(mta, "detect_bitcoin_cycle", return_value=cycle):
            res = self.an.analyze(btc_price=arr["closes"][-1] * 0.99)
        self.assertEqual(res.bias, "bearish")
        self.assertFalse(res.allows_new_longs)
        self.assertTrue(res.allows_new_shorts)

    def test_analyze_bearish_neutralized_by_live(self):
        # 4 bearish / 7 total => ratio 0.571 (>=0.5 and <0.7)
        arr = self._candles(n=250)
        stats = mta.TFStats("daily")
        stats.sma200 = 100.0
        stats.ema12 = 50.0
        stats.ema26 = 100.0
        stats.adx = 30.0
        stats.price_vs_sma200_pct = -10.0
        stats.recent_return_pct = 0.0   # not <-10 -> not counted
        stats.atr_pct = 1.0
        stats.trend = "bear"
        cycle = mta.MacroCyclePhase("distribution", 40, "bearish", 0.7, "desc")
        with self._patch_fetch(arr)[0], self._patch_fetch(arr)[1], \
                mock.patch.object(self.an, "compute_tf_stats", return_value=stats), \
                mock.patch.object(mta, "detect_bitcoin_cycle", return_value=cycle):
            # live momentum positive and bearish_ratio<0.7 -> neutralized
            res = self.an.analyze(btc_price=arr["closes"][-1] * 1.03)
        self.assertEqual(res.bias, "neutral")

    def test_analyze_risk_off(self):
        # 3 bearish / 7 total => ratio 0.428 (0.3 <= ratio < 0.5)
        arr = self._candles(n=250)
        stats = mta.TFStats("daily")
        stats.sma200 = 100.0
        stats.ema12 = 50.0
        stats.ema26 = 100.0
        stats.adx = 30.0
        stats.price_vs_sma200_pct = -3.0   # not <-5 -> not counted
        stats.recent_return_pct = -3.0     # not <-10 -> not counted
        stats.atr_pct = 1.0
        stats.trend = "bear"               # adx>25 & bear -> counted
        cycle = mta.MacroCyclePhase("distribution", 40, "bearish", 0.7, "desc")
        with self._patch_fetch(arr)[0], self._patch_fetch(arr)[1], \
                mock.patch.object(self.an, "compute_tf_stats", return_value=stats), \
                mock.patch.object(mta, "detect_bitcoin_cycle", return_value=cycle):
            res = self.an.analyze(btc_price=arr["closes"][-1] * 0.99)
        self.assertEqual(res.bias, "risk_off")

    def test_analyze_risk_off_neutralized_by_live(self):
        arr = self._candles(n=250)
        stats = mta.TFStats("daily")
        stats.sma200 = 100.0
        stats.ema12 = 50.0
        stats.ema26 = 100.0
        stats.adx = 30.0
        stats.price_vs_sma200_pct = -3.0
        stats.recent_return_pct = -3.0
        stats.atr_pct = 1.0
        stats.trend = "bear"
        cycle = mta.MacroCyclePhase("distribution", 40, "bearish", 0.7, "desc")
        with self._patch_fetch(arr)[0], self._patch_fetch(arr)[1], \
                mock.patch.object(self.an, "compute_tf_stats", return_value=stats), \
                mock.patch.object(mta, "detect_bitcoin_cycle", return_value=cycle):
            res = self.an.analyze(btc_price=arr["closes"][-1] * 1.03)
        self.assertEqual(res.bias, "neutral")

    def test_analyze_neutral(self):
        arr = self._candles(n=250)
        stats = mta.TFStats("daily")
        stats.sma200 = 100.0
        stats.ema12 = 100.0
        stats.ema26 = 100.0
        stats.adx = 5.0
        stats.price_vs_sma200_pct = 0.0
        stats.recent_return_pct = 0.0
        stats.atr_pct = 1.0
        stats.trend = "neutral"
        cycle = mta.MacroCyclePhase("expansion", 12, "bullish", 1.25, "desc")
        with self._patch_fetch(arr)[0], self._patch_fetch(arr)[1], \
                mock.patch.object(self.an, "compute_tf_stats", return_value=stats), \
                mock.patch.object(mta, "detect_bitcoin_cycle", return_value=cycle):
            res = self.an.analyze(btc_price=arr["closes"][-1])
        self.assertEqual(res.bias, "neutral")

    def test_analyze_btc_price_zero(self):
        # exercises the `if btc_price > 0 and closes:` False branch (no live return)
        arr = self._candles(n=250)
        stats = mta.TFStats("daily")
        stats.sma200 = 100.0
        stats.ema12 = 200.0
        stats.ema26 = 100.0
        stats.adx = 5.0
        stats.price_vs_sma200_pct = 10.0
        stats.recent_return_pct = 15.0
        stats.atr_pct = 1.0
        stats.trend = "bull"
        cycle = mta.MacroCyclePhase("expansion", 12, "bullish", 1.25, "desc")
        with self._patch_fetch(arr)[0], self._patch_fetch(arr)[1], \
                mock.patch.object(self.an, "compute_tf_stats", return_value=stats), \
                mock.patch.object(mta, "detect_bitcoin_cycle", return_value=cycle):
            res = self.an.analyze(btc_price=0.0)
        # 4 bullish / 7 -> ratio 0.571 < 0.6 -> neutral
        self.assertEqual(res.bias, "neutral")

    def test_analyze_low_adx(self):
        # adx <= 25 -> factor #3 block skipped (False branch of `if btc_adx > 25`)
        arr = self._candles(n=250)
        stats = mta.TFStats("daily")
        stats.sma200 = 100.0
        stats.ema12 = 200.0
        stats.ema26 = 100.0
        stats.adx = 20.0
        stats.price_vs_sma200_pct = 10.0
        stats.recent_return_pct = 15.0
        stats.atr_pct = 1.0
        stats.trend = "bull"
        cycle = mta.MacroCyclePhase("expansion", 12, "bullish", 1.25, "desc")
        with self._patch_fetch(arr)[0], self._patch_fetch(arr)[1], \
                mock.patch.object(self.an, "compute_tf_stats", return_value=stats), \
                mock.patch.object(mta, "detect_bitcoin_cycle", return_value=cycle):
            res = self.an.analyze(btc_price=arr["closes"][-1])
        # 4 bullish / 7 -> ratio 0.571 < 0.6 -> neutral
        self.assertEqual(res.bias, "neutral")

    def test_analyze_mania_cycle(self):
        # cycle.bias == "neutral" (mania) -> neither bullish nor bearish branch taken
        arr = self._candles(n=250)
        stats = mta.TFStats("daily")
        stats.sma200 = 100.0
        stats.ema12 = 100.0
        stats.ema26 = 100.0
        stats.adx = 5.0
        stats.price_vs_sma200_pct = 0.0
        stats.recent_return_pct = 0.0
        stats.atr_pct = 1.0
        stats.trend = "neutral"
        cycle = mta.MacroCyclePhase("mania", 28, "neutral", 1.0, "desc")
        with self._patch_fetch(arr)[0], self._patch_fetch(arr)[1], \
                mock.patch.object(self.an, "compute_tf_stats", return_value=stats), \
                mock.patch.object(mta, "detect_bitcoin_cycle", return_value=cycle):
            res = self.an.analyze(btc_price=arr["closes"][-1])
        self.assertEqual(res.bias, "neutral")

    def test_analyze_high_atr(self):
        # atr_pct > 5 -> bearish factor added (factor #6 branch)
        arr = self._candles(n=250)
        stats = mta.TFStats("daily")
        stats.sma200 = 100.0
        stats.ema12 = 200.0
        stats.ema26 = 100.0
        stats.adx = 5.0
        stats.price_vs_sma200_pct = 10.0
        stats.recent_return_pct = 15.0
        stats.atr_pct = 8.0
        stats.trend = "bull"
        cycle = mta.MacroCyclePhase("expansion", 12, "bullish", 1.25, "desc")
        with self._patch_fetch(arr)[0], self._patch_fetch(arr)[1], \
                mock.patch.object(self.an, "compute_tf_stats", return_value=stats), \
                mock.patch.object(mta, "detect_bitcoin_cycle", return_value=cycle):
            res = self.an.analyze(btc_price=arr["closes"][-1])
        # 4 bullish / 1 bearish / 7 -> bullish ratio 0.571 < 0.6 -> neutral
        self.assertEqual(res.bias, "neutral")


class TestToDict(unittest.TestCase):
    def test_composite_to_dict(self):
        c = mta.CompositeMacroSignal(bias="bullish", confidence=0.8, risk_multiplier=1.2,
                                     allows_new_longs=True, allows_new_shorts=False,
                                     cycle_phase="expansion", tf_signals={"daily": "bull"},
                                     reason="r", btc_price=100.0)
        d = c.to_dict()
        self.assertEqual(d["bias"], "bullish")
        self.assertEqual(d["confidence"], 0.8)
        self.assertEqual(d["btc_price"], 100.0)


if __name__ == "__main__":
    unittest.main()
