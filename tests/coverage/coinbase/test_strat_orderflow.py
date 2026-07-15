import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from _strat_helpers import bars_from

from coinbase.src.protocols import Direction
from coinbase.src.strat_orderflow import (
    SmartMoneyFlowStrategy,
    OrderFlowState,
)


class TestOrderFlowOnBar(unittest.TestCase):
    def _bars(self, n=25, base=100.0):
        closes = [base + i * 0.1 for i in range(n)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        vols = [1000.0] * n
        return bars_from(closes, highs=highs, lows=lows, vols=vols)

    def test_no_product(self):
        s = SmartMoneyFlowStrategy()
        bars = self._bars()
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_insufficient_history(self):
        s = SmartMoneyFlowStrategy()
        s.set_product_id("ETH-USD")
        bars = self._bars(15)
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_atr_zero(self):
        s = SmartMoneyFlowStrategy()
        s.set_product_id("ETH-USD")
        closes = [100.0] * 25
        bars = bars_from(closes, highs=closes, lows=closes)
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_no_setup(self):
        s = SmartMoneyFlowStrategy()
        s.set_product_id("ETH-USD")
        bars = self._bars()
        self.assertIsNone(s.on_bar(bars[-1], bars[:-1]))

    def test_cvd_setup(self):
        s = SmartMoneyFlowStrategy()
        s.set_product_id("ETH-USD")
        bars = self._bars()
        s._check_cvd_divergence = lambda bar, state, atr: None
        s._check_volume_absorption = lambda bar, state, atr, vols: None
        s._check_ad_line = lambda bar, closes, vols, atr: _fake_setup()
        self.assertIsNotNone(s.on_bar(bars[-1], bars[:-1]))

    def test_absorption_setup(self):
        s = SmartMoneyFlowStrategy()
        s.set_product_id("ETH-USD")
        bars = self._bars()
        s._check_cvd_divergence = lambda bar, state, atr: None
        s._check_volume_absorption = lambda bar, state, atr, vols: _fake_setup()
        s._check_ad_line = lambda bar, closes, vols, atr: None
        self.assertIsNotNone(s.on_bar(bars[-1], bars[:-1]))

    def test_name(self):
        self.assertEqual(SmartMoneyFlowStrategy().name(), "smart_money_flow")

    def test_cvd_truncation(self):
        s = SmartMoneyFlowStrategy()
        s.set_product_id("ETH-USD")
        bars = self._bars()
        for _ in range(35):
            s.on_bar(bars[-1], bars[:-1])
        st = s._state["ETH-USD"]
        self.assertLessEqual(len(st.bid_volume), s.cvd_lookback)

    def test_absorption_large_range(self):
        s = SmartMoneyFlowStrategy()
        st = OrderFlowState()
        st.bid_volume = [0.4] * 20
        st.ask_volume = [0.6] * 20
        vols = [100.0] * 19 + [100000.0]
        bar = bars_from([100.0], highs=[200.0], lows=[50.0], opens=[100.0],
                        vols=[100000.0])[0]
        self.assertIsNone(s._check_volume_absorption(bar, st, 1.0, vols))


def _fake_setup():
    from coinbase.src.protocols import Direction, Bar, BracketSetup
    return BracketSetup(direction=Direction.SHORT, entry_price=100, stop_price=98,
                        target_price=104, risk_reward=2.0, confidence=0.5,
                        reason="x", strategy_name="smart_money_flow", atr=1.0)


class TestCVDState(unittest.TestCase):
    def test_direction_neutral_short(self):
        st = OrderFlowState()
        self.assertEqual(st.cvd_direction, "neutral")

    def test_direction_rising(self):
        st = OrderFlowState()
        st.cvd = [100.0] * 5
        st.cvd[-1] = 105.0
        self.assertEqual(st.cvd_direction, "rising")

    def test_direction_falling(self):
        st = OrderFlowState()
        st.cvd = [100.0] * 5
        st.cvd[-1] = 95.0
        self.assertEqual(st.cvd_direction, "falling")

    def test_direction_neutral(self):
        st = OrderFlowState()
        st.cvd = [100.0] * 5
        st.cvd[-1] = 100.0
        self.assertEqual(st.cvd_direction, "neutral")


class TestCheckCVD(unittest.TestCase):
    def _state(self, cvd_vals, bid_vals):
        st = OrderFlowState()
        st.cvd = cvd_vals
        st.bid_volume = bid_vals
        return st

    def test_short(self):
        s = SmartMoneyFlowStrategy()
        st = self._state([1.0] * 20, [1.0] * 20)
        s._trend = lambda v, n: 0.05 if v == [100.0] else -0.05
        bar = bars_from([100.0])[0]
        setup = s._check_cvd_divergence(bar, st, 1.0)
        self.assertEqual(setup.direction, Direction.SHORT)

    def test_long(self):
        s = SmartMoneyFlowStrategy()
        st = self._state([1.0] * 20, [1.0] * 20)
        s._trend = lambda v, n: -0.05 if v == [100.0] else 0.05
        bar = bars_from([100.0])[0]
        setup = s._check_cvd_divergence(bar, st, 1.0)
        self.assertEqual(setup.direction, Direction.LONG)

    def test_no_data(self):
        s = SmartMoneyFlowStrategy()
        st = self._state([1.0] * 5, [1.0] * 5)
        bar = bars_from([100.0])[0]
        self.assertIsNone(s._check_cvd_divergence(bar, st, 1.0))

    def test_no_divergence(self):
        s = SmartMoneyFlowStrategy()
        st = self._state([1.0] * 20, [1.0] * 20)
        s._trend = lambda v, n: 0.0
        bar = bars_from([100.0])[0]
        self.assertIsNone(s._check_cvd_divergence(bar, st, 1.0))

    def test_low_rr(self):
        s = SmartMoneyFlowStrategy()
        st = self._state([1.0] * 20, [1.0] * 20)
        s._trend = lambda v, n: 0.05 if v == [100.0] else -0.05
        bar = bars_from([100.0])[0]
        setup = s._check_cvd_divergence(bar, st, 100.0)
        # huge atr -> rr < 1.2? rr is constant 1.5, so still returns
        self.assertIsNotNone(setup)


class TestCheckAbsorption(unittest.TestCase):
    def _state(self, bid=0.4, ask=0.6, n=20):
        st = OrderFlowState()
        st.bid_volume = [bid] * n
        st.ask_volume = [ask] * n
        return st

    def test_vol_not_spiked(self):
        s = SmartMoneyFlowStrategy()
        st = self._state()
        vols = [1000.0] * 20
        bar = bars_from([100.0], vols=[1000.0])[0]
        self.assertIsNone(s._check_volume_absorption(bar, st, 1.0, vols))

    def test_short(self):
        s = SmartMoneyFlowStrategy()
        st = self._state(bid=0.4, ask=0.6)
        vols = [100.0] * 19 + [100000.0]
        bar = bars_from([101.0], highs=[101.2], lows=[100.9], opens=[100.5],
                        vols=[100000.0])[0]
        setup = s._check_volume_absorption(bar, st, 1.0, vols)
        self.assertEqual(setup.direction, Direction.SHORT)

    def test_long(self):
        s = SmartMoneyFlowStrategy()
        st = self._state(bid=0.6, ask=0.4)
        vols = [100.0] * 19 + [100000.0]
        bar = bars_from([99.0], highs=[99.2], lows=[98.5], opens=[99.5],
                        vols=[100000.0])[0]
        setup = s._check_volume_absorption(bar, st, 1.0, vols)
        self.assertEqual(setup.direction, Direction.LONG)

    def test_neutral(self):
        s = SmartMoneyFlowStrategy()
        st = self._state(bid=0.5, ask=0.5)
        vols = [100.0] * 19 + [100000.0]
        bar = bars_from([100.0], highs=[100.2], lows=[99.8], opens=[100.0],
                        vols=[100000.0])[0]
        self.assertIsNone(s._check_volume_absorption(bar, st, 1.0, vols))

    def test_insufficient(self):
        s = SmartMoneyFlowStrategy()
        st = self._state(n=3)
        vols = [100.0] * 5
        bar = bars_from([100.0])[0]
        self.assertIsNone(s._check_volume_absorption(bar, st, 1.0, vols))

    def test_low_rr(self):
        s = SmartMoneyFlowStrategy()
        st = self._state(bid=0.4, ask=0.6)
        vols = [100.0] * 19 + [100000.0]
        bar = bars_from([101.0], highs=[101.2], lows=[100.9], opens=[100.5],
                        vols=[100000.0])[0]
        setup = s._check_volume_absorption(bar, st, 100.0, vols)
        self.assertIsNotNone(setup)


class TestCheckAD(unittest.TestCase):
    def _bar(self):
        return bars_from([100.0])[0]

    def test_short(self):
        s = SmartMoneyFlowStrategy()
        closes = [100 + i for i in range(25)]
        vols = [1000.0] * 25
        cnt = {"n": 0}

        def _t(v, n):
            cnt["n"] += 1
            return 0.05 if cnt["n"] == 1 else -0.05

        s._trend = _t
        setup = s._check_ad_line(self._bar(), closes, vols, 1.0)
        self.assertEqual(setup.direction, Direction.SHORT)

    def test_long(self):
        s = SmartMoneyFlowStrategy()
        closes = [100 + i for i in range(25)]
        vols = [1000.0] * 25
        cnt = {"n": 0}

        def _t(v, n):
            cnt["n"] += 1
            return -0.05 if cnt["n"] == 1 else 0.05

        s._trend = _t
        setup = s._check_ad_line(self._bar(), closes, vols, 1.0)
        self.assertEqual(setup.direction, Direction.LONG)

    def test_none(self):
        s = SmartMoneyFlowStrategy()
        closes = [100 + i for i in range(25)]
        vols = [1000.0] * 25
        s._trend = lambda v, n: 0.0
        self.assertIsNone(s._check_ad_line(self._bar(), closes, vols, 1.0))

    def test_insufficient(self):
        s = SmartMoneyFlowStrategy()
        closes = [100.0] * 5
        vols = [1000.0] * 5
        self.assertIsNone(s._check_ad_line(self._bar(), closes, vols, 1.0))


class TestOrderFlowHelpers(unittest.TestCase):
    def test_bid_ask_empty(self):
        self.assertEqual(SmartMoneyFlowStrategy._estimate_bid_ask_volume([]), (0.0, 0.0))

    def test_bid_ask_bull(self):
        bar = bars_from([101.0], opens=[100.0])[0]
        bid, ask = SmartMoneyFlowStrategy._estimate_bid_ask_volume([bar])
        self.assertGreater(ask, bid)

    def test_bid_ask_bear(self):
        bar = bars_from([99.0], opens=[100.0])[0]
        bid, ask = SmartMoneyFlowStrategy._estimate_bid_ask_volume([bar])
        self.assertGreater(bid, ask)

    def test_bid_ask_flat(self):
        bar = bars_from([100.0], opens=[100.0])[0]
        bid, ask = SmartMoneyFlowStrategy._estimate_bid_ask_volume([bar])
        self.assertEqual(bid, ask)

    def test_update_clusters_new(self):
        st = OrderFlowState()
        bar = bars_from([100.0])[0]
        SmartMoneyFlowStrategy._update_volume_clusters(st, bar, [1000.0])
        self.assertEqual(len(st.volume_clusters), 1)

    def test_update_clusters_existing(self):
        st = OrderFlowState()
        st.volume_clusters = [{"price": 100.0, "volume": 10.0, "count": 1}]
        bar = bars_from([100.0])[0]
        SmartMoneyFlowStrategy._update_volume_clusters(st, bar, [1000.0])
        self.assertEqual(len(st.volume_clusters), 1)
        self.assertEqual(st.volume_clusters[0]["count"], 2)

    def test_update_clusters_truncate(self):
        st = OrderFlowState()
        st.volume_clusters = [{"price": float(i), "volume": 1.0, "count": 1} for i in range(60)]
        bar = bars_from([100.0])[0]
        SmartMoneyFlowStrategy._update_volume_clusters(st, bar, [1000.0])
        self.assertLessEqual(len(st.volume_clusters), 50)

    def test_trend_short(self):
        self.assertEqual(SmartMoneyFlowStrategy._trend([1.0], 10), 0.0)

    def test_trend_normal(self):
        vals = [100 + i for i in range(10)]
        self.assertGreater(SmartMoneyFlowStrategy._trend(vals, 10), 0.0)

    def test_atr_short(self):
        self.assertEqual(SmartMoneyFlowStrategy._estimate_atr([1, 2], [1, 2], [1, 2]), 0.0)

    def test_atr_normal(self):
        closes = [100 + i for i in range(20)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        self.assertGreater(SmartMoneyFlowStrategy._estimate_atr(closes, highs, lows), 0.0)


if __name__ == "__main__":
    unittest.main()
