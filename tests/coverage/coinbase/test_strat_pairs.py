import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from _strat_helpers import bars_from

from coinbase.src.protocols import Direction, InstrumentType
import coinbase.src.strat_pairs as _sp
from coinbase.src.strat_pairs import (
    CointegratedPairsStrategy,
    PairState,
    _ols_hedge_ratio,
    _adf_test,
    _adf_pvalue,
    DEFAULT_PAIRS,
)


def make_strat(**kw):
    return CointegratedPairsStrategy(**kw)


class TestPairsOnBar(unittest.TestCase):
    def _bar(self, close):
        return bars_from([close])[0]

    def _prep(self, s, eth, btc, product="ETH-USD"):
        s.set_product_id(product)
        s._price_cache = {"ETH-USD": eth, "BTC-USD": btc}
        s._bars_seen = 300
        for st in s._pair_states.values():
            st.last_check_bars = 0
        _sp._ols_hedge_ratio = lambda y, x: 1.0
        _sp._adf_test = lambda r: 0.05

    def test_no_product(self):
        s = make_strat()
        self.assertIsNone(s.on_bar(self._bar(100.0), []))

    def test_short_divergence(self):
        s = make_strat()
        self._prep(s, [100.0] * 29 + [130.0], [100.0] * 30)
        setup = s.on_bar(self._bar(130.0), [])
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.SHORT)
        self.assertEqual(setup.instrument_type, InstrumentType.SPOT)

    def test_long_divergence(self):
        s = make_strat()
        self._prep(s, [100.0] * 29 + [70.0], [100.0] * 30)
        setup = s.on_bar(self._bar(70.0), [])
        self.assertIsNotNone(setup)
        self.assertEqual(setup.direction, Direction.LONG)

    def test_not_cointegrated(self):
        s = make_strat()
        self._prep(s, [100.0] * 29 + [130.0], [100.0] * 30)
        _sp._adf_test = lambda r: 0.5
        self.assertIsNone(s.on_bar(self._bar(130.0), []))

    def test_retrain_not_due(self):
        s = make_strat()
        self._prep(s, [100.0] * 29 + [130.0], [100.0] * 30)
        s._bars_seen = 10
        for st in s._pair_states.values():
            st.last_check_bars = 10
        self.assertIsNone(s.on_bar(self._bar(130.0), []))

    def test_sec_matched_continue(self):
        s = make_strat()
        s.set_product_id("BTC-USD")
        s._price_cache = {"ETH-USD": [100.0] * 30, "BTC-USD": [100.0] * 30}
        _sp._ols_hedge_ratio = lambda y, x: 1.0
        _sp._adf_test = lambda r: 0.05
        self.assertIsNone(s.on_bar(self._bar(100.0), []))

    def test_prices_insufficient(self):
        s = make_strat()
        self._prep(s, [100.0] * 20, [100.0] * 20)
        self.assertIsNone(s.on_bar(self._bar(100.0), []))

    def test_zero_std(self):
        s = make_strat()
        self._prep(s, [100.0] * 30, [100.0] * 30)
        self.assertIsNone(s.on_bar(self._bar(100.0), []))

    def test_in_position_reset(self):
        s = make_strat()
        spread = [float(i + 1) for i in range(29)] + [14.5]
        self._prep(s, [100.0 + v for v in spread], [100.0] * 30)
        st = s._pair_states[("ETH-USD", "BTC-USD")]
        st.in_position = True
        st.position_direction = Direction.LONG
        self.assertIsNone(s.on_bar(self._bar(114.5), []))
        self.assertFalse(st.in_position)

    def test_cache_cleanup(self):
        s = make_strat()
        extra = {f"X{i}-USD": [] for i in range(51)}
        self._prep(s, [100.0] * 29 + [130.0], [100.0] * 30)
        _sp._adf_test = lambda r: 0.5
        s._price_cache.update(extra)
        self.assertIsNone(s.on_bar(self._bar(130.0), []))
        empty_keys = [k for k, v in s._price_cache.items() if len(v) == 0]
        self.assertEqual(len(empty_keys), 0)


class TestPairsHelpers(unittest.TestCase):
    def test_ols_short(self):
        self.assertEqual(_ols_hedge_ratio([1, 2], [1, 2]), 1.0)

    def test_ols_zero_den(self):
        self.assertEqual(_ols_hedge_ratio([1.0] * 10, [2.0] * 10), 1.0)

    def test_ols_normal(self):
        y = [float(i) for i in range(10)]
        x = [float(i) for i in range(10)]
        r = _ols_hedge_ratio(y, x)
        self.assertAlmostEqual(r, 1.0, places=3)

    def test_adf_short(self):
        self.assertEqual(_adf_test([1.0] * 5), 0.5)

    def test_adf_zero_den(self):
        self.assertEqual(_adf_test([1.0] * 10), 0.5)

    def test_adf_normal(self):
        resid = [float(i) for i in range(10)]
        self.assertGreaterEqual(_adf_test(resid), 0.0)

    def test_adf_pvalue(self):
        self.assertAlmostEqual(_adf_pvalue(-4.0, 30), 0.01)
        self.assertAlmostEqual(_adf_pvalue(-3.0, 30), 0.05)
        self.assertAlmostEqual(_adf_pvalue(-2.6, 30), 0.10)
        self.assertAlmostEqual(_adf_pvalue(0.0, 30), 0.50)

    def test_store_price(self):
        s = make_strat()
        s._store_price("ETH-USD", 100.0)
        self.assertEqual(len(s._price_cache["ETH-USD"]), 1)

    def test_store_price_truncate(self):
        s = make_strat(lookback=50)
        s._price_cache["ETH-USD"] = [100.0] * 200
        s._store_price("ETH-USD", 100.0)
        self.assertLessEqual(len(s._price_cache["ETH-USD"]), 100)

    def test_pair_state_ready(self):
        st = PairState(primary="A", secondary="B")
        self.assertFalse(st.is_ready)
        st.spread = [1.0] * 30
        self.assertFalse(st.is_ready)
        st.cointegrated = True
        self.assertTrue(st.is_ready)

    def test_default_pairs_none(self):
        self.assertIn(None, DEFAULT_PAIRS)

    def test_atr_short(self):
        self.assertEqual(CointegratedPairsStrategy._estimate_atr([1, 2], [], []), 0.0)

    def test_atr_normal(self):
        closes = [100 + i for i in range(20)]
        self.assertGreater(CointegratedPairsStrategy._estimate_atr(closes, [], []), 0.0)


if __name__ == "__main__":
    unittest.main()
