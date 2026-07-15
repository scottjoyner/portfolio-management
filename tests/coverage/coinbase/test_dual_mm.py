import unittest
from unittest import mock
from coinbase.src.dual_mm import (
    DualMarketMaker, MarketMakingStrategy, MMState, Quote,
)
from coinbase.src.protocols import Direction, Bar, BracketSetup


def make_bar(close, high=None, low=None, volume=1000.0):
    high = high if high is not None else close
    low = low if low is not None else close
    return Bar(timestamp=1.0, open=close, high=high, low=low, close=close, volume=volume)


def make_closes_bars(closes):
    return [Bar(timestamp=float(i), open=c, high=c, low=c, close=c, volume=1000.0)
            for i, c in enumerate(closes)]


class TestDualMarketMaker(unittest.TestCase):
    def test_state_enum(self):
        self.assertEqual(MMState.SPAWN.value, "spawn")

    def test_record_trade(self):
        mm = DualMarketMaker()
        mm.record_trade("BTC-USD", 1.5, 100.0)
        mm.record_trade("BTC-USD", -0.5, 101.0)
        self.assertAlmostEqual(mm._inventory["BTC-USD"], 1.0)

    def test_generate_quotes_refresh_gate(self):
        mm = DualMarketMaker(quote_refresh_bars=2)
        bar = make_bar(100.0)
        q1 = mm.generate_quotes("BTC-USD", bar, [])
        self.assertIsNotNone(q1)
        # immediately again -> cached (bars_since < refresh)
        q2 = mm.generate_quotes("BTC-USD", bar, [])
        self.assertIs(q2, q1)

    def test_generate_quotes_mid_zero(self):
        mm = DualMarketMaker()
        bar = make_bar(0.0, high=0.0, low=0.0)
        self.assertIsNone(mm.generate_quotes("BTC-USD", bar, []))

    def test_generate_quotes_flat_inventory(self):
        mm = DualMarketMaker()
        bar = make_bar(100.0)
        q = mm.generate_quotes("BTC-USD", bar, make_closes_bars([100.0] * 30))
        self.assertIsNotNone(q)
        self.assertGreater(q.spread_bps, 0)
        self.assertGreater(q.bid_price, 0)

    def test_generate_quotes_with_inventory_lean(self):
        mm = DualMarketMaker(inventory_limit=0.3)
        mm.record_trade("BTC-USD", 10.0, 100.0)
        bar = make_bar(100.0)
        q = mm.generate_quotes("BTC-USD", bar, make_closes_bars([100.0] * 30))
        self.assertIsNotNone(q)
        self.assertGreater(q.inventory_pct, 0)

    def test_generate_quotes_high_vol_ratio(self):
        closes = [100.0] * 80 + [100.0, 110.0] * 10
        mm = DualMarketMaker()
        bar = make_bar(closes[-1])
        q = mm.generate_quotes("BTC-USD", bar, make_closes_bars(closes[:-1]))
        self.assertIsNotNone(q)

    def test_record_fill(self):
        mm = DualMarketMaker()
        buy = mm.record_fill("BTC-USD", "buy", 100.0, 1.0)
        self.assertEqual(buy["inventory"], 1.0)
        sell = mm.record_fill("BTC-USD", "sell", 100.0, 0.5)
        self.assertAlmostEqual(sell["inventory"], 0.5)

    def test_summary(self):
        mm = DualMarketMaker()
        mm.record_trade("BTC-USD", 2.0, 100.0)
        s = mm.summary()
        self.assertEqual(s["active_inventories"]["BTC-USD"], 2.0)

    def test_atr_helper(self):
        mm = DualMarketMaker()
        self.assertEqual(mm._estimate_atr([1, 2], [2, 3], [1, 2]), 0.0)


class TestMarketMakingStrategy(unittest.TestCase):
    def setUp(self):
        self.mm = DualMarketMaker()
        self.strat = MarketMakingStrategy(self.mm)
        self.assertEqual(self.strat.name(), "market_making")

    def test_set_product_id(self):
        self.strat.set_product_id("ETH-USD")
        self.assertEqual(self.strat._current_pid, "ETH-USD")

    def test_on_bar_quote_none(self):
        bar = make_bar(0.0, high=0.0, low=0.0)
        self.assertIsNone(self.strat.on_bar(bar, []))

    def test_on_bar_zero_atr(self):
        bars = make_closes_bars([100.0] * 30)
        self.strat.set_product_id("BTC-USD")
        self.assertIsNone(self.strat.on_bar(bars[-1], bars[:-1]))

    def test_on_bar_volume_zero(self):
        self.strat.set_product_id("BTC-USD")
        self.mm.record_trade("BTC-USD", 5.0, 100.0)
        bars = make_closes_bars(list(range(20, 45)))
        bar = make_bar(bars[-1].close, volume=0.0)
        res = self.strat.on_bar(bar, bars[:-1])
        self.assertIsNotNone(res)

    def test_on_bar_volume_positive(self):
        self.strat.set_product_id("BTC-USD")
        self.mm.record_trade("BTC-USD", 5.0, 100.0)
        bars = make_closes_bars(list(range(20, 45)))
        bar = make_bar(bars[-1].close, volume=5000.0)
        res = self.strat.on_bar(bar, bars[:-1])
        self.assertIsNotNone(res)
        self.assertIn(res.direction, (Direction.LONG, Direction.SHORT))

    def test_atr_static(self):
        self.assertEqual(MarketMakingStrategy._estimate_atr([1, 2], [2, 3], [1, 2]), 0.0)

    def test_generate_quotes_lean_away_short_inventory(self):
        # negative inventory beyond limit -> bid leaned, inventory_pct capped at 1.0
        mm = DualMarketMaker(inventory_limit=0.3)
        mm.record_trade("BTC-USD", -100.0, 100.0)
        bar = make_bar(100.0)
        # short history so vol_ratio stays 1.0 (avoids the vol_ratio>2 branch)
        q = mm.generate_quotes("BTC-USD", bar, make_closes_bars([100.0] * 5))
        self.assertIsNotNone(q)
        self.assertEqual(q.inventory_pct, 1.0)
        self.assertGreater(q.bid_size, 0.0)

    def test_generate_quotes_high_vol_ratio_widens_spread(self):
        # vol_ratio uses the TAIL (most-recent) window of history; a volatile
        # tail triggers the wide-spread branch (vol_ratio > 2).
        closes = [100.0] * 100 + [100.0, 200.0] * 10
        mm = DualMarketMaker()
        bar = make_bar(closes[-1])
        q = mm.generate_quotes("BTC-USD", bar, make_closes_bars(closes[:-1]))
        self.assertIsNotNone(q)
        # vol_ratio > 2 widens the dynamic spread (base case flat history -> ~16bps)
        self.assertGreater(q.spread_bps, 16.0)

    def test_vol_ratio_keys_off_head_not_tail(self):
        # VERIFIES FIX (dual_mm.py): vol_ratio now uses the TAIL/most-recent
        # window of history, not the HEAD. Two histories share an identical
        # steady head but differ only in the tail: the tail-volatile one must
        # widen the spread more than the steady one.
        tail_vol = [100.0] * 100 + [100.0, 200.0] * 10
        tail_steady = [100.0] * 120
        mm_v = DualMarketMaker()
        mm_s = DualMarketMaker()
        q_v = mm_v.generate_quotes("X", make_bar(100.0), make_closes_bars(tail_vol[:-1]))
        q_s = mm_s.generate_quotes("X", make_bar(100.0), make_closes_bars(tail_steady[:-1]))
        self.assertGreater(q_v.spread_bps, q_s.spread_bps)

    def test_on_bar_flat_inventory_no_entries(self):
        mm = DualMarketMaker()
        strat = MarketMakingStrategy(mm)
        strat.set_product_id("BTC-USD")
        bars = make_closes_bars(list(range(20, 45)))
        # inventory == target == 0 -> neither LONG nor SHORT leg -> None
        self.assertIsNone(strat.on_bar(bars[-1], bars[:-1]))

    def test_on_bar_both_legs_random_choice(self):
        # target_inventory>0 with inventory between -target and +target -> both legs
        mm = DualMarketMaker(target_inventory=10.0)
        mm.record_trade("BTC-USD", 5.0, 100.0)
        strat = MarketMakingStrategy(mm)
        strat.set_product_id("BTC-USD")
        bars = make_closes_bars(list(range(20, 45)))
        with mock.patch("coinbase.src.dual_mm.random.choice", side_effect=lambda x: x[0]):
            res = strat.on_bar(bars[-1], bars[:-1])
        self.assertEqual(res.direction, Direction.LONG)


class TestDualMMQuoteRefreshBug(unittest.TestCase):
    """VERIFIES FIX (dual_mm.py:56-58): the quote-refresh counter is now
    incremented on the cached path, so quotes refresh after the cooldown."""

    def test_quote_refreshes_after_cooldown(self):
        mm = DualMarketMaker(quote_refresh_bars=1)
        q1 = mm.generate_quotes("X", make_bar(100.0), [])
        # Within cooldown -> cached (same object, counter was incremented).
        q_cached = mm.generate_quotes("X", make_bar(200.0), [])
        self.assertIs(q_cached, q1)
        # After the cooldown elapses -> a fresh quote reflecting new market data.
        q2 = mm.generate_quotes("X", make_bar(200.0), [])
        self.assertIsNot(q2, q1)
        self.assertNotEqual(q2.bid_price, q1.bid_price)

    def test_counter_incremented_on_cached_path(self):
        mm = DualMarketMaker(quote_refresh_bars=2)
        mm.generate_quotes("X", make_bar(100.0), [])
        self.assertEqual(mm._bars_since_quote["X"], 0)
        # cached call must bump the counter so a future refresh can occur
        mm.generate_quotes("X", make_bar(100.0), [])
        self.assertEqual(mm._bars_since_quote["X"], 1)


if __name__ == "__main__":
    unittest.main()
