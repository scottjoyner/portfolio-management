"""Tests for coinbase/src/slippage_model.py"""
import time
import unittest
from unittest import mock

from coinbase.src import slippage_model as sm


def make_book(product="BTC-USD", bid=99.0, ask=101.0, bid_size=10.0, ask_size=10.0):
    from coinbase.src.slippage_model import OrderBookSnapshot, OrderBookLevel
    return OrderBookSnapshot(
        product_id=product,
        timestamp=time.time(),
        bids=[OrderBookLevel(price=bid, size=bid_size)],
        asks=[OrderBookLevel(price=ask, size=ask_size)],
    )


class TestOrderBookSnapshot(unittest.TestCase):
    def test_empty(self):
        from coinbase.src.slippage_model import OrderBookSnapshot
        b = OrderBookSnapshot(product_id="X", timestamp=0)
        self.assertEqual(b.best_bid, 0.0)
        self.assertEqual(b.best_ask, 0.0)
        self.assertEqual(b.mid_price, 0.0)
        self.assertEqual(b.spread_bps, 0.0)

    def test_props(self):
        b = make_book()
        self.assertEqual(b.best_bid, 99.0)
        self.assertEqual(b.best_ask, 101.0)
        self.assertEqual(b.mid_price, 100.0)
        self.assertGreater(b.spread_bps, 0)

    def test_depth_buy(self):
        b = make_book()
        d = b.depth_at_bps(50000.0, sm.Side.BUY)
        self.assertGreaterEqual(d, 10.0)

    def test_depth_sell(self):
        b = make_book()
        d = b.depth_at_bps(50000.0, sm.Side.SELL)
        self.assertGreaterEqual(d, 10.0)

    def test_depth_zero_mid(self):
        from coinbase.src.slippage_model import OrderBookSnapshot
        b = OrderBookSnapshot(product_id="X", timestamp=0)
        self.assertEqual(b.depth_at_bps(50.0, sm.Side.BUY), 0.0)

    def test_depth_break(self):
        from coinbase.src.slippage_model import OrderBookSnapshot, OrderBookLevel
        b = OrderBookSnapshot(product_id="X", timestamp=time.time(),
                              asks=[OrderBookLevel(price=101, size=5),
                                    OrderBookLevel(price=200, size=5)])
        # 50 bps from mid (100) => max 105, so second level excluded
        d = b.depth_at_bps(50.0, sm.Side.BUY)
        self.assertEqual(d, 5.0)


class TestOrderBookCache(unittest.TestCase):
    def test_update_get(self):
        c = sm.OrderBookCache(ttl_s=5.0)
        c.update(make_book("BTC-USD"))
        self.assertIsNotNone(c.get("BTC-USD"))

    def test_get_expired(self):
        c = sm.OrderBookCache(ttl_s=5.0)
        b = make_book("BTC-USD")
        b.timestamp = time.time() - 100
        c.update(b)
        self.assertIsNone(c.get("BTC-USD"))

    def test_get_missing(self):
        c = sm.OrderBookCache()
        self.assertIsNone(c.get("NOPE"))

    def test_spread_default(self):
        c = sm.OrderBookCache()
        self.assertEqual(c.get_spread_bps("NOPE"), 10.0)

    def test_depth_default(self):
        c = sm.OrderBookCache()
        self.assertEqual(c.get_depth("NOPE", 5.0, sm.Side.BUY), 0.0)

    def test_depth_real(self):
        c = sm.OrderBookCache()
        c.update(make_book("BTC-USD"))
        self.assertGreater(c.get_depth("BTC-USD", 50000.0, sm.Side.BUY), 0)


class TestSlippageModel(unittest.TestCase):
    def setUp(self):
        self.cache = sm.OrderBookCache()
        self.cache.update(make_book("BTC-USD", bid=99, ask=101))
        self.model = sm.SlippageModel(self.cache)

    def test_estimate_with_book(self):
        est = self.model.estimate_slippage(
            "BTC-USD", sm.Side.BUY, sm.OrderType.MARKET, 1.0, 100.0, 1_000_000, atr=1.0, urgency=1.0)
        self.assertGreater(est.expected_fill_price, 100.0)
        self.assertGreaterEqual(est.slippage_bps, self.model.min_slippage_bps)

    def test_estimate_no_book(self):
        est = self.model.estimate_slippage(
            "ETH-USD", sm.Side.SELL, sm.OrderType.MARKET, 1.0, 100.0, 1_000_000)
        self.assertLess(est.expected_fill_price, 100.0)
        self.assertGreaterEqual(est.slippage_bps, self.model.min_slippage_bps)
        self.assertLessEqual(est.slippage_bps, self.model.max_slippage_bps)

    def test_estimate_limit_maker_buy(self):
        est = self.model.estimate_slippage(
            "BTC-USD", sm.Side.BUY, sm.OrderType.LIMIT, 1.0, 98.0, 1_000_000)
        self.assertGreater(est.maker_probability, 0)
        self.assertGreaterEqual(est.queue_position_estimate, 0)

    def test_estimate_limit_maker_sell(self):
        est = self.model.estimate_slippage(
            "BTC-USD", sm.Side.SELL, sm.OrderType.LIMIT, 1.0, 102.0, 1_000_000)
        self.assertGreater(est.maker_probability, 0)

    def test_estimate_limit_dist_branches(self):
        # within 5 bps
        self.model.estimate_slippage("BTC-USD", sm.Side.BUY, sm.OrderType.LIMIT, 1.0, 99.0, 1_000_000)
        # within 20 bps
        e = self.model.estimate_slippage("BTC-USD", sm.Side.SELL, sm.OrderType.LIMIT, 1.0, 200.0, 1_000_000)
        self.assertEqual(e.maker_probability, self.model.base_maker_prob * 0.1)

    def test_estimate_limit_at_bid_ask(self):
        # BUY price >= best_bid => maker_prob *2
        e = self.model.estimate_slippage("BTC-USD", sm.Side.BUY, sm.OrderType.LIMIT, 1.0, 99.0, 1_000_000)
        self.assertEqual(e.maker_probability, self.model.base_maker_prob * 2)
        # SELL price <= best_ask => maker_prob *2
        e2 = self.model.estimate_slippage("BTC-USD", sm.Side.SELL, sm.OrderType.LIMIT, 1.0, 101.0, 1_000_000)
        self.assertEqual(e2.maker_probability, self.model.base_maker_prob * 2)

    def test_estimate_limit_within_5_and_20(self):
        # Build a book tight around mid (100) so the <=5bps / <=20bps branches
        # are reachable without triggering the at-bid/ask *2 branch.
        from coinbase.src.slippage_model import OrderBookSnapshot, OrderBookLevel
        cache = sm.OrderBookCache()
        cache.update(OrderBookSnapshot(product_id="X", timestamp=time.time(),
                                       bids=[OrderBookLevel(price=99.96, size=10)],
                                       asks=[OrderBookLevel(price=100.04, size=10)]))
        model = sm.SlippageModel(cache)
        # BUY price 99.95 (< best_bid 99.96, dist 5bps) -> base_maker_prob
        e1 = model.estimate_slippage("X", sm.Side.BUY, sm.OrderType.LIMIT, 1.0, 99.95, 1_000_000)
        self.assertEqual(e1.maker_probability, model.base_maker_prob)
        # BUY price 99.8 (< best_bid, dist 20bps) -> base_maker_prob * 0.5
        e2 = model.estimate_slippage("X", sm.Side.BUY, sm.OrderType.LIMIT, 1.0, 99.85, 1_000_000)
        self.assertEqual(e2.maker_probability, model.base_maker_prob * 0.5)
        # SELL price 100.05 (> best_ask 100.04, dist 5bps) -> base_maker_prob
        e3 = model.estimate_slippage("X", sm.Side.SELL, sm.OrderType.LIMIT, 1.0, 100.05, 1_000_000)
        self.assertEqual(e3.maker_probability, model.base_maker_prob)
        # SELL price 100.1 (> best_ask, dist 10bps) -> base_maker_prob * 0.5
        e4 = model.estimate_slippage("X", sm.Side.SELL, sm.OrderType.LIMIT, 1.0, 100.1, 1_000_000)
        self.assertEqual(e4.maker_probability, model.base_maker_prob * 0.5)

    def test_depth_sell_break(self):
        from coinbase.src.slippage_model import OrderBookSnapshot, OrderBookLevel
        b = OrderBookSnapshot(product_id="X", timestamp=time.time(),
                              bids=[OrderBookLevel(price=99, size=5),
                                    OrderBookLevel(price=50, size=5)])
        # 50 bps from mid (100) => min 95, first bid 99 included, second 50 excluded
        d = b.depth_at_bps(50.0, sm.Side.SELL)
        self.assertEqual(d, 5.0)

    def test_estimate_limit_no_book(self):
        est = self.model.estimate_slippage(
            "ETH-USD", sm.Side.BUY, sm.OrderType.LIMIT, 1.0, 100.0, 1_000_000)
        self.assertEqual(est.maker_probability, self.model.base_maker_prob)
        self.assertEqual(est.queue_position_estimate, 0)

    def test_estimate_no_volume(self):
        est = self.model.estimate_slippage(
            "BTC-USD", sm.Side.BUY, sm.OrderType.MARKET, 1.0, 100.0, 0.0)
        self.assertEqual(est.partial_fill_pct, 0.5)

    def test_vol_ratio_zero_volume(self):
        est = self.model.estimate_slippage(
            "BTC-USD", sm.Side.BUY, sm.OrderType.MARKET, 1.0, 100.0, 0.0)
        self.assertIsInstance(est.fill_probability, float)

    def test_market_order_cost(self):
        price, slip, fee = self.model.estimate_market_order_cost(
            "BTC-USD", sm.Side.BUY, 1.0, 100.0, 1_000_000, atr=1.0)
        self.assertGreater(price, 100.0)
        self.assertGreaterEqual(slip, 0)
        est = self.model.estimate_slippage(
            "BTC-USD", sm.Side.BUY, sm.OrderType.MARKET, 1.0, 100.0, 1_000_000, atr=1.0)
        self.assertEqual(fee, est.maker_probability)


class TestGlobals(unittest.TestCase):
    def test_get_book_cache(self):
        c = sm.get_book_cache()
        self.assertIsInstance(c, sm.OrderBookCache)

    def test_get_slippage_model(self):
        m = sm.get_slippage_model()
        self.assertIsInstance(m, sm.SlippageModel)
        # second call returns the cached singleton (already-set branch)
        m2 = sm.get_slippage_model()
        self.assertIs(m, m2)


if __name__ == "__main__":
    unittest.main()
