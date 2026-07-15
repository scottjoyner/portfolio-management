import unittest

from trading_system.market_data.orderbook.book import OrderBook, BookLevel


class TestOrderBook(unittest.TestCase):
    def test_init_and_update(self):
        ob = OrderBook("BTC-USD", levels=5)
        self.assertEqual(ob.product_id, "BTC-USD")
        self.assertEqual(ob.levels, 5)

    def test_update_add_and_remove(self):
        ob = OrderBook("BTC-USD")
        ob.update("buy", 100.0, 2.0)
        ob.update("sell", 101.0, 3.0)
        self.assertEqual(ob._bids[100.0], 2.0)
        # size 0 removes
        ob.update("buy", 100.0, 0.0)
        self.assertNotIn(100.0, ob._bids)
        # side != buy -> asks
        ob.update("ask", 102.0, 1.0)
        self.assertIn(102.0, ob._asks)

    def test_snapshot(self):
        ob = OrderBook("BTC-USD")
        ob.snapshot([[100.0, 2.0], [99.0, 1.0]], [[101.0, 3.0]])
        self.assertEqual(ob._bids[100.0], 2.0)
        self.assertEqual(ob._asks[101.0], 3.0)

    def test_top_of_book_empty(self):
        ob = OrderBook("BTC-USD")
        tob = ob.top_of_book()
        self.assertEqual(tob["bid_px"], 0.0)
        self.assertEqual(tob["ask_px"], 0.0)
        self.assertEqual(tob["spread"], 0.0)
        self.assertEqual(tob["mid"], 0.0)

    def test_top_of_book(self):
        ob = OrderBook("BTC-USD")
        ob.update("buy", 100.0, 2.0)
        ob.update("buy", 99.0, 1.0)
        ob.update("sell", 101.0, 3.0)
        tob = ob.top_of_book()
        self.assertEqual(tob["bid_px"], 100.0)
        self.assertEqual(tob["bid_sz"], 2.0)
        self.assertEqual(tob["ask_px"], 101.0)
        self.assertEqual(tob["ask_sz"], 3.0)
        self.assertEqual(tob["spread"], 1.0)
        self.assertEqual(tob["mid"], 100.5)

    def test_depth(self):
        ob = OrderBook("BTC-USD")
        ob.update("buy", 100.0, 2.0)
        ob.update("buy", 99.0, 1.0)
        ob.update("buy", 98.0, 1.0)
        ob.update("sell", 101.0, 3.0)
        ob.update("sell", 102.0, 2.0)
        d = ob.depth(levels=2)
        self.assertEqual([lvl.price for lvl in d["bids"]], [100.0, 99.0])
        self.assertEqual([lvl.price for lvl in d["asks"]], [101.0, 102.0])
        self.assertIsInstance(d["bids"][0], BookLevel)

    def test_accumulate_depth(self):
        ob = OrderBook("BTC-USD")
        ob.update("buy", 100.0, 2.0)
        ob.update("buy", 99.0, 1.0)
        ob.update("sell", 101.0, 3.0)
        ob.update("sell", 102.0, 2.0)
        self.assertEqual(ob.accumulate_depth("buy", 2), 3.0)
        self.assertEqual(ob.accumulate_depth("sell", 1), 3.0)
        # unknown side string falls through to asks target
        self.assertEqual(ob.accumulate_depth("zzz", 10), 5.0)

    def test_clear(self):
        ob = OrderBook("BTC-USD")
        ob.update("buy", 100.0, 2.0)
        ob.clear()
        self.assertEqual(ob._bids, {})


if __name__ == "__main__":
    unittest.main()
