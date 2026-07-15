import unittest
import tempfile
import os
from datetime import datetime, timezone

from trading_system.market_data.trades.recorder import TradeRecorder, TradeRecord


class TestTradeRecorder(unittest.TestCase):
    def test_record_and_to_dict(self):
        r = TradeRecorder(max_records=3)
        t = r.record("BTC-USD", "BUY", 100.0, 0.5, "id1")
        self.assertIsInstance(t, TradeRecord)
        d = t.to_dict()
        self.assertEqual(d["product_id"], "BTC-USD")
        self.assertEqual(d["side"], "BUY")
        self.assertEqual(d["price"], 100.0)
        self.assertEqual(d["size"], 0.5)
        self.assertEqual(d["trade_id"], "id1")
        self.assertIsInstance(d["timestamp"], str)

    def test_default_timestamp(self):
        t = TradeRecord("ETH-USD", "SELL", 10.0, 1.0)
        self.assertIsInstance(t.timestamp, datetime)

    def test_recent_and_by_product(self):
        r = TradeRecorder()
        r.record("BTC-USD", "BUY", 100.0, 1.0)
        r.record("ETH-USD", "SELL", 10.0, 2.0)
        r.record("BTC-USD", "BUY", 101.0, 1.0)
        self.assertEqual(len(r.recent(100)), 3)
        btc = r.by_product("BTC-USD", 100)
        self.assertEqual(len(btc), 2)
        self.assertEqual(btc[-1].price, 101.0)
        self.assertEqual(len(r.by_product("DOGE-USD")), 0)

    def test_max_records_truncation(self):
        r = TradeRecorder(max_records=2)
        r.record("A", "BUY", 1.0, 1.0)
        r.record("B", "BUY", 2.0, 1.0)
        r.record("C", "BUY", 3.0, 1.0)
        self.assertEqual(len(r._trades), 2)
        self.assertEqual(r._trades[0].product_id, "B")

    def test_clear(self):
        r = TradeRecorder()
        r.record("A", "BUY", 1.0, 1.0)
        r.clear()
        self.assertEqual(r._trades, [])


if __name__ == "__main__":
    unittest.main()
