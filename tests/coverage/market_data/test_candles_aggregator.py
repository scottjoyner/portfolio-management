import unittest
from datetime import datetime, timezone

from trading_system.market_data.candles.aggregator import Candle, CandleAggregator


def _ts(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


class TestCandle(unittest.TestCase):
    def test_to_dict(self):
        c = Candle("BTC-USD", _ts(1000), 1.0, 2.0, 0.5, 1.5, 10.0, trades=3)
        d = c.to_dict()
        self.assertEqual(d["product_id"], "BTC-USD")
        self.assertEqual(d["trades"], 3)
        self.assertEqual(d["open"], 1.0)


class TestAggregator(unittest.TestCase):
    def test_ingest_first_and_update(self):
        a = CandleAggregator("BTC-USD", window_seconds=60)
        c = a.ingest_trade(100.0, 1.0, _ts(1000))
        self.assertIsNone(c)
        self.assertEqual(a._current.open, 100.0)
        # same bucket -> updates high/low/close/volume
        a.ingest_trade(105.0, 2.0, _ts(1020))
        a.ingest_trade(95.0, 0.5, _ts(1040))
        self.assertEqual(a._current.high, 105.0)
        self.assertEqual(a._current.low, 95.0)
        self.assertEqual(a._current.close, 95.0)
        self.assertEqual(a._current.volume, 3.5)
        self.assertEqual(a._current.trades, 3)

    def test_ingest_new_bucket(self):
        a = CandleAggregator("BTC-USD", window_seconds=60)
        a.ingest_trade(100.0, 1.0, _ts(1000))
        # next bucket (>= 60s later)
        a.ingest_trade(200.0, 2.0, _ts(1100))
        self.assertEqual(len(a._candles), 1)
        self.assertEqual(a._candles[0].close, 100.0)
        self.assertEqual(a._current.open, 200.0)

    def test_get_and_clear(self):
        a = CandleAggregator("BTC-USD", window_seconds=60)
        a.ingest_trade(100.0, 1.0, _ts(1000))
        a.ingest_trade(200.0, 1.0, _ts(1100))
        allc = a.get_candles()
        self.assertEqual(len(allc), 2)
        # count truncation
        self.assertEqual(len(a.get_candles(count=1)), 1)
        a.clear()
        self.assertEqual(a._candles, [])
        self.assertIsNone(a._current)
        # get with no current
        self.assertEqual(a.get_candles(), [])


if __name__ == "__main__":
    unittest.main()
