from __future__ import annotations

import datetime
from unittest import TestCase

from market_data.storage.manager import MarketDataStore


class TestMarketDataStore(TestCase):
    def setUp(self):
        self.store = MarketDataStore(["BTC-USD", "ETH-USD"])

    def test_init_creates_per_product(self):
        self.assertIn("BTC-USD", self.store.candle_agg)
        self.assertIn("ETH-USD", self.store.features)
        self.assertIn("BTC-USD", self.store.indicators)
        self.assertIn("ETH-USD", self.store.orderbooks)

    def test_ingest_trade(self):
        ts = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        self.store.ingest_trade("BTC-USD", 100.0, 1.0, "buy", ts)
        self.assertEqual(self.store.candle_agg["BTC-USD"]._current.close, 100.0)
        self.assertEqual(self.store.trades._trades[-1].product_id, "BTC-USD")

    def test_ingest_orderbook_snapshot(self):
        self.store.ingest_orderbook_snapshot("BTC-USD", [[100.0, 2.0]], [[101.0, 3.0]])
        top = self.store.orderbooks["BTC-USD"].top_of_book()
        self.assertEqual(top["bid_px"], 100.0)
        self.assertEqual(top["ask_px"], 101.0)

    def test_ingest_orderbook_update(self):
        self.store.ingest_orderbook_update("BTC-USD", "buy", 100.0, 2.0)
        self.assertEqual(self.store.orderbooks["BTC-USD"]._bids[100.0], 2.0)

    def test_features_for(self):
        ts = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        for p in [100.0, 101.0, 102.0, 103.0]:
            self.store.ingest_trade("BTC-USD", p, 1.0, "buy", ts)
        self.store.ingest_orderbook_snapshot("BTC-USD", [[100.0, 2.0]], [[101.0, 3.0]])
        feats = self.store.features_for("BTC-USD")
        self.assertEqual(feats["product_id"], "BTC-USD")
        self.assertIn("sma_20", feats)
        self.assertIn("ema_20", feats)
        self.assertIn("rsi_14", feats)
        self.assertIn("zscore_20", feats)
        self.assertIn("bb_upper", feats)
        self.assertIn("bb_mid", feats)
        self.assertIn("bb_lower", feats)
