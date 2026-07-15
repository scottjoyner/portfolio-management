from __future__ import annotations

import datetime
import sys
import types
import unittest
from datetime import datetime, timezone
from decimal import Decimal

from market_data.candles.aggregator import Candle, CandleAggregator
from market_data.features.compute import FeatureComputer, FeatureSet
from market_data.indicators.technical import TechnicalIndicatorSet
from market_data.microstructure.features import (
    MicrostructureFeatureBuilder,
    ToxicFlowEstimator,
    TopOfBook,
    TradePrint,
)
from market_data.orderbook.book import BookLevel, OrderBook
from market_data.trades.recorder import TradeRecord, TradeRecorder


class TestMicrostructureFeatures(unittest.TestCase):
    def test_top_of_book_dataclass(self):
        b = TopOfBook(100.0, 2.0, 100.1, 2.2)
        self.assertEqual(b.bid_px, 100.0)
        self.assertEqual(b.ask_sz, 2.2)

    def test_trade_print_dataclass(self):
        t = TradePrint("buy", 1.5, 100.0)
        self.assertEqual(t.side, "buy")

    def test_microprice_zero_denom(self):
        b = TopOfBook(100.0, 0.0, 100.1, 0.0)
        self.assertEqual(MicrostructureFeatureBuilder.microprice(b), 100.05)

    def test_microprice_normal(self):
        b = TopOfBook(100.0, 2.0, 100.1, 2.0)
        # (100.1*2 + 100.0*2) / 4 = 100.05
        self.assertAlmostEqual(MicrostructureFeatureBuilder.microprice(b), 100.05)

    def test_imbalance_zero_denom(self):
        b = TopOfBook(100.0, 0.0, 100.1, 0.0)
        self.assertEqual(MicrostructureFeatureBuilder.imbalance(b), 0.0)

    def test_imbalance_normal(self):
        b = TopOfBook(100.0, 3.0, 100.1, 1.0)
        self.assertAlmostEqual(MicrostructureFeatureBuilder.imbalance(b), 0.5)

    def test_toxic_flow_buy(self):
        t = ToxicFlowEstimator(bucket_volume=5.0)
        out = t.update(TradePrint("buy", 3.0, 100.0))
        self.assertEqual(out, 0.0)
        out = t.update(TradePrint("BUY", 3.0, 100.0))  # upper() handles case
        # buy=6, sell=0 -> total 6 >= 5; toxic = (6-0)/6 = 1.0
        self.assertAlmostEqual(out, 1.0)
        # after flush, counters reset
        self.assertEqual(t.buy_vol, 0.0)

    def test_toxic_flow_sell_accumulates(self):
        t = ToxicFlowEstimator(bucket_volume=4.0)
        out = t.update(TradePrint("sell", 2.0, 100.0))
        self.assertEqual(out, 0.0)
        out = t.update(TradePrint("sell", 2.0, 100.0))
        # sell=4 -> total 4 >= 4; toxic = |0-4|/4 = 1.0
        self.assertAlmostEqual(out, 1.0)

    def test_toxic_flow_mixed(self):
        t = ToxicFlowEstimator(bucket_volume=10.0)
        t.update(TradePrint("buy", 6.0, 100.0))
        out = t.update(TradePrint("sell", 5.0, 100.0))
        # buy=6, sell=5 -> total 11 >=10; toxic = |6-5|/11 ~ 0.0909
        self.assertAlmostEqual(out, 1.0 / 11.0, places=6)

    def test_toxic_flow_min_bucket(self):
        t = ToxicFlowEstimator(bucket_volume=0.0)
        self.assertGreater(t.bucket_volume, 0)


class TestOrderBook(unittest.TestCase):
    def test_init(self):
        ob = OrderBook("BTC-USD", levels=5)
        self.assertEqual(ob.product_id, "BTC-USD")
        self.assertEqual(ob.levels, 5)

    def test_update_buy_and_remove(self):
        ob = OrderBook("BTC-USD")
        ob.update("buy", 100.0, 2.0)
        ob.update("sell", 101.0, 2.0)
        self.assertEqual(ob._bids[100.0], 2.0)
        ob.update("buy", 100.0, 0.0)
        self.assertNotIn(100.0, ob._bids)
        ob.update("sell", 101.0, 0.0)
        self.assertNotIn(101.0, ob._asks)

    def test_snapshot_extra_elements(self):
        ob = OrderBook("BTC-USD")
        ob.snapshot([[100.0, 2.0, 5], [99.0, 1.0]], [[101.0, 3.0, 4]])
        self.assertEqual(ob._bids[100.0], 2.0)
        self.assertEqual(ob._asks[101.0], 3.0)

    def test_top_of_book_empty(self):
        ob = OrderBook("BTC-USD")
        top = ob.top_of_book()
        self.assertEqual(top["bid_px"], 0.0)
        self.assertEqual(top["ask_px"], 0.0)
        self.assertEqual(top["spread"], 0.0)
        self.assertEqual(top["mid"], 0.0)

    def test_top_of_book_filled(self):
        ob = OrderBook("BTC-USD")
        ob.update("buy", 100.0, 2.0)
        ob.update("sell", 101.0, 3.0)
        top = ob.top_of_book()
        self.assertEqual(top["bid_px"], 100.0)
        self.assertEqual(top["bid_sz"], 2.0)
        self.assertEqual(top["ask_px"], 101.0)
        self.assertEqual(top["ask_sz"], 3.0)
        self.assertEqual(top["spread"], 1.0)
        self.assertEqual(top["mid"], 100.5)

    def test_depth(self):
        ob = OrderBook("BTC-USD")
        for p in [100.0, 99.0, 98.0]:
            ob.update("buy", p, 1.0)
        for p in [101.0, 102.0, 103.0]:
            ob.update("sell", p, 1.0)
        d = ob.depth(levels=2)
        self.assertEqual([b.price for b in d["bids"]], [100.0, 99.0])
        self.assertEqual([a.price for a in d["asks"]], [101.0, 102.0])
        self.assertIsInstance(d["bids"][0], BookLevel)

    def test_accumulate_depth_buy_sell(self):
        ob = OrderBook("BTC-USD")
        for p in [100.0, 99.0, 98.0]:
            ob.update("buy", p, 1.0)
        for p in [101.0, 102.0]:
            ob.update("sell", p, 2.0)
        self.assertAlmostEqual(ob.accumulate_depth("buy", 2), 2.0)
        self.assertAlmostEqual(ob.accumulate_depth("sell", 1), 2.0)

    def test_clear(self):
        ob = OrderBook("BTC-USD")
        ob.update("buy", 100.0, 1.0)
        ob.clear()
        self.assertEqual(ob._bids, {})
        self.assertEqual(ob._asks, {})


class TestTechnicalIndicators(unittest.TestCase):
    def test_ingest_truncates(self):
        ind = TechnicalIndicatorSet(max_samples=3)
        for p in [1.0, 2.0, 3.0, 4.0, 5.0]:
            ind.ingest(p, 10.0)
        self.assertEqual(ind._prices, [3.0, 4.0, 5.0])
        self.assertEqual(ind._volumes, [10.0, 10.0, 10.0])

    def test_sma_not_enough(self):
        ind = TechnicalIndicatorSet()
        ind.ingest(10.0)
        self.assertEqual(ind.sma(20), 0.0)

    def test_sma_sufficient(self):
        ind = TechnicalIndicatorSet()
        for p in [10.0, 20.0]:
            ind.ingest(p)
        self.assertEqual(ind.sma(2), 15.0)

    def test_ema_not_enough(self):
        ind = TechnicalIndicatorSet()
        ind.ingest(10.0)
        self.assertEqual(ind.ema(20), 0.0)

    def test_ema_sufficient(self):
        ind = TechnicalIndicatorSet()
        for p in range(1, 22):
            ind.ingest(float(p))
        val = ind.ema(20)
        self.assertIsInstance(val, float)
        self.assertGreater(val, 11.0)

    def test_rsi_not_enough(self):
        ind = TechnicalIndicatorSet()
        for p in range(1, 10):
            ind.ingest(float(p))
        self.assertEqual(ind.rsi(14), 50.0)

    def test_rsi_zero_loss(self):
        ind = TechnicalIndicatorSet()
        prices = [100.0 + i for i in range(15)]
        for p in prices:
            ind.ingest(p)
        self.assertEqual(ind.rsi(14), 100.0)

    def test_rsi_normal(self):
        ind = TechnicalIndicatorSet()
        prices = [100.0, 101.0, 100.0, 99.0, 100.0, 101.0, 100.0, 99.0,
                  100.0, 101.0, 100.0, 99.0, 100.0, 101.0, 100.0]
        for p in prices:
            ind.ingest(p)
        rsi = ind.rsi(14)
        self.assertTrue(0.0 <= rsi <= 100.0)

    def test_stddev_not_enough(self):
        ind = TechnicalIndicatorSet()
        ind.ingest(10.0)
        self.assertEqual(ind._stddev(20), 0.0)

    def test_bollinger_bands(self):
        ind = TechnicalIndicatorSet()
        for p in [10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0,
                  10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0]:
            ind.ingest(p)
        bb = ind.bollinger_bands(20, 2.0)
        self.assertEqual(bb["mid"], 10.0)
        self.assertEqual(bb["upper"], 10.0)
        self.assertEqual(bb["lower"], 10.0)

    def test_zscore_not_enough(self):
        ind = TechnicalIndicatorSet()
        ind.ingest(10.0)
        self.assertEqual(ind.zscore(20), 0.0)

    def test_zscore_zero_std(self):
        ind = TechnicalIndicatorSet()
        for _ in range(20):
            ind.ingest(10.0)
        self.assertEqual(ind.zscore(20), 0.0)

    def test_zscore_normal(self):
        ind = TechnicalIndicatorSet()
        for p in range(1, 21):
            ind.ingest(float(p))
        z = ind.zscore(20)
        self.assertGreater(z, 1.0)
        self.assertLess(z, 2.0)

    def test_volume_sma_not_enough(self):
        ind = TechnicalIndicatorSet()
        ind.ingest(10.0)
        self.assertEqual(ind.volume_sma(20), 0.0)

    def test_volume_sma_sufficient(self):
        ind = TechnicalIndicatorSet()
        for v in [10.0, 20.0]:
            ind.ingest(5.0, v)
        self.assertEqual(ind.volume_sma(2), 15.0)

    def test_volume_ratio_zero_avg(self):
        ind = TechnicalIndicatorSet()
        ind.ingest(5.0)
        self.assertEqual(ind.volume_ratio(20), 1.0)

    def test_volume_ratio_normal(self):
        ind = TechnicalIndicatorSet()
        for v in [10.0, 10.0]:
            ind.ingest(5.0, v)
        self.assertEqual(ind.volume_ratio(2), 1.0)

    def test_volume_ratio_empty_volumes(self):
        ind = TechnicalIndicatorSet()
        for _ in range(3):
            ind.ingest(5.0)
        self.assertEqual(ind.volume_ratio(2), 1.0)


class TestCandles(unittest.TestCase):
    def test_candle_to_dict(self):
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        c = Candle("BTC-USD", ts, 100.0, 101.0, 99.0, 100.5, 12.0, 3)
        d = c.to_dict()
        self.assertEqual(d["product_id"], "BTC-USD")
        self.assertEqual(d["open"], 100.0)
        self.assertEqual(d["trades"], 3)
        self.assertEqual(d["timestamp"], ts.isoformat())

    def test_ingest_first_trade(self):
        agg = CandleAggregator("BTC-USD", window_seconds=60)
        res = agg.ingest_trade(100.0, 1.0, datetime(2024, 1, 1, 0, 0, 30, tzinfo=timezone.utc))
        self.assertIsNone(res)
        self.assertIsNotNone(agg._current)
        self.assertEqual(agg._current.open, 100.0)

    def test_ingest_same_bucket(self):
        agg = CandleAggregator("BTC-USD", window_seconds=60)
        ts = datetime(2024, 1, 1, 0, 0, 30, tzinfo=timezone.utc)
        agg.ingest_trade(100.0, 1.0, ts)
        agg.ingest_trade(105.0, 2.0, ts)
        self.assertEqual(agg._current.high, 105.0)
        self.assertEqual(agg._current.low, 100.0)
        self.assertEqual(agg._current.close, 105.0)
        self.assertEqual(agg._current.volume, 3.0)
        self.assertEqual(agg._current.trades, 2)

    def test_ingest_new_bucket_appends(self):
        agg = CandleAggregator("BTC-USD", window_seconds=60)
        ts1 = datetime(2024, 1, 1, 0, 0, 30, tzinfo=timezone.utc)
        ts2 = datetime(2024, 1, 1, 0, 1, 30, tzinfo=timezone.utc)
        agg.ingest_trade(100.0, 1.0, ts1)
        agg.ingest_trade(200.0, 1.0, ts2)
        self.assertEqual(len(agg._candles), 1)
        self.assertEqual(agg._candles[0].open, 100.0)
        self.assertEqual(agg._current.open, 200.0)

    def test_get_candles_and_clear(self):
        agg = CandleAggregator("BTC-USD", window_seconds=60)
        ts1 = datetime(2024, 1, 1, 0, 0, 30, tzinfo=timezone.utc)
        ts2 = datetime(2024, 1, 1, 0, 1, 30, tzinfo=timezone.utc)
        agg.ingest_trade(100.0, 1.0, ts1)
        agg.ingest_trade(200.0, 1.0, ts2)
        all_c = agg.get_candles()
        self.assertEqual(len(all_c), 2)
        limited = agg.get_candles(count=1)
        self.assertEqual(len(limited), 1)
        agg.clear()
        self.assertEqual(agg.get_candles(), [])
        self.assertIsNone(agg._current)


class TestTrades(unittest.TestCase):
    def test_trade_record_to_dict(self):
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        tr = TradeRecord("BTC-USD", "buy", 100.0, 1.0, ts, "id1")
        d = tr.to_dict()
        self.assertEqual(d["product_id"], "BTC-USD")
        self.assertEqual(d["trade_id"], "id1")
        self.assertEqual(d["timestamp"], ts.isoformat())

    def test_record(self):
        r = TradeRecorder()
        t = r.record("BTC-USD", "buy", 100.0, 1.0, "id1")
        self.assertEqual(t.trade_id, "id1")

    def test_max_records_truncation(self):
        r = TradeRecorder(max_records=3)
        for i in range(5):
            r.record("BTC-USD", "buy", 100.0, 1.0, f"id{i}")
        self.assertEqual(len(r._trades), 3)
        self.assertEqual(r._trades[0].trade_id, "id2")

    def test_recent(self):
        r = TradeRecorder()
        for i in range(5):
            r.record("BTC-USD", "buy", 100.0, 1.0, f"id{i}")
        self.assertEqual(len(r.recent(count=2)), 2)

    def test_by_product(self):
        r = TradeRecorder()
        r.record("BTC-USD", "buy", 100.0, 1.0)
        r.record("ETH-USD", "buy", 50.0, 1.0)
        self.assertEqual(len(r.by_product("ETH-USD")), 1)
        self.assertEqual(len(r.by_product("ETH-USD", count=1)), 1)

    def test_clear(self):
        r = TradeRecorder()
        r.record("BTC-USD", "buy", 100.0, 1.0)
        r.clear()
        self.assertEqual(r._trades, [])


class TestFeatures(unittest.TestCase):
    def test_featureset_to_dict(self):
        fs = FeatureSet(product_id="BTC-USD", mid_price=100.0, spread_bps=10.0, buy_ratio_1m=0.7)
        d = fs.to_dict()
        self.assertEqual(d["product_id"], "BTC-USD")
        self.assertEqual(d["mid_price"], 100.0)
        self.assertEqual(d["spread_bps"], 10.0)
        self.assertEqual(d["buy_ratio_1m"], 0.7)

    def test_ingest_buy_sell(self):
        fc = FeatureComputer("BTC-USD")
        fc.ingest_trade(100.0, 1.0, "buy")
        fc.ingest_trade(101.0, 2.0, "sell")
        self.assertEqual(fc._buys, [1.0])
        self.assertEqual(fc._sells, [2.0])

    def test_compute_with_bid_ask(self):
        fc = FeatureComputer("BTC-USD")
        fc.ingest_trade(100.0, 1.0, "buy")
        fc.ingest_trade(101.0, 1.0, "sell")
        fs = fc.compute(bid=100.0, ask=101.0)
        self.assertEqual(fs.mid_price, 100.5)
        self.assertAlmostEqual(fs.spread_bps, (1.0 / 100.5) * 10000, places=4)
        self.assertAlmostEqual(fs.microprice, (101.0 * 1.0 + 100.0 * 1.0) / 2.0)
        self.assertAlmostEqual(fs.imbalance, 0.0)
        self.assertAlmostEqual(fs.buy_ratio_1m, 0.5)
        self.assertEqual(fs.volume_1m, 2.0)
        self.assertEqual(fs.trade_count_1m, 2)

    def test_compute_without_bid_ask(self):
        fc = FeatureComputer("BTC-USD")
        fc.ingest_trade(100.0, 1.0, "buy")
        fs = fc.compute()
        self.assertEqual(fs.mid_price, 100.0)
        self.assertEqual(fs.spread_bps, 0.0)
        self.assertEqual(fs.microprice, 100.0)

    def test_compute_zero_mid_spread(self):
        fc = FeatureComputer("BTC-USD")
        fs = fc.compute(bid=0.0, ask=0.0)
        self.assertEqual(fs.mid_price, 0.0)
        self.assertEqual(fs.spread_bps, 0.0)

    def test_compute_buy_ratio_only(self):
        fc = FeatureComputer("BTC-USD")
        fc.ingest_trade(100.0, 4.0, "buy")
        fs = fc.compute(bid=100.0, ask=101.0)
        self.assertAlmostEqual(fs.buy_ratio_1m, 1.0)
        self.assertAlmostEqual(fs.imbalance, 1.0)
        self.assertAlmostEqual(fs.microprice, 100.0)

    def test_compute_volatility_short(self):
        fc = FeatureComputer("BTC-USD")
        fc.ingest_trade(100.0, 1.0, "buy")
        fs = fc.compute(bid=100.0, ask=101.0)
        self.assertEqual(fs.volatility_1m_bps, 0.0)

    def test_compute_volatility_normal(self):
        fc = FeatureComputer("BTC-USD")
        for p in [100.0, 101.0, 102.0]:
            fc.ingest_trade(p, 1.0, "buy")
        fs = fc.compute(bid=100.0, ask=101.0)
        self.assertGreaterEqual(fs.volatility_1m_bps, 0.0)

    def test_compute_ingest_truncates(self):
        fc = FeatureComputer("BTC-USD")
        for i in range(600):
            fc.ingest_trade(100.0 + i * 0.001, 1.0, "buy")
        self.assertEqual(len(fc._prices), 500)


if __name__ == "__main__":
    unittest.main()
