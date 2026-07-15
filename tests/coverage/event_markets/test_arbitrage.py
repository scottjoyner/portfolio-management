"""Coverage tests for event_markets.arbitrage."""
import json
import math
import tempfile
import os
from unittest import TestCase, mock

from event_markets.unified_client import PredictionMarket
import event_markets.arbitrage as A
from event_markets.arbitrage import (
    _platform_fee, _tokenize, normalize_question, _extract_numbers,
    _numbers_compatible, _comparator, _semantic_similarity, _similarity,
    _yes_price, ArbitrageOpportunity, ArbitrageLeg, EventArbitrageScanner,
    format_arbitrage,
)


def mk_market(**kw):
    base = dict(
        platform="kalshi", market_id="KX", question="Will BTC hit $100k?",
        outcomes=["YES", "NO"], outcome_prices={"YES": 0.3, "NO": 0.7},
        volume=50000.0, end_date="2026-12-31T00:00:00Z", is_open=True,
        yes_bid=0.28, yes_ask=0.32, spread=0.04, liquidity_score=0.8,
        category="crypto", raw_data={},
    )
    base.update(kw)
    return PredictionMarket(**base)


class TestHelpers(TestCase):
    def test_platform_fee_kalshi(self):
        # kalshi fee = rate*p*(1-p)
        self.assertAlmostEqual(_platform_fee("kalshi", 0.5), 0.07 * 0.5 * 0.5)
        self.assertAlmostEqual(_platform_fee("kalshi", 0.0), 0.0)
        self.assertAlmostEqual(_platform_fee("kalshi", 1.0), 0.0)

    def test_platform_fee_polymarket(self):
        self.assertAlmostEqual(_platform_fee("polymarket", 0.5), 0.005)
        self.assertAlmostEqual(_platform_fee("polymarket", 0.0), 0.005)

    def test_tokenize(self):
        toks = _tokenize("Will BTC reach $100k by December 2026?")
        self.assertIn("bitcoin", toks)
        self.assertIn("december", toks)
        # stopwords removed
        self.assertNotIn("will", toks)
        self.assertNotIn("by", toks)

    def test_normalize_question(self):
        n = normalize_question("Will BTC reach $100k by December 2026?")
        self.assertIn("bitcoin", n)
        # deterministic sorted
        self.assertEqual(n, normalize_question("BTC reach 100k December 2026 will?"))

    def test_extract_numbers(self):
        self.assertEqual(_extract_numbers("$100k"), {100000})
        self.assertEqual(_extract_numbers("1.5M"), {1500000})
        self.assertEqual(_extract_numbers("2b"), {2000000000})
        self.assertEqual(_extract_numbers("no numbers here"), set())

    def test_numbers_compatible(self):
        self.assertTrue(_numbers_compatible(set(), set()))
        self.assertTrue(_numbers_compatible({100000}, set()))
        self.assertTrue(_numbers_compatible({100000}, {101000}))
        self.assertFalse(_numbers_compatible({100000}, {50000}))

    def test_comparator(self):
        self.assertEqual(_comparator("BTC above $100k"), "above")
        self.assertEqual(_comparator("BTC below $100k"), "below")
        self.assertEqual(_comparator("BTC reaches $100k"), "above")
        self.assertEqual(_comparator("x > y"), "above")
        self.assertEqual(_comparator("x < y"), "below")
        self.assertEqual(_comparator("BTC above and below $100k"), "")

    def test_similarity(self):
        self.assertAlmostEqual(_similarity(["a", "b"], ["a", "b"]), 1.0)
        self.assertAlmostEqual(_similarity([], ["a"]), 0.0)
        self.assertAlmostEqual(_similarity(["a"], ["b"]), 0.0)

    def test_semantic_similarity_different_numbers(self):
        # same words, different strike thresholds -> 0
        self.assertEqual(
            _semantic_similarity("BTC above $100k", "BTC above $50k",
                                 ["bitcoin", "above"], ["bitcoin", "above"]), 0.0)

    def test_semantic_similarity_same(self):
        s = _semantic_similarity("BTC above $100k", "BTC above $100k",
                                 ["bitcoin", "above"], ["bitcoin", "above"])
        self.assertGreater(s, 0.0)

    def test_semantic_similarity_base_zero(self):
        self.assertEqual(_semantic_similarity("aaa", "bbb", ["aaa"], ["bbb"]), 0.0)

    def test_yes_price(self):
        m = mk_market(outcome_prices={"YES": 0.4})
        self.assertEqual(_yes_price(m), 0.4)
        m2 = mk_market(outcome_prices={"X": 0.9})
        self.assertEqual(_yes_price(m2), 0.9)


class TestDataclasses(TestCase):
    def test_opportunity_to_dict(self):
        leg = ArbitrageLeg("kalshi", "KX", "q", "YES", "BUY", 0.3)
        opp = ArbitrageOpportunity(
            event_key="ek", category="crypto", platform_buy="kalshi",
            platform_hedge="polymarket", leg_buy=leg, leg_hedge=leg,
            buy_yes_price=0.3, hedge_yes_price=0.7, total_cost=0.6,
            guaranteed_payout=1.0, edge=0.4, edge_pct=0.4, confidence=0.5,
            reason="r",
        )
        d = opp.to_dict()
        self.assertEqual(d["buy_market_id"], "KX")
        self.assertEqual(d["hedge_market_id"], "KX")
        self.assertEqual(d["category"], "crypto")

    def test_format_arbitrage(self):
        leg = ArbitrageLeg("kalshi", "KX", "q", "YES", "BUY", 0.3)
        opp = ArbitrageOpportunity(
            event_key="ek", category="crypto", platform_buy="kalshi",
            platform_hedge="polymarket", leg_buy=leg, leg_hedge=leg,
            buy_yes_price=0.3, hedge_yes_price=0.7, total_cost=0.6,
            guaranteed_payout=1.0, edge=0.4, edge_pct=0.4, confidence=0.5,
            reason="r",
        )
        out = format_arbitrage(opp)
        self.assertIn("kalshi", out)


class FakeStream:
    def __init__(self):
        self.data = {}

    def latest(self, platform, key):
        return self.data.get((platform, key))

    def subscriptions(self, markets):
        return {"polymarket_asset_ids": [], "kalshi_tickers": []}


class TestScanner(TestCase):
    def test_init(self):
        sc = EventArbitrageScanner(min_edge=0.02)
        self.assertEqual(sc.min_edge, 0.02)
        self.assertIsNotNone(sc.client)

    def test_slippage_bps_zero_volume(self):
        m = mk_market(volume=0.0)
        self.assertEqual(EventArbitrageScanner._slippage_bps(m), 50.0)

    def test_slippage_bps_normal(self):
        m = mk_market(volume=50000.0, spread=0.02)
        bps = EventArbitrageScanner._slippage_bps(m, 1000)
        self.assertLessEqual(bps, 200.0)

    def test_stream_key_polymarket(self):
        m = mk_market(platform="polymarket",
                      raw_data={"token_ids": ["tokA", "tokB"]})
        self.assertEqual(EventArbitrageScanner._stream_key(m), "tokA")
        m2 = mk_market(platform="polymarket", raw_data={})
        self.assertEqual(EventArbitrageScanner._stream_key(m2), "")
        m3 = mk_market(platform="kalshi", market_id="KX")
        self.assertEqual(EventArbitrageScanner._stream_key(m3), "KX")

    def test_apply_stream_prices(self):
        sc = EventArbitrageScanner()
        fs = FakeStream()
        from event_markets.streaming import PriceUpdate
        fs.data[("polymarket", "tokA")] = PriceUpdate(
            "polymarket", "tokA", 0.6, 0.58, 0.62)
        sc._stream = fs
        m = mk_market(platform="polymarket", market_id="P1",
                      raw_data={"token_ids": ["tokA"]}, outcome_prices={"YES": 0.3})
        n = sc._apply_stream_prices([m])
        self.assertEqual(n, 1)
        self.assertEqual(m.outcome_prices["YES"], 0.6)
        self.assertEqual(m.yes_bid, 0.58)

    def test_apply_stream_prices_no_stream(self):
        sc = EventArbitrageScanner()
        m = mk_market()
        self.assertEqual(sc._apply_stream_prices([m]), 0)

    def test_apply_stream_prices_zero_yes(self):
        sc = EventArbitrageScanner()
        fs = FakeStream()
        from event_markets.streaming import PriceUpdate
        fs.data[("polymarket", "tokA")] = PriceUpdate("polymarket", "tokA", 0.0)
        sc._stream = fs
        m = mk_market(platform="polymarket", raw_data={"token_ids": ["tokA"]})
        self.assertEqual(sc._apply_stream_prices([m]), 0)

    def test_apply_stream_prices_partial_bid(self):
        sc = EventArbitrageScanner()
        fs = FakeStream()
        from event_markets.streaming import PriceUpdate
        fs.data[("polymarket", "tokA")] = PriceUpdate(
            "polymarket", "tokA", 0.6, 0.0, 0.0)
        sc._stream = fs
        m = mk_market(platform="polymarket", raw_data={"token_ids": ["tokA"]})
        self.assertEqual(sc._apply_stream_prices([m]), 1)
        self.assertEqual(m.yes_bid, 0.28)

    def test_apply_stream_prices_missing_and_two(self):
        sc = EventArbitrageScanner()
        fs = FakeStream()
        from event_markets.streaming import PriceUpdate
        fs.data[("polymarket", "tokA")] = PriceUpdate("polymarket", "tokA", 0.6)
        sc._stream = fs
        m1 = mk_market(platform="polymarket", raw_data={"token_ids": ["tokA"]})
        m2 = mk_market(platform="polymarket", market_id="P2", raw_data={"token_ids": ["tokB"]})
        self.assertEqual(sc._apply_stream_prices([m1, m2]), 1)

    def test_stream_subscriptions(self):
        sc = EventArbitrageScanner()
        fs = FakeStream()
        sc._stream = fs
        m1 = mk_market(platform="polymarket", raw_data={"token_ids": ["tokA"]})
        m2 = mk_market(platform="kalshi", market_id="KX")
        subs = sc.stream_subscriptions([m1, m2])
        self.assertEqual(subs["polymarket_asset_ids"], ["tokA"])
        self.assertEqual(subs["kalshi_tickers"], ["KX"])

    def test_stream_subscriptions_no_key(self):
        sc = EventArbitrageScanner()
        fs = FakeStream()
        sc._stream = fs
        m1 = mk_market(platform="polymarket", raw_data={})
        m2 = mk_market(platform="kalshi", market_id="KX")
        subs = sc.stream_subscriptions([m1, m2])
        self.assertEqual(subs["polymarket_asset_ids"], [])
        self.assertEqual(subs["kalshi_tickers"], ["KX"])

    def test_scan_markets_only_one_platform(self):
        sc = EventArbitrageScanner()
        self.assertEqual(sc.scan_markets([mk_market(platform="kalshi")]), [])

    def test_scan_markets_low_volume(self):
        sc = EventArbitrageScanner()
        k = mk_market(platform="kalshi", volume=10.0)
        p = mk_market(platform="polymarket", volume=10.0)
        self.assertEqual(sc.scan_markets([k, p]), [])

    def test_scan_markets_not_open(self):
        sc = EventArbitrageScanner()
        k = mk_market(platform="kalshi", is_open=False)
        p = mk_market(platform="polymarket")
        self.assertEqual(sc.scan_markets([k, p]), [])

    def test_scan_markets_low_similarity(self):
        sc = EventArbitrageScanner(similarity_threshold=0.9)
        k = mk_market(platform="kalshi", question="Will BTC hit $100k?")
        p = mk_market(platform="polymarket", question="Will ETH hit $5000?")
        self.assertEqual(sc.scan_markets([k, p]), [])

    def test_scan_markets_finds_arb(self):
        sc = EventArbitrageScanner()
        k = mk_market(platform="kalshi", question="Will BTC hit $100k?",
                      outcome_prices={"YES": 0.3, "NO": 0.7})
        p = mk_market(platform="polymarket", question="Will BTC hit $100k?",
                      outcome_prices={"YES": 0.7, "NO": 0.3})
        with mock.patch.object(sc, "_record_paper_trade", return_value=None):
            opps = sc.scan_markets([k, p])
        self.assertEqual(len(opps), 1)
        self.assertGreater(opps[0].edge, 0)

    def test_scan_markets_no_edge(self):
        sc = EventArbitrageScanner()
        # very close prices -> no edge
        k = mk_market(platform="kalshi", question="Will BTC hit $100k?",
                      outcome_prices={"YES": 0.5, "NO": 0.5})
        p = mk_market(platform="polymarket", question="Will BTC hit $100k?",
                      outcome_prices={"YES": 0.51, "NO": 0.49})
        with mock.patch.object(sc, "_record_paper_trade", return_value=None):
            opps = sc.scan_markets([k, p])
        self.assertEqual(opps, [])

    def test_scan_markets_right_cheaper(self):
        sc = EventArbitrageScanner()
        # kalshi YES (0.7) more expensive than polymarket YES (0.3)
        k = mk_market(platform="kalshi", question="Will BTC hit $100k?",
                      outcome_prices={"YES": 0.7, "NO": 0.3})
        p = mk_market(platform="polymarket", question="Will BTC hit $100k?",
                      outcome_prices={"YES": 0.3, "NO": 0.7})
        with mock.patch.object(sc, "_record_paper_trade", return_value=None):
            opps = sc.scan_markets([k, p])
        self.assertEqual(len(opps), 1)
        self.assertEqual(opps[0].platform_buy, "polymarket")

    def test_scan_markets_edge_too_small(self):
        sc = EventArbitrageScanner()
        # prices sum to ~1 -> edge below min_edge
        k = mk_market(platform="kalshi", question="Will BTC hit $100k?",
                      outcome_prices={"YES": 0.49, "NO": 0.51})
        p = mk_market(platform="polymarket", question="Will BTC hit $100k?",
                      outcome_prices={"YES": 0.51, "NO": 0.49})
        with mock.patch.object(sc, "_record_paper_trade", return_value=None):
            opps = sc.scan_markets([k, p])
        self.assertEqual(opps, [])

    def test_scan_markets_no_paper_record(self):
        sc = EventArbitrageScanner(record_paper_trades=False)
        k = mk_market(platform="kalshi", question="Will BTC hit $100k?",
                      outcome_prices={"YES": 0.3, "NO": 0.7})
        p = mk_market(platform="polymarket", question="Will BTC hit $100k?",
                      outcome_prices={"YES": 0.7, "NO": 0.3})
        with mock.patch.object(sc, "_record_paper_trade") as rec:
            opps = sc.scan_markets([k, p])
        self.assertEqual(len(opps), 1)
        rec.assert_not_called()

    def test_scan_markets_zero_yes(self):
        sc = EventArbitrageScanner()
        k = mk_market(platform="kalshi", outcome_prices={"YES": 0.0})
        p = mk_market(platform="polymarket")
        self.assertEqual(sc.scan_markets([k, p]), [])

    def test_scan_calls_client(self):
        sc = EventArbitrageScanner()
        fake_client = mock.MagicMock()
        fake_client.search_all_categories.return_value = {
            "crypto": [mk_market(platform="kalshi"), mk_market(platform="polymarket")],
        }
        sc.client = fake_client
        with mock.patch.object(EventArbitrageScanner, "scan_markets",
                               return_value=[]) as sm:
            sc.scan()
            sm.assert_called_once()

    def test_event_key(self):
        k = mk_market(platform="kalshi", market_id="KX", question="BTC 100k?")
        p = mk_market(platform="polymarket", market_id="PX", question="BTC 100k?")
        key = EventArbitrageScanner._event_key(k, p)
        self.assertIn("::", key)

    def test_load_save_paper_trades(self):
        fd, path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        try:
            with mock.patch.object(A, "PAPER_TRADES_PATH", pathlib_path(path)):
                self.assertEqual(EventArbitrageScanner._load_paper_trades(), [])
                trades = [{"event_key": "x"}]
                EventArbitrageScanner._save_paper_trades(trades)
                self.assertEqual(EventArbitrageScanner._load_paper_trades(), trades)
        finally:
            os.unlink(path)

    def test_load_paper_trades_corrupt(self):
        fd, path = tempfile.mkstemp(suffix=".json")
        os.write(fd, b"not json{")
        os.close(fd)
        try:
            with mock.patch.object(A, "PAPER_TRADES_PATH", pathlib_path(path)):
                self.assertEqual(EventArbitrageScanner._load_paper_trades(), [])
        finally:
            os.unlink(path)

    def test_record_paper_trade_dedup(self):
        sc = EventArbitrageScanner()
        with mock.patch.object(A, "PAPER_TRADES_PATH", pathlib_path(tempfile.mktemp())):
            with mock.patch.object(sc, "_load_paper_trades", return_value=[]):
                with mock.patch.object(sc, "_save_paper_trades") as save:
                    buy = mk_market(platform="kalshi")
                    hedge = mk_market(platform="polymarket")
                    r = sc._record_paper_trade("ek", "crypto", buy, 0.3, hedge, 0.7, 0.4, 0.4, 0.5)
                    self.assertIsNotNone(r)
                    self.assertTrue(save.called)
                    # duplicate within 24h -> None
                    save.reset_mock()
                    r2 = sc._record_paper_trade("ek", "crypto", buy, 0.3, hedge, 0.7, 0.4, 0.4, 0.5)
                    self.assertIsNone(r2)
                    self.assertFalse(save.called)

    def test_record_paper_trade_old_timestamp(self):
        sc = EventArbitrageScanner()
        old = {"event_key": "ek", "timestamp": "2000-01-01T00:00:00Z"}
        with mock.patch.object(A, "PAPER_TRADES_PATH", pathlib_path(tempfile.mktemp())):
            with mock.patch.object(sc, "_load_paper_trades", return_value=[old]):
                with mock.patch.object(sc, "_save_paper_trades") as save:
                    buy = mk_market(platform="kalshi")
                    hedge = mk_market(platform="polymarket")
                    r = sc._record_paper_trade("ek", "crypto", buy, 0.3, hedge, 0.7, 0.4, 0.4, 0.5)
                    self.assertIsNotNone(r)
                    self.assertTrue(save.called)

    def test_record_paper_trade_bad_timestamp(self):
        sc = EventArbitrageScanner()
        old = {"event_key": "ek", "timestamp": "not-a-date"}
        with mock.patch.object(A, "PAPER_TRADES_PATH", pathlib_path(tempfile.mktemp())):
            with mock.patch.object(sc, "_load_paper_trades", return_value=[old]):
                with mock.patch.object(sc, "_save_paper_trades") as save:
                    buy = mk_market(platform="kalshi")
                    hedge = mk_market(platform="polymarket")
                    r = sc._record_paper_trade("ek", "crypto", buy, 0.3, hedge, 0.7, 0.4, 0.4, 0.5)
                    self.assertIsNotNone(r)
                    self.assertTrue(save.called)


def pathlib_path(p):
    import pathlib
    return pathlib.Path(p)


if __name__ == "__main__":
    import unittest
    unittest.main()
