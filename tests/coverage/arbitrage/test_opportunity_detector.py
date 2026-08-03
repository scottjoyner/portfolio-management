#!/usr/bin/env python3
"""Tests for trading_system.arbitrage.opportunity_detector."""

from datetime import datetime
import unittest

from trading_system.arbitrage.opportunity_detector import Opportunity, OpportunityDetector


class TestOpportunity(unittest.TestCase):
    def test_post_init_sets_timestamp(self):
        opportunity = Opportunity(
            kalshi_market_id="K",
            polymarket_slug="P",
            kalshi_price=0.5,
            polymarket_price=0.6,
            title_kalshi="t",
            title_polymarket="q",
            divergence=0.1,
            arbitrage_potential_pct=0.097,
        )
        self.assertIsNotNone(opportunity.timestamp)

    def test_post_init_keeps_timestamp(self):
        timestamp = datetime(2025, 1, 1)
        opportunity = Opportunity(
            kalshi_market_id="K",
            polymarket_slug="P",
            kalshi_price=0.5,
            polymarket_price=0.6,
            title_kalshi="t",
            title_polymarket="q",
            divergence=0.1,
            arbitrage_potential_pct=0.097,
            timestamp=timestamp,
        )
        self.assertIs(opportunity.timestamp, timestamp)


class TestDetectorBasic(unittest.TestCase):
    def test_init_defaults(self):
        detector = OpportunityDetector()
        self.assertEqual(detector.kalshi_markets, [])
        self.assertEqual(detector.opportunities, [])

    def test_from_dict_normalizes_percent_prices(self):
        detector = OpportunityDetector()
        detector.from_dict({
            "markets": [{"market_id": "K1", "title": "t1", "bid": 50}],
            "events": [{"slug": "P1", "question": "q1", "bid": 60}],
        })
        self.assertEqual(detector.kalshi_price_map, {"K1": 0.5})
        self.assertEqual(detector.polymarket_price_map, {"P1": 0.6})

    def test_to_dict_kalshi_normalizes_int_and_float(self):
        detector = OpportunityDetector()
        detector.kalshi_markets = [
            {"id": "X", "market_id": "XM", "bid": 50, "title": "t"},
            {"id": "Y", "market_id": "YM", "bid": 50.0, "title": "t"},
        ]
        result = detector.to_dict_kalshi()
        self.assertEqual(result[0]["bid"], 0.5)
        self.assertEqual(result[1]["bid"], 0.5)
        self.assertEqual(result[0]["market_id"], "XM")

    def test_to_dict_polymarket_normalizes_int_and_float(self):
        detector = OpportunityDetector()
        detector.polymarket_events = [
            {"id": "X", "slug": "XS", "bid": 50, "question": "q"},
            {"id": "Y", "slug": "YS", "bid": 50.0, "question": "q"},
        ]
        result = detector.to_dict_polymarket()
        self.assertEqual(result[0]["bid"], 0.5)
        self.assertEqual(result[1]["bid"], 0.5)
        self.assertEqual(result[0]["slug"], "XS")

    def test_to_list_int_and_float(self):
        detector = OpportunityDetector()
        detector.kalshi_markets = [{"id": "X", "market_id": "XM", "bid": 50, "title": "t"}]
        detector.polymarket_events = [{"id": "Y", "slug": "YS", "bid": 50.0, "question": "q"}]
        self.assertEqual(len(detector.to_list()), 2)

    def test_to_dict_empty(self):
        self.assertEqual(OpportunityDetector().to_dict()["opportunities"], [])

    def test_to_list_serializes_opportunity(self):
        detector = OpportunityDetector()
        detector.opportunities = [
            Opportunity(
                kalshi_market_id="K",
                polymarket_slug="P",
                kalshi_price=0.5,
                polymarket_price=0.6,
                title_kalshi="t",
                title_polymarket="q",
                divergence=0.1,
                arbitrage_potential_pct=0.097,
            )
        ]
        result = detector.to_list()
        self.assertEqual(result[0]["kalshi_market_id"], "K")

    def test_from_list(self):
        detector = OpportunityDetector.from_list([
            {"market_id": "K1", "bid": 50},
            {"slug": "P1", "bid": 60},
        ])
        self.assertEqual(detector.kalshi_price_map, {"K1": 0.5})
        self.assertEqual(detector.polymarket_price_map, {"P1": 0.6})

    def test_to_json(self):
        self.assertIn("opportunities", OpportunityDetector.to_json(OpportunityDetector()))


class TestDetect(unittest.TestCase):
    def _mk(self, kalshi_bid, pm_bid, ktitle="Same title here", ptitle="Same title here"):
        detector = OpportunityDetector()
        detector.kalshi_markets = [{"market_id": "K", "title": ktitle, "bid": kalshi_bid}]
        detector.polymarket_events = [{"slug": "P", "question": ptitle, "bid": pm_bid}]
        return detector

    def test_kalshi_cheaper_branch(self):
        opportunities = self._mk(60, 70.0).detect_opportunities()
        self.assertEqual(len(opportunities), 1)
        self.assertGreater(opportunities[0].arbitrage_potential_pct, 0)

    def test_pm_cheaper_branch(self):
        self.assertEqual(len(self._mk(80.0, 70.0).detect_opportunities()), 1)

    def test_divergence_below_threshold(self):
        self.assertEqual(self._mk(70.05, 70.0).detect_opportunities(), [])

    def test_no_similarity_match(self):
        self.assertEqual(
            self._mk(60, 70.0, ktitle="Apple pie", ptitle="Banana bread").detect_opportunities(),
            [],
        )

    def test_find_pm_matches(self):
        detector = self._mk(60, 70.0, ktitle="Bitcoin market", ptitle="Bitcoin market")
        self.assertEqual(len(detector._find_pm_matches(detector.kalshi_markets[0])), 1)


if __name__ == "__main__":
    unittest.main()
