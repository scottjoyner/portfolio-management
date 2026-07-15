#!/usr/bin/env python3
"""Tests for trading_system.arbitrage.opportunity_detector."""

import sys
import unittest
from unittest.mock import patch

import trading_system.arbitrage.opportunity_detector as mod
from trading_system.arbitrage.opportunity_detector import (
    Opportunity,
    OpportunityDetector,
)


class TestOpportunity(unittest.TestCase):
    def test_post_init_sets_timestamp(self):
        o = Opportunity(
            kalshi_market_id='K', polymarket_slug='P',
            kalshi_price=0.5, polymarket_price=0.6,
            title_kalshi='t', title_polymarket='q',
            divergence=0.1, arbitrage_potential_pct=10.0,
        )
        self.assertIsNotNone(o.timestamp)

    def test_post_init_keeps_timestamp(self):
        from datetime import datetime
        ts = datetime(2025, 1, 1)
        o = Opportunity(
            kalshi_market_id='K', polymarket_slug='P',
            kalshi_price=0.5, polymarket_price=0.6,
            title_kalshi='t', title_polymarket='q',
            divergence=0.1, arbitrage_potential_pct=10.0,
            timestamp=ts,
        )
        self.assertIs(o.timestamp, ts)


class TestDetectorBasic(unittest.TestCase):
    def test_init_defaults(self):
        d = OpportunityDetector()
        self.assertEqual(d.kalshi_markets, [])
        self.assertEqual(d.opportunities, [])

    def test_from_dict(self):
        d = OpportunityDetector()
        d.from_dict({
            'markets': [{'market_id': 'K1', 'title': 't1', 'bid': 50}],
            'events': [{'slug': 'P1', 'question': 'q1', 'bid': 60}],
        })
        self.assertEqual(d.kalshi_price_map, {'K1': 50})
        self.assertEqual(d.polymarket_price_map, {'P1': 60})

    def test_to_dict_kalshi_int_and_float(self):
        d = OpportunityDetector()
        d.kalshi_markets = [
            {'id': 'X', 'market_id': 'XM', 'bid': 50, 'title': 't'},
            {'id': 'Y', 'market_id': 'YM', 'bid': 50.0, 'title': 't'},
        ]
        res = d.to_dict_kalshi()
        self.assertEqual(res[0]['bid'], 0.5)
        self.assertEqual(res[1]['bid'], 50.0)
        self.assertEqual(res[0]['market_id'], 'X')

    def test_to_dict_polymarket_int_and_float(self):
        d = OpportunityDetector()
        d.polymarket_events = [
            {'id': 'X', 'slug': 'XS', 'bid': 50, 'question': 'q'},
            {'id': 'Y', 'slug': 'YS', 'bid': 50.0, 'question': 'q'},
        ]
        res = d.to_dict_polymarket()
        self.assertEqual(res[0]['bid'], 0.5)
        self.assertEqual(res[1]['bid'], 50.0)
        self.assertEqual(res[0]['slug'], 'XS')

    def test_to_list_int_and_float(self):
        d = OpportunityDetector()
        d.kalshi_markets = [{'id': 'X', 'market_id': 'XM', 'bid': 50, 'title': 't'}]
        d.polymarket_events = [
            {'id': 'Y', 'slug': 'YS', 'bid': 50.0, 'question': 'q'}]
        res = d.to_list()
        self.assertEqual(len(res), 2)

    def test_to_dict_empty(self):
        d = OpportunityDetector()
        res = d.to_dict()
        self.assertEqual(res['opportunities'], [])

    def test_to_list_with_opportunity_hits_buggy_call(self):
        d = OpportunityDetector()
        d.kalshi_markets = [{'id': 'X', 'market_id': 'XM', 'bid': 50, 'title': 't'}]
        d.opportunities = ['fake']
        # Line 169 calls self.to_dict(op) which raises (source bug, wrong arg).
        with self.assertRaises(TypeError):
            d.to_list()

    def test_from_list(self):
        data = [
            {'market_id': 'K1', 'bid': 50},
            {'slug': 'P1', 'bid': 60},
        ]
        d = OpportunityDetector.from_list(data)
        self.assertEqual(d.kalshi_price_map, {'K1': 50})
        self.assertEqual(d.polymarket_price_map, {'P1': 60})

    def test_to_json(self):
        d = OpportunityDetector()
        s = OpportunityDetector.to_json(d)
        self.assertIn('opportunities', s)


class TestDetect(unittest.TestCase):
    def _mk(self, kalshi_bid, pm_bid, ktitle='Same title here',
            ptitle='Same title here'):
        d = OpportunityDetector()
        d.kalshi_markets = [
            {'market_id': 'K', 'title': ktitle, 'bid': kalshi_bid}]
        d.polymarket_events = [
            {'slug': 'P', 'question': ptitle, 'bid': pm_bid}]
        return d

    def test_kalshi_cheaper_branch(self):
        d = self._mk(60, 70.0)
        opps = d.detect_opportunities()
        self.assertEqual(len(opps), 1)
        self.assertGreater(opps[0].arbitrage_potential_pct, 0)

    def test_pm_cheaper_branch(self):
        d = self._mk(80.0, 70.0)
        opps = d.detect_opportunities()
        self.assertEqual(len(opps), 1)

    def test_divergence_below_threshold(self):
        d = self._mk(70.05, 70.0)
        opps = d.detect_opportunities()
        self.assertEqual(opps, [])

    def test_no_similarity_match(self):
        d = self._mk(60, 70.0, ktitle='Apple pie', ptitle='Banana bread')
        opps = d.detect_opportunities()
        self.assertEqual(opps, [])

    def test_find_pm_matches(self):
        d = self._mk(60, 70.0, ktitle='Bitcoin market',
                     ptitle='Bitcoin market')
        matches = d._find_pm_matches(d.kalshi_markets[0])
        self.assertEqual(len(matches), 1)


if __name__ == '__main__':
    unittest.main()
