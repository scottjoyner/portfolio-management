#!/usr/bin/env python3
"""Tests for trading_system.arbitrage.detect_opportunities."""

import sys
import unittest
from unittest.mock import patch, mock_open

import trading_system.arbitrage.detect_opportunities as mod


class TestNormalizeAndSimilarity(unittest.TestCase):
    def test_normalize_string(self):
        self.assertEqual(mod.normalize_string("  Hello, WORLD!  "), "hello world")
        self.assertEqual(mod.normalize_string("BTC-100K"), "btc100k")

    def test_calculate_text_similarity(self):
        s = mod.calculate_text_similarity(
            "Bitcoin will hit 100k", "bitcoin will hit 100k"
        )
        self.assertGreater(s, 0.9)
        self.assertLessEqual(s, 1.0)


class TestLoadSampleData(unittest.TestCase):
    def test_load_sample_data(self):
        kalshi, pm = mod.load_sample_data()
        self.assertEqual(len(kalshi), 3)
        self.assertEqual(len(pm), 3)


class TestDetectOpportunities(unittest.TestCase):
    def test_detect_finds_opportunities(self):
        kalshi, pm = mod.load_sample_data()
        opps = mod.detect_opportunities(kalshi, pm)
        self.assertTrue(len(opps) >= 1)
        first = opps[0]
        self.assertIn('divergence', first)
        # sorted descending by potential
        for a, b in zip(opps, opps[1:]):
            self.assertGreaterEqual(
                a['arbitrage_potential_pct'], b['arbitrage_potential_pct']
            )

    def test_no_match_low_threshold(self):
        kalshi = [{'market_id': 'X', 'title': 'Apple pie recipe', 'bid_pct': 50}]
        pm = [{'slug': 'y', 'question': 'Banana bread method', 'bid_pct': 50}]
        opps = mod.detect_opportunities(kalshi, pm, similarity_threshold=0.99)
        self.assertEqual(opps, [])

    def test_divergence_threshold(self):
        kalshi = [{'market_id': 'X', 'title': 'Will BTC hit 100k', 'bid_pct': 50}]
        pm = [{'slug': 'y', 'question': 'Will BTC hit 100k', 'bid_pct': 50.5}]
        opps = mod.detect_opportunities(
            kalshi, pm, similarity_threshold=0.9, min_divergence=0.1
        )
        self.assertEqual(opps, [])

    def test_bad_bid_continues(self):
        kalshi = [{'market_id': 'X', 'title': 'Will BTC hit 100k', 'bid_pct': 'notanumber'}]
        pm = [{'slug': 'y', 'question': 'Will BTC hit 100k', 'bid_pct': 50.5}]
        # Should not raise; bad conversion is caught
        opps = mod.detect_opportunities(kalshi, pm, similarity_threshold=0.9)
        self.assertEqual(opps, [])


class TestAnalyzeOpportunity(unittest.TestCase):
    def test_tuple_returns_default(self):
        res = mod.analyze_opportunity(("a", "b"))
        self.assertEqual(res['buy_platform'], None)
        self.assertEqual(res['roi_pct'], 0.0)

    def test_empty_returns_default(self):
        res = mod.analyze_opportunity([])
        self.assertEqual(res['buy_platform'], None)

    def test_kalshi_cheaper(self):
        opp = {
            'kalshi': {'bid_pct': 40, 'category': 'cryptocurrency'},
            'polymarket_event': {'bid_pct': 60},
        }
        res = mod.analyze_opportunity(opp)
        self.assertEqual(res['buy_platform'], 'kalshi')
        self.assertEqual(res['sell_platform'], 'polymarket')
        self.assertGreater(res['net_profit'], 0)

    def test_polymarket_cheaper(self):
        opp = {
            'kalshi': {'bid_pct': 60, 'category': 'cryptocurrency'},
            'polymarket_event': {'bid_pct': 40},
        }
        res = mod.analyze_opportunity(opp)
        self.assertEqual(res['buy_platform'], 'polymarket')
        self.assertEqual(res['sell_platform'], 'kalshi')
        self.assertGreater(res['net_profit'], 0)


def _fake_opps(*args, **kwargs):
    return [{
        'id': 'KLS-PM',
        'kalshi': {'market_id': 'BTC-JAN', 'bid_pct': 58.5},
        'polymarket_event': {'slug': 'btc-jan', 'bid_pct': 46.8},
        'arbitrage_potential_pct': 11.7,
    }]


def _fake_analysis(*args, **kwargs):
    return {
        'buy_platform': 'kalshi', 'sell_platform': 'polymarket',
        'buy_price': 0.585, 'sell_price': 0.532,
        'position_size_usd': 5000, 'contract_units': 8547,
        'buy_cost': 5000.0, 'sell_revenue': 4545.0,
        'gross_profit': 455.0, 'buy_fees': 100.0, 'sell_fees': 90.0,
        'net_profit': 265.0, 'roi_pct': 5.3,
    }


class TestMain(unittest.TestCase):
    def test_main_writes_results(self):
        m = mock_open()
        with patch.object(mod, 'detect_opportunities', _fake_opps), \
                patch.object(mod, 'analyze_opportunity', _fake_analysis), \
                patch.object(mod, 'open', m):
            rc = mod.main()
        self.assertEqual(rc, 0)
        self.assertTrue(m.called)

    def test_main_no_opportunities(self):
        m = mock_open()
        with patch.object(mod, 'detect_opportunities', return_value=[]), \
                patch.object(mod, 'open', m):
            rc = mod.main()
        self.assertEqual(rc, 0)


if __name__ == '__main__':
    unittest.main()
