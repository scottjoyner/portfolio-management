#!/usr/bin/env python3
"""Tests for trading_system.arbitrage.orchestrator."""

import sys
import json
import logging
import unittest
from datetime import datetime
from unittest.mock import patch, mock_open, MagicMock

_real_fh = logging.FileHandler
logging.FileHandler = lambda *a, **k: MagicMock()

import trading_system.arbitrage.orchestrator as mod
from trading_system.arbitrage.orchestrator import ArbitrageOrchestrator
from trading_system.arbitrage.real_time_arbitrage import MarketPair, ArbitrageOpportunity

logging.FileHandler = _real_fh


def _opp(kalshi_price=0.5, pm_price=0.6):
    mp = MarketPair('K', 'P', kalshi_price, pm_price, 't', 'q', 0.1, 10.0,
                    datetime(2025, 1, 1))
    return ArbitrageOpportunity(mp)


class FakeMgr:
    def __init__(self):
        self.calls = []

    def place_order(self, **kw):
        self.calls.append(kw)
        return {'order_id': 'ORD'}


class TestOrchestratorInit(unittest.TestCase):
    def test_init(self):
        o = ArbitrageOrchestrator()
        self.assertIsNotNone(o.scraper)
        self.assertIsNotNone(o.manager)
        self.assertIsNone(o.last_run_time)


class TestRunOpportunityDetection(unittest.TestCase):
    def _markets(self):
        return (
            [{'market_id': 'K', 'title': 'Bitcoin market', 'bid': 60}],
            [{'slug': 'P', 'question': 'Bitcoin market', 'bid': 70.0}],
        )

    def test_detection_finds(self):
        o = ArbitrageOrchestrator()
        km, pm = self._markets()
        o.scraper.scrape_markets = lambda *a, **k: {
            'kalshi_markets': km, 'polymarket_events': pm}
        m = mock_open()
        with patch('builtins.open', m):
            res = o.run_opportunity_detection(category='all')
        self.assertTrue(len(res) >= 1)

    def test_detection_no_opps(self):
        o = ArbitrageOrchestrator()
        o.scraper.scrape_markets = lambda *a, **k: {
            'kalshi_markets': [],
            'polymarket_events': [{'slug': 'P', 'question': 'Q', 'bid': 50}]}
        res = o.run_opportunity_detection(category='all')
        self.assertEqual(res, [])

    def test_detection_execute_in_loop(self):
        # Force opportunities from detect and exercise the printing/save loop
        o = ArbitrageOrchestrator()
        km, pm = self._markets()
        o.scraper.scrape_markets = lambda *a, **k: {
            'kalshi_markets': km, 'polymarket_events': pm}
        m = mock_open()
        with patch('builtins.open', m):
            res = o.run_opportunity_detection(category='all', limit=10)
        self.assertTrue(len(res) >= 1)


class TestExecuteTopOpportunities(unittest.TestCase):
    def test_empty(self):
        o = ArbitrageOrchestrator()
        o.manager.last_opportunities = []
        self.assertEqual(o.execute_top_opportunities(), [])

    def test_execute(self):
        o = ArbitrageOrchestrator()
        o.manager.last_opportunities = [_opp(0.5, 0.6), _opp(0.7, 0.6)]
        o.manager._execute_single_trade = lambda opp: {
            'buy_order_id': 'B', 'sell_order_id': 'S', 'expected_profit': 1.0}
        results = o.execute_top_opportunities(top_n=3, strategy='balanced')
        self.assertEqual(len(results), 2)

    def test_execute_handles_error(self):
        o = ArbitrageOrchestrator()
        o.manager.last_opportunities = [_opp(0.5, 0.6)]
        o.manager._execute_single_trade = lambda *a, **k: (_ for _ in ()).throw(
            Exception('boom'))
        results = o.execute_top_opportunities()
        self.assertEqual(results, [])

    def test_execute_none_result(self):
        o = ArbitrageOrchestrator()
        o.manager.last_opportunities = [_opp(0.5, 0.6)]
        o.manager._execute_single_trade = lambda opp: None
        results = o.execute_top_opportunities()
        self.assertEqual(results, [])


class TestRunFullPipeline(unittest.TestCase):
    def test_no_opps(self):
        o = ArbitrageOrchestrator()
        o.run_opportunity_detection = lambda *a, **k: []
        o._save_complete_results = lambda *a, **k: None
        opps, res = o.run_full_pipeline()
        self.assertEqual(opps, [])

    def test_with_opps(self):
        o = ArbitrageOrchestrator()
        o.run_opportunity_detection = lambda *a, **k: [_opp(0.5, 0.6)]
        o.execute_top_opportunities = lambda *a, **k: [{'x': 1}]
        o._save_complete_results = lambda *a, **k: None
        opps, res = o.run_full_pipeline()
        self.assertEqual(len(opps), 1)


class TestExecuteSingleTrade(unittest.TestCase):
    def test_dict_kalshi_buy(self):
        o = ArbitrageOrchestrator()
        o.kalshi_manager = FakeMgr()
        o.polymarket_manager = FakeMgr()
        opp = {
            'kalshi_market_id': 'K', 'polymarket_slug': 'P',
            'kalshi_price': 0.5, 'polymarket_price': 0.6,
            'kalshi_title': 't', 'polymarket_question': 'q',
            'divergence': 0.1, 'arbitrage_potential_pct': 10.0,
            'timestamp': datetime(2025, 1, 1),
        }
        res = o._execute_single_trade(opp)
        self.assertIn('buy_result', res)
        self.assertTrue(o.kalshi_manager.calls)  # kalshi buy
        self.assertTrue(o.polymarket_manager.calls)  # polymarket sell

    def test_object_pm_buy(self):
        o = ArbitrageOrchestrator()
        o.kalshi_manager = FakeMgr()
        o.polymarket_manager = FakeMgr()
        res = o._execute_single_trade(_opp(0.7, 0.6))
        self.assertIn('buy_result', res)
        # buy is polymarket, sell is kalshi
        self.assertTrue(o.polymarket_manager.calls)
        self.assertTrue(o.kalshi_manager.calls)


if __name__ == '__main__':
    unittest.main()
