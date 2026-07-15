#!/usr/bin/env python3
"""Tests for trading_system.arbitrage.real_time_arbitrage."""

import sys
import json
import os
import unittest
from datetime import datetime
from unittest.mock import patch, mock_open, MagicMock

import trading_system.arbitrage.real_time_arbitrage as mod
from trading_system.arbitrage.real_time_arbitrage import (
    MarketPair,
    KalshiAPI,
    PolymarketAPI,
    ArbitrageOpportunity,
    ArbitrageManager,
    detect_opportunities,
)


class FakeResp:
    def __init__(self, status_code=200, json_data=None, text=''):
        self.status_code = status_code
        self._json = json_data
        self.text = text

    def json(self):
        return self._json


class TestMarketPair(unittest.TestCase):
    def test_post_init_sets_timestamp(self):
        mp = MarketPair('K', 'P', 0.5, 0.6, 't', 'q', 0.1, 10.0, None)
        self.assertIsNotNone(mp.timestamp)

    def test_post_init_keeps_timestamp(self):
        ts = datetime(2025, 1, 1)
        mp = MarketPair('K', 'P', 0.5, 0.6, 't', 'q', 0.1, 10.0, ts)
        self.assertIs(mp.timestamp, ts)


class TestKalshiAPI(unittest.TestCase):
    def test_get_markets_no_key_uses_sample(self):
        api = KalshiAPI()
        markets = api.get_markets()
        self.assertEqual(len(markets), 2)
        self.assertIn('bid', markets[0])

    def test_get_markets_with_key_success(self):
        api = KalshiAPI(api_key='k')
        api.session.get = lambda *a, **k: FakeResp(200, {
            'items': [{'market_id': 'M', 'full_title': 'T',
                       'category': 'c', 'market_type': 'mt', 'bid_price': '55'}]})
        markets = api.get_markets()
        self.assertEqual(markets[0]['market_id'], 'M')

    def test_get_markets_with_key_exception_falls_back(self):
        api = KalshiAPI(api_key='k')
        api.session.get = lambda *a, **k: (_ for _ in ()).throw(Exception('boom'))
        markets = api.get_markets()
        self.assertEqual(len(markets), 2)

    def test_scrape_markets_file_exists(self):
        api = KalshiAPI()
        data = {'markets': [{'id': 'M', 'title': 'T',
                             'category': 'c', 'ticker': 'tk', 'bid': 55}]}
        m = mock_open(read_data=json.dumps(data))
        with patch.object(mod.os.path, 'exists', return_value=True), \
                patch('builtins.open', m):
            markets = api._scrape_markets()
        self.assertEqual(markets[0]['market_id'], 'M')
        self.assertEqual(markets[0]['bid'], 0.55)

    def test_scrape_markets_exception(self):
        api = KalshiAPI()
        with patch.object(mod.os.path, 'exists', return_value=False):
            markets = api._scrape_markets()
        self.assertEqual(len(markets), 2)

    def test_get_markets_with_key_non200_falls_back(self):
        api = KalshiAPI(api_key='k')
        api.session.get = lambda *a, **k: FakeResp(400)
        markets = api.get_markets()
        self.assertEqual(len(markets), 2)

    def test_scrape_markets_file_open_error(self):
        api = KalshiAPI()
        with patch.object(mod.os.path, 'exists', return_value=True), \
                patch('builtins.open', side_effect=Exception('no read')):
            markets = api._scrape_markets()
        self.assertEqual(markets, [])

    def test_place_order_no_key(self):
        api = KalshiAPI()
        order = api.place_order('M', 'buy', 10, 0.5)
        self.assertIn('order_id', order)

    def test_place_order_with_key_201(self):
        api = KalshiAPI(api_key='k')
        api.session.post = lambda *a, **k: FakeResp(201, {'order': {'id': 'O1'}})
        order = api.place_order('M', 'buy', 10, 0.5)
        self.assertEqual(order['order_id'], 'O1')

    def test_place_order_with_key_not_201(self):
        api = KalshiAPI(api_key='k')
        api.session.post = lambda *a, **k: FakeResp(400)
        order = api.place_order('M', 'buy', 10, 0.5)
        self.assertIsNone(order)

    def test_place_order_with_key_exception(self):
        api = KalshiAPI(api_key='k')
        api.session.post = lambda *a, **k: (_ for _ in ()).throw(Exception('boom'))
        order = api.place_order('M', 'buy', 10, 0.5)
        self.assertIn('order_id', order)

    def test_mock_order(self):
        api = KalshiAPI()
        order = api._mock_order('M', 'buy', 10, 0.5)
        self.assertEqual(order['total_cost'], 5.0)


class TestPolymarketAPI(unittest.TestCase):
    def test_get_events_no_key_uses_sample(self):
        api = PolymarketAPI()
        events = api.get_events()
        self.assertEqual(len(events), 2)

    def test_get_events_with_key_success(self):
        api = PolymarketAPI(api_key='k')
        api.session.get = lambda *a, **k: FakeResp(200, {
            'results': [{'slug': 'S', 'title': 'T',
                         'primaryTopic': 'crypto', 'volume_1h': '100'}]})
        events = api.get_events()
        self.assertEqual(events[0]['slug'], 'S')

    def test_get_events_with_key_bad_volume(self):
        api = PolymarketAPI(api_key='k')
        api.session.get = lambda *a, **k: FakeResp(200, {
            'results': [{'slug': 'S', 'title': 'T',
                         'primaryTopic': 'crypto', 'volume_1h': 'abc',
                         'volume': 'xyz'}]})
        events = api.get_events()
        self.assertEqual(events[0]['bid'], 0.5)

    def test_get_events_with_key_category_filter(self):
        api = PolymarketAPI(api_key='k')
        api.session.get = lambda *a, **k: FakeResp(200, {
            'results': [{'slug': 'S', 'title': 'T',
                         'primaryTopic': 'sports', 'volume_1h': '100'}]})
        events = api.get_events(category='sports')
        self.assertEqual(len(events), 1)
        # Non-matching category returns no events (filtered out)
        events2 = api.get_events(category='politics')
        self.assertEqual(len(events2), 0)

    def test_get_events_with_key_exception(self):
        api = PolymarketAPI(api_key='k')
        api.session.get = lambda *a, **k: (_ for _ in ()).throw(Exception('boom'))
        events = api.get_events()
        self.assertEqual(len(events), 2)

    def test_get_events_with_key_non200_falls_back(self):
        api = PolymarketAPI(api_key='k')
        api.session.get = lambda *a, **k: FakeResp(400)
        events = api.get_events()
        self.assertEqual(len(events), 2)

    def test_scrape_events_file_open_error(self):
        api = PolymarketAPI()
        with patch.object(mod.os.path, 'exists', return_value=True), \
                patch('builtins.open', side_effect=Exception('no read')):
            events = api._scrape_events()
        self.assertEqual(events, [])

    def test_scrape_events_file_exists(self):
        api = PolymarketAPI()
        data = {'events': [{'id': 'S', 'question': 'Q',
                            'topic': 't', 'bid': 55}]}
        m = mock_open(read_data=json.dumps(data))
        with patch.object(mod.os.path, 'exists', return_value=True), \
                patch('builtins.open', m):
            events = api._scrape_events()
        self.assertEqual(events[0]['slug'], 'S')
        self.assertEqual(events[0]['bid'], 0.55)

    def test_place_order_no_key(self):
        api = PolymarketAPI()
        order = api.place_order('S', 'buy', 10, 0.5)
        self.assertIn('order_id', order)

    def test_place_order_with_key_201(self):
        api = PolymarketAPI(api_key='k')
        api.session.post = lambda *a, **k: FakeResp(201, {'order': {'id': 'O1'}})
        order = api.place_order('S', 'buy', 10, 0.5)
        self.assertEqual(order['order_id'], 'O1')

    def test_place_order_with_key_not_201(self):
        api = PolymarketAPI(api_key='k')
        api.session.post = lambda *a, **k: FakeResp(400)
        order = api.place_order('S', 'buy', 10, 0.5)
        self.assertIsNone(order)

    def test_place_order_with_key_exception(self):
        api = PolymarketAPI(api_key='k')
        api.session.post = lambda *a, **k: (_ for _ in ()).throw(Exception('boom'))
        order = api.place_order('S', 'buy', 10, 0.5)
        self.assertIn('order_id', order)

    def test_mock_order(self):
        api = PolymarketAPI()
        order = api._mock_order('S', 'buy', 10, 0.5)
        self.assertEqual(order['total_cost'], 5.0)


def _pair(kalshi_price=0.5, pm_price=0.6):
    return MarketPair('K', 'P', kalshi_price, pm_price, 't', 'q', 0.1, 10.0,
                      datetime(2025, 1, 1))


class TestArbitrageOpportunity(unittest.TestCase):
    def test_buy_kalshi(self):
        o = ArbitrageOpportunity(_pair(0.5, 0.6))
        self.assertEqual(o.buy_platform, 'kalshi')
        self.assertEqual(o.sell_platform, 'polymarket')

    def test_buy_polymarket(self):
        o = ArbitrageOpportunity(_pair(0.7, 0.6))
        self.assertEqual(o.buy_platform, 'polymarket')
        self.assertEqual(o.sell_platform, 'kalshi')

    def test_strategies(self):
        for strat in ('balanced', 'kalshi_first', 'pm_first', 'weird'):
            o = ArbitrageOpportunity(_pair(0.5, 0.6), strategy=strat)
            self.assertGreater(o.buy_units, 0)

    def test_estimate_profit_buy_kalshi(self):
        o = ArbitrageOpportunity(_pair(0.5, 0.6))
        prof = o.estimate_profit()
        self.assertIn('net_profit', prof)
        self.assertGreater(prof['buy_cost'], 0)

    def test_estimate_profit_buy_polymarket(self):
        o = ArbitrageOpportunity(_pair(0.7, 0.6))
        prof = o.estimate_profit()
        self.assertIn('net_profit', prof)


class TestDetect(unittest.TestCase):
    def _mk(self, kbid, pbid, ktitle='Bitcoin market', ptitle='Bitcoin market'):
        return ([{'market_id': 'K', 'title': ktitle, 'bid': kbid}],
                [{'slug': 'P', 'question': ptitle, 'bid': pbid}])

    def test_detect_finds(self):
        k, p = self._mk(60, 70.0)
        opps = detect_opportunities(k, p)
        self.assertTrue(len(opps) >= 1)

    def test_detect_no_match(self):
        k, p = self._mk(60, 70.0, ktitle='Apple pie', ptitle='Banana bread')
        self.assertEqual(detect_opportunities(k, p), [])

    def test_detect_low_divergence(self):
        k, p = self._mk(60, 60)
        self.assertEqual(detect_opportunities(k, p), [])

    def test_detect_int_bids(self):
        k, p = self._mk(60, 70)
        opps = detect_opportunities(k, p)
        self.assertTrue(len(opps) >= 1)


class FakeManager:
    def __init__(self, *a, **k):
        self.last_opportunities = []

    def run_full_cycle(self, *a, **k):
        return ([], [])


class TestArbitrageManager(unittest.TestCase):
    def test_init(self):
        m = ArbitrageManager()
        self.assertIsNotNone(m.kalshi)
        self.assertIsNotNone(m.polymarket)

    def test_run_detection(self):
        m = ArbitrageManager()
        opps = m.run_detection()
        self.assertIsInstance(opps, list)

    def test_execute_trades_empty(self):
        m = ArbitrageManager()
        self.assertEqual(m.execute_trades(), [])

    def test_execute_trades(self):
        m = ArbitrageManager()
        m.last_opportunities = [ArbitrageOpportunity(_pair(0.5, 0.6))]
        results = m.execute_trades()
        self.assertEqual(len(results), 1)

    def test_execute_trades_error(self):
        m = ArbitrageManager()
        opp = ArbitrageOpportunity(_pair(0.5, 0.6))
        m.last_opportunities = [opp]
        m.kalshi.place_order = lambda *a, **k: (_ for _ in ()).throw(Exception('boom'))
        results = m.execute_trades()
        self.assertEqual(results, [])

    def test_run_full_cycle_no_opps(self):
        m = ArbitrageManager()
        m.run_detection = lambda *a, **k: []
        opps, res = m.run_full_cycle()
        self.assertEqual(opps, [])

    def test_run_full_cycle_with_opps(self):
        m = ArbitrageManager()
        opp = ArbitrageOpportunity(_pair(0.5, 0.6))
        m.run_detection = lambda *a, **k: [opp]
        m.execute_trades = lambda *a, **k: [{'x': 1}]
        m._save_results = lambda *a, **k: None
        opps, res = m.run_full_cycle()
        self.assertEqual(len(opps), 1)

    def test_save_results(self):
        m = ArbitrageManager()
        m = ArbitrageManager()
        opp = ArbitrageOpportunity(_pair(0.5, 0.6))
        mo = mock_open()
        with patch('builtins.open', mo):
            m._save_results([opp], [{'x': 1}])
        self.assertTrue(mo.called)


class TestMain(unittest.TestCase):
    def test_main_mock_mode(self):
        m = mock_open()
        with patch('builtins.open', m):
            rc = mod.main()
        self.assertEqual(rc, 0)

    def test_main_real_mode(self):
        m = mock_open()
        opp = ArbitrageOpportunity(_pair(0.5, 0.6))

        class FakeMgr:
            def __init__(self, *a, **k):
                pass

            def run_full_cycle(self, *a, **k):
                return ([opp], [{'x': 1}])

        with patch.dict(os.environ, {'KALSHI_API_KEY': 'k', 'POLYMARKET_API_KEY': 'p'}), \
                patch.object(mod, 'ArbitrageManager', FakeMgr), \
                patch('builtins.open', m):
            rc = mod.main()
        self.assertEqual(rc, 0)

    def test_main_real_mode_no_opps(self):
        m = mock_open()

        class FakeMgr:
            def __init__(self, *a, **k):
                pass

            def run_full_cycle(self, *a, **k):
                return ([], [])

        with patch.dict(os.environ, {'KALSHI_API_KEY': 'k', 'POLYMARKET_API_KEY': 'p'}), \
                patch.object(mod, 'ArbitrageManager', FakeMgr), \
                patch('builtins.open', m):
            rc = mod.main()
        self.assertEqual(rc, 0)


if __name__ == '__main__':
    unittest.main()
