#!/usr/bin/env python3
"""Tests for trading_system.arbitrage.main."""

import sys
import json
import unittest
from unittest.mock import patch, mock_open, MagicMock

import trading_system.arbitrage.main as mod
import trading_system.arbitrage.opportunity_detector as od_mod
import trading_system.arbitrage.arb_trader as at_mod


class FakeKalshi:
    def __init__(self):
        self.orders = []

    def get_markets(self, category=None):
        return []

    def create_order(self, **kw):
        return {'order_id': 'K', 'status': 'open', 'total_cost': 1.0}


class FakePM:
    def __init__(self):
        self.orders = []

    def get_events(self, category=None):
        return []

    def create_order(self, **kw):
        return {'order_id': 'P', 'status': 'open', 'total_cost': 1.0}


class FakeResult:
    kalshi_order_id = 'K'
    polymarket_order_id = 'P'
    kalshi_status = 'open'
    polymarket_status = 'open'


class FakeOpp:
    pass


class FakeDetectorNoOpp:
    def from_dict(self, d):
        pass

    def detect_opportunities(self):
        return []


class FakeDetector:
    def from_dict(self, d):
        pass

    def detect_opportunities(self):
        return [FakeOpp()]

    def to_dict(self, op):
        return {}


class FakeTrader:
    def execute_all_opportunities(self, opps):
        return [FakeResult()]


class TestMain(unittest.TestCase):
    def test_main_no_opportunities(self):
        with patch.object(mod, 'MockKalshiClient', FakeKalshi), \
                patch.object(mod, 'MockPolymarketClient', FakePM), \
                patch.object(od_mod, 'OpportunityDetector', FakeDetectorNoOpp):
            rc = mod.main()
        self.assertEqual(rc, 0)

    def test_main_with_opportunities(self):
        m = mock_open()
        with patch.object(mod, 'MockKalshiClient', FakeKalshi), \
                patch.object(mod, 'MockPolymarketClient', FakePM), \
                patch.object(od_mod, 'OpportunityDetector', FakeDetector), \
                patch.object(at_mod, 'ArbitrageTrader', FakeTrader, create=True), \
                patch('builtins.open', m):
            rc = mod.main()
        self.assertEqual(rc, 0)
        self.assertTrue(m.called)


class TestMarketPair(unittest.TestCase):
    def test_post_init_sets_timestamp(self):
        from datetime import datetime as dt
        mp = mod.MarketPair('K', 'P', 0.5, 0.6, 't', 'q', 0.1, 10.0, None)
        self.assertIsNotNone(mp.timestamp)

    def test_post_init_keeps_timestamp(self):
        from datetime import datetime as dt
        ts = dt(2025, 1, 1)
        mp = mod.MarketPair('K', 'P', 0.5, 0.6, 't', 'q', 0.1, 10.0, ts)
        self.assertIs(mp.timestamp, ts)


if __name__ == '__main__':
    unittest.main()


class FakeResp:
    def __init__(self, status_code=200, json_data=None):
        self.status_code = status_code
        self._json = json_data
        self.text = ''

    def json(self):
        return self._json


class TestMockKalshiClient(unittest.IsolatedAsyncioTestCase):
    def test_init(self):
        c = mod.MockKalshiClient()
        self.assertEqual(c.orders, [])

    async def test_get_markets_no_key(self):
        c = mod.MockKalshiClient()
        markets = await c.get_markets(category='cryptocurrency')
        self.assertEqual(markets, [])

    async def test_get_markets_with_key_success(self):
        c = mod.MockKalshiClient()
        data = {'items': [{'market_id': 'M', 'full_title': 'T',
                           'category': 'c', 'market_type': 'mt',
                           'bid_price': '55'}]}
        with patch.dict('os.environ', {'KALSHI_API_KEY': 'k'}), \
                patch('requests.get', return_value=FakeResp(200, data)):
            markets = await c.get_markets(category='cryptocurrency')
        self.assertEqual(markets[0]['market_id'], 'M')

    async def test_get_markets_with_key_error_raises(self):
        c = mod.MockKalshiClient()
        with patch.dict('os.environ', {'KALSHI_API_KEY': 'k'}), \
                patch('requests.get', side_effect=Exception('boom')):
            with self.assertRaises(Exception):
                await c.get_markets(category='cryptocurrency')

    async def test_get_markets_with_key_non200(self):
        c = mod.MockKalshiClient()
        with patch.dict('os.environ', {'KALSHI_API_KEY': 'k'}), \
                patch('requests.get', return_value=FakeResp(400)):
            markets = await c.get_markets(category='cryptocurrency')
        self.assertEqual(markets, [])

    async def test_create_order_no_key(self):
        c = mod.MockKalshiClient()
        order = await c.create_order('M', 'buy', 10, 0.5)
        self.assertIn('order_id', order)

    async def test_create_order_with_key_201(self):
        c = mod.MockKalshiClient()
        with patch.dict('os.environ', {'KALSHI_API_KEY': 'k'}), \
                patch('requests.post', return_value=FakeResp(201, {'order': {'id': 'O'}})):
            order = await c.create_order('M', 'buy', 10, 0.5)
        self.assertEqual(order['order_id'], 'O')

    async def test_create_order_with_key_failure(self):
        c = mod.MockKalshiClient()
        with patch.dict('os.environ', {'KALSHI_API_KEY': 'k'}), \
                patch('requests.post', return_value=FakeResp(400)):
            order = await c.create_order('M', 'buy', 10, 0.5)
        self.assertIsNone(order)


class TestMockPolymarketClient(unittest.IsolatedAsyncioTestCase):
    def test_init(self):
        c = mod.MockPolymarketClient()
        self.assertEqual(c.orders, [])

    async def test_get_events_no_key(self):
        c = mod.MockPolymarketClient()
        events = await c.get_events(category='cryptocurrency')
        self.assertEqual(events, [])

    async def test_get_events_with_key_success(self):
        c = mod.MockPolymarketClient()
        data = {'results': [{'slug': 'S', 'title': 'T',
                             'primaryTopic': 'crypto', 'volume_1h': '100'}]}
        with patch.dict('os.environ', {'POLYMARKET_API_KEY': 'p'}), \
                patch('requests.get', return_value=FakeResp(200, data)):
            events = await c.get_events(category='cryptocurrency')
        self.assertEqual(events[0]['slug'], 'S')
        self.assertEqual(events[0]['bid'], 100.0)

    async def test_get_events_with_key_missing_volume(self):
        c = mod.MockPolymarketClient()
        data = {'results': [{'slug': 'S', 'title': 'T',
                             'primaryTopic': 'crypto'}]}
        with patch.dict('os.environ', {'POLYMARKET_API_KEY': 'p'}), \
                patch('requests.get', return_value=FakeResp(200, data)):
            events = await c.get_events(category='cryptocurrency')
        self.assertEqual(events[0]['bid'], 0.0)

    async def test_get_events_with_key_error_raises(self):
        c = mod.MockPolymarketClient()
        with patch.dict('os.environ', {'POLYMARKET_API_KEY': 'p'}), \
                patch('requests.get', side_effect=Exception('boom')):
            with self.assertRaises(Exception):
                await c.get_events(category='cryptocurrency')

    async def test_create_order_no_key(self):
        c = mod.MockPolymarketClient()
        order = await c.create_order('S', 'buy', 10, 0.5)
        self.assertIn('order_id', order)

    async def test_create_order_with_key_201(self):
        c = mod.MockPolymarketClient()
        with patch.dict('os.environ', {'POLYMARKET_API_KEY': 'p'}), \
                patch('requests.post', return_value=FakeResp(201, {'order': {'id': 'O'}})):
            order = await c.create_order('S', 'buy', 10, 0.5)
        self.assertEqual(order['order_id'], 'O')

    async def test_create_order_with_key_failure(self):
        c = mod.MockPolymarketClient()
        with patch.dict('os.environ', {'POLYMARKET_API_KEY': 'p'}), \
                patch('requests.post', return_value=FakeResp(400)):
            order = await c.create_order('S', 'buy', 10, 0.5)
        self.assertIsNone(order)

    async def test_get_markets_key_non200_reads_file(self):
        c = mod.MockKalshiClient()
        data = {'markets': [{'id': 'M', 'title': 'T', 'category': 'c',
                            'ticker': 'tk', 'bid': 55}]}
        m = mock_open(read_data=json.dumps(data))
        with patch.dict('os.environ', {'KALSHI_API_KEY': 'k'}), \
                patch('requests.get', return_value=FakeResp(400)), \
                patch('os.path.exists', return_value=True), \
                patch('builtins.open', m):
            markets = await c.get_markets(category='crypto')
        self.assertEqual(markets[0]['bid'], 0.55)

    async def test_get_events_key_non200_reads_file(self):
        c = mod.MockPolymarketClient()
        data = {'events': [{'id': 'S', 'question': 'Q', 'topic': 't', 'bid': 55}]}
        m = mock_open(read_data=json.dumps(data))
        with patch.dict('os.environ', {'POLYMARKET_API_KEY': 'p'}), \
                patch('requests.get', return_value=FakeResp(400)), \
                patch('os.path.exists', return_value=True), \
                patch('builtins.open', m):
            events = await c.get_events(category='crypto')
        self.assertEqual(events[0]['bid'], 0.55)


class FakeRaise:
    def __init__(self):
        self.orders = []

    def get_markets(self, category=None):
        raise RuntimeError('boom')

    def get_events(self, category=None):
        raise RuntimeError('boom')

    def create_order(self, **kw):
        return {'order_id': 'X'}


class TestMainErrorPaths(unittest.TestCase):
    def test_main_kalshi_load_error(self):
        with patch.object(mod, 'MockKalshiClient', FakeRaise), \
                patch.object(mod, 'MockPolymarketClient', FakePM):
            rc = mod.main()
        self.assertEqual(rc, 1)

    def test_main_pm_load_error(self):
        class FakeRaisePM:
            def __init__(self):
                self.orders = []

            def get_markets(self, category=None):
                return []

            def get_events(self, category=None):
                raise RuntimeError('boom')

            def create_order(self, **kw):
                return {'order_id': 'X'}

        with patch.object(mod, 'MockKalshiClient', FakeKalshi), \
                patch.object(mod, 'MockPolymarketClient', FakeRaisePM):
            rc = mod.main()
        self.assertEqual(rc, 1)
