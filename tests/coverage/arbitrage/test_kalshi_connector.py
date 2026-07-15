#!/usr/bin/env python3
"""Tests for trading_system.arbitrage.kalshi_connector."""

import sys
import json
import unittest
from unittest.mock import patch, MagicMock

import urllib.error
import urllib.request

import trading_system.arbitrage.kalshi_connector as mod
from trading_system.arbitrage.kalshi_connector import (
    KalshiConnector,
    KalshiConnectorError,
    NotFoundError,
    RateLimitError,
)


class FakeResponse:
    def __init__(self, status, payload, headers=None):
        self.status = status
        self._payload = payload
        self._headers = headers or {}

    def read(self):
        return json.dumps(self._payload).encode()

    def getheader(self, name, default=None):
        return self._headers.get(name, default)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeHTTPError(urllib.error.HTTPError):
    def __init__(self, url, code, reason, headers=None, fp=None):
        super().__init__(url, code, reason, headers or {}, fp)
        self.code = code

    def getheader(self, name, default=None):
        return (self.headers or {}).get(name, default)


class TestGetRequest(unittest.TestCase):
    def test_success_with_headers(self):
        resp = FakeResponse(200, {'a': 1})
        with patch.object(urllib.request, 'urlopen', return_value=resp):
            data = mod._get_request('http://x', headers={'X': '1'})
        self.assertEqual(data['a'], 1)

    def test_success_list(self):
        resp = FakeResponse(200, [1, 2, 3])
        with patch.object(urllib.request, 'urlopen', return_value=resp):
            data = mod._get_request('http://x')
        self.assertEqual(data, [1, 2, 3])

    def test_429_retries_then_success(self):
        r429 = FakeResponse(429, {}, {'Retry-After': '1'})
        r200 = FakeResponse(200, {'ok': True})
        with patch.object(urllib.request, 'urlopen', side_effect=[r429, r200]), \
                patch('time.sleep'):
            data = mod._get_request('http://x')
        self.assertEqual(data['ok'], True)

    def test_404_raises(self):
        resp = FakeResponse(404, {})
        with patch.object(urllib.request, 'urlopen', return_value=resp):
            with self.assertRaises(NotFoundError):
                mod._get_request('http://x')

    def test_500_returns_empty(self):
        resp = FakeResponse(503, {'e': 1})
        with patch.object(urllib.request, 'urlopen', return_value=resp):
            data = mod._get_request('http://x')
        self.assertEqual(data, {})

    def test_httperror_429_retries(self):
        err = FakeHTTPError('http://x', 429, 'rate', {'Retry-After': '1'})
        r200 = FakeResponse(200, {'ok': True})
        with patch.object(urllib.request, 'urlopen', side_effect=[err, r200]), \
                patch('time.sleep'):
            data = mod._get_request('http://x')
        self.assertEqual(data['ok'], True)

    def test_httperror_other_raises(self):
        err = FakeHTTPError('http://x', 500, 'boom')
        with patch.object(urllib.request, 'urlopen', side_effect=err):
            with self.assertRaises(KalshiConnectorError):
                mod._get_request('http://x')

    def test_urlerror_raises(self):
        with patch.object(urllib.request, 'urlopen',
                          side_effect=urllib.error.URLError('conn')):
            with self.assertRaises(KalshiConnectorError):
                mod._get_request('http://x')


class TestPublicFunctions(unittest.TestCase):
    def test_fetch_markets(self):
        resp = FakeResponse(200, {'markets': [{'market_id': 'A'}]})
        with patch.object(urllib.request, 'urlopen', return_value=resp):
            data = mod.fetch_markets(limit=5)
        self.assertEqual(data['markets'][0]['market_id'], 'A')

    def test_get_market_found(self):
        resp = FakeResponse(200, {'title': 't'})
        with patch.object(urllib.request, 'urlopen', return_value=resp):
            data = mod.get_market('X')
        self.assertEqual(data['title'], 't')

    def test_get_market_not_found(self):
        resp = FakeResponse(200, {'other': 1})
        with patch.object(urllib.request, 'urlopen', return_value=resp):
            self.assertIsNone(mod.get_market('X'))

    def test_get_market_order_book(self):
        resp = FakeResponse(200, {'title': 't', 'bid': 50, 'ask': 55, 'no': 45})
        with patch.object(urllib.request, 'urlopen', return_value=resp):
            ob = mod.get_market_order_book('X')
        self.assertEqual(ob['yes_bid'], 50.0)
        self.assertEqual(ob['no_bid'], -10.0)

    def test_get_market_order_book_none(self):
        resp = FakeResponse(200, {'other': 1})
        with patch.object(urllib.request, 'urlopen', return_value=resp):
            self.assertIsNone(mod.get_market_order_book('X'))

    def test_get_user_positions(self):
        resp = FakeResponse(200, {'p': 1})
        with patch.object(urllib.request, 'urlopen', return_value=resp):
            data = mod.get_user_positions('u')
        self.assertEqual(data['p'], 1)

    def test_get_user_positions_no_user(self):
        resp = FakeResponse(200, {'p': 1})
        with patch.object(urllib.request, 'urlopen', return_value=resp):
            data = mod.get_user_positions()
        self.assertEqual(data['p'], 1)

    def test_get_order_history(self):
        resp = FakeResponse(200, {'h': 1})
        with patch.object(urllib.request, 'urlopen', return_value=resp):
            data = mod.get_order_history(limit=10)
        self.assertEqual(data['h'], 1)


class TestConnectorClass(unittest.TestCase):
    def test_init_and_methods(self):
        c = KalshiConnector()
        self.assertEqual(c.max_retries, 3)
        resp = FakeResponse(200, {'markets': [{'market_id': 'A'}]})
        with patch.object(urllib.request, 'urlopen', return_value=resp):
            self.assertEqual(c.fetch_markets(limit=1)['markets'][0]['market_id'], 'A')
        resp2 = FakeResponse(200, {'title': 't'})
        with patch.object(urllib.request, 'urlopen', return_value=resp2):
            self.assertEqual(c.get_market('X')['title'], 't')
        resp3 = FakeResponse(200, {'title': 't', 'bid': 50, 'ask': 55, 'no': 45})
        with patch.object(urllib.request, 'urlopen', return_value=resp3):
            self.assertEqual(c.get_order_book('X')['yes_ask'], 55.0)
        with patch.object(urllib.request, 'urlopen', return_value=resp):
            self.assertEqual(c.get_positions('u')['markets'][0]['market_id'], 'A')
        with patch.object(urllib.request, 'urlopen', return_value=resp):
            self.assertEqual(c.get_order_history(limit=5)['markets'][0]['market_id'], 'A')


class TestConnectivity(unittest.TestCase):
    def test_connectivity_ok(self):
        resp = FakeResponse(200, {'markets': [{'market_id': 'A'}]})
        with patch.object(urllib.request, 'urlopen', return_value=resp), \
                patch('sys.stdout'):
            self.assertTrue(mod.test_kalshi_connectivity())

    def test_connectivity_no_markets(self):
        resp = FakeResponse(200, {'markets': []})
        with patch.object(urllib.request, 'urlopen', return_value=resp), \
                patch('sys.stdout'):
            self.assertFalse(mod.test_kalshi_connectivity())

    def test_connectivity_error(self):
        with patch.object(urllib.request, 'urlopen',
                          side_effect=KalshiConnectorError('conn')), \
                patch('sys.stdout'):
            self.assertFalse(mod.test_kalshi_connectivity())


if __name__ == '__main__':
    unittest.main()
