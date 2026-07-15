#!/usr/bin/env python3
"""Tests for trading_system.arbitrage.polymarket_connector."""

import sys
import json
import unittest
from unittest.mock import patch

import urllib.error
import urllib.request

import trading_system.arbitrage.polymarket_connector as mod
from trading_system.arbitrage.polymarket_connector import (
    PolymarketConnector,
    PolymarketConnectorError,
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


def urlopen_return(resp):
    return patch.object(urllib.request, 'urlopen', return_value=resp)


class TestGetRequest(unittest.TestCase):
    def test_success_dict(self):
        with urlopen_return(FakeResponse(200, {'a': 1})):
            self.assertEqual(mod._get_request('http://x')['a'], 1)

    def test_success_list(self):
        with urlopen_return(FakeResponse(200, [1, 2])):
            self.assertEqual(mod._get_request('http://x'), [1, 2])

    def test_429_retries(self):
        r429 = FakeResponse(429, {}, {'Retry-After': '1'})
        r200 = FakeResponse(200, {'ok': True})
        with patch.object(urllib.request, 'urlopen', side_effect=[r429, r200]), \
                patch('time.sleep'):
            self.assertEqual(mod._get_request('http://x')['ok'], True)

    def test_404_raises(self):
        with urlopen_return(FakeResponse(404, {})):
            with self.assertRaises(NotFoundError):
                mod._get_request('http://x')

    def test_httperror_429_retries(self):
        err = FakeHTTPError('http://x', 429, 'rate', {'Retry-After': '1'})
        r200 = FakeResponse(200, {'ok': True})
        with patch.object(urllib.request, 'urlopen', side_effect=[err, r200]), \
                patch('time.sleep'):
            self.assertEqual(mod._get_request('http://x')['ok'], True)

    def test_httperror_other_raises(self):
        err = FakeHTTPError('http://x', 500, 'boom')
        with patch.object(urllib.request, 'urlopen', side_effect=err):
            with self.assertRaises(PolymarketConnectorError):
                mod._get_request('http://x')

    def test_urlerror_raises(self):
        with patch.object(urllib.request, 'urlopen',
                          side_effect=urllib.error.URLError('conn')):
            with self.assertRaises(PolymarketConnectorError):
                mod._get_request('http://x')

    def test_with_headers(self):
        with urlopen_return(FakeResponse(200, {'a': 1})):
            self.assertEqual(mod._get_request('http://x', {'X': '1'})['a'], 1)


class TestPublicFunctions(unittest.TestCase):
    def test_fetch_trending_dict(self):
        with urlopen_return(FakeResponse(200, {'marketSummaries': [{'s': 1}]})):
            self.assertEqual(len(mod.fetch_trending_markets(5)), 1)

    def test_fetch_trending_list(self):
        with urlopen_return(FakeResponse(200, [{'s': 1}, {'s': 2}])):
            self.assertEqual(len(mod.fetch_trending_markets(5)), 2)

    def test_search_markets(self):
        with urlopen_return(FakeResponse(200, {'events': [{'e': 1}]})):
            res = mod.search_markets('bitcoin')
        self.assertEqual(res['query'], 'bitcoin')
        self.assertEqual(len(res['events']), 1)

    def test_get_market_found(self):
        with urlopen_return(FakeResponse(200, {'question': 'q'})):
            self.assertEqual(mod.get_market('X')['question'], 'q')

    def test_get_market_not_found(self):
        with urlopen_return(FakeResponse(200, [1, 2])):
            self.assertIsNone(mod.get_market('X'))

    def test_get_market_order_book(self):
        payload = {
            'question': 'q',
            'volume': 100,
            'closed': False,
            'outcomePrices': [{'outcome': 'Yes', 'price': 0.46}],
            'outcomes': [{'outcome': 'Yes'}],
        }
        with urlopen_return(FakeResponse(200, payload)):
            ob = mod.get_market_order_book('X')
        self.assertEqual(ob['question'], 'q')
        self.assertTrue(ob['open'])
        self.assertEqual(len(ob['outcomePricesClean']), 1)

    def test_get_market_order_book_none(self):
        with urlopen_return(FakeResponse(200, [1])):
            self.assertIsNone(mod.get_market_order_book('X'))

    def test_get_condition_orderbook(self):
        with urlopen_return(FakeResponse(200, {'bids': [1], 'asks': [2]})):
            res = mod.get_condition_orderbook('tok')
        self.assertEqual(res['token_id'], 'tok')
        self.assertEqual(res['bids'], [1])

    def test_get_condition_orderbook_error(self):
        err = FakeHTTPError('http://x', 403, 'no')
        with patch.object(urllib.request, 'urlopen', side_effect=err):
            self.assertEqual(mod.get_condition_orderbook('tok'), {})

    def test_search_conditions(self):
        with urlopen_return(FakeResponse(200, {'items': [{'c': 1}]})):
            self.assertEqual(len(mod.search_conditions('q')), 1)

    def test_search_conditions_error(self):
        err = FakeHTTPError('http://x', 403, 'no')
        with patch.object(urllib.request, 'urlopen', side_effect=err):
            self.assertEqual(mod.search_conditions('q'), [])

    def test_fetch_history(self):
        with urlopen_return(FakeResponse(200, {'trades': [1, 2, 3]})):
            res = mod.fetch_history('cond')
        self.assertEqual(res['total_trades'], 3)

    def test_fetch_history_error(self):
        err = FakeHTTPError('http://x', 403, 'no')
        with patch.object(urllib.request, 'urlopen', side_effect=err):
            self.assertEqual(mod.fetch_history('cond'), {})

    def test_fetch_trades_with_slug(self):
        with urlopen_return(FakeResponse(200, {'items': [1, 2]})):
            res = mod.fetch_trades(market_slug='X', limit=2)
        self.assertEqual(res['total_trades'], 2)

    def test_fetch_trades_no_slug(self):
        with urlopen_return(FakeResponse(200, {'items': [1]})):
            res = mod.fetch_trades(limit=1)
        self.assertEqual(res['total_trades'], 1)

    def test_fetch_trades_error(self):
        err = FakeHTTPError('http://x', 403, 'no')
        with patch.object(urllib.request, 'urlopen', side_effect=err):
            self.assertEqual(mod.fetch_trades('X'), {})


class TestParsers(unittest.TestCase):
    def test_parse_outcome_prices(self):
        res = mod._parse_outcome_prices([{'outcome': 'Yes', 'price': 0.46}])
        self.assertEqual(res, ['"Yes": 46'])

    def test_parse_outcomes_list(self):
        self.assertEqual(mod._parse_outcomes([{'outcome': 'Y'}]), ['Y'])

    def test_parse_outcomes_str_json(self):
        self.assertEqual(mod._parse_outcomes('[{"outcome": "Y"}]'), ['Y'])

    def test_parse_outcomes_str_plain(self):
        self.assertEqual(mod._parse_outcomes('plain'), ['plain'])

    def test_parse_outcomes_other(self):
        self.assertEqual(mod._parse_outcomes(123), [])


class TestConnectorClass(unittest.TestCase):
    def test_methods(self):
        c = PolymarketConnector()
        self.assertEqual(c.base_url, mod.GAMMA_API)
        with urlopen_return(FakeResponse(200, {'marketSummaries': [{'s': 1}]})):
            self.assertEqual(len(c.fetch_trending(1)), 1)
        with urlopen_return(FakeResponse(200, {'events': [{'e': 1}]})):
            self.assertEqual(len(c.search('q')['events']), 1)
        with urlopen_return(FakeResponse(200, {'question': 'q'})):
            self.assertEqual(c.get_market('X')['question'], 'q')
        payload = {'question': 'q', 'outcomePrices': [{'outcome': 'Yes', 'price': 0.5}],
                   'outcomes': [{'outcome': 'Yes'}]}
        with urlopen_return(FakeResponse(200, payload)):
            self.assertIsNotNone(c.get_order_book('X'))
        with urlopen_return(FakeResponse(200, {'bids': [], 'asks': []})):
            self.assertEqual(c.get_condition_orderbook('t')['token_id'], 't')
        with urlopen_return(FakeResponse(200, {'items': []})):
            self.assertEqual(c.search_conditions('q'), [])
        with urlopen_return(FakeResponse(200, {'trades': []})):
            self.assertEqual(c.fetch_history('c')['condition_id'], 'c')
        with urlopen_return(FakeResponse(200, {'items': []})):
            self.assertEqual(c.fetch_trades('X')['total_trades'], 0)


class TestConnectivity(unittest.TestCase):
    def test_connectivity_ok(self):
        with urlopen_return(FakeResponse(200, {'marketSummaries': [{'slug': 's'}]})), \
                patch('sys.stdout'):
            self.assertTrue(mod.test_polymarket_connectivity())

    def test_connectivity_no_markets(self):
        with urlopen_return(FakeResponse(200, {'marketSummaries': []})), \
                patch('sys.stdout'):
            self.assertFalse(mod.test_polymarket_connectivity())

    def test_connectivity_error(self):
        with patch.object(urllib.request, 'urlopen',
                          side_effect=PolymarketConnectorError('conn')), \
                patch('sys.stdout'):
            self.assertFalse(mod.test_polymarket_connectivity())


if __name__ == '__main__':
    unittest.main()
