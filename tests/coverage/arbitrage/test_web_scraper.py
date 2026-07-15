#!/usr/bin/env python3
"""Tests for trading_system.arbitrage.web_scraper."""

import sys
import json
import unittest
from unittest.mock import patch, MagicMock

import trading_system.arbitrage.web_scraper as mod
from trading_system.arbitrage.web_scraper import (
    KalshiWebScraper,
    PolymarketWebScraper,
    CombinedMarketScraper,
)


class FakeResp:
    def __init__(self, status_code=200, json_data=None, text=''):
        self.status_code = status_code
        self._json = json_data
        self.text = text

    def json(self):
        return self._json


class FakeEl:
    def select_one(self, sel):
        return self

    def get_text(self, strip=False):
        return "Some Title 55.5"

    def get(self, k, default=None):
        return default

    def __getitem__(self, k):
        return "https://x/p.m/myslug"


class FakeSoup:
    def select(self, sel):
        return [FakeEl(), FakeEl()]

    def select_one(self, sel):
        return FakeEl()


class TestKalshiScraper(unittest.TestCase):
    def test_scrape_api_success(self):
        ks = KalshiWebScraper()
        ks.session.get = lambda *a, **k: FakeResp(200, {
            'items': [{'market_id': 'M', 'full_title': 'T',
                       'category': 'Crypto', 'market_type': 'mt',
                       'bid_price': '55'}]})
        markets = ks.scrape_markets()
        self.assertEqual(len(markets), 1)
        self.assertEqual(markets[0]['bid'], 0.55)
        self.assertEqual(markets[0]['source'], 'kalshi_public_api')

    def test_scrape_api_success_category_match(self):
        ks = KalshiWebScraper()
        ks.session.get = lambda *a, **k: FakeResp(200, {
            'items': [{'market_id': 'M', 'full_title': 'T',
                       'category': 'Crypto', 'market_type': 'mt',
                       'bid_price': '55'}]})
        markets = ks.scrape_markets(category='crypto')
        self.assertEqual(markets[0]['category'], 'crypto')

    def test_scrape_api_success_category_no_match(self):
        ks = KalshiWebScraper()
        ks.session.get = lambda *a, **k: FakeResp(200, {
            'items': [{'market_id': 'M', 'full_title': 'T',
                       'category': 'Crypto', 'market_type': 'mt',
                       'bid_price': '55'}]})
        markets = ks.scrape_markets(category='politics')
        self.assertEqual(markets[0]['category'], '')

    def test_scrape_from_web_bad_element(self):
        ks = KalshiWebScraper()
        ks.session.get = lambda *a, **k: FakeResp(200, text='<html></html>')

        class BadEl:
            def select_one(self, sel):
                raise Exception('parse fail')

        class BadSoup:
            def select(self, sel):
                return [BadEl()]

            def select_one(self, sel):
                return BadEl()

        with patch.object(mod, 'BeautifulSoup', return_value=BadSoup()):
            markets = ks._scrape_from_web()
        self.assertEqual(markets, [])

    def test_scrape_api_error_falls_to_sample(self):
        ks = KalshiWebScraper()
        ks.session.get = lambda *a, **k: FakeResp(500)
        markets = ks.scrape_markets()
        self.assertEqual(len(markets), 1)
        self.assertEqual(markets[0]['source'], 'sample_data')

    def test_scrape_api_exception_falls_to_sample(self):
        ks = KalshiWebScraper()
        ks.session.get = lambda *a, **k: (_ for _ in ()).throw(Exception('boom'))
        markets = ks.scrape_markets()
        self.assertEqual(markets[0]['source'], 'sample_data')

    def test_scrape_from_web_with_elements(self):
        ks = KalshiWebScraper()
        ks.session.get = lambda *a, **k: FakeResp(200, text='<html></html>')
        with patch.object(mod, 'BeautifulSoup', return_value=FakeSoup()):
            markets = ks._scrape_from_web()
        self.assertTrue(len(markets) >= 1)
        self.assertEqual(markets[0]['source'], 'kalshi_web')

    def test_scrape_from_web_exception(self):
        ks = KalshiWebScraper()
        ks.session.get = lambda *a, **k: (_ for _ in ()).throw(Exception('boom'))
        markets = ks._scrape_from_web()
        self.assertEqual(markets[0]['source'], 'sample_data')


class TestPolymarketScraper(unittest.TestCase):
    def test_scrape_api_success(self):
        ps = PolymarketWebScraper()
        ps.session.get = lambda *a, **k: FakeResp(200, {
            'results': [{'slug': 's', 'title': 't', 'primaryTopic': 'crypto',
                         'volume_1h': '100'}]})
        events = ps.scrape_events()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['source'], 'polygon_io_public')

    def test_scrape_api_bad_volume_excepts(self):
        ps = PolymarketWebScraper()
        ps.session.get = lambda *a, **k: FakeResp(200, {
            'results': [{'slug': 's', 'title': 't', 'primaryTopic': 'crypto',
                         'volume_1h': 'abc', 'volume': 'xyz'}]})
        events = ps.scrape_events()
        self.assertEqual(events[0]['bid'], 0.5)

    def test_scrape_api_empty_then_web(self):
        calls = {'n': 0}

        def fake_get(*a, **k):
            calls['n'] += 1
            if calls['n'] == 1:
                return FakeResp(200, {'results': []})
            return FakeResp(200, text='<html></html>')

        ps = PolymarketWebScraper()
        ps.session.get = fake_get
        with patch.object(mod, 'BeautifulSoup', return_value=FakeSoup()):
            events = ps.scrape_events()
        self.assertTrue(len(events) >= 1)
        self.assertEqual(events[0]['source'], 'polymarket_web')

    def test_scrape_api_exception_then_web(self):
        calls = {'n': 0}

        def fake_get(*a, **k):
            calls['n'] += 1
            if calls['n'] == 1:
                return (_ for _ in ()).throw(Exception('boom'))
            return FakeResp(200, text='<html></html>')

        ps = PolymarketWebScraper()
        ps.session.get = fake_get
        with patch.object(mod, 'BeautifulSoup', return_value=FakeSoup()):
            events = ps.scrape_events()
        self.assertTrue(len(events) >= 1)

    def test_scrape_web_exception_then_sample(self):
        ps = PolymarketWebScraper()
        ps.session.get = lambda *a, **k: (_ for _ in ()).throw(Exception('boom'))
        events = ps.scrape_events()
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]['source'], 'sample_data')


class TestCombined(unittest.TestCase):
    def test_scrape_markets(self):
        c = CombinedMarketScraper()
        data = c.scrape_markets(category='all', limit=10)
        self.assertIn('kalshi_markets', data)
        self.assertIn('polymarket_events', data)

    def test_save_to_file(self):
        c = CombinedMarketScraper()
        m = MagicMock()
        with patch.object(mod, 'open', m), \
                patch.object(mod, 'json') as jmock:
            c.save_to_file({'a': 1}, '/tmp/x.json')
        self.assertTrue(jmock.dump.called)


class TestMain(unittest.TestCase):
    def test_main(self):
        class FakeCombined:
            def scrape_markets(self, **kwargs):
                return {
                    'kalshi_markets': [{'title': 'T', 'bid': 0.5, 'source': 's'}],
                    'polymarket_events': [{'question': 'Q', 'bid': 0.5, 'source': 's'}],
                }

            def save_to_file(self, data, path):
                pass

        with patch.object(mod, 'CombinedMarketScraper', FakeCombined):
            rc = mod.main()
        self.assertEqual(rc, 0)


if __name__ == '__main__':
    unittest.main()
