#!/usr/bin/env python3
"""Tests for trading_system.arbitrage.arb_trader."""

import asyncio
import sys
import unittest
from datetime import datetime
from unittest.mock import patch, AsyncMock

import trading_system.arbitrage.arb_trader as mod
from trading_system.arbitrage.arb_trader import (
    OrderSide,
    TradeExecutionResult,
    RateLimiter,
    ConnectionHealthMonitor,
)


class TestEnumsAndDataclass(unittest.TestCase):
    def test_order_side(self):
        self.assertEqual(OrderSide.BUY.value, 1)
        self.assertEqual(OrderSide.SELL.value, 2)

    def test_result_post_init_sets(self):
        r = TradeExecutionResult()
        self.assertIsNotNone(r.timestamp)

    def test_result_post_init_keeps(self):
        ts = datetime(2025, 1, 1)
        r = TradeExecutionResult(timestamp=ts)
        self.assertIs(r.timestamp, ts)


class FakeClient:
    def __init__(self):
        self.calls = []
        self.raise_on_call = False

    def create_order(self, **kw):
        self.calls.append(kw)
        if self.raise_on_call:
            raise RuntimeError('boom')
        return {
            'order_id': 'ORD',
            'status': 'open',
            'total_cost': kw.get('quantity', 0) * kw.get('unit_price', 0),
        }


class TestConnectionHealthMonitor(unittest.TestCase):
    def test_check_health(self):
        m = ConnectionHealthMonitor()
        self.assertTrue(asyncio.get_event_loop().run_until_complete(m.check_health()))

    def test_on_error_no_callbacks(self):
        m = ConnectionHealthMonitor()
        asyncio.get_event_loop().run_until_complete(m.on_error(ValueError('x')))

    def test_on_error_with_callback(self):
        m = ConnectionHealthMonitor()
        seen = []

        async def cb(e):
            seen.append(e)

        m.error_callbacks.append(cb)
        asyncio.get_event_loop().run_until_complete(m.on_error(ValueError('y')))
        self.assertEqual(len(seen), 1)

    def test_on_health_check_callbacks(self):
        m = ConnectionHealthMonitor()
        called = []

        async def cb():
            called.append(1)

        m.health_check_callbacks.append(cb)
        asyncio.get_event_loop().run_until_complete(m.on_health_check())
        self.assertEqual(called, [1])


class TestExecuteArbitrage(unittest.TestCase):
    def _make(self, kalshi_bid, pm_bid, kalshi_int=False, pm_int=False):
        m = ConnectionHealthMonitor()
        m.kalshi_client = FakeClient()
        m.polymarket_client = FakeClient()
        kalshi_market = {'market_id': 'M', 'bid': (int(kalshi_bid) if kalshi_int else float(kalshi_bid)),
                         'title': 't'}
        pm_event = {'slug': 'S', 'bid': (int(pm_bid) if pm_int else float(pm_bid)),
                    'question': 'q'}
        return m, kalshi_market, pm_event

    def test_buy_kalshi_balanced(self):
        m, km, pm = self._make(50.0, 60.0)
        res = m.execute_arbitrage_opportunity(kalshi_market=km, polymarket_event=pm,
                                              strategy='balanced')
        self.assertIsInstance(res, TradeExecutionResult)
        self.assertEqual(m.kalshi_client.calls[0]['side'], OrderSide.BUY)
        self.assertEqual(m.polymarket_client.calls[0]['side'], OrderSide.SELL)

    def test_sell_kalshi_balanced(self):
        m, km, pm = self._make(70.0, 60.0)
        res = m.execute_arbitrage_opportunity(kalshi_market=km, polymarket_event=pm,
                                              strategy='balanced')
        self.assertEqual(m.kalshi_client.calls[0]['side'], OrderSide.SELL)
        self.assertEqual(m.polymarket_client.calls[0]['side'], OrderSide.BUY)

    def test_strategy_kalshi_first(self):
        m, km, pm = self._make(50.0, 60.0)
        m.execute_arbitrage_opportunity(kalshi_market=km, polymarket_event=pm,
                                        strategy='kalshi_first')
        # kalshi gets larger allocation
        self.assertGreater(m.kalshi_client.calls[0]['quantity'],
                           m.polymarket_client.calls[0]['quantity'])

    def test_strategy_pm_first(self):
        m, km, pm = self._make(50.0, 60.0)
        m.execute_arbitrage_opportunity(kalshi_market=km, polymarket_event=pm,
                                        strategy='pm_first')
        self.assertGreater(m.polymarket_client.calls[0]['quantity'],
                           m.kalshi_client.calls[0]['quantity'])

    def test_int_bids(self):
        m, km, pm = self._make(50, 60, kalshi_int=True, pm_int=True)
        m.execute_arbitrage_opportunity(kalshi_market=km, polymarket_event=pm)
        self.assertEqual(m.kalshi_client.calls[0]['side'], OrderSide.BUY)

    def test_execute_all_opportunities(self):
        m, km, pm = self._make(50.0, 60.0)
        results = m.execute_all_opportunities([{'kalshi': km, 'polymarket': pm}])
        self.assertEqual(len(results), 1)

    def test_execute_all_opportunities_handles_error(self):
        m, km, pm = self._make(50.0, 60.0)
        m.kalshi_client.raise_on_call = True
        results = m.execute_all_opportunities([{'kalshi': km, 'polymarket': pm}])
        self.assertEqual(len(results), 0)


class TestRateLimiter(unittest.TestCase):
    def test_burst_no_sleep(self):
        slept = []
        lim = RateLimiter(requests_per_second=1000, burst_size=3)
        for _ in range(3):
            asyncio.get_event_loop().run_until_complete(lim.acquire())
        # No sleep expected during burst

    def test_rate_limit_sleeps(self):
        slept = []
        real_sleep = asyncio.sleep

        async def fake_sleep(t):
            slept.append(t)

        with patch('asyncio.sleep', fake_sleep):
            lim = RateLimiter(requests_per_second=1000, burst_size=1)
            asyncio.get_event_loop().run_until_complete(lim.acquire())  # burst
            asyncio.get_event_loop().run_until_complete(lim.acquire())  # sleeps
        self.assertTrue(len(slept) >= 1)


if __name__ == '__main__':
    unittest.main()
