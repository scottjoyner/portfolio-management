#!/usr/bin/env python3
"""Tests for trading_system.arbitrage.start."""

import sys
import unittest
import importlib
from unittest.mock import patch

import trading_system.arbitrage.start as mod


class TestCheckApiKeys(unittest.TestCase):
    def test_no_keys(self):
        with patch.dict('os.environ', {}, clear=True):
            hk, hp = mod.check_api_keys()
        self.assertFalse(hk)
        self.assertFalse(hp)

    def test_with_keys(self):
        with patch.dict('os.environ', {'KALSHI_API_KEY': 'k',
                                       'POLYMARKET_API_KEY': 'p'}):
            hk, hp = mod.check_api_keys()
        self.assertTrue(hk)
        self.assertTrue(hp)


class TestMain(unittest.TestCase):
    def _run_with_fake(self, fake_return, env):
        orig = mod.main
        mod.main = lambda: fake_return
        try:
            with patch.dict('os.environ', env, clear=True):
                rc = orig()
        finally:
            mod.main = orig
        return rc

    def test_main_mock_mode(self):
        rc = self._run_with_fake(0, {})
        self.assertEqual(rc, 0)

    def test_main_real_mode(self):
        rc = self._run_with_fake(0, {'KALSHI_API_KEY': 'k',
                                      'POLYMARKET_API_KEY': 'p'})
        self.assertEqual(rc, 0)

    def test_main_returns_value(self):
        rc = self._run_with_fake(42, {})
        self.assertEqual(rc, 42)


class TestMainExceptions(unittest.TestCase):
    def _run_raises(self, exc, expected):
        orig = mod.main
        mod.main = lambda: (_ for _ in ()).throw(exc)
        try:
            with patch.dict('os.environ', {}, clear=True):
                rc = orig()
        finally:
            mod.main = orig
        return rc

    def test_keyboard_interrupt(self):
        self.assertEqual(self._run_raises(KeyboardInterrupt(), 130), 130)

    def test_exception(self):
        self.assertEqual(self._run_raises(ValueError('boom'), 1), 1)


if __name__ == '__main__':
    unittest.main()
