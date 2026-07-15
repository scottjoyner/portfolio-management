#!/usr/bin/env python3
"""Tests for trading_system.arbitrage.run_tests (module-level script)."""

import sys
import importlib
import unittest
from unittest.mock import patch

import trading_system.arbitrage.detect_opportunities as det_mod


def good_opps():
    return [{
        'kalshi': {'market_id': 'M', 'bid_pct': 50, 'category': 'cryptocurrency'},
        'polymarket_event': {'slug': 'S', 'bid_pct': 60},
        'arbitrage_potential_pct': 5.0,
    }]


class TestRunTests(unittest.TestCase):
    def test_runs_with_opportunities(self):
        with patch.object(det_mod, 'detect_opportunities', return_value=good_opps()):
            import trading_system.arbitrage.run_tests as mod
        self.assertTrue(hasattr(mod, 'load_sample_data'))

    def test_runs_no_opportunities(self):
        with patch.object(det_mod, 'detect_opportunities', return_value=[]):
            import trading_system.arbitrage.run_tests as mod
            importlib.reload(mod)
        self.assertTrue(hasattr(mod, 'load_sample_data'))


if __name__ == '__main__':
    unittest.main()
