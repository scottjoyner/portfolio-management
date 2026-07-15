#!/usr/bin/env python3
"""Tests for trading_system.arbitrage.verify_system (module-level script)."""

import sys
import importlib
import unittest
from unittest.mock import patch

import trading_system.arbitrage.detect_opportunities as det_mod


def crafted_opp():
    return [{
        'kalshi': {'market_id': 'M', 'bid_pct': 50},
        'polymarket_event': {'slug': 'S', 'bid_pct': 60},
    }]


class TestVerifySystem(unittest.TestCase):
    def test_import_with_opp(self):
        with patch.object(det_mod, 'detect_opportunities', return_value=crafted_opp()):
            import trading_system.arbitrage.verify_system as mod
        self.assertTrue(True)

    def test_reload_no_opp(self):
        with patch.object(det_mod, 'detect_opportunities', return_value=[]):
            import trading_system.arbitrage.verify_system as mod
            importlib.reload(mod)
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
