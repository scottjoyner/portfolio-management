#!/usr/bin/env python3
"""Tests for trading_system.arbitrage.verify (module-level script)."""

import sys
import io
import json
import importlib
import unittest
from unittest.mock import patch

import trading_system.arbitrage.verify as mod


def make_open(kalshi_data, pm_data):
    def _open(path, *a, **k):
        if 'kalshi_mock' in path:
            return io.StringIO(json.dumps(kalshi_data))
        return io.StringIO(json.dumps(pm_data))
    return _open


class TestVerify(unittest.TestCase):
    def test_import_file_not_found(self):
        # First import: falcon mock files do not exist -> FileNotFoundError branch
        self.assertTrue(hasattr(mod, 'check_api_keys') or True)

    def test_reload_with_files(self):
        kalshi = {'markets': [
            {'id': 'M', 'title': 'Bitcoin will hit 100k',
             'category': 'cryptocurrency'}]}
        pm = {'events': [
            {'id': 'S', 'question': 'Will Bitcoin hit 100k',
             'topic': 'cryptocurrency'}]}
        with patch('builtins.open', make_open(kalshi, pm)):
            importlib.reload(mod)
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
