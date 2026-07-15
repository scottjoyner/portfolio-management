"""Tests for trading_system.connectors.coinbase_v3 (CoinbaseConnectorV3)."""
import asyncio
import importlib
import json
import subprocess
import unittest
from subprocess import CompletedProcess, TimeoutExpired
from unittest.mock import patch, MagicMock

from trading_system.connectors import coinbase_v3 as cbv3
from trading_system.connectors.coinbase_v3 import CoinbaseConnectorV3, CoinbaseConnector


def _cp(stdout, returncode=0):
    return CompletedProcess([], returncode, stdout=stdout, stderr="")


def make_fake(overrides=None):
    overrides = overrides or {}

    def fake_run(cmd, **kwargs):
        s = " ".join(cmd)
        if "--version" in s:
            return _cp(json.dumps({"price": 123.0}))
        if overrides.get("mode") == "error":
            return _cp("", returncode=1)
        if overrides.get("mode") == "badjson":
            return _cp("not json")
        if overrides.get("mode") == "timeout":
            raise TimeoutExpired(cmd, 5)
        if overrides.get("mode") == "filenotfound":
            raise FileNotFoundError("no coinbase")
        if "candles" in s or "list" in s or "portfolios" in s or "fills" in s:
            out = [{"id": "x", "price": 1.0}]
        elif "preview" in s:
            out = {"total_fee": 1.0, "total_cost": 100.0, "estimated_fill_price": 50.0}
        elif "create" in s:
            out = {"id": "o1", "status": "filled", "filled_size": 1,
                   "filled_value": 100, "average_filled_price": 100,
                   "total_fees": 0.5, "created_at": "t", "updated_at": "t"}
        elif "orders" in s and "get" in s:
            out = {"id": "o1"}
        elif "orders" in s and "cancel" in s:
            out = {"id": "o1"}
        else:
            out = {"price": 123.0, "accounts": [{"uuid": "a", "name": "n"}],
                   "id": "x", "bids": [], "asks": []}
        return _cp(json.dumps(out))

    return fake_run


class TestCoinbaseV3(unittest.IsolatedAsyncioTestCase):
    def test_alias(self):
        self.assertIs(CoinbaseConnector, CoinbaseConnectorV3)

    async def test_verify_cli_failure(self):
        with patch.object(subprocess, "run", side_effect=FileNotFoundError("x")):
            with self.assertRaises(RuntimeError):
                CoinbaseConnectorV3()

    async def test_get_price(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            self.assertEqual(cb.get_price("BTC-USD")["price"], 123.0)

    async def test_list_products(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            self.assertIsInstance(cb.list_products(), list)

    async def test_list_products_dict(self):
        # non-list result wrapped
        fake = lambda cmd, **k: _cp(json.dumps({"id": "x"}))
        with patch.object(subprocess, "run", fake):
            cb = CoinbaseConnectorV3()
            self.assertIsInstance(cb.list_products(), list)

    async def test_get_order_book(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            self.assertIn("bids", cb.get_order_book("BTC-USD"))

    async def test_get_candles(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            self.assertIsInstance(cb.get_candles("BTC-USD"), list)

    async def test_get_balances(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            self.assertIn("accounts", cb.get_balances())

    async def test_get_portfolios(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            self.assertIsInstance(cb.get_portfolios(), list)

    async def test_get_portfolio(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            self.assertEqual(cb.get_portfolio("p")["id"], "x")

    async def test_create_portfolio(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            self.assertIsInstance(cb.create_portfolio("n"), list)

    async def test_get_portfolio(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            self.assertIsInstance(cb.get_portfolio("p"), list)

    async def test_preview_order(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            p = cb.preview_order("BTC-USD", "BUY", quote_size=100)
            self.assertEqual(p.total_fee, 1.0)

    async def test_preview_order_full(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            p = cb.preview_order("BTC-USD", "BUY", quote_size=100,
                                 base_size=1, limit_price=50, portfolio_id="pid")
            self.assertEqual(p.limit_price, 50)

    async def test_create_order(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            o = cb.create_order("BTC-USD", "BUY", quote_size=100,
                                client_order_id="cid", limit_price=50,
                                portfolio_id="pid")
            self.assertEqual(o.order_id, "o1")
            self.assertEqual(o.filled_size, 1.0)

    async def test_create_order_generated_id(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            o = cb.create_order("BTC-USD", "SELL", base_size=1)
            self.assertEqual(o.order_id, "o1")

    async def test_get_order(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            self.assertEqual(cb.get_order("o1")["id"], "o1")

    async def test_list_orders(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            self.assertIsInstance(cb.list_orders(), list)
            self.assertIsInstance(cb.list_orders(product_id="BTC-USD"), list)

    async def test_cancel_order(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            self.assertEqual(cb.cancel_order("o1")["id"], "o1")

    async def test_get_fills(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            self.assertIsInstance(cb.get_fills(), list)
            self.assertIsInstance(cb.get_fills(product_id="BTC-USD"), list)

    async def test_get_conversion_quote(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            self.assertEqual(cb.get_conversion_quote("USD", "USDC", 1)["id"], "x")

    async def test_execute_conversion(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            self.assertEqual(cb.execute_conversion("q", "USD", "USDC")["id"], "x")

    async def test_get_fees(self):
        with patch.object(subprocess, "run", make_fake()):
            cb = CoinbaseConnectorV3()
            self.assertEqual(cb.get_fees()["id"], "x")

    async def test_run_command_error(self):
        with patch.object(subprocess, "run", make_fake({"mode": "error"})):
            cb = CoinbaseConnectorV3()
            with self.assertRaises(RuntimeError):
                cb.get_price("BTC-USD")

    async def test_run_command_bad_json(self):
        with patch.object(subprocess, "run", make_fake({"mode": "badjson"})):
            cb = CoinbaseConnectorV3()
            with self.assertRaises(RuntimeError):
                cb.get_price("BTC-USD")

    async def test_run_command_timeout(self):
        with patch.object(subprocess, "run", make_fake({"mode": "timeout"})):
            cb = CoinbaseConnectorV3()
            with self.assertRaises(RuntimeError):
                cb.get_price("BTC-USD")

    async def test_run_command_file_not_found(self):
        with patch.object(subprocess, "run", make_fake({"mode": "filenotfound"})):
            cb = CoinbaseConnectorV3(environment="sandbox")
            with self.assertRaises(RuntimeError):
                cb.get_price("BTC-USD")

    async def test_main_block(self):
        # Execute the module-level `if __name__ == "__main__"` block.
        cbv3.__name__ = "__main__"
        try:
            with patch.object(subprocess, "run", make_fake()):
                importlib.reload(cbv3)
        finally:
            cbv3.__name__ = "trading_system.connectors.coinbase_v3"


if __name__ == "__main__":
    unittest.main()
