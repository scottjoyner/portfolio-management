"""Tests for the fail-closed Kraken compatibility connector."""

import asyncio
import unittest

from trading_system.connectors.kraken import (
    AuthenticationError,
    KrakenConnector,
    KrakenConnectorError,
    MarketUnavailableError,
)


def run(coro):
    return asyncio.run(coro)


class TestKrakenInit(unittest.TestCase):
    def test_init_defaults(self):
        connector = KrakenConnector()
        self.assertEqual(connector.api_key, "")
        self.assertEqual(connector.api_secret, "")
        self.assertEqual(connector.base_url, "https://api.kraken.com")

    def test_init_with_keys(self):
        connector = KrakenConnector(api_key="KrakenAPIkey", api_secret="secret")
        self.assertEqual(connector.api_key, "KrakenAPIkey")
        self.assertEqual(connector.api_secret, "secret")

    def test_exception_hierarchy(self):
        self.assertTrue(issubclass(AuthenticationError, KrakenConnectorError))
        self.assertTrue(issubclass(MarketUnavailableError, KrakenConnectorError))


class TestKrakenConnection(unittest.TestCase):
    def test_public_connect_requires_no_credentials(self):
        connector = KrakenConnector()
        run(connector.connect())
        self.assertTrue(connector._connected)

    def test_connect_preserves_runtime_credentials(self):
        connector = KrakenConnector(api_key="runtime-key", api_secret="runtime-secret")
        run(connector.connect())
        self.assertTrue(connector._connected)
        self.assertEqual(connector.api_key, "runtime-key")


class TestKrakenPrices(unittest.TestCase):
    def test_prices_fail_closed_when_disconnected(self):
        connector = KrakenConnector()
        with self.assertRaisesRegex(KrakenConnectorError, "connector_not_connected"):
            run(connector.get_current_prices(["XBT/USD"]))

    def test_prices_never_invent_market_data(self):
        connector = KrakenConnector()
        run(connector.connect())
        self.assertEqual(run(connector.get_current_prices(["XBT/USD", "ETH/USD"])), {})

    def test_prices_empty(self):
        connector = KrakenConnector()
        run(connector.connect())
        self.assertEqual(run(connector.get_current_prices([])), {})


class TestKrakenBalances(unittest.TestCase):
    def test_private_balance_requires_credentials(self):
        connector = KrakenConnector()
        run(connector.connect())
        with self.assertRaisesRegex(AuthenticationError, "kraken_credentials_required"):
            run(connector.get_account_balance())

    def test_private_balance_requires_connection(self):
        connector = KrakenConnector(api_key="key", api_secret="secret")
        with self.assertRaisesRegex(KrakenConnectorError, "connector_not_connected"):
            run(connector.get_account_balance())

    def test_configured_balance_returns_empty_until_http_client_is_wired(self):
        connector = KrakenConnector(api_key="key", api_secret="secret")
        run(connector.connect())
        self.assertEqual(run(connector.get_account_balance()), {})


if __name__ == "__main__":
    unittest.main()
