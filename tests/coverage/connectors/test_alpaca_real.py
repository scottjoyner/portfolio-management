"""Tests for trading_system/connectors/alpaca_real.py (real Alpaca REST connector).

HTTP is fully mocked via unittest.mock.patch on the `requests` module's
get/post attributes used by the connector.
"""

import asyncio
import unittest
import requests
from unittest.mock import patch, MagicMock

from trading_system.connectors import alpaca_real

GET = "trading_system.connectors.alpaca_real.requests.get"
POST = "trading_system.connectors.alpaca_real.requests.post"


def run(coro):
    return asyncio.run(coro)


def make_response(json_data=None, status_code=200, raise_http=False,
                  raise_exc=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else {}
    if raise_http:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=resp
        )
    elif raise_exc is not None:
        resp.raise_for_status.side_effect = raise_exc
    else:
        resp.raise_for_status.return_value = None
    return resp


class TestAlpacaRealInit(unittest.TestCase):
    def test_init_oauth(self):
        c = alpaca_real.AlpacaConnector(oauth_token="tok")
        self.assertEqual(c.auth_headers["Authorization"], "Bearer tok")
        self.assertNotIn("apikey-id", c.trade_headers)

    def test_init_live_no_oauth(self):
        c = alpaca_real.AlpacaConnector(
            api_key="pk_live", api_secret="sec", paper_trading=False
        )
        self.assertEqual(c.auth_headers["apikey-id"], "pk_live")
        self.assertEqual(c.auth_headers["apikey-secret"], "sec")
        self.assertEqual(c.trade_headers["apikey-id"], "pk_live")
        self.assertEqual(c.trade_headers["apikey-secret"], "sec")

    def test_init_paper(self):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        self.assertEqual(c.auth_headers, {})
        self.assertNotIn("apikey-id", c.trade_headers)

    def test_init_env_fallback(self):
        with patch.dict("os.environ", {"ALPACA_API_KEY": "pk_env"}):
            c = alpaca_real.AlpacaConnector()
            self.assertEqual(c.api_key, "pk_env")


class TestAlpacaRealConnect(unittest.TestCase):
    def test_connect_paper(self):
        c = alpaca_real.AlpacaConnector()
        run(c.connect())
        self.assertTrue(c._connected)

    def test_connect_live_with_secret(self):
        c = alpaca_real.AlpacaConnector(api_key="pk_live_x", api_secret="s", paper_trading=False)
        run(c.connect())
        self.assertTrue(c._connected)

    def test_connect_live_no_secret_raises(self):
        c = alpaca_real.AlpacaConnector(api_key="pk_live_x", paper_trading=False)
        with self.assertRaises(ValueError):
            run(c.connect())


@patch(GET)
@patch(POST)
class TestAlpacaRealGetAccount(unittest.TestCase):
    def test_not_connected(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector()
        self.assertEqual(run(c.get_account()), {})

    def test_oauth_success(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(oauth_token="tok")
        c._connected = True
        mock_get.return_value = make_response({"cash": 100})
        self.assertEqual(run(c.get_account()), {"cash": 100})
        mock_get.assert_called_once()

    def test_live_secret_success(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="k", api_secret="s", paper_trading=False)
        c._connected = True
        mock_get.return_value = make_response({"cash": 1})
        self.assertEqual(run(c.get_account()), {"cash": 1})

    def test_paper_success(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        c._connected = True
        mock_get.return_value = make_response({"cash": 2})
        self.assertEqual(run(c.get_account()), {"cash": 2})

    def test_http_401(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        c._connected = True
        mock_get.return_value = make_response(status_code=401, raise_http=True)
        self.assertEqual(run(c.get_account()), {})

    def test_http_403(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        c._connected = True
        mock_get.return_value = make_response(status_code=403, raise_http=True)
        self.assertEqual(run(c.get_account()), {})

    def test_http_other(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        c._connected = True
        mock_get.return_value = make_response(status_code=500, raise_http=True)
        self.assertEqual(run(c.get_account()), {})

    def test_timeout(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        c._connected = True
        mock_get.side_effect = requests.exceptions.Timeout()
        self.assertEqual(run(c.get_account()), {})

    def test_generic_exception(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        c._connected = True
        mock_get.side_effect = RuntimeError("boom")
        self.assertEqual(run(c.get_account()), {})


@patch(GET)
@patch(POST)
class TestAlpacaRealPrices(unittest.TestCase):
    def test_not_connected(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector()
        self.assertEqual(run(c.get_current_prices(["AAPL"])), {})

    def test_oauth_success(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(oauth_token="tok")
        c._connected = True
        mock_get.return_value = make_response([{"symbol": "AAPL", "last": 175.0}])
        self.assertEqual(run(c.get_current_prices(["AAPL"])), {"AAPL": 175.0})

    def test_live_secret_success(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="k", api_secret="s", paper_trading=False)
        c._connected = True
        mock_get.return_value = make_response([{"symbol": "MSFT", "last": 420.0}])
        self.assertEqual(run(c.get_current_prices(["MSFT"])), {"MSFT": 420.0})

    def test_paper_success(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        c._connected = True
        mock_get.return_value = make_response([{"symbol": "TSLA", "last": 198.0}])
        self.assertEqual(run(c.get_current_prices(["TSLA"])), {"TSLA": 198.0})

    def test_quote_without_last(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        c._connected = True
        mock_get.return_value = make_response([{"symbol": "TSLA"}])
        self.assertEqual(run(c.get_current_prices(["TSLA"])), {})

    def test_http_error(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        c._connected = True
        mock_get.return_value = make_response(status_code=401, raise_http=True)
        self.assertEqual(run(c.get_current_prices(["AAPL"])), {})

    def test_generic_exception(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        c._connected = True
        mock_get.side_effect = RuntimeError("boom")
        self.assertEqual(run(c.get_current_prices(["AAPL"])), {})


@patch(GET)
@patch(POST)
class TestAlpacaRealSubmitOrder(unittest.TestCase):
    def test_not_connected(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector()
        self.assertEqual(run(c.submit_market_order("AAPL", "buy", 1)), {})

    def test_oauth_success_filled(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(oauth_token="tok")
        c._connected = True
        mock_post.return_value = make_response(
            {"id": "1", "symbol": "AAPL", "status": "filled",
             "filled_qty": 1, "filled_avg_price": "175.5"}
        )
        res = run(c.submit_market_order("AAPL", "buy", 1))
        self.assertEqual(res["id"], "1")

    def test_paper_success_no_fill(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        c._connected = True
        mock_post.return_value = make_response(
            {"id": "2", "symbol": "MSFT", "status": "submitted"}
        )
        res = run(c.submit_market_order("MSFT", "sell", 2, client_order_id="c1"))
        self.assertEqual(res["id"], "2")

    def test_live_secret_success(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="k", api_secret="s", paper_trading=False)
        c._connected = True
        mock_post.return_value = make_response({"id": "3"})
        self.assertEqual(run(c.submit_market_order("TSLA")).get("id"), "3")

    def test_http_401(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        c._connected = True
        mock_post.return_value = make_response(status_code=401, raise_http=True)
        self.assertEqual(run(c.submit_market_order("AAPL")), {})

    def test_http_400(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        c._connected = True
        resp = make_response(status_code=400, raise_http=True)
        resp.json.return_value = {"detail": "bad symbol"}
        mock_post.return_value = resp
        self.assertEqual(run(c.submit_market_order("AAPL")), {})

    def test_http_other(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        c._connected = True
        mock_post.return_value = make_response(status_code=500, raise_http=True)
        self.assertEqual(run(c.submit_market_order("AAPL")), {})

    def test_timeout(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        c._connected = True
        mock_post.side_effect = requests.exceptions.Timeout()
        self.assertEqual(run(c.submit_market_order("AAPL")), {})

    def test_generic_exception(self, mock_post, mock_get):
        c = alpaca_real.AlpacaConnector(api_key="pk_test_x")
        c._connected = True
        mock_post.side_effect = RuntimeError("boom")
        self.assertEqual(run(c.submit_market_order("AAPL")), {})


if __name__ == "__main__":
    unittest.main()
