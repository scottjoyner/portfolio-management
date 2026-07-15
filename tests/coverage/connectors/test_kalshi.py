"""Tests for trading_system.connectors.kalshi (KalshiConnector)."""
import unittest
from unittest.mock import MagicMock, patch

from trading_system.connectors import kalshi as kalshi_mod
from trading_system.connectors.kalshi import KalshiConnector


def make_session(get_resp=None, post_resp=None):
    sess = MagicMock()
    if get_resp is not None:
        sess.get.return_value = get_resp
    if post_resp is not None:
        sess.post.return_value = post_resp
    return sess


class Resp:
    def __init__(self, status_code=200, data=None, text="", raise_exc=None):
        self.status_code = status_code
        self._data = data if data is not None else {}
        self.text = text
        self.raise_exc = raise_exc

    def json(self):
        return self._data


class TestKalshiConnector(unittest.IsolatedAsyncioTestCase):
    async def test_connect_ok_with_limits(self):
        sess = make_session(get_resp=Resp(200, {
            "balance": {"total": 100}, "collateral": 50, "limits": {"daily": 10}}))
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            self.assertTrue(await c.connect())

    async def test_connect_ok_no_limits(self):
        sess = make_session(get_resp=Resp(200, {"balance": {"total": 1}, "collateral": 0}))
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            self.assertTrue(await c.connect())

    async def test_connect_fail_status(self):
        sess = make_session(get_resp=Resp(403, {}, text="no"))
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            self.assertFalse(await c.connect())

    async def test_connect_exception(self):
        sess = make_session()
        sess.get.side_effect = RuntimeError("boom")
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            self.assertFalse(await c.connect())

    async def test_get_markets_with_category(self):
        sess = make_session(get_resp=Resp(200, {"markets": [{"id": 1}]}))
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            out = await c.get_markets(category="Elections")
        self.assertEqual(out, [{"id": 1}])

    async def test_get_markets_no_category(self):
        sess = make_session(get_resp=Resp(200, {"markets": [{"id": 2}]}))
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            out = await c.get_markets()
        self.assertEqual(out, [{"id": 2}])

    async def test_get_markets_fail(self):
        sess = make_session(get_resp=Resp(500))
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            self.assertEqual(await c.get_markets(), [])

    async def test_get_markets_exception(self):
        sess = make_session()
        sess.get.side_effect = RuntimeError("boom")
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            self.assertEqual(await c.get_markets(), [])

    async def test_get_market_history_full(self):
        sess = make_session(get_resp=Resp(200, {"data": [1, 2]}))
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            out = await c.get_market_history("m", start_time="a", end_time="b")
        self.assertEqual(out, {"market_id": "m", "prices": [1, 2]})

    async def test_get_market_history_minimal(self):
        sess = make_session(get_resp=Resp(200, {"data": []}))
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            out = await c.get_market_history("m")
        self.assertEqual(out["prices"], [])

    async def test_get_market_history_fail(self):
        sess = make_session(get_resp=Resp(500))
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            out = await c.get_market_history("m")
        self.assertEqual(out, {"market_id": "m", "prices": []})

    async def test_get_market_history_exception(self):
        sess = make_session()
        sess.get.side_effect = RuntimeError("boom")
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            out = await c.get_market_history("m")
        self.assertEqual(out["prices"], [])

    async def test_place_order_filled(self):
        sess = make_session(post_resp=Resp(200, {
            "id": "o1", "status": "filled", "price": 0.5, "filledSize": 10}))
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            out = await c.place_market_order("m", "call", 10, price=0.5, client_order_id="c1")
        self.assertEqual(out["id"], "o1")

    async def test_place_order_pending(self):
        sess = make_session(post_resp=Resp(201, {
            "id": "o2", "status": "pending", "price": 0}))
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            out = await c.place_market_order("m", "put", 5)
        self.assertEqual(out["id"], "o2")

    async def test_place_order_404(self):
        sess = make_session(post_resp=Resp(404))
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            self.assertEqual(await c.place_market_order("m", "call", 1), {})

    async def test_place_order_400_error(self):
        sess = make_session(post_resp=Resp(400, {"status": "error", "message": "bad"}))
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            self.assertEqual(await c.place_market_order("m", "call", 1), {})

    async def test_place_order_other(self):
        sess = make_session(post_resp=Resp(500, {"status": "fail"}))
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            self.assertEqual(await c.place_market_order("m", "call", 1), {})

    async def test_place_order_exception(self):
        sess = make_session()
        sess.post.side_effect = RuntimeError("boom")
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            self.assertEqual(await c.place_market_order("m", "call", 1), {})

    async def test_check_account_balance(self):
        sess = make_session(get_resp=Resp(200, {"balance": {"total": 5}}))
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            self.assertEqual(await c._check_account_balance(), {"total": 5})

    async def test_check_account_balance_exception(self):
        sess = make_session()
        sess.get.side_effect = RuntimeError("boom")
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            self.assertEqual(await c._check_account_balance(), {})

    async def test_get_positions(self):
        sess = make_session()
        with patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            c = KalshiConnector("k", "s")
            self.assertEqual(await c.get_positions(), [])

    async def test_main_mock_mode(self):
        sess = make_session()
        with patch.dict("os.environ", {
                "KALSHI_API_KEY": "", "KALSHI_API_SECRET": ""}, clear=False), \
             patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
            await kalshi_mod.main()

    async def test_main_with_key_and_env(self):
        sess = make_session()
        import pathlib
        real_exists = pathlib.Path.exists
        pathlib.Path.exists = lambda self: True
        try:
            with patch.dict("os.environ", {
                    "KALSHI_API_KEY": "realkey",
                    "KALSHI_API_SECRET": "realsecret"},
                    clear=False), \
                 patch("trading_system.connectors.kalshi.requests.Session", return_value=sess):
                await kalshi_mod.main()
        finally:
            pathlib.Path.exists = real_exists


if __name__ == "__main__":
    unittest.main()
