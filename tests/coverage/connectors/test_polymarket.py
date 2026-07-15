"""Tests for trading_system.connectors.polymarket (PolymarketConnector)."""
import unittest
from unittest.mock import MagicMock, patch

from trading_system.connectors import polymarket as polymarket_mod
from trading_system.connectors.polymarket import PolymarketConnector


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


class TestPolymarketConnector(unittest.IsolatedAsyncioTestCase):
    def test_init_wallet_lower(self):
        c = PolymarketConnector(api_key="k", wallet_address="0XABC", chain="Solana")
        self.assertEqual(c.wallet_address, "0xabc")
        self.assertEqual(c.chain, "solana")

    def test_init_no_key(self):
        with self.assertRaises(RuntimeError):
            PolymarketConnector(api_key=None)

    async def test_connect_ok_funded(self):
        sess = make_session(get_resp=Resp(200, {"balance": {"amount": "100.00"}}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k", wallet_address="0x" + "a" * 40)
            self.assertTrue(await c.connect())

    async def test_connect_ok_unfunded(self):
        sess = make_session(get_resp=Resp(200, {"balance": {"amount": "0"}}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertTrue(await c.connect())

    async def test_connect_ok_negative(self):
        sess = make_session(get_resp=Resp(200, {"balance": {"amount": "-5"}}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertTrue(await c.connect())

    async def test_connect_fail(self):
        sess = make_session(get_resp=Resp(403, text="no"))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertFalse(await c.connect())

    async def test_connect_exception(self):
        sess = make_session()
        sess.get.side_effect = RuntimeError("boom")
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertFalse(await c.connect())

    async def test_query_markets_event_list(self):
        sess = make_session(get_resp=Resp(200, [{"id": 1}, {"id": 2}]))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            out = await c.query_markets(event="us-pres-24")
        self.assertEqual(len(out), 2)

    async def test_query_markets_event_dict_markets(self):
        sess = make_session(get_resp=Resp(200, {"markets": [{"id": 1}]}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            out = await c.query_markets(event="us-pres-24")
        self.assertEqual(out, [{"id": 1}])

    async def test_query_markets_event_single(self):
        sess = make_session(get_resp=Resp(200, {"event": {"title": "x"},
                                                 "markets": [{"id": 1}]}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            out = await c.query_markets(event="us-pres-24")
        self.assertEqual(out, [{"id": 1}])

    async def test_query_markets_no_event(self):
        sess = make_session()
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.query_markets(), [])

    async def test_query_markets_fail(self):
        sess = make_session(get_resp=Resp(500))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.query_markets(event="e"), [])

    async def test_query_markets_exception(self):
        sess = make_session()
        sess.get.side_effect = RuntimeError("boom")
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.query_markets(event="e"), [])

    async def test_market_details_ok(self):
        sess = make_session(get_resp=Resp(200, {"event": {"title": "t"},
                                                "markets": [{"description": "yes"}]}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.get_market_details("m"), {"event": {"title": "t"}, "markets": [{"description": "yes"}]})

    async def test_market_details_fail(self):
        sess = make_session(get_resp=Resp(500))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.get_market_details("m"), {})

    async def test_market_details_exception(self):
        sess = make_session()
        sess.get.side_effect = RuntimeError("boom")
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.get_market_details("m"), {})

    async def test_place_bet_filled(self):
        sess = make_session(post_resp=Resp(200, {
            "status": "filled", "orderId": "o1",
            "fillPrice": 0.5, "filledAmountUsd": 10}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            out = await c.place_bet("m", 0, amount_usdc=10, limit_price=0.5)
        self.assertEqual(out["orderId"], "o1")

    async def test_place_bet_pending(self):
        sess = make_session(post_resp=Resp(201, {"status": "open"}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            out = await c.place_bet("m", 1)
        self.assertEqual(out["status"], "open")

    async def test_place_bet_400(self):
        sess = make_session(post_resp=Resp(400, {"detail": "bad"}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.place_bet("m", 0), {})

    async def test_place_bet_other(self):
        sess = make_session(post_resp=Resp(500, {}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.place_bet("m", 0), {})

    async def test_place_bet_exception(self):
        sess = make_session()
        sess.post.side_effect = RuntimeError("boom")
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.place_bet("m", 0), {})

    async def test_get_order_book(self):
        sess = make_session(get_resp=Resp(200, {"markets": [
            {"description": "yes", "bids": [{"price": "0.6", "size": 1}],
             "asks": [{"price": "0.7", "size": 2}]}]}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.get_order_book("m"), {})

    async def test_get_order_book_fail(self):
        sess = make_session(get_resp=Resp(500))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.get_order_book("m"), {})

    async def test_get_order_book_exception(self):
        sess = make_session()
        sess.get.side_effect = RuntimeError("boom")
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.get_order_book("m"), {})

    async def test_check_account_balance(self):
        sess = make_session(get_resp=Resp(200, {"balance": {"amount": "5"}}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.check_account_balance(), {"balance": {"amount": "5"}})

    async def test_check_account_balance_exception(self):
        sess = make_session()
        sess.get.side_effect = RuntimeError("boom")
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.check_account_balance(), {})

    async def test_connect_invalid_balance(self):
        # Invalid decimal amount raises before the float() call; outer except
        # catches it and connect() returns False.
        sess = make_session(get_resp=Resp(200, {"balance": {"amount": "notanumber"}}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertFalse(await c.connect())

    async def test_query_markets_event_dict_no_markets(self):
        sess = make_session(get_resp=Resp(200, {"foo": "bar"}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.query_markets(event="e"), [])

    async def test_market_details_no_markets(self):
        sess = make_session(get_resp=Resp(200, {"event": {"title": "t"}}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            out = await c.get_market_details("m")
        self.assertEqual(out["event"]["title"], "t")

    async def test_get_order_book_empty_markets(self):
        sess = make_session(get_resp=Resp(200, {"markets": []}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.get_order_book("m"), {})

    async def test_get_order_book_no_bids_asks(self):
        sess = make_session(get_resp=Resp(200, {"markets": [{"description": "x"}]}))
        with patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
            c = PolymarketConnector(api_key="k")
            self.assertEqual(await c.get_order_book("m"), {})

    async def test_main_no_wallet(self):
        sess = make_session()
        import pathlib
        real_exists = pathlib.Path.exists
        pathlib.Path.exists = lambda self: True
        try:
            with patch.dict("os.environ", {
                    "POLYMARKET_API_KEY": "", "POLYMARKET_WALLET_ADDRESS": "",
                    "POLYMARKET_CHAIN": "ethereum"}, clear=False), \
                 patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
                await polymarket_mod.main()
        finally:
            pathlib.Path.exists = real_exists

    async def test_main_wallet_no_env(self):
        sess = make_session()
        import pathlib
        real_exists = pathlib.Path.exists
        pathlib.Path.exists = lambda self: False
        try:
            with patch.dict("os.environ", {
                    "POLYMARKET_API_KEY": "realkey",
                    "POLYMARKET_WALLET_ADDRESS": "0x" + "a" * 40,
                    "POLYMARKET_CHAIN": "ethereum"}, clear=False), \
                 patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
                await polymarket_mod.main()
        finally:
            pathlib.Path.exists = real_exists

    async def test_main_wallet_wrong_len(self):
        sess = make_session()
        import pathlib
        real_exists = pathlib.Path.exists
        pathlib.Path.exists = lambda self: False
        try:
            with patch.dict("os.environ", {
                    "POLYMARKET_API_KEY": "realkey",
                    "POLYMARKET_WALLET_ADDRESS": "0x" + "a" * 39,
                    "POLYMARKET_CHAIN": "ethereum"}, clear=False), \
                 patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
                await polymarket_mod.main()
        finally:
            pathlib.Path.exists = real_exists

    async def test_main_wallet_no_0x(self):
        sess = make_session()
        import pathlib
        real_exists = pathlib.Path.exists
        pathlib.Path.exists = lambda self: False
        try:
            with patch.dict("os.environ", {
                    "POLYMARKET_API_KEY": "realkey",
                    "POLYMARKET_WALLET_ADDRESS": "x" * 42,
                    "POLYMARKET_CHAIN": "ethereum"}, clear=False), \
                 patch("trading_system.connectors.polymarket.requests.Session", return_value=sess):
                await polymarket_mod.main()
        finally:
            pathlib.Path.exists = real_exists


if __name__ == "__main__":
    unittest.main()
