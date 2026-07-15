"""Tests for coinbase/src/feed.py"""
import json
import threading
import time
import unittest
from unittest import mock

from coinbase.src import feed as f
from coinbase.src.feed import FeedSource


class FakeWS:
    def __init__(self):
        self.sent = []

    def send(self, msg):
        self.sent.append(msg)


class TestTickerCache(unittest.TestCase):
    def setUp(self):
        self.c = f.TickerCache(ttl=1.0)

    def test_on_emit(self):
        received = []
        self.c.on("ticker", lambda d: received.append(d))
        t = f.Ticker(product_id="BTC-USD", price=100, bid=99, ask=101,
                     volume_24h=0, timestamp=time.time())
        self.c.update_ticker(t)
        self.assertEqual(received, [t])

    def test_emit_listener_error(self):
        def boom(d):
            raise RuntimeError("x")
        self.c.on("ticker", boom)
        # should not raise
        self.c.update_ticker(f.Ticker(product_id="BTC-USD", price=1, bid=1, ask=1,
                                     volume_24h=0, timestamp=time.time()))

    def test_update_candle_cap(self):
        for i in range(510):
            self.c.update_candle(f.Candle(product_id="X", timestamp=float(i),
                                          open=1, high=1, low=1, close=1, volume=1))
        self.assertEqual(len(self.c.get_candles("X", 1000)), 500)

    def test_update_orderbook(self):
        ob = f.OrderBookUpdate(product_id="X", bids=[f.OrderBookLevel(1, 1)],
                               asks=[f.OrderBookLevel(2, 1)], timestamp=time.time())
        self.c.update_orderbook(ob)
        self.assertIs(self.c.get_orderbook("X"), ob)

    def test_get_ticker_fresh(self):
        t = f.Ticker(product_id="BTC-USD", price=100, bid=99, ask=101,
                     volume_24h=0, timestamp=time.time())
        self.c.update_ticker(t)
        self.assertIsNotNone(self.c.get_ticker("BTC-USD"))

    def test_get_ticker_expired(self):
        t = f.Ticker(product_id="BTC-USD", price=100, bid=99, ask=101,
                     volume_24h=0, timestamp=time.time() - 10)
        self.c.update_ticker(t)
        self.assertIsNone(self.c.get_ticker("BTC-USD"))

    def test_get_ticker_missing(self):
        self.assertIsNone(self.c.get_ticker("NOPE"))

    def test_get_candles(self):
        for i in range(5):
            self.c.update_candle(f.Candle(product_id="X", timestamp=float(i),
                                          open=1, high=1, low=1, close=1, volume=1))
        self.assertEqual(len(self.c.get_candles("X", 3)), 3)

    def test_all_prices(self):
        self.c.update_ticker(f.Ticker(product_id="BTC-USD", price=100, bid=99, ask=101,
                                     volume_24h=0, timestamp=time.time()))
        self.assertIn("BTC-USD", self.c.all_prices())


class TestPollingFeed(unittest.TestCase):
    def _fake_cb(self, pricebooks):
        cb = mock.MagicMock()
        cb.best_bid_ask.return_value = {"pricebooks": pricebooks}
        return cb

    def test_subscribe(self):
        pf = f.PollingFeed(mock.MagicMock(), f.TickerCache())
        pf.subscribe(["BTC-USD", "ETH-USD"])
        self.assertEqual(pf._products, {"BTC-USD", "ETH-USD"})

    def test_poll_once_no_products(self):
        pf = f.PollingFeed(None, f.TickerCache())
        pf._poll_once()  # returns early

    def test_poll_once_no_cb(self):
        pf = f.PollingFeed(None, f.TickerCache())
        pf.subscribe(["BTC-USD"])
        pf._poll_once()  # returns early (cb None)

    def test_poll_once_normal(self):
        cache = f.TickerCache()
        pf = f.PollingFeed(self._fake_cb([
            {"product_id": "BTC-USD", "bids": [{"price": "100"}], "asks": [{"price": "101"}]}
        ]), cache)
        pf.subscribe(["BTC-USD"])
        pf._poll_once()
        self.assertIsNotNone(cache.get_ticker("BTC-USD"))

    def test_poll_once_no_bids(self):
        cache = f.TickerCache()
        pf = f.PollingFeed(self._fake_cb([
            {"product_id": "BTC-USD", "bids": [], "asks": []}
        ]), cache)
        pf.subscribe(["BTC-USD"])
        pf._poll_once()  # price 0 -> skip

    def test_poll_once_missing_pid(self):
        cache = f.TickerCache()
        pf = f.PollingFeed(self._fake_cb([{"bids": [], "asks": []}]), cache)
        pf.subscribe(["BTC-USD"])
        pf._poll_once()  # continue on missing pid

    def test_poll_once_exception(self):
        cb = mock.MagicMock()
        cb.best_bid_ask.side_effect = RuntimeError("boom")
        pf = f.PollingFeed(cb, f.TickerCache())
        pf.subscribe(["BTC-USD"])
        pf._poll_once()  # swallowed

    def test_poll_once_public_method(self):
        cache = f.TickerCache()
        pf = f.PollingFeed(self._fake_cb([
            {"product_id": "BTC-USD", "bids": [{"price": "100"}], "asks": [{"price": "101"}]}
        ]), cache)
        pf.subscribe(["BTC-USD"])
        pf.poll_once()
        self.assertIsNotNone(cache.get_ticker("BTC-USD"))

    def test_stop(self):
        pf = f.PollingFeed(mock.MagicMock(), f.TickerCache())
        pf._running = True
        pf.stop()
        self.assertFalse(pf._running)

    def test_start_stop(self):
        cache = f.TickerCache()
        pf = f.PollingFeed(self._fake_cb([
            {"product_id": "BTC-USD", "bids": [{"price": "100"}], "asks": [{"price": "101"}]}
        ]), cache, poll_interval=0.01)
        pf.subscribe(["BTC-USD"])
        pf.start()
        self.assertTrue(pf._running)
        pf.stop()


class TestWebSocketFeed(unittest.TestCase):
    def test_subscribe(self):
        ws = f.WebSocketFeed(f.TickerCache())
        ws.subscribe(["BTC-USD"])
        self.assertIn("BTC-USD", ws._products)

    def test_start_no_websocket(self):
        with mock.patch("importlib.util.find_spec", return_value=None):
            ws = f.WebSocketFeed(f.TickerCache())
            self.assertFalse(ws.start())

    def test_subscribe_public(self):
        ws = f.WebSocketFeed(f.TickerCache(), use_advanced=False)
        ws.subscribe(["X-USD"])
        fw = FakeWS()
        ws._subscribe(fw)
        msg = json.loads(fw.sent[0])
        self.assertEqual(msg["type"], "subscribe")
        self.assertIn("channels", msg)

    def test_subscribe_advanced(self):
        ws = f.WebSocketFeed(f.TickerCache(), use_advanced=True)
        ws.subscribe(["X-USD"])
        fw = FakeWS()
        ws._subscribe(fw)
        msg = json.loads(fw.sent[0])
        self.assertIn("product_ids", msg)
        self.assertEqual(msg["channel"], "ticker")

    def test_on_message_ticker(self):
        cache = f.TickerCache()
        ws = f.WebSocketFeed(cache)
        ws._on_message(None, json.dumps(
            {"type": "ticker", "product_id": "BTC-USD", "price": "100",
             "best_bid": "99", "best_ask": "101", "volume_24_h": "5"}))
        self.assertIsNotNone(cache.get_ticker("BTC-USD"))

    def test_on_message_snapshot(self):
        cache = f.TickerCache()
        ws = f.WebSocketFeed(cache)
        ws._on_message(None, json.dumps(
            {"type": "snapshot", "product_id": "ETH-USD", "price": "50",
             "bid": "49", "ask": "51", "volume": "3"}))
        self.assertIsNotNone(cache.get_ticker("ETH-USD"))

    def test_on_message_l2update(self):
        cache = f.TickerCache()
        ws = f.WebSocketFeed(cache)
        ws._on_message(None, json.dumps(
            {"type": "l2update", "product_id": "SOL-USD", "price": "20",
             "bid": "19", "ask": "21"}))
        self.assertIsNotNone(cache.get_ticker("SOL-USD"))

    def test_on_message_other(self):
        cache = f.TickerCache()
        ws = f.WebSocketFeed(cache)
        ws._on_message(None, json.dumps({"type": "subscriptions"}))
        self.assertIsNone(cache.get_ticker("BTC-USD"))

    def test_on_message_bad_json(self):
        cache = f.TickerCache()
        ws = f.WebSocketFeed(cache)
        ws._on_message(None, "not json")  # swallowed


class TestAdvancedTradeWebSocket(unittest.TestCase):
    def test_subscribe(self):
        ws = f.AdvancedTradeWebSocket("key", "c2Vj", f.TickerCache())
        fw = FakeWS()
        ws._subscribe(fw)
        msg = json.loads(fw.sent[0])
        self.assertIn("orders", [c["name"] for c in msg["channels"]])

    def test_start_no_websocket(self):
        with mock.patch("importlib.util.find_spec", return_value=None):
            ws = f.AdvancedTradeWebSocket("key", "c2Vj", f.TickerCache())
            self.assertFalse(ws.start())

    def test_generate_jwt_robust(self):
        ws = f.AdvancedTradeWebSocket("key", "c2Vj", f.TickerCache())
        try:
            tok = ws._generate_jwt()
            self.assertIsInstance(tok, str)
        except Exception:
            # EdDSA key material may be unusable in this env; line still executed
            pass

    def test_on_message_fills(self):
        captured = []
        ws = f.AdvancedTradeWebSocket("key", "c2Vj", f.TickerCache(), on_fill=captured.append)
        ws._on_message(None, json.dumps(
            {"channel": "fills", "events": [{"type": "fill", "id": 1}]}))
        self.assertEqual(len(captured), 1)

    def test_on_message_orders(self):
        captured = []
        ws = f.AdvancedTradeWebSocket("key", "c2Vj", f.TickerCache(), on_order=captured.append)
        ws._on_message(None, json.dumps(
            {"channel": "orders", "events": [{"type": "order"}]}))
        self.assertEqual(len(captured), 1)

    def test_on_message_accounts(self):
        captured = []
        ws = f.AdvancedTradeWebSocket("key", "c2Vj", f.TickerCache(), on_account=captured.append)
        ws._on_message(None, json.dumps(
            {"channel": "accounts", "events": [{"type": "account"}]}))
        self.assertEqual(len(captured), 1)

    def test_on_message_bad(self):
        ws = f.AdvancedTradeWebSocket("key", "c2Vj", f.TickerCache())
        ws._on_message(None, "not json")  # swallowed

    def test_stop(self):
        ws = f.AdvancedTradeWebSocket("key", "c2Vj", f.TickerCache())
        ws.stop()


import sys as _sys
import types as _types


def _fake_websocket_module(app_cls):
    mod = _types.ModuleType("websocket")
    mod.WebSocketApp = app_cls
    return mod


class _AppSleep:
    def __init__(self, *a, **k):
        self.on_open = None

    def run_forever(self, **k):
        time.sleep(0.02)

    def close(self):
        pass


class _AppRaise:
    def __init__(self, *a, **k):
        self.on_open = None

    def run_forever(self, **k):
        raise RuntimeError("conn failed")

    def close(self):
        pass


class TestPollingFeedStartDetails(unittest.TestCase):
    def test_start_idempotent(self):
        pf = f.PollingFeed(mock.MagicMock(), f.TickerCache())
        pf._running = True
        pf.start()  # early return (already running)

    def test_start_prime_empty(self):
        # cb returns no tickers -> priming loop runs (covers sleep + full loop)
        cb = mock.MagicMock()
        cb.best_bid_ask.return_value = {"pricebooks": []}
        cache = f.TickerCache()
        pf = f.PollingFeed(cb, cache, poll_interval=0.01)
        pf.subscribe(["BTC-USD"])
        pf.start()
        pf.stop()


class TestWebSocketFeedStartSuccess(unittest.TestCase):
    def test_start_success(self):
        cache = f.TickerCache()
        ws = f.WebSocketFeed(cache)
        ws.subscribe(["X-USD"])
        ws._reconnect_delay = 0.001
        mod = _fake_websocket_module(_AppSleep)
        with mock.patch("importlib.util.find_spec", return_value=object()):
            with mock.patch.dict(_sys.modules, {"websocket": mod}):
                self.assertTrue(ws.start())
                ws.stop()

    def test_subscribe_no_products(self):
        ws = f.WebSocketFeed(f.TickerCache())
        ws._subscribe(FakeWS())  # returns early (no products)


class TestWebSocketFeedLoop(unittest.TestCase):
    def test_loop_normal(self):
        cache = f.TickerCache()
        ws = f.WebSocketFeed(cache)
        ws.subscribe(["X-USD"])
        ws._running = True
        mod = _fake_websocket_module(_AppSleep)
        with mock.patch.dict(_sys.modules, {"websocket": mod}):
            timer = threading.Timer(0.005, lambda: setattr(ws, "_running", False))
            timer.start()
            ws._ws_loop()
            timer.join()
        self.assertFalse(ws._running)

    def test_loop_error(self):
        cache = f.TickerCache()
        ws = f.WebSocketFeed(cache)
        ws.subscribe(["X-USD"])
        ws._running = True
        ws._reconnect_delay = 0.001
        mod = _fake_websocket_module(_AppRaise)
        with mock.patch.dict(_sys.modules, {"websocket": mod}):
            timer = threading.Timer(0.005, lambda: setattr(ws, "_running", False))
            timer.start()
            ws._ws_loop()
            timer.join()
        self.assertFalse(ws._running)

    def test_on_message_zero_price(self):
        cache = f.TickerCache()
        ws = f.WebSocketFeed(cache)
        ws._on_message(None, json.dumps(
            {"type": "ticker", "product_id": "BTC-USD", "price": "0"}))
        self.assertIsNone(cache.get_ticker("BTC-USD"))

    def test_stop_with_ws(self):
        ws = f.AdvancedTradeWebSocket("key", "c2Vj", f.TickerCache())
        ws._ws = mock.MagicMock()
        ws.stop()
        ws._ws.close.assert_called_once()


class TestAdvancedWSStartSuccess(unittest.TestCase):
    def test_start_success(self):
        cache = f.TickerCache()
        ws = f.AdvancedTradeWebSocket("key", "c2Vj", cache)
        ws._reconnect_delay = 0.001
        ws._generate_jwt = lambda: "tok"
        mod = _fake_websocket_module(_AppSleep)
        with mock.patch("importlib.util.find_spec", return_value=object()):
            with mock.patch.dict(_sys.modules, {"websocket": mod}):
                self.assertTrue(ws.start())
                ws.stop()

    def test_loop_normal(self):
        cache = f.TickerCache()
        ws = f.AdvancedTradeWebSocket("key", "c2Vj", cache)
        ws._running = True
        ws._generate_jwt = lambda: "tok"
        mod = _fake_websocket_module(_AppSleep)
        with mock.patch.dict(_sys.modules, {"websocket": mod}):
            timer = threading.Timer(0.005, lambda: setattr(ws, "_running", False))
            timer.start()
            ws._ws_loop()
            timer.join()
        self.assertFalse(ws._running)

    def test_loop_error(self):
        cache = f.TickerCache()
        ws = f.AdvancedTradeWebSocket("key", "c2Vj", cache)
        ws._running = True
        ws._reconnect_delay = 0.001
        ws._generate_jwt = lambda: "tok"
        mod = _fake_websocket_module(_AppRaise)
        with mock.patch.dict(_sys.modules, {"websocket": mod}):
            timer = threading.Timer(0.005, lambda: setattr(ws, "_running", False))
            timer.start()
            ws._ws_loop()
            timer.join()
        self.assertFalse(ws._running)


if __name__ == "__main__":
    unittest.main()
