"""Coverage tests for event_markets.streaming."""
import json
import time
import threading
from unittest import TestCase, mock

import event_markets.streaming as S


class FakeApp:
    """websocket.WebSocketApp stand-in whose run_forever returns immediately."""
    def __init__(self, url, header=None, on_open=None, on_message=None,
                 on_error=None, on_close=None):
        self.url = url
        self.header = header
        self.on_open = on_open
        self.on_message = on_message
        self.on_error = on_error
        self.on_close = on_close

    def run_forever(self, ping_interval=0, ping_timeout=0):
        # Simulate an open then immediately return so the run() loop re-checks _stop.
        if self.on_open:
            self.on_open(self)
        return

    def close(self):
        pass


class TestPriceUpdate(TestCase):
    def test_defaults(self):
        u = S.PriceUpdate("kalshi", "KX")
        self.assertEqual(u.platform, "kalshi")
        self.assertEqual(u.market_id, "KX")
        self.assertEqual(u.yes_price, 0.0)


class TestBaseStream(TestCase):
    def test_available(self):
        s = S._BaseStream(lambda u: None, name="t")
        self.assertTrue(s.available())

    def test_base_subscribe_and_headers_empty(self):
        s = S._BaseStream(lambda u: None, name="t")
        self.assertEqual(s._subscribe_messages(), [])
        self.assertEqual(s._headers(), [])

    def test_emit_calls_callback(self):
        captured = []
        s = S._BaseStream(lambda u: captured.append(u), name="t")
        upd = S.PriceUpdate("kalshi", "KX", 0.5)
        s._emit(upd)
        self.assertEqual(captured, [upd])

    def test_emit_swallows_callback_error(self):
        def bad(u):
            raise RuntimeError("boom")
        s = S._BaseStream(bad, name="t")
        # should not raise
        s._emit(S.PriceUpdate("kalshi", "KX", 0.5))

    def test_on_open_sends_subscribe_messages(self):
        sent = []
        s = S._BaseStream(lambda u: None, name="t")
        s._subscribe_messages = lambda: [{"type": "x"}]
        fake_app = mock.MagicMock()
        fake_app.send.side_effect = lambda m: sent.append(m)
        s._on_open(fake_app)
        self.assertTrue(s.connected)
        self.assertEqual(s._backoff, 1.0)
        self.assertEqual(sent, ['{"type": "x"}'])

    def test_stop_with_app_raises(self):
        s = S._BaseStream(lambda u: None, name="t")
        fake_app = mock.MagicMock()
        fake_app.close.side_effect = RuntimeError("closed")
        s._app = fake_app
        s.stop()  # exception swallowed

    def test_on_open_subscribe_send_error_swallowed(self):
        s = S._BaseStream(lambda u: None, name="t")
        s._subscribe_messages = lambda: [{"type": "x"}]
        fake_app = mock.MagicMock()
        fake_app.send.side_effect = RuntimeError("nope")
        s._on_open(fake_app)  # should not raise
        self.assertTrue(s.connected)

    def test_on_message_list_and_dict(self):
        captured = []
        s = S._BaseStream(lambda u: captured.append(u), name="t")

        # dict path
        s._handle = lambda d: captured.append(("h", d))
        s._on_message(None, json.dumps({"event_type": "book"}))
        # list path
        s._on_message(None, json.dumps([{"event_type": "book"}, {"event_type": "book"}]))
        self.assertEqual(len(captured), 3)

        # invalid json -> nothing
        s._on_message(None, "not json")
        # non dict/list
        s._on_message(None, "123")

    def test_on_error_and_close(self):
        s = S._BaseStream(lambda u: None, name="t")
        s._on_error(None, "err")
        s._on_close(None)
        self.assertFalse(s.connected)

    def test_stop_no_app(self):
        s = S._BaseStream(lambda u: None, name="t")
        s.stop()  # no app -> no exception

    def test_stop_with_app(self):
        s = S._BaseStream(lambda u: None, name="t")
        fake_app = mock.MagicMock()
        s._app = fake_app
        s.stop()
        fake_app.close.assert_called_once()

    def test_run_ws_none(self):
        # Force _ws to None to exercise the disabled path
        with mock.patch.object(S, "_ws", None):
            s = S._BaseStream(lambda u: None, name="t")
            self.assertFalse(s.available())
            s.run()  # should just warn and return

    def test_run_loop_with_mocked_app(self):
        captured = []
        s = S._BaseStream(lambda u: captured.append(u), name="t")
        with mock.patch.object(S, "_ws") as ws:
            ws.WebSocketApp = FakeApp
            # Speed up the reconnect loop so it doesn't actually sleep.
            with mock.patch.object(S.time, "sleep", lambda *a, **k: None):
                t = threading.Thread(target=s.run, daemon=True)
                t.start()
                time.sleep(0.05)
                s._stop.set()
                t.join(timeout=2)
        self.assertTrue(s._stop.is_set())


class TestPolymarketStream(TestCase):
    def test_empty_subscribe(self):
        s = S.PolymarketStream([], lambda u: None)
        self.assertEqual(s._subscribe_messages(), [])

    def test_subscribe_messages(self):
        s = S.PolymarketStream(["tok1", "tok2"], lambda u: None)
        self.assertEqual(
            s._subscribe_messages(),
            [{"type": "market", "assets_ids": ["tok1", "tok2"]}],
        )

    def test_handle_book(self):
        captured = []
        s = S.PolymarketStream(["tok1"], lambda u: captured.append(u))
        s._handle({"event_type": "book", "asset_id": "tok1",
                   "bids": [{"price": "0.4"}], "asks": [{"price": "0.6"}]})
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0].yes_price, 0.5)

    def test_handle_book_single_side(self):
        captured = []
        s = S.PolymarketStream(["tok1"], lambda u: captured.append(u))
        s._handle({"event_type": "book", "asset_id": "tok1",
                   "bids": [], "asks": [{"price": "0.6"}]})
        self.assertEqual(captured[0].yes_price, 0.6)

    def test_handle_book_empty_asks(self):
        captured = []
        s = S.PolymarketStream(["tok1"], lambda u: captured.append(u))
        s._handle({"event_type": "book", "asset_id": "tok1",
                   "bids": [{"price": "0.4"}]})
        self.assertEqual(captured[0].yes_price, 0.4)

    def test_handle_book_no_bids_or_asks(self):
        captured = []
        s = S.PolymarketStream(["tok1"], lambda u: captured.append(u))
        s._handle({"event_type": "book", "asset_id": "tok1"})
        self.assertEqual(captured[0].yes_price, 0.0)

    def test_handle_list_with_non_dict(self):
        captured = []
        s = S.PolymarketStream(["tok1"], lambda u: captured.append(u))
        s._handle = lambda d: captured.append(d)
        s._on_message(None, json.dumps([{"event_type": "book", "asset_id": "tok1"}, "notdict"]))
        self.assertEqual(len(captured), 1)

    def test_handle_price_change(self):
        captured = []
        s = S.PolymarketStream(["tok1"], lambda u: captured.append(u))
        s._handle({"event_type": "price_change", "asset_id": "tok1", "price": "0.7"})
        self.assertEqual(captured[0].yes_price, 0.7)

    def test_handle_zero_price_ignored(self):
        captured = []
        s = S.PolymarketStream(["tok1"], lambda u: captured.append(u))
        s._handle({"event_type": "price_change", "asset_id": "tok1", "price": "0"})
        self.assertEqual(captured, [])

    def test_handle_other_type(self):
        captured = []
        s = S.PolymarketStream(["tok1"], lambda u: captured.append(u))
        s._handle({"event_type": "something_else", "asset_id": "tok1"})
        self.assertEqual(captured, [])


class TestKalshiStream(TestCase):
    def test_empty_subscribe(self):
        s = S.KalshiStream([], lambda u: None)
        self.assertEqual(s._subscribe_messages(), [])

    def test_demo_url(self):
        s = S.KalshiStream(["KX"], lambda u: None, env="demo")
        self.assertIn("demo-api.kalshi.co", s.url)

    def test_available_without_auth(self):
        s = S.KalshiStream([], lambda u: None)
        self.assertFalse(s.available())

    def test_available_with_auth(self):
        s = S.KalshiStream(["KX"], lambda u: None, api_key_id="k",
                           private_key_path="/tmp/does-not-exist.pem")
        self.assertTrue(s.available())

    def test_handle_ticker(self):
        captured = []
        s = S.KalshiStream(["KX"], lambda u: captured.append(u))
        s._handle({"type": "ticker", "msg": {"market_ticker": "KX",
                  "yes_bid": 40, "yes_ask": 60, "price": 50}})
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0].platform, "kalshi")
        self.assertAlmostEqual(captured[0].yes_price, 0.5)

    def test_handle_ticker_no_bid_ask(self):
        captured = []
        s = S.KalshiStream(["KX"], lambda u: captured.append(u))
        s._handle({"type": "ticker", "msg": {"market_ticker": "KX", "price": 50}})
        self.assertAlmostEqual(captured[0].yes_price, 0.5)

    def test_handle_non_ticker(self):
        captured = []
        s = S.KalshiStream(["KX"], lambda u: captured.append(u))
        s._handle({"type": "something_else"})
        self.assertEqual(captured, [])

    def test_headers_no_key(self):
        s = S.KalshiStream(["KX"], lambda u: None)
        self.assertEqual(s._headers(), [])

    def test_headers_missing_path(self):
        # api_key_id set but no private_key_path -> _load_key returns None -> []
        s = S.KalshiStream(["KX"], lambda u: None, api_key_id="mykey")
        self.assertEqual(s._headers(), [])

    def test_load_key_no_path(self):
        s = S.KalshiStream(["KX"], lambda u: None, api_key_id="mykey")
        self.assertIsNone(s._load_key())

    def test_handle_ticker_missing_prices(self):
        captured = []
        s = S.KalshiStream(["KX"], lambda u: captured.append(u))
        s._handle({"type": "ticker", "msg": {"market_ticker": "KX"}})
        self.assertEqual(captured[0].yes_price, 0.0)

    def test_handle_ticker_no_market_ticker(self):
        captured = []
        s = S.KalshiStream(["KX"], lambda u: captured.append(u))
        # ticker key absent -> ticker == "" -> no emit
        s._handle({"type": "ticker", "msg": {"price": 50}})
        self.assertEqual(captured, [])

    def test_handle_ticker_yes_bid_ask_only(self):
        captured = []
        s = S.KalshiStream(["KX"], lambda u: captured.append(u))
        s._handle({"type": "ticker", "msg": {"market_ticker": "KX",
                  "yes_bid": 40, "yes_ask": 60}})
        self.assertAlmostEqual(captured[0].yes_price, 0.5)

    def test_headers_with_key(self):
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        import tempfile, os
        fd, path = tempfile.mkstemp(suffix=".pem")
        with os.fdopen(fd, "wb") as f:
            f.write(pem)
        try:
            s = S.KalshiStream(["KX"], lambda u: None, api_key_id="mykey",
                               private_key_path=path)
            headers = s._headers()
            self.assertEqual(len(headers), 3)
            self.assertTrue(headers[0].startswith("KALSHI-ACCESS-KEY: mykey"))
        finally:
            os.unlink(path)


class TestStreamManager(TestCase):
    def test_key(self):
        mgr = S.PredictionMarketStreamManager()
        self.assertEqual(mgr._key("kalshi", "KX"), "kalshi:KX")

    def test_on_update(self):
        mgr = S.PredictionMarketStreamManager()
        upd = S.PriceUpdate("kalshi", "KX", 0.5)
        mgr._on_update(upd)
        self.assertIn("kalshi:KX", mgr._cache)

    def test_latest(self):
        mgr = S.PredictionMarketStreamManager()
        upd = S.PriceUpdate("kalshi", "KX", 0.5)
        mgr._on_update(upd)
        self.assertIsNotNone(mgr.latest("kalshi", "KX"))
        self.assertIsNone(mgr.latest("kalshi", "NOPE"))
        # max_age expiry
        mgr._cache["kalshi:KX"].ts = 0
        self.assertIsNone(mgr.latest("kalshi", "KX", max_age_s=30))

    def test_no_streams(self):
        mgr = S.PredictionMarketStreamManager()
        mgr.start()
        mgr.stop()
        self.assertFalse(mgr.any_connected())
        self.assertEqual(mgr.status()["cached_markets"], 0)

    def test_start_stop_with_unavailable(self):
        s = S.PolymarketStream([], lambda u: None)  # unavailable (no asset ids)
        mgr = S.PredictionMarketStreamManager(polymarket_asset_ids=["tok1"])
        # Force the stream to be unavailable to exercise the else branch
        mgr._streams[0].available = lambda: False
        mgr.start()
        mgr.stop()

    def test_status_with_stream(self):
        mgr = S.PredictionMarketStreamManager(polymarket_asset_ids=["tok1"])
        st = mgr.status()
        self.assertIn("streams", st)
        self.assertEqual(len(st["streams"]), 1)

    def test_status_with_recent_message(self):
        from event_markets.streaming import KalshiStream, PriceUpdate
        k = KalshiStream(["KX"], lambda u: None, api_key_id="k",
                         private_key_path="/tmp/nope.pem")
        k._emit(PriceUpdate("kalshi", "KX", 0.5))
        mgr = S.PredictionMarketStreamManager()
        mgr._streams = [k]
        st = mgr.status()
        self.assertIsNotNone(st["streams"][0]["last_msg_age_s"])


if __name__ == "__main__":
    import unittest
    unittest.main()
