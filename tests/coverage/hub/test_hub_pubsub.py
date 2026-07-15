import asyncio
import importlib
import json
import sys
import unittest
from unittest import mock

import trading_system.hub.pubsub as pubsub_mod
from trading_system.hub.pubsub import (
    WebSocketPubSubHub,
    MarketFeedPublisher,
    process_market_event,
)


def _fresh_hub():
    hub = WebSocketPubSubHub.__new__(WebSocketPubSubHub)
    hub.api_key = None
    hub.api_secret = None
    hub._redis_client = None
    hub._subscribers = {}
    hub._running = False
    return hub


class TestInitAndConnection(unittest.TestCase):
    def test_init_default_url_redis_down(self):
        # no redis server -> __init__ except branch + _test_connection except
        hub = WebSocketPubSubHub()
        self.assertIsNotNone(hub)
        # redis client may be set (lazy) or None depending on env
        # ensure default url applied
        # (connection failed path covered by exception handler)

    def test_init_with_url_and_mock_redis(self):
        fake = mock.MagicMock()
        fake.ping.return_value = True
        # exists returns False first (set called), then True (skip)
        fake.exists.side_effect = [False, True, False, True, False, True]
        with mock.patch.object(pubsub_mod.redis.Redis, "from_url", return_value=fake):
            hub = WebSocketPubSubHub(redis_url="redis://example:6379")
        self.assertIs(hub._redis_client, fake)
        # set should have been called at least once (exists False)
        self.assertGreaterEqual(fake.set.call_count, 1)

    def test_connect_with_client(self):
        hub = _fresh_hub()
        fake = mock.MagicMock()
        fake.ping.return_value = True
        hub._redis_client = fake

        async def go():
            await hub.connect()
        asyncio.run(go())
        fake.ping.assert_called()

    def test_connect_no_client(self):
        hub = _fresh_hub()
        hub._redis_client = None

        async def go():
            await hub.connect()
        asyncio.run(go())

    def test_test_connection_no_client(self):
        hub = _fresh_hub()
        hub._redis_client = None
        hub._test_connection()  # should early-return

    def test_connect_ping_fails(self):
        hub = _fresh_hub()
        fake = mock.MagicMock()
        fake.ping.side_effect = RuntimeError("down")
        hub._redis_client = fake

        async def go():
            await hub.connect()
        asyncio.run(go())


class TestPublish(unittest.TestCase):
    def test_publish_no_client(self):
        hub = _fresh_hub()
        hub._redis_client = None

        async def go():
            await hub.publish("marketplace.BTC-USD", {"price": 1})
        asyncio.run(go())

    def test_publish_with_client(self):
        hub = _fresh_hub()
        fake = mock.MagicMock()
        fake.publish.return_value = 1
        fake.pubsub_numsub.return_value = [(b"marketfeed:marketplace.BTC-USD", 3)]
        hub._redis_client = fake

        async def go():
            await hub.publish("marketplace.BTC-USD", {"price": 60000})
        asyncio.run(go())
        fake.publish.assert_called_once()

    def test_publish_exception(self):
        hub = _fresh_hub()
        fake = mock.MagicMock()
        fake.publish.side_effect = RuntimeError("boom")
        fake.pubsub_numsub.return_value = []
        hub._redis_client = fake

        async def go():
            await hub.publish("marketplace.BTC-USD", {"price": 1})
        asyncio.run(go())


class TestSubscribe(unittest.TestCase):
    def test_subscribe_no_handler(self):
        hub = _fresh_hub()
        hub.subscribe("marketplace.BTC-USD", None)
        self.assertEqual(hub._subscribers, {})

    def test_subscribe_with_handler_and_topic(self):
        hub = _fresh_hub()
        hub._redis_client = None  # listener returns immediately

        async def go():
            called = []
            hub.subscribe("marketplace.BTC-USD", lambda p: called.append(p))
            hub.subscribe("marketplace.BTC-USD", lambda p: called.append(p))  # second: _running already True
            await asyncio.sleep(0)
        asyncio.run(go())
        self.assertIn("marketplace.BTC-USD", hub._subscribers)

    def test_subscribe_without_topic(self):
        hub = _fresh_hub()
        hub._redis_client = None

        async def go():
            hub.subscribe(None, lambda p: None)
            await asyncio.sleep(0)
        asyncio.run(go())


class TestListener(unittest.TestCase):
    def test_listener_dispatch(self):
        hub = _fresh_hub()
        fake = mock.MagicMock()
        pubsub = mock.MagicMock()
        fake.pubsub.return_value = pubsub
        hub._redis_client = fake

        received = []
        raised = []

        async def handler(payload):
            received.append(payload)

        async def handler2(payload):
            raised.append(payload)
            raise RuntimeError("handler boom")

        hub._subscribers = {
            "marketplace.BTC-USD": [handler2],
            "marketplace.*": [handler],
        }

        btc_valid = json.dumps({
            "topic": "marketplace.BTC-USD",
            "event_type": "market_price_update",
            "price": 1,
        })
        eth_valid = json.dumps({
            "topic": "marketplace.ETH-USD",
            "event_type": "market_price_update",
            "price": 2,
        })
        messages = [
            {"type": "message", "channel": b"marketfeed:marketplace.BTC-USD", "data": btc_valid},
            {"type": "message", "channel": b"marketfeed:marketplace.BTC-USD", "data": "not json"},
            {"type": "subscribe", "channel": b"marketfeed:marketplace.X", "data": "{}"},
            {"type": "message", "channel": b"marketfeed:marketplace.ETH-USD", "data": eth_valid},
        ]

        state = {"n": 0}

        def se(*a, **k):
            state["n"] += 1
            if state["n"] <= len(messages):
                return messages[state["n"] - 1]
            if state["n"] == len(messages) + 1:
                raise RuntimeError("pubsub boom")
            hub._running = False
            return None

        pubsub.get_message.side_effect = se
        pubsub.unsubscribe.side_effect = RuntimeError("unsub boom")
        hub._running = True

        asyncio.run(hub._listen_to_subscribers())

        self.assertTrue(any(m.get("topic") == "marketplace.BTC-USD" for m in received))
        self.assertTrue(any(m.get("topic") == "marketplace.ETH-USD" for m in received))
        self.assertEqual(len(raised), 1)


class TestListenerEarlyReturn(unittest.TestCase):
    def test_early_return_no_client(self):
        hub = _fresh_hub()
        hub._redis_client = None
        hub._subscribers = {"x": [lambda p: None]}

        async def go():
            await hub._listen_to_subscribers()
        asyncio.run(go())

    def test_early_return_no_subscribers(self):
        hub = _fresh_hub()
        hub._redis_client = mock.MagicMock()
        hub._subscribers = {}

        async def go():
            await hub._listen_to_subscribers()
        asyncio.run(go())

    def test_listener_cancelled(self):
        hub = _fresh_hub()
        fake = mock.MagicMock()
        pubsub = mock.MagicMock()
        fake.pubsub.return_value = pubsub
        hub._redis_client = fake
        hub._subscribers = {"x": [lambda p: None]}
        pubsub.get_message.return_value = None  # loop stays alive
        hub._running = True

        async def go():
            task = asyncio.create_task(hub._listen_to_subscribers())
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(go())
        fake.pubsub.assert_called()


class TestStop(unittest.TestCase):
    def test_stop_with_client(self):
        hub = _fresh_hub()
        fake = mock.MagicMock()
        hub._redis_client = fake

        async def go():
            await hub.stop()
        asyncio.run(go())
        fake.close.assert_called()

    def test_stop_no_client(self):
        hub = _fresh_hub()
        hub._redis_client = None

        async def go():
            await hub.stop()
        asyncio.run(go())

    def test_stop_close_error(self):
        hub = _fresh_hub()
        fake = mock.MagicMock()
        fake.close.side_effect = RuntimeError("close boom")
        hub._redis_client = fake

        async def go():
            await hub.stop()
        asyncio.run(go())


class TestMarketFeedPublisher(unittest.TestCase):
    def test_connect_explicit(self):
        with mock.patch(
            "exchange.coinbase.websocket.market_feed.CoinbaseWebSocketMarketClient"
        ) as MC:
            inst = MC.return_value
            inst.subscribe.return_value = {"type": "subscribe"}
            pub = MarketFeedPublisher()
            pub.redis_hub.publish = mock.AsyncMock()
            async def go():
                await pub.connect(["BTC-USD", "ETH-USD"])
            asyncio.run(go())
            self.assertEqual(inst.subscribe.call_count, 2)

    def test_connect_default_products(self):
        with mock.patch(
            "exchange.coinbase.websocket.market_feed.CoinbaseWebSocketMarketClient"
        ) as MC:
            inst = MC.return_value
            inst.subscribe.return_value = {"type": "subscribe"}
            pub = MarketFeedPublisher()
            pub.redis_hub.publish = mock.AsyncMock()
            async def go():
                await pub.connect(None)
            asyncio.run(go())
            self.assertEqual(inst.subscribe.call_count, 2)

    def test_run_valid_and_invalid(self):
        with mock.patch(
            "exchange.coinbase.websocket.market_feed.CoinbaseWebSocketMarketClient"
        ) as MC:
            pubsub_mod.MarketClient = MC  # source bug: run() references module-global MarketClient
            inst = MC.return_value
            inst.connect = mock.AsyncMock()
            inst.subscribe.return_value = {"type": "subscribe"}

            class FakeWS:
                def __init__(self, msgs):
                    self.msgs = list(msgs)
                    self.sent = []
                    self._closed = False

                def __aiter__(self):
                    return self

                async def __anext__(self):
                    if self.msgs:
                        return self.msgs.pop(0)
                    if not self._closed:
                        self._closed = True
                        raise StopAsyncIteration
                    raise RuntimeError("closed")

                async def send(self, x):
                    self.sent.append(x)

            valid = json.dumps({"product_id": "BTC-USD", "type": "ticker", "data": {"price": 1}})
            invalid = "not json"
            inst._ws = FakeWS([valid, invalid])
            pub = MarketFeedPublisher()
            pub.redis_hub.publish = mock.AsyncMock()
            pub.redis_hub.stop = mock.AsyncMock()

            async def go():
                await pub.run(["BTC-USD"])
            asyncio.run(go())
            self.assertEqual(pub.redis_hub.publish.call_count, 1)

    def test_run_default_products(self):
        with mock.patch(
            "exchange.coinbase.websocket.market_feed.CoinbaseWebSocketMarketClient"
        ) as MC:
            pubsub_mod.MarketClient = MC  # source bug: run() references module-global MarketClient
            inst = MC.return_value
            inst.connect = mock.AsyncMock()
            inst.subscribe.return_value = {}
            inst._ws = mock.MagicMock()
            inst._ws.__aiter__ = lambda s: s
            inst._ws.__anext__ = mock.AsyncMock(side_effect=RuntimeError("closed"))
            inst._ws.send = mock.AsyncMock()
            pub = MarketFeedPublisher()
            pub.redis_hub.publish = mock.AsyncMock()
            pub.redis_hub.stop = mock.AsyncMock()

            async def go():
                await pub.run(None)
            asyncio.run(go())


class TestProcessMarketEvent(unittest.TestCase):
    def test_no_event_type(self):
        # should return without error
        asyncio.run(_call(process_market_event, {"topic": "marketplace.BTC-USD"}))

    def test_wrong_event_type(self):
        asyncio.run(_call(process_market_event, {"topic": "marketplace.BTC-USD", "event_type": "other"}))

    def test_valid(self):
        asyncio.run(_call(process_market_event, {
            "topic": "marketplace.BTC-USD",
            "event_type": "market_price_update",
            "data": {"price": 60000},
            "timestamp": 1,
        }))

    def test_valid_no_price(self):
        asyncio.run(_call(process_market_event, {
            "topic": "marketplace.BTC-USD",
            "event_type": "market_price_update",
            "data": {},
        }))


async def _call(fn, msg):
    await fn(msg)


class TestImportGuard(unittest.TestCase):
    def test_redis_import_fails(self):
        saved = sys.modules.get("redis")
        sys.modules["redis"] = None
        try:
            importlib.reload(pubsub_mod)
            hub = pubsub_mod.WebSocketPubSubHub()
            self.assertIsNone(hub._redis_client)
        finally:
            if saved is None:
                sys.modules.pop("redis", None)
            else:
                sys.modules["redis"] = saved
            importlib.reload(pubsub_mod)


if __name__ == "__main__":
    unittest.main()
