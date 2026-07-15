import asyncio
import unittest
from unittest import mock

from core.events.ws_hub import PubSubHub, hub


class FakeWS:
    def __init__(self, async_send=True, raise_on_send=False):
        self.sent = []
        self.raise_on_send = raise_on_send
        self.async_send = async_send

    async def send_text(self, payload):
        if self.raise_on_send:
            raise RuntimeError("send boom")
        self.sent.append(payload)


class SyncRaisingWS:
    def send_text(self, payload):
        raise RuntimeError("sync send boom")


class TestPubSubHub(unittest.TestCase):
    def test_subscribe_and_publish(self):
        h = PubSubHub()
        ws = FakeWS()
        asyncio.run(h.subscribe("ch", ws))
        self.assertIn(ws, h._channels["ch"])
        asyncio.run(h.publish("ch", {"a": 1}))
        self.assertEqual(ws.sent, ['{"a": 1}'])

    def test_publish_no_subscribers(self):
        h = PubSubHub()
        asyncio.run(h.publish("empty", {"a": 1}))

    def test_publish_send_error_discards(self):
        h = PubSubHub()
        ws = FakeWS(raise_on_send=True)
        asyncio.run(h.subscribe("ch", ws))
        asyncio.run(h.publish("ch", {"a": 1}))
        # ws should have been discarded after error
        self.assertNotIn(ws, h._channels["ch"])

    def test_publish_wrong_channel_isolated(self):
        h = PubSubHub()
        ws1 = FakeWS()
        ws2 = FakeWS()
        asyncio.run(h.subscribe("ch1", ws1))
        asyncio.run(h.subscribe("ch2", ws2))
        asyncio.run(h.publish("ch1", {"x": 1}))
        self.assertEqual(ws1.sent, ['{"x": 1}'])
        self.assertEqual(ws2.sent, [])

    def test_unsubscribe(self):
        h = PubSubHub()
        ws = FakeWS()
        asyncio.run(h.subscribe("ch", ws))
        asyncio.run(h.unsubscribe("ch", ws))
        self.assertNotIn(ws, h._channels.get("ch", set()))

    def test_publish_sync_no_running_loop(self):
        h = PubSubHub()
        ws = FakeWS()
        h._channels.setdefault("ch", set()).add(ws)
        # called from sync context -> RuntimeError -> return early
        h.publish_sync("ch", {"a": 1})
        self.assertEqual(ws.sent, [])

    def test_publish_sync_no_subscribers_running_loop(self):
        h = PubSubHub()

        async def go():
            h.publish_sync("empty", {"a": 1})
        asyncio.run(go())

    def test_publish_sync_running_loop_ensure_future(self):
        h = PubSubHub()
        ws = FakeWS()
        h._channels.setdefault("ch", set()).add(ws)

        async def go():
            h.publish_sync("ch", {"a": 1})
            await asyncio.sleep(0)  # let scheduled task run
        asyncio.run(go())
        self.assertEqual(ws.sent, ['{"a": 1}'])

    def test_publish_sync_not_running_loop_run_until_complete(self):
        h = PubSubHub()

        class SyncWS:
            def send_text(self, payload):
                return None

        ws = SyncWS()
        h._channels.setdefault("ch", set()).add(ws)
        fake_loop = mock.MagicMock()
        fake_loop.is_running.return_value = False
        fake_loop.run_until_complete = mock.MagicMock()

        async def go():
            with mock.patch.object(asyncio, "get_running_loop", return_value=fake_loop):
                h.publish_sync("ch", {"a": 1})
        asyncio.run(go())
        fake_loop.run_until_complete.assert_called_once()

    def test_publish_sync_send_error_discards(self):
        h = PubSubHub()
        ws = SyncRaisingWS()
        h._channels.setdefault("ch", set()).add(ws)

        async def go():
            h.publish_sync("ch", {"a": 1})
            await asyncio.sleep(0)
        asyncio.run(go())
        # ws must be discarded after synchronous send failure
        self.assertNotIn(ws, h._channels["ch"])

    def test_module_singleton(self):
        self.assertIsInstance(hub, PubSubHub)


if __name__ == "__main__":
    unittest.main()
