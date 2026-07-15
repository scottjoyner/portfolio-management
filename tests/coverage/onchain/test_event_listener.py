from __future__ import annotations

import asyncio
from unittest import TestCase, IsolatedAsyncioTestCase
from unittest.mock import MagicMock

from onchain.pollers.event_listener import EventListener


class TestEventListener(TestCase):
    def test_init_defaults(self):
        el = EventListener(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        self.assertFalse(el._running)
        self.assertEqual(el._event_handlers, [])
        self.assertEqual(el._log_filters, {})

    def test_init_with_handlers(self):
        h = MagicMock()
        el = EventListener(rpc_endpoints={}, db_session_factory=lambda: None, event_handlers=[h])
        self.assertEqual(el._event_handlers, [h])

    def test_feed_health(self):
        el = EventListener(rpc_endpoints={}, db_session_factory=lambda: None)
        self.assertEqual(el.feed_health, {"status": "stopped"})
        el._running = True
        self.assertEqual(el.feed_health, {"status": "online"})

    def test_on_event(self):
        el = EventListener(rpc_endpoints={}, db_session_factory=lambda: None)
        handler = MagicMock()
        el.on_event("swaps", handler)
        self.assertEqual(len(el._event_handlers), 1)
        self.assertEqual(el._event_handlers[0]["channel"], "swaps")
        self.assertIs(el._event_handlers[0]["handler"], handler)

    def test_record_health_success(self):
        el = EventListener(rpc_endpoints={}, db_session_factory=lambda: None)
        asyncio.run(el._record_health_success("base"))
        self.assertEqual(el._log_filters["base"], [])

    def test_close(self):
        el = EventListener(rpc_endpoints={}, db_session_factory=lambda: None)
        el._running = True
        asyncio.run(el.close())
        self.assertFalse(el._running)


class TestEventListenerAsync(IsolatedAsyncioTestCase):
    async def test_subscribe_success(self):
        el = EventListener(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        el._running = True

        async def fake_sleep(_):
            el._running = False

        with _patch_sleep(fake_sleep):
            await el.subscribe_to_events(network="base")
        self.assertIn("base", el._log_filters)

    async def test_subscribe_fetch_raises(self):
        el = EventListener(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        el._running = True

        async def boom(network, topics):
            raise RuntimeError("fetch failed")

        async def fake_sleep(_):
            el._running = False

        el._fetch_latest_events = boom
        with _patch_sleep(fake_sleep):
            await el.subscribe_to_events(network="base")

    async def test_fetch_latest_events_no_rpc(self):
        el = EventListener(rpc_endpoints={}, db_session_factory=lambda: None)
        with self.assertRaises(ValueError):
            await el._fetch_latest_events("base", None)

    async def test_fetch_latest_events_ok(self):
        el = EventListener(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        await el._fetch_latest_events("base", ["0xt"])
        self.assertEqual(el._log_filters["base"], [])


class _patch_sleep:
    def __init__(self, fake):
        self.fake = fake
        self.orig = None

    def __enter__(self):
        self.orig = asyncio.sleep
        asyncio.sleep = self.fake
        return self

    def __exit__(self, *a):
        asyncio.sleep = self.orig
