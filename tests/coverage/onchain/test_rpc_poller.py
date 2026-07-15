from __future__ import annotations

from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock

from onchain.workers.rpc_poller import RpcPollerWorker


class TestRpcPollerWorker(TestCase):
    def test_init_defaults(self):
        w = RpcPollerWorker(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        self.assertIsNone(w._poller)
        self.assertIsNone(w._metadata_poller)
        self.assertIsNone(w._event_listener)
        self.assertEqual(w._event_handlers, [])

    def test_health_before_init(self):
        w = RpcPollerWorker(rpc_endpoints={}, db_session_factory=lambda: None)
        self.assertEqual(w.health, {"status": "stopped", "feed_health": {}})

    def test_fetch_token_metadata_not_initialized(self):
        w = RpcPollerWorker(rpc_endpoints={}, db_session_factory=lambda: None)
        with self.assertRaises(RuntimeError):
            import asyncio
            asyncio.run(w.fetch_token_metadata("0xTOK"))

    def test_stop_before_init(self):
        w = RpcPollerWorker(rpc_endpoints={}, db_session_factory=lambda: None)
        import asyncio
        asyncio.run(w.stop())  # no pollers initialized; both guards skip


class TestRpcPollerWorkerAsync(IsolatedAsyncioTestCase):
    async def test_initialize(self):
        w = RpcPollerWorker(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        await w.initialize()
        self.assertIsNotNone(w._poller)
        self.assertIsNotNone(w._metadata_poller)
        self.assertIsNotNone(w._event_listener)

    async def test_health_after_init(self):
        w = RpcPollerWorker(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        await w.initialize()
        health = w.health
        self.assertEqual(health["status"], "running")
        self.assertIn("status", health["feed_health"])

    async def test_start_polling(self):
        w = RpcPollerWorker(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        await w.initialize()
        w._event_listener.subscribe_to_events = AsyncMock()
        await w.start_polling()
        self.assertTrue(w._poller._running)
        w._event_listener.subscribe_to_events.assert_awaited_once()

    async def test_fetch_token_metadata_ok(self):
        w = RpcPollerWorker(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        await w.initialize()
        w._metadata_poller.fetch_token_metadata = AsyncMock(return_value={"address": "0xTOK"})
        meta = await w.fetch_token_metadata("0xTOK")
        self.assertEqual(meta, {"address": "0xTOK"})

    async def test_stop(self):
        w = RpcPollerWorker(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        await w.initialize()
        w._poller.close = AsyncMock()
        w._event_listener.close = AsyncMock()
        await w.stop()
        w._poller.close.assert_awaited_once()
        w._event_listener.close.assert_awaited_once()
