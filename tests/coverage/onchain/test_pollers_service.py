from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest import TestCase, IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from onchain.pollers.service import OnchainPoller


class TestOnchainPoller(TestCase):
    def test_init_defaults(self):
        p = OnchainPoller(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        self.assertFalse(p._running)
        self.assertEqual(p._event_handlers, [])
        self.assertEqual(p._poll_tasks, {})
        self.assertEqual(p._health_records, [])
        self.assertEqual(p._last_poll_times, {})

    def test_register_handler(self):
        p = OnchainPoller(rpc_endpoints={}, db_session_factory=lambda: None)
        h = MagicMock()
        asyncio.run(p.register_handler("swaps", h))
        self.assertEqual(p._event_handlers[-1]["channel"], "swaps")

    def test_feed_health_stopped(self):
        p = OnchainPoller(rpc_endpoints={}, db_session_factory=lambda: None)
        health = p.feed_health
        self.assertEqual(health["status"], "stopped")
        self.assertIsNone(health["last_poll"])
        self.assertEqual(health["pending_pools"], 0)

    def test_feed_health_with_poll(self):
        p = OnchainPoller(rpc_endpoints={}, db_session_factory=lambda: None)
        p._last_poll_times["base"] = datetime.now(timezone.utc)
        health = p.feed_health
        self.assertEqual(health["status"], "stopped")
        self.assertIsNotNone(health["last_poll"])

    def test_record_health_failure(self):
        p = OnchainPoller(rpc_endpoints={}, db_session_factory=lambda: None)
        asyncio.run(p._record_health_failure("base", "boom"))
        self.assertEqual(len(p._health_records), 1)
        self.assertEqual(p._health_records[0]["status"], "failed")

    def test_record_health_success_no_prior(self):
        p = OnchainPoller(rpc_endpoints={}, db_session_factory=lambda: None)
        asyncio.run(p._record_health_success("base", datetime.now(timezone.utc), 12.0))
        self.assertEqual(len(p._health_records), 1)
        self.assertEqual(p._health_records[0]["status"], "healthy")
        self.assertEqual(p._health_records[0]["latency_ms"], 12.0)

    def test_record_health_success_removes_failure(self):
        p = OnchainPoller(rpc_endpoints={}, db_session_factory=lambda: None)
        asyncio.run(p._record_health_failure("base", "boom"))
        asyncio.run(p._record_health_success("base", datetime.now(timezone.utc), 5.0))
        self.assertEqual(len(p._health_records), 1)
        self.assertEqual(p._health_records[0]["status"], "healthy")

    def test_close_no_tasks(self):
        p = OnchainPoller(rpc_endpoints={}, db_session_factory=lambda: None)
        asyncio.run(p.close())
        self.assertFalse(p._running)

    def test_close_cancels_tasks(self):
        p = OnchainPoller(rpc_endpoints={}, db_session_factory=lambda: None)
        task = MagicMock()
        p._poll_tasks["t1"] = task
        asyncio.run(p.close())
        task.cancel.assert_called_once()


def _make_aiohttp(status=200, text='{"jsonrpc":"2.0","result":{}}'):
    client_session = MagicMock()
    http_cm = MagicMock()
    get_cm = MagicMock()
    resp = MagicMock()
    resp.status = status
    resp.text = AsyncMock(return_value=text)
    http_cm.get.return_value = get_cm
    get_cm.__aenter__.return_value = resp
    get_cm.__aexit__.return_value = False
    client_session.return_value.__aenter__.return_value = http_cm
    client_session.return_value.__aexit__.return_value = False
    return client_session


class TestOnchainPollerAsync(IsolatedAsyncioTestCase):
    async def test_poll_pools_success(self):
        p = OnchainPoller(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        p._running = True
        client = _make_aiohttp()

        async def fake_sleep(_):
            p._running = False

        with patch("onchain.pollers.service.aiohttp.ClientSession", client), _patch_sleep(fake_sleep):
            await p.poll_pools(network="base", pools=["0xpool"])
        self.assertIn("base", p._last_poll_times)

    async def test_poll_pools_rpc_missing(self):
        p = OnchainPoller(rpc_endpoints={}, db_session_factory=lambda: None)
        p._running = True

        async def fake_sleep(_):
            p._running = False

        with _patch_sleep(fake_sleep):
            await p.poll_pools(network="base")
        # exception is caught and logged by the poll loop; loop exits cleanly
        self.assertFalse(p._running)

    async def test_fetch_pools_aiohttp_error_records_failure(self):
        p = OnchainPoller(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        client = MagicMock(side_effect=RuntimeError("conn refused"))
        with patch("onchain.pollers.service.aiohttp.ClientSession", client):
            with self.assertRaises(RuntimeError):
                await p._fetch_pools("base", ["0xpool"])
        self.assertTrue(any(r["status"] == "failed" for r in p._health_records))

    async def test_fetch_pools_bad_status(self):
        p = OnchainPoller(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        client = _make_aiohttp(status=500, text="err")
        with patch("onchain.pollers.service.aiohttp.ClientSession", client):
            # Should not raise (status != 200 just warns)
            await p._fetch_pools("base", ["0xpool"])

    async def test_fetch_pools_bad_json(self):
        p = OnchainPoller(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        client = _make_aiohttp(status=200, text="not-json")
        with patch("onchain.pollers.service.aiohttp.ClientSession", client):
            with self.assertRaises(ValueError):
                await p._fetch_pools("base", ["0xpool"])

    async def test_fetch_pools_rpc_absent(self):
        p = OnchainPoller(rpc_endpoints={}, db_session_factory=lambda: None)
        with self.assertRaises(ValueError):
            await p._fetch_pools("base", None)

    async def test_process_pool_data_ok(self):
        p = OnchainPoller(rpc_endpoints={}, db_session_factory=lambda: None)
        await p._process_pool_data('{"jsonrpc":"2.0","result":{}}', MagicMock())
        self.assertTrue(any(r["status"] == "healthy" for r in p._health_records))


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
