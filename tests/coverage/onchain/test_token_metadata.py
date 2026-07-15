from __future__ import annotations

from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, patch

from onchain.pollers.token_metadata import TokenMetadataPoller


class TestTokenMetadataPoller(TestCase):
    def test_init(self):
        p = TokenMetadataPoller(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        self.assertEqual(p._cache_ttl_seconds, 86400)
        self.assertEqual(p._cached_metadata, {})

    def test_get_all_cached(self):
        import asyncio
        p = TokenMetadataPoller(rpc_endpoints={}, db_session_factory=lambda: None)
        p._cached_metadata["0xA"] = {"address": "0xA"}
        self.assertEqual(asyncio.run(p.get_all_cached()), [{"address": "0xA"}])

    def test_clear_cache(self):
        import asyncio
        p = TokenMetadataPoller(rpc_endpoints={}, db_session_factory=lambda: None)
        p._cached_metadata["0xA"] = {"address": "0xA"}
        asyncio.run(p.clear_cache())
        self.assertEqual(p._cached_metadata, {})


class TestTokenMetadataPollerAsync(IsolatedAsyncioTestCase):
    async def test_fetch_token_metadata_ok(self):
        p = TokenMetadataPoller(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        with patch.object(p, "_call_contract", new=AsyncMock(return_value={"name": "T", "symbol": "T", "decimals": 18})):
            meta = await p.fetch_token_metadata("0xTOK")
        self.assertIsNotNone(meta)
        self.assertEqual(meta["address"], "0xTOK")
        self.assertEqual(meta["decimals"], 18)
        self.assertIn("0xTOK", p._cached_metadata)

    async def test_fetch_token_metadata_no_result(self):
        p = TokenMetadataPoller(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        with patch.object(p, "_call_contract", new=AsyncMock(return_value=None)):
            meta = await p.fetch_token_metadata("0xTOK")
        self.assertIsNone(meta)

    async def test_fetch_token_metadata_exception(self):
        p = TokenMetadataPoller(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        with patch.object(p, "_fetch_from_chain", new=AsyncMock(side_effect=RuntimeError("boom"))):
            meta = await p.fetch_token_metadata("0xTOK")
        self.assertIsNone(meta)

    async def test_fetch_from_chain_dict(self):
        p = TokenMetadataPoller(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        with patch.object(p, "_call_contract", new=AsyncMock(return_value={"0x12345678901234567890": 18})):
            meta = await p._fetch_from_chain("0xTOK", "base")
        self.assertEqual(meta["symbol"], 18)
        self.assertEqual(meta["decimals"], 18)

    async def test_fetch_from_chain_non_dict(self):
        p = TokenMetadataPoller(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        with patch.object(p, "_call_contract", new=AsyncMock(return_value="notadict")):
            meta = await p._fetch_from_chain("0xTOK", "base")
        self.assertIsNone(meta)

    async def test_call_contract_passthrough(self):
        p = TokenMetadataPoller(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        self.assertIsNone(await p._call_contract("0xTOK", "base", []))

    async def test_get_cached_metadata_hit(self):
        p = TokenMetadataPoller(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        p._cached_metadata["0xTOK_base"] = {"address": "0xTOK"}
        with patch.object(p, "fetch_token_metadata", new=AsyncMock()) as fetch:
            meta = await p.get_cached_metadata("0xTOK")
        self.assertEqual(meta, {"address": "0xTOK"})
        fetch.assert_not_called()

    async def test_get_cached_metadata_miss(self):
        p = TokenMetadataPoller(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        with patch.object(p, "fetch_token_metadata", new=AsyncMock(side_effect=lambda a: {"address": a})) as fetch:
            meta = await p.get_cached_metadata("0xMISSING")
        self.assertEqual(meta, {"address": "0xMISSING"})
        fetch.assert_called_once()

    async def test_get_cached_metadata_force_refresh(self):
        p = TokenMetadataPoller(rpc_endpoints={"base": "http://x"}, db_session_factory=lambda: None)
        p._cached_metadata["0xTOK_base"] = {"address": "0xTOK"}
        with patch.object(p, "fetch_token_metadata", new=AsyncMock(return_value={"address": "0xNEW"})) as fetch:
            meta = await p.get_cached_metadata("0xTOK", force_refresh=True)
        self.assertEqual(meta, {"address": "0xNEW"})
        fetch.assert_called_once()
