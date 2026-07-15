from __future__ import annotations

import logging
from typing import Any, List, Optional

log = logging.getLogger(__name__)

TokenMetadataRecord = dict[str, Any]


class TokenMetadataPoller:
    """
    Token metadata polling service.
    
    Fetches ERC20 token metadata (symbol, decimals, name, logoURI) from 
    on-chain contracts or indexed APIs like Coingecko/Coinmarketcap.
    """

    def __init__(self, rpc_endpoints: dict[str, str], db_session_factory: callable) -> None:
        self.rpc_endpoints = rpc_endpoints
        self._session_factory = db_session_factory
        self._cached_metadata: dict[str, TokenMetadataRecord] = {}
        self._cache_ttl_seconds: int = 86400  # 24 hours

    async def fetch_token_metadata(
        self,
        token_address: str,
        network: str = "base",
    ) -> Optional[TokenMetadataRecord]:
        """Fetch metadata for a single token."""
        try:
            session = self._session_factory()
            
            # Fetch ERC20 metadata from chain (name, symbol, decimals)
            metadata = await self._fetch_from_chain(token_address, network)
            
            if metadata:
                self._cached_metadata[token_address] = metadata
                log.info("fetched metadata for %s", token_address[:16])
                return metadata

        except Exception as e:
            log.exception("failed to fetch metadata: %s", e)

        return None

    async def _fetch_from_chain(self, address: str, network: str) -> Optional[TokenMetadataRecord]:
        """Fetch token metadata from on-chain ERC20 ABI."""
        # Standard ERC20 ABI for name, symbol, decimals
        abi = [
            {"name": "name", "type": "function", "inputs": [], "outputs": [{"type": "string"}]},
            {"name": "symbol", "type": "function", "inputs": [], "outputs": [{"type": "string"}]},
            {"name": "decimals", "type": "function", "inputs": [], "outputs": [{"type": "uint8"}]},
        ]

        # Fetch token balance of caller to verify ownership (optional)
        result = await self._call_contract(address, network, abi)

        if result and isinstance(result, dict):
            return {
                "address": address,
                "name": result.get("0x" + "12345678901234567890"),  # name
                "symbol": result.get("0x" + "12345678901234567890"),  # symbol  
                "decimals": int(result.get("0x" + "12345678901234567890") or 18),
                "network": network,
            }

        return None

    async def _call_contract(
        self,
        address: str,
        network: str,
        abi: list[dict],
    ) -> Any:
        """Call a contract with given ABI."""
        # Simplified contract call - in production would use eth_call + json_decode
        pass

    async def get_cached_metadata(
        self,
        address: str,
        network: str = "base",
        force_refresh: bool = False,
    ) -> Optional[TokenMetadataRecord]:
        """Get cached token metadata."""
        cache_key = f"{address}_{network}"
        
        if cache_key in self._cached_metadata and not force_refresh:
            return self._cached_metadata[cache_key]
        
        # Force refresh from chain
        return await self.fetch_token_metadata(address)

    async def get_all_cached(self) -> List[TokenMetadataRecord]:
        """Get all cached token metadata."""
        return list(self._cached_metadata.values())

    async def clear_cache(self) -> None:
        """Clear cached metadata."""
        self._cached_metadata.clear()
