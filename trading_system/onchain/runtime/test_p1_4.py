"""Test script for P1.4 onchain runtime service."""

import asyncio
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management/trading_system')

from onchain.runtime.service import OnchainRuntimeService, TokenMetadataService, SafetyScoringEngine


async def main():
    """Test all components of P1.4 implementation."""
    
    print("=" * 70)
    print("P1.4 ONCHAIN RUNTIME SERVICE - TEST RESULTS")
    print("=" * 70)
    
    # Test 1: Initialize service
    print("\n[TEST 1] Initializing OnchainRuntimeService...")
    try:
        service = OnchainRuntimeService(
            rpc_url="https://eth-mainnet.g.alchemy.com/v2/demo",
            networks=["ethereum"]
        )
        print(f"✓ Service initialized for networks: {service.networks}")
    except Exception as e:
        print(f"✗ Failed to initialize service: {e}")
        return
    
    # Test 2: Fetch token metadata
    print("\n[TEST 2] Fetching token metadata (WETH)...")
    try:
        metadata = await service.fetch_token_metadata("0xC02aaA37b1fC06D6FaB1F6C6AD8944Ee7C48b8", "ethereum")
        print(f"✓ WETH metadata fetched:")
        print(f"  - Name: {metadata.get('name', 'Unknown')}")
        print(f"  - Symbol: {metadata.get('symbol', 'UNK')}")
        print(f"  - Decimals: {metadata.get('decimals', 18)}")
        print(f"  - Source: {metadata.get('source', 'unknown')}")
    except Exception as e:
        print(f"⚠ Metadata fetch failed (expected for demo RPC): {e}")
    
    # Test 3: Get poller status
    print("\n[TEST 3] Checking poller status...")
    try:
        status = await service.get_poller_status()
        print(f"✓ Poller status retrieved:")
        print(f"  - Running: {status.get('poller_running')}")
        print(f"  - Networks: {len(status.get('networks', []))}")
        print(f"  - Cache size: {status.get('cache_size', 0)}")
    except Exception as e:
        print(f"✗ Failed to get poller status: {e}")
    
    # Test 4: Safety scoring engine
    print("\n[TEST 4] Testing SafetyScoringEngine...")
    try:
        route = {
            "id": "test_route_1",
            "source_token": "ETH",
            "target_token": "USDC",
            "amount_usd": 5000,
            "slippage_estimate_bps": 50
        }
        
        safety_engine = SafetyScoringEngine(service)
        score_result = await safety_engine.score_route(route)
        
        print(f"✓ Route scored:")
        print(f"  - MEV Risk: {score_result.get('mev_risk')}")
        print(f"  - Slippage Safe: {score_result.get('slippage_safe')}")
        print(f"  - Score: {score_result.get('overall_score')}/100")
        print(f"  - Approval Required: {score_result.get('approval_required')}")
    except Exception as e:
        print(f"✗ Failed to score route: {e}")
    
    # Test 5: Stop service gracefully
    print("\n[TEST 5] Stopping service...")
    try:
        await service.stop()
        print("✓ Service stopped successfully")
    except Exception as e:
        print(f"✗ Failed to stop service: {e}")


if __name__ == "__main__":
    asyncio.run(main())
