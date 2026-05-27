# Onchain Ingestion Runtime — Safety & Architecture Guide

## Overview

The onchain ingestion runtime provides safe, credential-gated access to Ethereum/Base 
onchain data for pool discovery, token metadata, and event tracking. All operations run in 
paper/shadow mode by default, requiring explicit operator approval for live execution paths.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    RPC Poller Worker                         │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────┐ │
│  │ Pool Data Poller │  │ Token Metadata   │  │ Event    │ │
│  │ (periodic)       │◄─►│ Fetcher         │  │ Listener │ │
│  └──────────────────┘  └──────────────────┘  └──────────┘ │
├─────────────────────────────────────────────────────────────┤
│                      Database Layer                          │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ • Pool snapshots & health records                   │    │
│  │ • Token metadata cache                              │    │
│  │ • Event logs & observations                         │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

## Safety Gates

### Paper Mode (Default)
- ✅ Fetch pool data without signing
- ✅ Read token metadata from chain
- ✅ Track swap/transfer events
- ❌ NO transaction signing or broadcast

### Shadow Mode (Requires Approval)
- ✅ Generate approval packets with gas/slippage estimates
- ✅ Compare RPC state against local DB
- ❌ NO live execution unless `LIVE_TRADING_ENABLED=true`

### Live Mode (Explicit Operator Approval Required)
- Requires `ONCHAIN_LIVE_ENABLED=true` in `.env`
- Requires signed operator approval
- Records all attempts in audit logs

## Usage Patterns

### Fetch Token Metadata

```python
from onchain.workers.rpc_poller import RpcPollerWorker

# Initialize worker
worker = RpcPollerWorker(
    rpc_endpoints={
        "base": "https://mainnet.base.org",
        "ethereum": "https://eth-mainnet.alchemyapi.io/v2/...",
    },
    db_session_factory=get_db_session,
)

# Fetch token metadata
metadata = await worker.fetch_token_metadata("0x...")
print(metadata["symbol"])  # e.g., "ETH"
print(metadata["decimals"])  # e.g., 18
```

### Subscribe to Events

```python
async def on_swap_event(event: dict):
    """Handle swap event."""
    print(f"Swap event: {event}")
    await persist_to_db(event)

worker.on_event("swaps", on_swap_event)
await worker.subscribe_to_events(network="base")
```

### Manual Poll Trigger

```bash
curl -X POST http://localhost:8000/onchain/poll/base
```

## Health Monitoring

The `feed_health` endpoint exposes:
- Current status (online/stopped)
- Last poll timestamp
- Pending pool requests count

Check via API:
```bash
curl http://localhost:8000/onchain/health
```

## Environment Configuration

`.env` file:

```bash
# Paper mode (safe default)
ONCHAIN_MODE=paper

# For shadow mode testing (still no signing)
ONCHAIN_LIVE_ENABLED=false

# RPC endpoints (example for Base network)
BASE_RPC=https://mainnet.base.org
ETHEREUM_RPC=https://eth-mainnet.alchemyapi.io/v2/YOUR_API_KEY
```

## Database Schema

### `token_metadata` table
| Column | Type | Purpose |
|--------|------|---------|
| address | String(42) | Token contract address (EIP-55 encoded) |
| symbol | String(10) | Token symbol (e.g., "ETH") |
| name | String(64) | Token name (e.g., "Ethereum") |
| decimals | Integer | Decimal places (typically 18) |
| network | String(20) | Network identifier (base/ethereum) |
| timestamp | DateTime | Metadata fetch time |

### `pool_snapshots` table
| Column | Type | Purpose |
|--------|------|---------|
| id | Integer | Primary key |
| pool_address | String(42) | Pool contract address |
| network | String(20) | Network identifier |
| reserve_0 | Numeric(20,8) | First token reserve |
| reserve_1 | Numeric(20,8) | Second token reserve |
| timestamp | DateTime | Snapshot time |

### `events` table
| Column | Type | Purpose |
|--------|------|---------|
| id | Integer | Primary key |
| event_type | String(32) | e.g., "Swap", "Transfer" |
| block_number | Integer | Ethereum block number |
| timestamp | DateTime | Event timestamp (UTC) |
| data | JSON | Raw event data |

## Security Considerations

### 1. RPC Endpoint Rotation
- Rotate endpoints periodically
- Don't store full API keys in source control
- Use environment variables only

### 2. Rate Limiting
- Implement token bucket rate limiting for RPC calls
- Respect RPC provider rate limits (e.g., 30/minute for free tier)

### 3. Private Key Handling
- Never expose private keys in logs/metrics
- Use hardware wallets or KMS for production
- Keep `ONCHAIN_MODE=paper` for development/testing

## Troubleshooting

### Feed Health Degradation

If `feed_health` shows failures:

1. Check RPC endpoint URLs are valid
2. Verify API keys have sufficient quota
3. Look at application logs for specific error messages
4. Test connectivity with curl directly to RPC endpoint

### Metadata Cache Issues

```python
worker.clear_cache()  # Clear stale metadata
metadata = await worker.fetch_token_metadata(address)  # Refetch
```

### Event Processing Errors

Check `onchain.pollers.event_listener` logs for:
- Failed event deserialization errors
- Invalid block number ranges
- Contract address mismatches
