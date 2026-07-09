# Prediction-Market Venue Provisioning & Validation

How to provision, configure, and validate the trading venues used by the
prediction-market subsystem (Kalshi + Polymarket) plus the Coinbase spot feed.

> **Jurisdiction / legal notice.** Provisioning a wallet works anywhere, but
> **Polymarket order execution is geoblocked by region** under Polymarket's
> CFTC settlement (e.g. US IPs are blocked). This repository does **not**
> circumvent that restriction. The operator is solely responsible for running
> execution only from a permitted jurisdiction and for complying with all
> applicable laws and each venue's Terms of Service. From a blocked region,
> Polymarket remains usable **read-only** (market data + arbitrage detection);
> execution stays disabled.

---

## 1. Configuration (`.env`)

| Key | Purpose |
|-----|---------|
| `KALSHI_API_KEY_ID` | Kalshi API key id (RSA-PSS auth) |
| `KALSHI_PRIVATE_KEY_PATH` | Path to the Kalshi RSA private key `.txt`/`.pem` |
| `KALSHI_API_BASE_URL` | `https://api.elections.kalshi.com/trade-api/v2` |
| `POLYMARKET_PRIVATE_KEY` | Polygon EOA private key (`0x…64 hex`) that signs orders & holds USDC.e |
| `POLYMARKET_FUNDER` | USDC-holding address; for an EOA this equals the signer address |
| `POLYMARKET_SIGNATURE_TYPE` | `0` = EOA (default), `1` = email/magic proxy, `2` = Gnosis safe |
| `POLYMARKET_CLOB_HOST` | `https://clob.polymarket.com` |
| `POLYMARKET_CHAIN_ID` | `137` (Polygon mainnet) |

Live execution is gated separately by `ARBITRAGE_LIVE_ENABLED` (default `false`).

---

## 2. Kalshi

Kalshi uses RSA-PSS request signing. Provide the API key id and the private-key
file path in `.env`. No on-chain steps are required. Validate with the script in
§5 — it checks auth, balance, and the read endpoints (`get_positions`,
`get_orders`, `get_fills`).

---

## 3. Polymarket wallet provisioning

All steps are exposed as a reusable module (`event_markets/provisioning.py`)
with a CLI. Polymarket collateral is **USDC.e (bridged USDC,
`0x2791…`)** — *not* native USDC (`0x3c49…`). Funding with native USDC requires
a one-time swap (step 3.3).

### 3.1 Generate a wallet
```bash
python -m event_markets.provisioning generate --keyfile /secure/path/pm_wallet.txt
```
Writes a `0600` key file containing the address + private key. **Back it up** —
losing the key means losing any funds sent to the address. Put the private key
into `.env` as `POLYMARKET_PRIVATE_KEY` and set `POLYMARKET_FUNDER` to the same
address (for an EOA).

### 3.2 Fund the wallet
Send to the address on **Polygon (chainId 137)**:
- **USDC.e** (`0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174`) — trading collateral
- a little **MATIC/POL** — gas for the allowance/approval transactions

If you sent **native USDC** instead, do the swap in 3.3.

### 3.3 Swap native USDC → USDC.e (only if needed)
Uses the Uniswap v3 0.01% pool (deep USDC/USDC.e liquidity), 0.5% slippage guard:
```bash
python -m event_markets.provisioning swap --yes            # swaps entire native-USDC balance
python -m event_markets.provisioning swap --amount 10 --yes # swap a specific amount
```

### 3.4 Set on-chain allowances
Approves USDC.e (ERC20) + Conditional Tokens (ERC1155) for Polymarket's three
exchange operators. Idempotent — re-running skips already-approved spenders:
```bash
python -m event_markets.provisioning allowances --yes
```

### 3.5 Refresh Polymarket's cached view
After funding/approvals, ask the CLOB to re-scan the wallet:
```bash
python -m event_markets.provisioning refresh
```
> This authenticated CLOB call is region-restricted; from a geoblocked region it
> will fail (expected). The `PolymarketExecutionClient` sends browser-style
> headers to avoid an unrelated Cloudflare `1010` block on this endpoint.

### 3.6 Check status any time
```bash
python -m event_markets.provisioning status
```
Reports MATIC / native-USDC / USDC.e balances and per-operator allowance state.

---

## 4. Contract reference (Polygon mainnet)

| Contract | Address |
|----------|---------|
| USDC.e (collateral) | `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174` |
| Native USDC | `0x3c499c542cEF5E3811e1192ce70d8cc03d5c3359` |
| Conditional Tokens (CTF, ERC1155) | `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` |
| Uniswap v3 SwapRouter02 | `0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45` |
| Polymarket operator — CTF Exchange | `0xE111180000d2663C0091e4f400237545B87B996B` |
| Polymarket operator — Neg Risk Adapter | `0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296` |
| Polymarket operator — Neg Risk CTF Exchange | `0xe2222d279d744050d28e00520010520000310F59` |

The operator set is exactly the spenders reported by the CLOB
`balance-allowance` API and is the authoritative list to approve.

---

## 5. Validate everything from the current host
```bash
python scripts/validate_venue_config.py
```
Read-only (no trades, no on-chain writes). Checks `.env` presence, Kalshi
auth/balance/read endpoints, Coinbase spot, the Polymarket wallet's on-chain
balances + allowances, and the Polymarket trading-region status.

Exit code `0` if no hard failures. A `WARN` on **Polymarket trading region**
means execution is not permitted from the current IP — provisioning is still
valid; only order placement is blocked.
