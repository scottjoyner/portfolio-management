# API Compatibility Matrix (Kalshi + Polymarket)

Last reviewed: 2026-05-21.
Official references used:
- Kalshi docs: https://docs.kalshi.com/getting_started
- Polymarket docs: https://docs.polymarket.com/developers/CLOB/clients/methods-l2
- Polymarket CLOB auth/v2 migration docs: https://docs.polymarket.com/v2-migration

| Feature | Venue | Repo implementation file | Current docs assumption | Status | Notes | Test coverage |
|---|---|---|---|---|---|---|
| Environment base URLs (demo/prod) | Kalshi | `packages/kalshi/src/client.ts` | Demo + prod REST/WS endpoints differ by env | mocked | Adapter exists but endpoint map is still stub-level | `tests/safety.test.mjs` (indirect) |
| API key + private key signing | Kalshi | `packages/kalshi/src/client.ts` | Signed headers required per authenticated call | needs-manual-review | Contract kept isolated; concrete signer integration pending | none |
| Public market discovery | Kalshi | `packages/kalshi/src/client.ts` | Public discovery endpoints available | mocked | Safe no-op stub only | none |
| Orderbook schema normalization | Kalshi | `packages/core/src/arbitrage.mjs` | Fixed-point style values must be normalized | mocked | Internal micros model used | `tests/arbitrage.test.mjs` |
| Authenticated order writes | Kalshi | `packages/execution/src/engine.mjs` | No blind retries on ambiguous writes | verified | Explicit reconcile-before-retry policy | `tests/safety.test.mjs` |
| CLOB V2 client/auth model | Polymarket | `packages/polymarket/src/client.ts` | L1/L2 auth split remains, L2 headers required for trading methods | blocked-by-credentials | Kept behind adapter boundary; no live writes in current repo | `tests/safety.test.mjs` (gates) |
| Gamma/public discovery | Polymarket | `packages/polymarket/src/client.ts` | Public market data readable without trading keys | mocked | Mode policy supports readonly without private key | mode tests |
| Limit-order trading semantics | Polymarket | `packages/execution/src/engine.mjs` | CLOB order behavior requires explicit order params; never blind market orders | mocked | Execution engine is gate-only in current stage | `tests/safety.test.mjs` |
| WebSocket market/user channels | Polymarket | `packages/polymarket/src/client.ts` | Separate market and user streams | needs-manual-review | Not implemented yet; documented gap | none |
| Rate limits/backoff | Both | adapter files | Read retries only; writes must reconcile | verified (policy) | Retry policy codified by ambiguous-write guard | `tests/safety.test.mjs` |

## Uncertainties intentionally isolated
- Kalshi exact payload fields for current order/fill schemas were not credential-validated in this environment.
- Polymarket V2 order payload details and signatures are intentionally left behind adapter interfaces until authenticated staging validation.
