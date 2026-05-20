# Configuration Surface

## Environment variables (from `core/config/settings.py`)
- `APP_ENV`
- `TRADING_MODE`
- `COINBASE_API_KEY`
- `COINBASE_API_SECRET`
- `COINBASE_PASSPHRASE`
- `COINBASE_PORTFOLIO_IDS`
- `DATABASE_URL`
- `REDIS_URL`
- `REQUIRE_APPROVALS`
- `LIVE_TRADING_ENABLED`
- `LOW_LATENCY_MODE`
- `GPU_ENABLED`
- `QUEUE_MODEL`
- `CANARY_ROLLOUT_PCT`

## File-based configs
- Backtest demo: `configs/backtest_demo.yaml`
- Paper demo: `configs/paper_demo.yaml`
- Onchain MM templates: `configs/onchain_mm/*`
- Docker env file consumed by compose: `.env` (expected by `docker-compose.yml`).

## Feature flags / mode gates
- Trading mode enum with live/shadow/canary variants.
- `LIVE_TRADING_ENABLED` safety gate for any LIVE* modes.
- `REQUIRE_APPROVALS` gate used for risk-sensitive action policy.
- `QUEUE_MODEL` now constrained to `simple|priority|pro_rata`.

## Validation coverage and issues
- **Improved in this pass**:
  - strict boolean env parsing (rejects invalid tokens)
  - canary rollout range enforcement (0..100)
  - cross-field safety checks for LIVE/CANARY combinations
- Remaining gaps:
  - URL structure for `DATABASE_URL` / `REDIS_URL` is not parsed/validated.
  - Coinbase credentials are optional even in live modes.

## Dangerous defaults / drift risks
- Default DB/Redis URLs assume local services, which can silently connect to wrong host in shared dev environments.
- Live credentials defaults are blank, so live mode misconfiguration can fail late unless startup checks are expanded.
- Compose config references image build without Dockerfile; local-only workflows can drift from container workflows.
