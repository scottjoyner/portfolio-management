# Remaining Risks

## Known unresolved issues
1. **Migration path incomplete**: Alembic revision exists but `alembic/env.py` is missing.
2. **Container path incomplete**: `docker-compose.yml` references image build without Dockerfile.
3. **Ops state durability**: operator previews/orders/fills are in-memory and lost on restart.
4. **Readiness depth**: `/health` is liveness-only and does not validate dependencies.

## Blocked items
- Full DB migration validation is blocked by incomplete Alembic wiring.
- Container validation is blocked by missing Dockerfile.

## Deferred items
- Persistence-backed ops read models.
- Contract tests between API response models and UI consumers.
- CI workflow implementation (`.github/workflows`) for automated quality checks.

## Confidence by subsystem
- Risk engine: **high**.
- Config bootstrap/safety: **medium-high** (improved with new tests).
- API control plane: **medium**.
- Replay/backtest paths: **medium-high**.
- Onchain module set: **medium-low** (many scaffolded components).
- Storage/migrations: **low** (incomplete runtime path).

## Recommended next pass
1. Add missing Alembic runtime files and migration smoke test.
2. Add Dockerfile and containerized test job.
3. Persist ops layer state (DB or Redis) and add restart-resilience tests.
4. Add readiness endpoint with DB/Redis/exchange dependency checks.
