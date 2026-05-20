# Gap Report

## 1) Critical correctness gaps
- **Config safety validation underpowered (fixed P1)**: env bools previously accepted any non-`true` string as false, creating silent config drift risk.
- **Canary/live mode invariants incomplete (fixed P1)**: no validation tying `TRADING_MODE` to rollout percentage or approvals.
- **Migration runtime incomplete (P0 deferred)**: `alembic/versions` exists but `alembic/env.py` missing, so migration command path is broken.
- **Container runtime contract broken (P1 deferred)**: `docker-compose.yml` uses `build: .` without a Dockerfile.

## 2) Testing gaps
- No dedicated tests existed for config parsing/validation in `core/config/settings.py` (fixed).
- No typecheck command documented as runnable baseline; mypy invocation previously failed immediately on package layout (partially fixed via config).
- Limited contract tests for API response schemas vs UI assumptions.

## 3) Operational gaps
- No CI workflows in repository root (`.github/workflows` absent).
- Health endpoint is liveness-only; no readiness checks for DB/Redis/exchange connectivity.
- Worker is heartbeat-only and lacks structured failure handling/restart diagnostics.

## 4) Documentation gaps
- README quickstart referenced `.env.example` but repo has no `.env.example` (addressed).
- No centralized audit/testing evidence docs existed (addressed via `docs/repo_audit/*` and `docs/testing/*`).

## 5) UX / contract gaps
- Ops API currently stores previews/orders/fills in memory only; restart drops state.
- No websocket transport despite realtime-oriented models.

## 6) Priority tiers
### P0 must fix now
- Add missing config validation tests and harden parsing/validation logic. ✅

### P1 should fix in this pass
- Fix lint issue blocking `ruff check`. ✅
- Make mypy executable in this mixed-package repo (`explicit_package_bases`, import handling). ✅
- Update README + testing docs to match actual runnable commands. ✅
- Align Makefile targets with documented CI/local commands (`make ci`). ✅
- Expand interface map to concrete `/ops/*` backend contract paths. ✅

### P2 improve if time allows
- Implement readiness probes and persistence-backed ops store.
- Add contract tests covering operator API models.

### Explicitly deferred
- Full Alembic wiring (`alembic/env.py` + migration runner integration).
- Container image build path completion (Dockerfile + compose verification).
