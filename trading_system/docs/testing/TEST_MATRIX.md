# Test Matrix

| Subsystem | Test type | Files | Status before | Status after | Notes |
|---|---|---|---|---|---|
| Config safety | Unit | `tests/unit/test_settings.py` | Partial coverage | Passing | Added `LIVE_AUTO` approvals invariant test in this pass. |
| API health/ops | Integration | `tests/integration/test_api_health.py`, `tests/integration/test_ops_api.py` | Passing | Passing | Re-ran ops contract-focused integration suite for consistency. |
| Risk engine | Unit | `tests/unit/test_risk_engine.py` | Passing | Passing | Existing coverage retained. |
| Onchain path/policy | Unit | `tests/unit/test_onchain_*` | Passing | Passing | No interface breaks from config/docs updates. |
| CLMM math | Unit | `tests/clmm/test_clmm_math.py` | Passing | Passing | Prior lint issue remains resolved. |
| Replay/backtest | Sim/replay | `tests/sim/*`, `tests/replay/*` | Passing | Passing | Smoke stability preserved. |
| Repo-wide static analysis | Lint/typecheck | entire repo | Passing | Passing | Verified again after consistency/doc changes. |
| Local command orchestration | Build/tooling smoke | `Makefile` (`make ci`) | Missing unified target | Passing | Added `typecheck` + `ci` targets and validated command. |
