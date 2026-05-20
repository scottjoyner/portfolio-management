# Regression Tests Added

## `tests/unit/test_settings.py`

### Added coverage
- Boolean env parsing accepts canonical truthy/falsy values.
- Invalid boolean env value raises explicit error.
- LIVE trading mode requires explicit live enable flag.
- `LIVE_AUTO` mode requires approvals to remain enabled.
- CANARY mode requires positive rollout percentage.
- Canary rollout percentage rejected in non-CANARY mode.
- Queue model input is normalized and validated.

### Risk/bug coverage rationale
These tests protect startup safety and prevent silent misconfiguration in production-sensitive paths (live/canary modes, approval gating, queue model selection).
