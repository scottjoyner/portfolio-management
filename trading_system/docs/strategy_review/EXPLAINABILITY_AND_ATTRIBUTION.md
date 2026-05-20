# Explainability and Attribution Review

## Improvements in this pass

- Added structured signal fields (`confidence`, `tags`, `features`, `warmup_passed`) for consistent downstream analytics.
- Standardized metadata to include strategy family and risk/capital hints.
- Added explicit disable/cooldown hooks for operational visibility.

## Remaining gaps

- Strategy classes still do not emit full order intent rationale payloads.
- PnL attribution tags are richer in onchain modules than core CEX strategy scaffolding.
- UI/reporting surfaces need direct consumption of new strategy metadata fields.

## Recommended next steps

1. Attach signal `features` into order attribution in execution path.
2. Add explainability regression tests per strategy family.
3. Emit disable/cooldown state in operator status APIs.

