# Generated Test Retirement Policy

The repository contains two distinct Python test surfaces:

1. **Maintained tests** under `tests/` and `trading_system/tests/`, excluding `tests/coverage/`. These are supported product and integration contracts and must pass.
2. **Generated coverage snapshots** under `tests/coverage/`. These are historical, largely machine-generated probes of public and private interfaces.

Generated snapshots are executed one file per Python process to prevent import-time mocks from contaminating unrelated tests. A snapshot may be retired only when it targets a removed or superseded contract, and every retirement must be listed in `tests/coverage/retired_tests.json` with a concrete reason.

The inventory runner records retired snapshots in its artifact summaries. It does not silently ignore them. Missing files, duplicate entries, empty reasons, active test failures, and per-file timeouts fail the workflow.

Retirement is appropriate for historical snapshots that depend on examples such as:

- removed private Rust bindings;
- pre-Alembic persistence models;
- legacy ledgers that the runtime now rejects safely;
- removed dashboard implementations;
- obsolete optimizer internals or external CLIs;
- historical on-chain behavior replaced by fail-closed policy.

A retired snapshot should be reactivated when it is rewritten against a supported interface. The exhaustive maintained suite remains the authoritative release gate.
