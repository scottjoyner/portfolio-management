# Operator Actions Rust Service

Small stdlib-only Rust sidecar for low-latency dashboard action queueing.

It intentionally does not execute trades. It records operator intent in `data/operator-actions.json` so Python daemons/supervisors can consume the queue with their own auth, dry-run, and risk checks.

Endpoints:

- `GET /health` -> service health
- `GET /actions` -> action catalog plus current queue
- `POST /actions/run` with JSON `{ "action": "refresh_market_data", "note": "optional" }` -> append a queued action

Run locally once Rust is installed:

```bash
cd services/operator-actions
cargo run --release
```

Docker Compose exposes it on port `8098`.
