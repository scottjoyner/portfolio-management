# Observability Runbook

This runbook covers the built-in observability added for production-like paper-only deployments.

## Metrics endpoint

The API exposes Prometheus-style process metrics at:

```text
GET /metrics
```

Current metrics:

```text
portfolio_requests_total
portfolio_responses_total{status="..."}
portfolio_errors_total
portfolio_uptime_seconds
portfolio_memory_rss_bytes
```

The endpoint is intentionally lightweight and process-local. It does not replace a full metrics stack.

## Structured logs

Every request handled by `server.p1.mjs` emits a JSON log event:

```json
{
  "event": "http_request",
  "requestId": "req-...",
  "method": "GET",
  "path": "/api/operator/summary",
  "status": 200,
  "durationMs": 12
}
```

Ship stdout/stderr from the container to the deployment platform log collector.

## Suggested alerts

Create alerts for:

- high `portfolio_errors_total` growth
- sustained 5xx responses
- API container restart loops
- `/health` probe failures
- Postgres connection failures
- migration failure during deployment
- runtime env validation failure at startup
- paper reconciliation breaks

## Suggested dashboards

At minimum:

- request rate
- 2xx/4xx/5xx counts
- p95 request duration from logs or ingress
- process memory RSS
- process uptime
- paper execution count
- open position count
- kill switch state

## Health vs readiness

`/health` is used for process and storage reachability.

`/ready` remains fail-closed for true production readiness because live trading is intentionally uncertified. Do not use `/ready` as the only Kubernetes readiness check until the live-certification model is finalized.

## Incident checklist

1. Check `/health`.
2. Check `/metrics`.
3. Check latest JSON request logs.
4. Check Postgres connectivity.
5. Check recent migration status.
6. Check kill switch state.
7. Stop all paper sessions if execution state is suspicious.
8. Roll back image if failures began after deployment.
9. Restore database only after confirming migration/data corruption and validating restore in a controlled environment.
