# Economic Decision and Local Intelligence API Contract

This contract extends `API_CONTRACT_P0_P1.md`. It covers paper/guarded economic evidence and intelligence execution. It does not certify live trading.

## Safety invariants

- `REMOTE_LLM_EXECUTION_ENABLED=false` disables remote inference even when a remote quote exists.
- `LOCAL_LLM_EXECUTION_REQUIRED=true` makes omitted quote locality resolve to local inference.
- A model quote may authorize purchasing intelligence; it never authorizes a trade.
- Provider execution occurs outside the PostgreSQL operator-state transaction.
- Actual measured/provider-reported cost supersedes the pre-call estimate.
- Reconciliation invalidates older execution decisions that used the estimate.
- Prompts and full model responses are not persisted in the operator ledger.
- A local quote is bound to a specific healthy node and loaded model. If that evidence changes, execution fails with a re-quote requirement.

## Routes

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/economics/dashboard` | Consolidated pricing, forecast, decision, execution-cost, calibration, maintenance, and attribution state. |
| GET | `/api/economics/model-pricing` | Versioned model-pricing snapshots and latest snapshot. |
| POST | `/api/economics/model-pricing/refresh` | Refresh a supplied or OpenRouter pricing catalog. Remote execution remains separately gated. |
| GET | `/api/economics/intelligence/nodes` | Health, loaded models, latency, context, and concurrency for configured local LM Studio/llama.cpp nodes. |
| POST | `/api/economics/model-quotes` | Create a local routed quote or an explicitly enabled remote quote. |
| POST | `/api/economics/intelligence/execute` | Execute an already-approved intelligence purchase and reconcile actual cost. |
| POST | `/api/economics/model-usage/reconcile` | Explicit idempotent usage reconciliation. |
| POST | `/api/economics/maintenance/run` | Run one guarded economic-maintenance cycle. |
| GET/POST | `/api/economics/forecasts` | List or create probabilistic price forecasts. |
| POST | `/api/economics/forecasts/:id/outcome` | Record a forecast outcome after its horizon. |
| GET/POST | `/api/economics/execution-costs` | List or create venue execution-cost snapshots. |
| POST | `/api/economics/coinbase/refresh` | Refresh a Coinbase quote, preview, and available fee evidence. |
| POST | `/api/economics/decisions/evaluate` | Evaluate value of information and net executable edge. |
| GET/POST | `/api/economics/attribution` | List or record paid-agent counterfactual attribution. |
| GET | `/api/economics/calibration` | Forecast calibration outcomes and summary. |

## Local node discovery

`GET /api/economics/intelligence/nodes`

A node is usable only when its OpenAI-compatible `GET /v1/models` endpoint succeeds. The response does not expose configured API keys.

```json
{
  "ok": true,
  "localRequired": true,
  "remoteEnabled": false,
  "nodes": [
    {
      "ok": true,
      "nodeId": "x1-370",
      "kind": "lmstudio",
      "models": ["exact-loaded-model-id"],
      "activeRequests": 0,
      "maxConcurrent": 1,
      "contextLength": 65536,
      "latencyMs": 12.5
    }
  ]
}
```

## Local quote

`POST /api/economics/model-quotes`

```json
{
  "localOrRemote": "local",
  "model": "exact-loaded-model-id",
  "promptTokens": 8000,
  "completionTokens": 800,
  "opportunityId": "opp-001",
  "purpose": "market-review"
}
```

Routing fails closed when no healthy node exposes the exact model, has sufficient context, or has available configured concurrency. A successful quote records:

- selected node ID and name;
- provider kind;
- context capacity;
- estimated prefill, decode, runtime, and queue seconds;
- estimated watts;
- electricity and depreciation assumptions;
- estimated local compute cost.

## Guarded intelligence execution

`POST /api/economics/intelligence/execute`

```json
{
  "modelQuoteId": "model-quote-001",
  "economicDecisionId": "economic-decision-001",
  "researchJobId": "job-001",
  "messages": [
    { "role": "user", "content": "Evaluate the supplied structured evidence." }
  ]
}
```

The call is accepted only when:

- quote and decision exist and match;
- `intelligenceAllowed=true`;
- quote is not already consumed;
- the quoted local node remains healthy;
- the exact model remains available;
- node concurrency remains available;
- remote execution is explicitly enabled for a remote quote.

The response includes the model result for the caller and persisted usage metadata. The persistent ledger stores generation ID, token counts, runtime, queue delay, throughput, node ID, and actual cost—not prompt or response text.

## Decision phases

### Intelligence purchase

The pre-call decision may set `intelligenceAllowed=true` while keeping `executionAllowed=false` with blocker `model_usage_not_reconciled`.

### Execution

After reconciliation, the caller must create a new decision using actual cost. Execution remains blocked unless forecast, venue cost, quote freshness, model usage, and net edge are all valid.
