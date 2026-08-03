# Intelligence routing policy

Portfolio OS separates **deployment permission** from **operator policy**.

The deployment controls whether local and remote providers exist at all. The Economics UI controls which configured providers may be selected, how much OpenRouter credit may be committed, and how much expected value a paid call must cover.

Live trading remains disabled. This policy governs research-model calls inside the supervised paper-trading lifecycle; it does not authorize an order.

## Deployment-level provider gates

Configure the host-managed production environment before starting Compose.

```dotenv
# Local fleet is the default.
LOCAL_LLM_EXECUTION_REQUIRED=true
LOCAL_LLM_NODES_JSON=[...]

# OpenRouter is opt-in and remains off by default.
REMOTE_LLM_EXECUTION_ENABLED=false
OPENROUTER_API_KEY=
OPENROUTER_APP_URL=
OPENROUTER_APP_NAME=Portfolio OS
```

To make OpenRouter available to the UI policy:

```dotenv
REMOTE_LLM_EXECUTION_ENABLED=true
OPENROUTER_API_KEY=replace-with-host-managed-key
```

Restart the API and economic worker after changing deployment variables. The key is passed only to those application services. It is not sent to the migration service, returned by the policy API, or stored in browser policy state.

A deliberately remote-only deployment may set `LOCAL_LLM_EXECUTION_REQUIRED=false` and leave the local-node variables empty. That configuration has no local fallback, so `economic_auto` requests fail closed whenever remote inference is unavailable or uneconomic.

## Browser operator session

Production requires a bearer token for reads and a CSRF token for mutations. Open the **Operator session** control at the lower-right of the console and enter the values from:

- `OPERATOR_ADMIN_TOKEN`
- `OPERATOR_CSRF_TOKEN`

The values are held in the current tab's `sessionStorage`. The fetch wrapper attaches them only to same-origin paths beginning with `/api/`. They are not placed in URLs, portfolio state, audit payloads, or remote-provider requests. A reverse proxy that already injects these headers may continue doing so without using the browser control.

## Economics UI knob

Open **Economics → Intelligence routing**.

### Local fleet only

- Remote quotes are blocked even when OpenRouter is deployed.
- Automatic requests select a healthy local OpenAI-compatible node.
- This is the default when no policy has been saved.

### Economic auto-selection

- The orchestrator requests a quote with `localOrRemote: "auto"`.
- OpenRouter is eligible only when the estimated remote cost is within both budget caps.
- Expected remote improvement must cover cost by at least **Minimum value coverage**.
- When explicit value-of-information evidence is absent, the policy may use a conservative learned estimate after at least five settled remote and five settled local attribution observations.
- The learned estimate is the remote lower-bound incremental value minus the local upper-bound incremental value. Insufficient history, non-positive learned improvement, a failed remote quote, missing credentials, or a budget violation falls back to local when fallback is enabled.

### OpenRouter eligible

- Explicit remote requests are permitted when deployment credentials are available and the request fits both caps.
- Automatic requests still prefer local unless the caller sets `preferRemote: true`.
- The economic decision engine must separately approve purchasing the intelligence.

## Budget semantics

- **Daily OpenRouter cap** limits committed remote quote cost for the current UTC date.
- **Per-request cap** limits one remote quote and may not exceed the daily cap.
- `quoted`, `running`, `usage_pending`, and `reconciled` remote records count against committed spend.
- Provider-reported actual cost replaces the estimate after reconciliation.
- `policy_blocked` and unselected `comparison_only` quotes do not count as committed spend.

The policy is checked once after a remote price quote and again immediately before provider execution. This prevents a stale UI choice or a changed daily budget from authorizing a later call.

## Economic and execution gates remain independent

A policy-eligible remote quote is not permission to trade. The lifecycle remains:

```text
forecast + venue-cost evidence
  -> model quote
  -> intelligence-purchase decision
  -> provider call
  -> actual usage reconciliation
  -> refreshed economic decision
  -> supervised paper execution
```

The pre-call decision may authorize buying intelligence. Reconciliation invalidates decisions made with estimated cost. A new post-reconciliation decision must still show positive executable edge before paper execution can proceed.

## Quote and job lineage

An automatic quote response includes `routingDecision.selected` and the persisted model quote. When the selected `modelQuoteId` is used to create a research job, the API inherits the quote's locality, provider, and model. Callers no longer need to repeat `localOrRemote`, which prevents an OpenRouter-selected quote from silently becoming a local job.

Example automatic quote request:

```json
{
  "localOrRemote": "auto",
  "localModel": "your-local-model-id",
  "remoteModel": "openrouter/model-id",
  "promptTokens": 1200,
  "completionTokens": 300,
  "expectedDecisionImprovementUsd": 0.75
}
```

An explicit estimate should come from calibrated opportunity evidence, not from the model being evaluated. When sufficient settled local-versus-remote attribution exists, the field may be omitted and the conservative learned estimate is used instead.

## Pull-and-test checks

After pulling the branch:

1. Start with `REMOTE_LLM_EXECUTION_ENABLED=false` and confirm the UI reports **Local only** or **Remote unavailable**.
2. Save each routing mode and refresh the page; the mode and caps should persist.
3. Confirm local node discovery and one local quote.
4. Enable OpenRouter in the host env, restart API and worker, and confirm the UI reports **OpenRouter runtime: Available** without displaying the key.
5. Set a very small per-request cap and confirm an oversized remote quote is blocked.
6. Use `economic_auto` with fallback enabled and confirm an unavailable or low-value remote comparison produces a local quote.
7. Execute only a paper research workflow; confirm provider-reported usage is reconciled and the trade decision requires refresh.
8. Close the browser tab and confirm the operator session values are no longer present in a new tab session.

Do not enable live trading as part of this validation.
