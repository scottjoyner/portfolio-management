# LLM Trade Monitoring Cron Plan

> Advisory only. This daemon reviews live trading state every 5 minutes and produces a vote, rationale, and scoreboard entries. It does not place orders.

## Goal

Add a local, multi-model oversight loop that watches the trader, scores the current risk/reward, and flags bad behavior early enough to stop the worst failures.

## Scope

- Read-only status review every 5 minutes.
- Local OpenAI-compatible endpoints first.
- LM Studio compatible `/v1/chat/completions` support.
- Multiple models voting independently.
- Scoreboard for model accuracy, calibration, and veto quality.
- Optional hard-stop recommendations only from the daemon, never direct order placement.

## Non-Goals

- No live order execution from the LLM loop.
- No hidden auto-trading logic.
- No prompt-driven order placement.
- No replacement for exchange-side risk checks, brackets, or kill switches.

## Architecture

```text
trader health/status
  → 5 min cron/daemon
  → model adapters (OpenAI-compatible / LM Studio)
  → structured review prompt
  → model votes + rationale
  → consensus / dissent summary
  → scoreboard update
  → alert / stop recommendation if needed
```

## Model Topology

- `vibethinker-3b`: fast tactical reviewer.
- `orinth-1.0-35b`: slower deep reviewer / arbiter.

Each model should receive the same compact JSON status snapshot and return:

- `vote`: `continue`, `warn`, `stop`, or `flatten_only`
- `confidence`: `0.0` to `1.0`
- `reason`: short explanation
- `key_risks`: list of concrete issues

## Status Inputs

Minimum inputs from the daemon:

- current equity, cash, realized pnl, drawdown
- open positions and notional
- recent fills and exits
- websocket health / last ticker age
- scan freshness
- outstanding approvals / bracket state
- current alerts from the trader watchdog
- live guard settings and caps

## Scoreboard

Track each model over rolling windows:

- veto precision: how often `stop` / `flatten_only` was justified
- false alarm rate
- missed-risk rate
- calibration by confidence bucket
- agreement rate with the final consensus
- time-to-warning before a bad event

This scoreboard is how we pick the best decisionmakers over time.

## Cron Schedule

- Run every 5 minutes.
- Stagger model calls by 10 to 30 seconds to avoid bursty local load.
- Persist one compact JSON result per cycle.
- Retain full traces for 7 days, summaries for 30 days.

## Vibethinker Loops

Use `vibethinker-3b` for short, frequent, cheap checks. Keep prompts tiny and verdict-oriented. The probe normalizes its one-line response into the shared JSON shape.

### 1. Fast Watchdog Loop

- Cadence: every 1 minute.
- Purpose: stale-feed detection, alert escalation, scan lag detection.
- Input: `health_ok`, `alerts`, `last_ticker_ts`, `last_scan_ts`, `drawdown`, `open_positions`.
- Output: `vote`, `confidence`, `reason`, `risks`, `action_items`.

Prompt template:

```text
Status: health_ok=<bool> alerts=<count> last_ticker_age_s=<int> last_scan_age_s=<int> drawdown=<pct> positions=<int>.
Reply in one line only: vote=<continue|warn|stop|flatten_only> confidence=<0-1> reason=<short>. No JSON.
```

### 2. Position Hygiene Loop

- Cadence: every 5 minutes.
- Purpose: detect bad exits, oversized positions, stuck brackets, correlated exposure.
- Input: open positions summary, notional, age, entry/stop/target, bracket status.
- Output: per-position `continue`, `warn`, `stop`, or `flatten_only`.

Prompt template:

```text
Review these positions for exit risk and hygiene only. Keep it brief and conservative.
Status: open_positions=<summary> notional=<usd> oldest_position_age_s=<int> bracket_errors=<count>.
Reply in one line only: vote=<continue|warn|stop|flatten_only> confidence=<0-1> reason=<short>. No JSON.
```

### 3. Trade Review Loop

- Cadence: every 5 minutes, aligned with scan cadence.
- Purpose: review the latest top candidates and judge whether they deserve attention.
- Input: top scan result summary, drawdown, win rate, live guardrails.
- Output: `continue` or `warn` for the candidate set.

Prompt template:

```text
Status: top_buy=<label> top_sell=<label> drawdown=<pct> win_rate=<pct> health_ok=<bool> alerts=<count>.
Reply in one line only: vote=<continue|warn|stop|flatten_only> confidence=<0-1> reason=<short>. No JSON.
```

### 4. Scoreboard Loop

- Cadence: every 30 minutes.
- Purpose: summarize model performance and error patterns.
- Input: last N votes, whether warnings were justified, whether any forced flattens occurred.
- Output: scoreboard delta record.

Prompt template:

```text
Review the last monitoring window and score judgment quality.
Inputs: votes=<summary> outcomes=<summary> false_alarms=<count> misses=<count>.
Reply in one line only: vote=<continue|warn|stop|flatten_only> confidence=<0-1> reason=<short>. No JSON.
Vote should reflect model quality, not trade direction.
```

### 5. Hourly Digest Loop

- Cadence: every 60 minutes.
- Purpose: produce a compact operator digest with changes since the last hour.
- Input: health snapshot, scan summary, paper/live deltas, alerts.
- Output: one short JSON digest for logs or dashboard.

Prompt template:

```text
Summarize the last hour of trading health in one JSON object.
Use only observed data. Keep it concise. Reply in one line only: vote=<continue|warn|stop|flatten_only> confidence=<0-1> reason=<short>. No JSON.
```

## Prompt Contract

The prompt should be strict:

- system message: advisory-only, no trading commands
- user message: compact trader status text or JSON
- fast models should reply in one-line verdict text that the daemon normalizes
- deeper models can return JSON if they reliably comply
- forbid invented market data
- force references to observed fields only

## Failure Policy

- If the daemon cannot reach the trader status endpoint, mark the cycle as `unhealthy`.
- If the models disagree strongly, prefer the most conservative vote.
- If a model returns invalid JSON, mark it failed and continue.
- If local model latency exceeds budget, degrade to fewer models instead of blocking the trader.

## Implementation Phases

### Phase 1: Status Collector

- Add a small CLI that fetches trader health/status and normalizes it to JSON.
- Support local HTTP endpoints only.
- Add a `--json` mode for cron.

### Phase 2: Model Adapters

- Add OpenAI-compatible adapter.
- Add LM Studio adapter.
- Support endpoint override via env vars.

### Phase 3: Voting Engine

- Run 3 reviewers plus 1 arbiter.
- Merge votes into a final recommendation.
- Emit a scoreboard record each cycle.

### Phase 4: Alerting Hooks

- Write warnings to the operator log.
- Optionally trigger local notifications.
- Never execute trades.

### Phase 5: Evaluation

- Compare LLM recommendations against realized trade outcomes.
- Keep only models that improve veto quality and reduce bad exits.

## Suggested Env Vars

```bash
LLM_MONITOR_ENABLED=true
LLM_MONITOR_INTERVAL_S=300
LLM_MONITOR_ENDPOINT=http://localhost:8080/health
LLM_API_BASE_VIBE=http://deathstar-xps-8920.tailcb8954.ts.net:1234/v1
LLM_API_BASE_ORINTH=http://127.0.0.1:1234/v1
LLM_API_KEY=local
LLM_MODELS=vibethinker-3b,orinth-1.0-35b
LLM_MONITOR_MAX_TOKENS=512
LLM_MONITOR_TIMEOUT_S=30
LLM_VIBE_MAX_TOKENS=512
LLM_ORINTH_MAX_TOKENS=1200
```

## Suggested Cron

```cron
* * * * *     /usr/bin/python3 /path/to/scripts/trading/llm_judge_probe.py --status-url http://localhost:9090/health --judge vibethinker=http://deathstar-xps-8920.tailcb8954.ts.net:1234/v1:vibethinker-3b
*/5 * * * *   /usr/bin/python3 /path/to/scripts/trading/llm_judge_probe.py --status-url http://localhost:9090/health --judge orinth=http://127.0.0.1:1234/v1:orinth-1.0-35b --max-tokens 1200
*/30 * * * *  /usr/bin/python3 /path/to/scripts/trading/llm_judge_probe.py --status-url http://localhost:9090/health --judge vibethinker=http://deathstar-xps-8920.tailcb8954.ts.net:1234/v1:vibethinker-3b --judge orinth=http://127.0.0.1:1234/v1:orinth-1.0-35b --max-tokens 1200
```

## Exit Criteria

- The daemon runs every 5 minutes without blocking trading.
- JSON output is valid on every cycle.
- Each model has a persistent scoreboard.
- A stale feed, runaway drawdown, or broken bracket produces a conservative warning.
- The system remains advisory-only until scoring is proven useful.
