# Trading System Value Review — 2026-09-11

Status: architecture/value review. This document does not authorize live trading.

## Executive conclusion

The research-integrity stack is useful, but it is supporting infrastructure rather than the current profit engine.

The work on canonical replay, evidence identity, experiment lineage, precommitted candidate budgets, one-shot terminal holdouts, and dependence-aware selection reduces self-deception and makes research results harder to falsify accidentally. That is valuable because an autonomous research agent will otherwise overfit quickly.

However, repository review shows that the current runtime path can still bypass that research authority, and parts of the opportunity economics mix incompatible units. Those issues are more important to expected trading value than adding increasingly elaborate statistical tests.

The correct near-term objective is therefore:

```text
trustworthy research
  -> certified strategy identity
  -> runtime uses that exact identity/config
  -> payoff-based expected dollars
  -> execution-cost-adjusted edge
  -> portfolio allocator / hard risk
  -> independent overseer
  -> shadow execution
  -> realized attribution + recalibration
```

More statistical sophistication is worthwhile only when it materially changes one of those decisions.

## What the research stack is actually buying us

### High value: accounting and replay correctness

Canonical source replay, Python/Rust semantic parity, execution-config binding, fee correctness, and deterministic evidence hashes prevent the system from optimizing against a measuring instrument that changes underneath it.

This work is foundational. A strategy-selection system cannot be better than its accounting and replay semantics.

### High value: search discipline

The research tournament addresses a real agent failure mode: repeatedly generate candidates until one looks good, then report only the winner.

Precommitting a candidate budget, recording failed attempts, using family-wise correction, sealing one candidate before terminal evaluation, and making terminal failure final all reduce this source of selection bias.

### Moderate value: dependence-aware block companion

The v2 selection slice adds a deliberately coarse block-level sign gate. It is useful because a streak of correlated trades should not count as dozens of independent confirmations.

It is intentionally not a full bootstrap framework. The block gate is cheap, deterministic, versioned, and auditable. If it rejects a candidate that only looks strong because its wins are clustered, it has paid for itself.

It does not establish that blocks are independent and should not be presented as doing so.

## Where the current system is not yet economically coherent

### P0 — opportunity expected value mixes incompatible units

`apps/api/src/opportunityGenerator.mjs` currently derives a strategy opportunity value as:

```text
expectedValue = weightedConfidence * winRate * 100
```

and then sets `grossExpectedValue` from that score.

`apps/api/src/opportunityFlowsLegacy.mjs::netExpectedValue()` subtracts estimated fees, slippage, gas, agent research cost, and model inference cost from `grossExpectedValue`.

Those costs are dollar amounts. The scanner-derived `expectedValue` is not dollars. Subtracting the two creates a number that looks economically meaningful while having no coherent unit.

For strategy-driven opportunities, expected value should instead be based on the proposed payoff distribution, for example:

```text
upside_usd   = quantity * abs(take_profit_price - entry_price)
downside_usd = quantity * abs(entry_price - stop_loss_price)
expected_pnl_usd = p_win * upside_usd - p_loss * downside_usd
net_edge_usd = expected_pnl_usd - execution_costs_usd - research/model_costs_usd
```

Probabilities must themselves be calibrated; backtest win rate is not automatically a calibrated forecast probability.

### P0 — runtime scanner bypasses the canonical research tournament

`scripts/strategy_signal_scanner.py`:

1. loads the current lookback candle window;
2. generates signals from that window;
3. backtests each signal's strategy on that same window;
4. filters candidates using total trades and win rate;
5. ranks across strategies/products and returns the winners.

That is an in-sample screening loop over many hypotheses. It currently does not require a tournament-certified candidate/evidence hash before a strategy becomes an opportunity.

The result can therefore look rigorously validated at the research layer while the runtime is selecting strategies through a different, much weaker gate.

### P0 — challenger promotion does not clearly control runtime behavior

`scripts/challenger_manager.py` writes `data/agent_runtime_config.json` with an `active_challenger_id` and candidate parameters.

Repository search currently finds that runtime-config path and `active_challenger_id` only inside the challenger manager itself. Until a production signal/execution path consumes and verifies that exact active challenger identity/config, promotion is primarily metadata rather than behavioral deployment.

### P1 — runtime opportunity metadata overstates validation

`apps/api/src/opportunityGenerator.mjs` labels generated strategy opportunities with:

```text
backtestStatus = live_data_30d_mock_tested
```

That label does not bind a canonical alpha-evidence hash, tournament experiment, terminal result, strategy source SHA, or exact candidate config.

A status string should not substitute for executable evidence identity.

### P1 — the execution overseer does not yet enforce economic edge or research certification

`packages/execution/src/overseer.mjs` provides useful structural controls: canonical TradeIntent hashing, risk-decision validation, confidence thresholds, TTLs, and live-mode blocking.

But `netExecutableEdgeUsd` is currently only carried in the intent envelope. Admission does not require positive finite executable edge, and it does not require a certified research/tournament identity for strategy-driven entry trades.

This is an important distinction: evidence can exist without influencing execution.

### P1 — short-horizon forecast weights are heuristic

`packages/economics/src/economicDecisionEngine.mjs` uses fixed default weights for naive, momentum, mean-reversion, and microstructure components. The calculation is coherent enough to be testable, but those weights should be treated as a forecasting hypothesis rather than established edge until shadow/live calibration shows otherwise.

## Highest-value implementation order

1. **Fix units in strategy opportunity economics.** Expected/gross/net values used for execution decisions must all be USD or all explicitly typed in another common unit. Never subtract dollar costs from a score.
2. **Bind certified research to runtime strategy identity.** Runtime entry signals should carry candidate ID, config hash, alpha evidence hash, tournament/terminal evidence when applicable, and exact source/runner identity.
3. **Stop using same-window backtest screening as entry certification.** It may remain as telemetry or a degradation alarm, but not as the authority that promotes a current strategy signal.
4. **Make entry admission require positive executable edge after costs and hard risk approval.** Exit/risk-reduction actions need separate semantics so the system can always reduce risk even when expected edge is negative.
5. **Shadow-trade the exact production decision path.** Measure forecast calibration, predicted-versus-realized edge, slippage, fill latency, turnover, drawdown, and opportunity cost/regret.
6. **Use those outcomes to learn.** Only after enough shadow/real outcomes exist should the system tune forecast weights, probability calibration, sizing fractions, block horizons, or model-spend policy.
7. **Add more sophisticated inference only where it changes decisions.** Stationary/block bootstrap, HAC-style uncertainty, or SPA/reality-check style comparisons may eventually be useful, but they should not outrank fixing runtime/economic disconnects.

## Decision on the current dependence slice

Keep it, but keep it small.

The accepted v2 design precommits a fold-local non-overlapping block size and minimum nonzero block count. Each block casts one positive/negative/zero direction vote from its constituent trade signs. A candidate must pass both the marginal trade-level exact sign test and the block-level exact sign test under the same precommitted family-wise candidate-search penalty.

This directly prevents a simple correlated streak from masquerading as many independent observations without pretending that a block bootstrap can manufacture certainty from a small dataset.

## What success should look like

The system should eventually be able to answer, for every proposed entry:

- Which exact strategy/config generated this?
- Which source data and code produced the validation evidence?
- How many alternatives were searched before it was selected?
- Did the edge survive cost, stability, dependence and terminal checks?
- What is the expected P&L in dollars at the proposed size?
- What execution costs and uncertainty reserves were deducted?
- What hard portfolio-risk limits constrain the size?
- Did the overseer validate the same immutable intent?
- What happened after the trade, and was the forecast calibrated?

If those questions cannot be answered from one traceable chain, additional model sophistication should not be considered a higher priority.

## Safety invariant

None of this review changes the current live-trading boundary. Research certification, positive expected edge, or shadow performance do not independently authorize real-capital execution. Live entry remains a separate operational and human-authorization decision.
