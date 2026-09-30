# Canonical Portfolio Allocator

Status: canonical portfolio-wide paper/demo capital allocation layer; live entry remains uncertified and blocked.

## Purpose

The portfolio allocator sits between a requested trade and the canonical per-intent capital-risk gate. Its job is to answer a different question from the risk engine:

> Given the entire authoritative portfolio state, how much of this request may consume capital right now?

The allocator is deterministic and downstream of the caller. Agents, strategies, and API clients may request size, but they do not choose the final capital allocation.

## Authorization chain

```text
requested trade
    |
    +--> requested TradeIntent hash
    |
    +--> one authoritative operator-state load
    |      + accounts / NAV / cash
    |      + positions
    |      + pending executions
    |      + high-water evidence
    |      + market / liquidity state
    |      + configured capital policy
    |      + optional covariance inputs
    |
    +--> PortfolioAllocation v1
    |      + APPROVE / REDUCE / EXIT_ONLY / REJECT
    |      + requested / approved / suggested notional
    |      + all portfolio budgets
    |      + requested and approved intent hashes
    |      + allocationDecisionHash
    |      + allocationHash + expiry
    |
    +--> approved TradeIntent
    |
    +--> CapitalRiskSnapshot v1
    |
    +--> canonical risk decision
    |
    +--> OverseerDecision v3
    v
paper/demo submission
```

Allocation and per-intent capital risk consume the same resolved authoritative state load during one evaluation. There is no second read between sizing and risk admission that could silently evaluate different portfolio states.

## Decision semantics

The allocation decision is one of:

- `APPROVE` — requested capital is inside every active limit.
- `REDUCE` — a manual paper/demo request is deterministically resized to the tightest permitted notional.
- `EXIT_ONLY` — portfolio state prohibits increasing risk, but a valid risk-reducing sell may proceed.
- `REJECT` — no admissible capital exists, required state is unavailable/invalid, or the request requires new economics at a smaller size.

Every applicable constraint is evaluated. The allocator does not stop at the first scale-down condition. The approved notional is the minimum across all finite applicable budgets.

## Default v1 policy

Current defaults are intentionally conservative and may be overridden only through authoritative configuration/context, not by a caller-owned approval flag:

| Limit | Default |
| --- | ---: |
| Maximum portfolio drawdown | 15% |
| Drawdown throttle begins | 10% |
| Maximum gross leverage | 1.5x NAV |
| Maximum net exposure | 100% NAV |
| Maximum symbol exposure | 20% NAV |
| Maximum strategy exposure | 30% NAV |
| Maximum correlation-cluster exposure | 40% NAV |
| Maximum single trade | 10% NAV |
| Minimum cash buffer | 10% NAV |
| Maximum spread | 100 bps |
| Maximum 24h-volume participation | 1% |
| Unknown-liquidity fallback trade cap | 5% NAV |
| Maximum covariance-risk proxy | 45% NAV |
| Minimum approved notional | $100 |
| Same-cluster fallback correlation | 0.75 |
| Cross-cluster fallback correlation | 0.25 |
| Default crypto volatility | 0.80 |
| Default equity volatility | 0.30 |
| Default other volatility | 1.00 |

The allocator also honors the existing authoritative `maxPositionSizeUsd` and `maxConcurrentTrades` settings. This layer does not raise those limits.

## High-water and drawdown authority

Drawdown protection must survive process restarts. A transient in-memory maximum is not sufficient.

The active provider reconstructs account high-water evidence from append-only operator audit events and mirrors the reconstructed value into the state supplied to the allocator. High-water marks advance monotonically when authoritative account NAV establishes a new peak.

Legacy persisted state is handled conservatively:

- if no high-water record exists and there is no open/pending risk, a new baseline may be initialized from current authoritative NAV;
- if no high-water record exists while risk is already in play, increasing risk fails closed with `high_water_mark_unavailable`;
- valid risk-reducing exits remain available under that fail-closed posture.

This prevents a restart or migration from silently resetting drawdown to zero.

Between the throttle start and hard drawdown limit, the allocator continuously reduces the maximum new notional. At or beyond the hard drawdown limit, the portfolio becomes `EXIT_ONLY` for increasing risk.

## Portfolio exposure accounting

The allocator reconstructs account-wide exposure from both open positions and pending executions.

Increasing orders reserve capital before fills, including pending orders on other symbols. The relevant budgets include:

- remaining cash after pending buy reservations and the required cash buffer;
- remaining gross-leverage capacity;
- remaining net-exposure capacity;
- remaining symbol exposure;
- remaining strategy exposure;
- remaining correlation-cluster exposure;
- the existing canonical per-symbol position-dollar cap;
- the configured concurrent-trade ceiling.

Pending executions are therefore part of portfolio capital demand, not invisible future work.

## Correlation and covariance

The allocator computes a covariance-style risk proxy for the current portfolio and proposed request.

When authoritative `portfolioRiskState.volatilityBySymbol` and `correlationMatrix` values are available they are used. Missing values fall back to deterministic policy assumptions based on asset type and configured correlation clusters. The allocation artifact records volatility sources and explicit correlation pairs so fallback use is visible rather than hidden.

The covariance proxy is a sizing control, not a claim of full statistical optimality. A later market-risk data pipeline should make the correlation/volatility inputs fresher, empirical, and independently attested.

## Liquidity stress

The target market contributes a first-class liquidity budget:

- spread above `maxSpreadBps` places increasing risk into `EXIT_ONLY` posture;
- known 24-hour market volume caps new notional at `maxLiquidityParticipationPct` of volume;
- when volume is unavailable, the stricter unknown-liquidity NAV fraction is used.

Liquidity failure is not converted into a favorable default.

## Risk-reducing exits

`EXIT_ONLY` is a survival mechanism, not an alternate way to increase risk.

A valid sell may continue when the portfolio is already beyond drawdown, leverage, net exposure, covariance, concurrent-trade, or liquidity-spread boundaries. The downstream capital-risk gate still requires sufficient account-owned inventory, so `EXIT_ONLY` does not authorize uncovered shorts.

## Economic/agent repricing after downsizing

Execution economics are not scale invariant. Commission, slippage, spread impact, model cost coverage, and executable edge may change when notional changes.

Therefore an economic/agent request that would otherwise be scaled down is **not** silently resized. The allocator returns:

- `REJECT`;
- `economic_reprice_required_after_allocation`;
- `suggestedNotionalUsd`.

The caller/orchestrator must regenerate execution-cost and economic-decision evidence at that suggested size and submit a fresh material intent. This prevents a favorable edge calculation for a larger trade from being reused for a different allocation.

## Approval-time refresh

Draft approval does not reserve a permanently favorable portfolio state.

At approval the engine:

1. verifies the stored allocation / risk / overseer authorization chain;
2. reloads authoritative operator state;
3. excludes the draft being approved from its own pending-reservation calculation;
4. reruns portfolio allocation;
5. reruns canonical per-intent capital risk;
6. reissues overseer authorization only for the exact unchanged approved intent.

If fresh portfolio conditions require a different size, approval fails with a replan requirement. The engine does not silently mutate an already-reviewed draft into a different trade.

## Integrity and expiry

`PortfolioAllocation` v1 binds:

- requested material intent hash;
- approved material intent hash;
- account / strategy / symbol / venue / side;
- requested, approved and suggested notional;
- scale and decision;
- policy and every derived budget;
- account/high-water state;
- current portfolio exposures;
- covariance and liquidity state;
- missing/invalid-state classifications;
- authoritative source hashes;
- evaluation and expiry timestamps.

The content receives an `allocationHash`, while the compact decision receives an independent `allocationDecisionHash`.

Stored authorization verifies both hashes, the exact approved intent binding, approval state, and expiry. Submission checks allocation freshness independently, and simulated fill processing checks it again before each fill.

## Audit lineage

Targeted execution submit and approval audit payloads include:

- `tradeIntentHash`;
- `portfolioAllocationHash`;
- `portfolioAllocationDecisionHash`;
- `capitalRiskSnapshotHash`;
- `riskDecisionHash`;
- final overseer decision.

This gives an operator a content-addressed path from the executed paper/demo trade back through portfolio sizing and capital admission.

## Migration behavior

Drafts created before Overseer v3 / PortfolioAllocation v1 do not contain the required allocation evidence. They fail closed after this code is deployed. Do not grandfather them. Recreate any still-valid intent through the normal request path so it receives fresh allocator, capital-risk, and overseer evidence.

## Deliberate limitations

This slice does not claim that autonomous live capital is ready. Remaining work includes, at minimum:

- production-quality empirical correlation/volatility ingestion with freshness and provenance guarantees;
- stronger multi-account / multi-currency aggregation and FX treatment;
- venue settlement, tax, borrow/funding and liquidation-risk modeling where applicable;
- portfolio objective optimization across multiple simultaneously competing candidate trades rather than one-at-a-time admission;
- shadow/live slippage and liquidity calibration;
- externally attested or signed risk-state evidence for stronger hostile-storage assumptions;
- live execution certification, supervised canary rules and explicit real-capital authorization.

## Safety invariant

`live` remains unconditionally blocked by the execution overseer. The portfolio allocator is a paper/demo capital-control layer and must not be used as justification for enabling autonomous real-capital trading.
