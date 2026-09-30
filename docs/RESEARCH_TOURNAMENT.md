# Research Tournament, Multiple-Testing, Dependence, and Terminal Holdout Contract

Status: canonical research-selection boundary for challenger promotion. This document does not authorize live trading.

## Why this boundary exists

Walk-forward validation does not by itself control two important research failure modes:

1. trying enough candidate variants until one looks good by chance; and
2. counting a cluster of serially related winning trades as though every trade were an independent confirmation.

A third failure mode is repeated inspection of a final holdout followed by candidate changes. The tournament therefore controls search budget, a coarse dependence horizon, and one-shot terminal evaluation.

```text
canonical full dataset
  -> commit search / embargo / terminal partition
  -> precommit candidate budget + family-wise alpha + dependence block horizon
  -> expose search manifest only
  -> record every candidate trial
  -> exact trade-level sign test
  -> fold-local non-overlapping block sign test
  -> Bonferroni correction against the full committed candidate budget
  -> require both marginal and block gates
  -> deterministically seal one eligible candidate
  -> open terminal window once for that candidate
  -> canonical compiled-Rust replay
  -> terminal evidence + append-only lineage
  -> challenger promotion gate
```

A terminal failure is final for that experiment.

## Experiment commitment

`scripts/research_tournament.py` builds a chronological plan from an exact canonical feed-cache snapshot:

```text
[ search ][ embargo ][ terminal holdout ]
```

Before candidate search begins, the experiment hash binds dataset manifests, strategy family, embargo/terminal boundaries, selection policy, candidate budget, family-wise alpha, minimum trade count, dependence block size, minimum nonzero block count, and derived per-trial alpha.

Default new-experiment policy:

```text
method                         precommitted_bonferroni_exact_sign_block_v2
block method                   nonoverlapping_fold_block_sign_v1
maximum candidate trials       20
family-wise alpha               0.05
minimum nonzero OOS trades      10
dependence block size           5 chronological trades per fold
minimum nonzero blocks          5
per-trial alpha                 0.0025
```

Changing the candidate family, alpha, block size, or minimum block count changes the experiment hash. These quantities cannot be relaxed after a favorable candidate appears.

Legacy v1 policy artifacts remain independently verifiable with their original semantics; the block gate applies to newly created v2 experiments rather than silently changing old evidence.

## Candidate lineage, budget, and concurrency

Every candidate submitted with a usable candidate ID consumes one committed search slot, including rejected and malformed candidates. Noncanonical evidence is persisted as a minimal fail-closed rejection envelope with finite conservative metrics rather than disappearing from search history.

`LineageStore.append()` serializes the complete hash-chain mutation under an interprocess lock. Tournament mutations use a separate registry lock across `create_from_snapshot`, `register_candidate`, `seal_selection`, and `run_terminal_holdout`. Lock order is registry -> lineage. This prevents cooperating concurrent workers from losing trials, oversubscribing the search budget, sealing two selections, or opening the terminal window twice through load/modify/save races.

Lineage and tournament persistence use strict JSON. NaN/Infinity, explicit duplicate lineage IDs, malformed replay-attestation shapes, and unserializable candidate payloads are rejected or converted into recorded fail-closed trials before dangerous state transitions.

These are local coordination/integrity controls, not protection against a privileged process that deliberately ignores locks or rewrites files.

## Marginal multiple-testing gate

The first candidate-level test remains an exact one-sided sign test over verified OOS trade returns:

```text
H0: P(trade return > 0) <= 0.5
H1: P(trade return > 0) > 0.5
```

Zero returns are omitted. Exact binomial-tail arithmetic remains in arbitrary-precision integers until the final bounded float conversion, so large samples do not overflow merely because `2**n` exceeds floating-point range.

The full precommitted candidate family is always used:

```text
per_trial_alpha = familywise_alpha / max_candidate_trials
marginal_adjusted_p = min(1, marginal_raw_p * max_candidate_trials)
```

Stopping early does not relax the threshold.

## Dependence-aware block companion

The v2 policy adds a deliberately coarse second gate. It is intended to prevent an obvious correlated streak from masquerading as many independent confirmations without pretending that a small dataset can support highly precise dependence modeling.

For each walk-forward fold independently:

1. preserve chronological trade-return order;
2. partition the fold into non-overlapping blocks of the precommitted size;
3. count positive and negative trades inside each block;
4. cast one positive vote if positives > negatives, one negative vote if negatives > positives, or a zero vote on a tie;
5. never construct a block across a fold boundary.

An exact one-sided sign test is then run over the block votes:

```text
block_adjusted_p = min(1, block_raw_p * max_candidate_trials)
```

A v2 candidate is eligible only if all of the following hold:

- minimum nonzero trade count is met;
- marginal raw p-value <= precommitted per-trial alpha;
- minimum nonzero block count is met;
- block raw p-value <= precommitted per-trial alpha.

For deterministic candidate ranking, the assessment exposes:

```text
adjusted_p_value = max(marginal_adjusted_p, block_adjusted_p)
```

This means the weaker of the two tests controls the candidate's significance ordering.

### What this does not prove

The block test does not prove that adjacent blocks are independent, determine an optimal dependence horizon, or solve long-memory/regime dependence. It is a conservative guard against naive per-trade sample-size inflation.

A future stationary/bootstrap, HAC-style, or other dependence-aware method should be added only when enough shadow/real outcome data exist to justify and calibrate it. Statistical sophistication is not itself a system objective.

## Deterministic selection

`seal_selection()` closes candidate intake and recomputes the exact assessment for every eligible candidate. `familywise_significant_net_pnl_then_quality_v2` ranks corrected-eligible candidates by:

1. smaller conservative adjusted p-value;
2. higher after-cost net P&L;
3. higher profit factor;
4. lower maximum drawdown;
5. higher annualized return;
6. evidence hash and candidate ID tie-breakers.

The selection artifact binds the complete v1/v2 policy and selected assessment. Selection lineage is parented to the complete registered candidate-trial event set.

## One-shot terminal evaluation

`run_terminal_holdout()` is allowed only after selection and only when no prior terminal evidence/event exists. It re-verifies the selected candidate's search assessment before loading terminal rows, reloads the exact committed terminal range, requires manifest equality, executes the selected strategy/config through canonical compiled Rust replay, stores raw terminal returns, recomputes metrics, binds the search assessment/policy/trial count, appends one terminal lineage event, and source-reverifies the finished artifact.

Default terminal policy:

```text
minimum completed trades      5
positive total return         required
minimum profit factor         1.00
maximum drawdown              25%
```

Changing thresholds or selecting another candidate after a terminal result requires a new experiment.

## Verification contract

Terminal verification recomputes the selected search assessment from embedded OOS returns and the committed policy. For v2 experiments this includes the exact block construction, block counts, block p-value, marginal p-value, family correction, and conservative adjusted p-value. It also verifies evidence hashes, strategy config identity, terminal metrics, exact canonical source replay, candidate-trial count, experiment/selection/terminal lineage bindings, and the one-shot terminal invariant.

## Economic role of this system

The tournament is a **false-discovery and evidence-integrity gate**, not an alpha generator and not a profit guarantee.

It becomes economically useful only when the production runtime actually consumes the exact certified strategy/config/evidence identity and when downstream expected-value calculations are expressed in coherent economic units. The current architectural value review is documented in `docs/TRADING_SYSTEM_VALUE_REVIEW.md`.

In particular, research rigor does not compensate for a runtime that selects strategies through a different in-sample gate, an opportunity layer that mixes scores with dollar costs, or an overseer that carries evidence/edge fields without using them for entry admission.

## Trust boundary and remaining work

The tournament cannot prove that a privileged researcher did not run unregistered experiments or read terminal data out-of-band. Local SHA-256 lineage and advisory locks are not cryptographic protection against privileged mutation.

Higher-value remaining work now includes:

- bind certified candidate/config/evidence identity into the actual runtime signal path;
- replace score-like opportunity EV with payoff-based expected dollars and explicit cost deductions;
- require positive executable edge plus hard risk authorization for new entry intents while preserving unrestricted risk-reduction/exit semantics;
- shadow-trade the exact production decision path and measure forecast calibration, slippage, latency, realized P&L, regret and drawdown;
- then calibrate probability models, forecast weights, sizing and dependence horizons from observed outcomes;
- later add isolated research-runner access controls, signed runner/build identity and externally immutable evidence storage.

## Safety invariant

Passing v2 search control plus a terminal holdout authorizes, at most, the existing supervised canary metadata path. It does not certify real-capital execution. Live trading remains blocked and requires separate shadow-live calibration, operational acceptance and explicit human authorization.
