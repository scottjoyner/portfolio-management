# Research Tournament, Multiple-Testing, and Terminal Holdout Contract

Status: canonical research-selection boundary for challenger promotion. This document does not authorize live trading.

## Why this boundary exists

Walk-forward validation is useful for model selection, but two forms of leakage remain if research is unconstrained:

1. an agent can try enough candidate variants until one looks good by chance; and
2. an agent can repeatedly inspect a final test window, modify the candidate, and try again.

The research tournament now controls both paths inside the supported workflow.

```text
canonical full dataset
  -> commit search / embargo / terminal partition
  -> precommit candidate-search budget + family-wise alpha
  -> expose search manifest only
  -> run and record every candidate trial on search data
  -> exact one-sided sign test on verified OOS trade returns
  -> Bonferroni correction against the full committed search budget
  -> deterministically seal one corrected-eligible candidate
  -> open terminal window for that selected candidate only
  -> canonical compiled-Rust replay
  -> terminal evidence + append-only lineage
  -> challenger promotion gate
```

A terminal failure is final for that experiment. The same experiment cannot select another candidate or rerun its terminal test after seeing the result.

## Experiment commitment

`scripts/research_tournament.py` builds an immutable chronological plan from an exact canonical feed-cache snapshot:

```text
[ search ][ embargo ][ terminal holdout ]
```

Before the first candidate is registered, the plan binds:

- experiment ID and strategy family;
- full dataset manifest/hash;
- search dataset manifest/hash;
- embargo length and embargo-row hash;
- terminal dataset manifest/hash;
- terminal bar count;
- deterministic selection-policy version;
- multiple-testing method;
- maximum candidate-trial budget;
- family-wise alpha;
- minimum nonzero trade count for the search significance test;
- derived per-trial Bonferroni alpha;
- experiment timestamp and SHA-256 experiment hash.

The default multiplicity policy is:

```text
method                         precommitted_bonferroni_exact_sign_v1
maximum candidate trials       20
family-wise alpha               0.05
minimum nonzero OOS trades      10
per-trial alpha                 0.0025
```

Changing the family size or alpha changes the experiment hash. Search therefore cannot observe a favorable candidate and then retroactively declare that fewer trials were intended.

The experiment plan contains the terminal manifest and content hash but not terminal OHLCV rows. Research agents should receive the committed plan and search data, not the terminal source rows.

## Candidate-trial lineage and budget

Every candidate submitted through the tournament is recorded, including rejected candidates. Every successfully registered candidate consumes one slot from the precommitted family size whether it ultimately fails alpha validation, replay provenance, dataset binding, walk-forward policy, multiplicity control, or evidence canonicalization.

A submission with a usable `candidate_id` but noncanonical evidence is not allowed to disappear from search history. The tournament stores a minimal fail-closed rejection envelope, conservative finite selection metrics, `validation_evidence_hash = null`, and an explicit `candidate_evidence_not_canonical` reason. That rejected trial still receives an index and lineage event and consumes search budget. This prevents malformed/NaN/unserializable evidence from becoming a way to perform uncounted candidate searches.

`register_candidate()` hard-stops when the committed budget is exhausted. A 20-trial experiment cannot register a 21st candidate and cannot enlarge the budget in place because the budget is part of the experiment hash.

A canonical trial carries alpha evidence that is checked for:

- internal alpha-evidence validity;
- canonical replay provenance;
- exact committed search dataset ID/hash;
- experiment strategy family;
- walk-forward pass state;
- precommitted family-wise search significance.

Each trial records candidate/source/config/evidence identity, deterministic selection metrics, the complete multiplicity assessment, eligibility/failure reasons, trial index, and append-only lineage ID/hash.

Recording failed trials matters: winner-only logging understates the amount of search performed and invalidates the family-size claim.

## Concurrent mutation integrity

Atomic file replacement alone is not enough to protect a hash chain or tournament budget from concurrent writers: two processes can read the same old head, independently construct the same next sequence/trial index, and then overwrite one another.

The research stack therefore serializes mutations with POSIX advisory file locks:

- `LineageStore.append()` holds an exclusive sibling lock across the complete read -> parent validation -> sequence/hash construction -> atomic replacement transaction;
- `ResearchTournament` holds a separate registry lock across each complete `create_from_snapshot`, `register_candidate`, `seal_selection`, and `run_terminal_holdout` state transition;
- tournament mutation lock order is registry -> lineage, so concurrent tournament workers cannot lose a trial, oversubscribe the candidate budget, seal two selections from the same state, or open the terminal window twice through a load/modify/save race;
- `create()` delegates to the locked `create_from_snapshot()` path rather than taking the same registry lock twice.

Lineage JSON is strict (`NaN`/`Infinity` rejected), explicit duplicate lineage IDs are rejected before mutation, and a lineage event is canonicalized/hashable before its file is changed. The lineage file is fsynced before atomic replacement and the parent directory is fsynced on supported filesystems for stronger crash durability.

Candidate registration also proves that the complete persisted trial is strict-JSON encodable **before** appending its candidate-trial lineage event. This specifically prevents malformed candidate evidence from leaving lineage ahead of registry state due to a predictable serialization failure.

These locks protect cooperating local processes. They are not a substitute for a trusted/isolated runner against a privileged process that can ignore advisory locks or rewrite the filesystem.

## Multiple-testing control

`scripts/selection_bias.py` implements a deterministic one-sided exact sign test over the verified OOS trade returns embedded in canonical alpha evidence.

The candidate null is:

```text
H0: P(trade return > 0) <= 0.5
H1: P(trade return > 0) > 0.5
```

Zero-return trades are omitted from the effective sample. At the boundary null, the number of positive returns is exactly `Binomial(n, 0.5)`, so the raw upper-tail p-value is deterministic and does not depend on a normal-return assumption or on return magnitudes.

The exact binomial tail keeps the power-of-two denominator and combinatorial sum in arbitrary-precision integer arithmetic until the final bounded probability conversion. Large OOS samples therefore do not fail merely because `2**n` exceeds floating-point exponent range; regression coverage exercises 2,000 nonzero trades.

Family-wise correction is deliberately conservative:

```text
per_trial_alpha = familywise_alpha / max_candidate_trials
adjusted_p      = min(1, raw_p * max_candidate_trials)
```

The **full committed family size** is always used. If an experiment commits to 20 candidate trials and stops after the first attractive result, that candidate is still tested as one member of a 20-hypothesis family. Early stopping therefore cannot make the threshold easier.

A candidate is multiplicity-eligible only if:

- its effective nonzero OOS trade count meets the committed minimum; and
- its raw exact-sign p-value is at or below the precommitted per-trial alpha.

This control is intentionally stricter than merely recording the number of candidates. It means a profitable-looking walk-forward artifact can still be rejected if its positive-return direction is not strong enough after the precommitted family-wise correction.

### Statistical limitation

The exact sign test does **not** make serially dependent trade outcomes independent. Bonferroni controls multiplicity across the declared candidate family, but dependence-aware inference within a candidate remains a separate requirement. A later slice should add block/bootstrap or another explicitly justified dependence-aware test rather than weakening this gate.

## Deterministic selection

`seal_selection()` permanently closes candidate intake for the experiment and independently recomputes the multiplicity assessment for every eligible candidate.

The versioned policy `familywise_significant_net_pnl_then_quality_v2` ranks only corrected-eligible candidates by:

1. smaller family-wise adjusted p-value;
2. higher after-cost net P&L;
3. higher profit factor;
4. lower maximum drawdown;
5. higher annualized return;
6. evidence hash and candidate ID as deterministic tie-breakers.

The selection artifact binds the experiment hash, complete multiplicity policy, selected assessment, trial count, maximum trial budget, remaining budget, ranked eligible IDs, selected metrics, timestamp, and selection hash. The `candidate_selection` lineage event is parented to the complete set of registered candidate-trial lineage events.

## One-shot terminal evaluation

`run_terminal_holdout()` is allowed only after selection and only when the experiment has no prior terminal event/evidence. Before loading terminal rows it re-verifies the selected candidate's multiplicity assessment against the original alpha OOS returns and checks that the candidate-trial count has not exceeded the committed budget.

The runner then:

1. reloads the exact committed terminal time range from canonical feed cache;
2. requires the source manifest to match the pre-search commitment;
3. uses the exact selected strategy/config, warmup, fees and hold controls;
4. replays terminal rows through the canonical compiled Rust path;
5. stores raw terminal trade returns and their hash;
6. recomputes terminal performance metrics and policy state;
7. binds the committed multiplicity policy, selected assessment, exact flattened search OOS returns/hash, actual trial count and maximum trial budget into terminal evidence;
8. writes one terminal lineage event parented to the sealed selection;
9. hashes and immediately source-reverifies the complete terminal artifact.

Default terminal policy:

```text
minimum completed trades      5
positive total return         required
minimum profit factor         1.00
maximum drawdown              25%
```

If the selected candidate fails, the experiment becomes `terminal_failed`. Changing thresholds or choosing a replacement candidate requires a new experiment with a newly committed terminal window.

## Verification contract

`verify_terminal_holdout_evidence()` checks, among other invariants:

- schema/method/selection-policy versions;
- final evidence and terminal-result hashes;
- terminal and search-return hashes;
- canonical multiple-testing policy shape;
- trial count is positive and no greater than the committed budget;
- selected sign-test/Bonferroni assessment recomputes exactly from embedded search OOS returns;
- selected candidate remains family-wise significant;
- normalized strategy config/hash;
- recomputed terminal metrics and pass/fail reasons;
- exact terminal feed-cache manifest and regenerated compiled-Rust returns;
- complete append-only lineage chain;
- experiment lineage committed the same multiplicity policy;
- number of `candidate_trial` lineage events equals the recorded trial count;
- selected trial lineage carries the same multiplicity assessment;
- selection lineage carries the same policy/assessment;
- the selection parent set exactly equals the full candidate-trial event set;
- exactly one terminal event exists and it is parented to the sealed selection.

Replaying the already committed selected candidate solely to verify persisted evidence is not a new candidate trial and does not create another terminal event.

## Challenger promotion binding

`ChallengerRegistry.evaluate()` preserves canonical alpha/replay diagnostics first. If alpha/replay would otherwise approve, terminal evidence additionally must verify against source and lineage, pass its terminal policy, match candidate ID/config/source SHA, and bind the exact alpha-evidence hash used by the promotion gate.

An approved challenger persists complete alpha and terminal artifacts. `promote()` independently re-verifies both source chains and all identity/hash bindings before writing supervised canary metadata. Canary config, evaluation lineage and promotion lineage carry both evidence hashes.

Because the terminal evidence itself binds the committed search policy, trial lineage and selected multiplicity assessment, a candidate cannot reach the supported promotion path merely by presenting a good alpha score after an unbounded search.

## Trust boundary and remaining limitations

This stack controls candidate multiplicity and concurrent state mutation **inside the canonical tournament**. It cannot prove that a privileged researcher did not run additional unregistered experiments elsewhere or read the underlying terminal feed cache out-of-band. Local SHA-256 lineage and advisory locks are deterministic integrity/coordination mechanisms, not cryptographic protection against privileged process/filesystem mutation.

Remaining scientific/trust work includes:

- dependence-aware block/bootstrap inference for serially correlated returns;
- trusted/isolated research-runner access controls so unregistered searches and terminal reads are not available to agents;
- signed build/runner identity and externally immutable evidence storage;
- regime/session/asset concentration diagnostics;
- empirical shadow/live slippage and latency distributions;
- broader typed configurable replay and stability coverage.

The legacy `scripts/backtest_framework/promote.py` helper is a separate config-generation utility, not challenger/capital-promotion authority. It can emit a caller-requested LIVE-labeled YAML and remains quarantined; do not use it to enable live trading.

## Safety invariant

Passing corrected search selection plus a terminal holdout authorizes, at most, the existing supervised canary metadata path. It does not certify real-capital execution. Live trading remains blocked and requires separate shadow-live calibration, adapter/runtime certification, operational acceptance and explicit human authorization.
