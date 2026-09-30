# Research Tournament and Terminal Holdout Contract

Status: canonical research-selection boundary for challenger promotion. This document does not authorize live trading.

## Why this boundary exists

Walk-forward validation is useful for model selection, but it is not an untouched final test if an agent can repeatedly inspect it, change parameters, and try again. The terminal-holdout controller separates iterative search from a final one-shot evaluation.

The trusted flow is:

```text
canonical full dataset
  -> commit search / embargo / terminal partition
  -> expose search manifest only
  -> run and record candidate trials on search data
  -> deterministically seal one selected candidate
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

The plan binds:

- experiment ID and strategy family;
- full dataset manifest/hash;
- search dataset manifest/hash;
- embargo length and embargo-row hash;
- terminal dataset manifest/hash;
- terminal bar count;
- deterministic selection-policy version;
- experiment timestamp and SHA-256 experiment hash.

The experiment plan contains the terminal manifest and content hash but not terminal OHLCV rows. Search infrastructure should give research agents the committed plan and search data, not the terminal source rows.

The partition is committed before candidate trials begin. Search and terminal timestamps must be disjoint, the embargo remains outside both windows, and the experiment refuses an undersized search or terminal region.

## Candidate-trial lineage

Every candidate submitted through the tournament is recorded, including rejected candidates.

A trial must carry canonical alpha evidence that:

- is internally valid;
- is replay-provenance-bound;
- uses the exact committed search dataset ID/hash;
- uses the experiment's strategy family;
- passed the walk-forward policy.

Each trial records:

- candidate ID/source SHA/config/hash;
- alpha-evidence hash and full artifact;
- deterministic selection metrics;
- eligibility state and failure reasons;
- append-only lineage ID/hash.

Recording failed trials matters: a tournament that logs only winners understates the amount of search performed and cannot support later selection-bias analysis.

## Deterministic selection

`seal_selection()` permanently closes candidate intake for the experiment.

Eligible candidates are ranked by the versioned policy `validated_net_pnl_then_quality_v1`:

1. higher after-cost net P&L;
2. higher profit factor;
3. lower maximum drawdown;
4. higher annualized return;
5. evidence hash and candidate ID as deterministic tie-breakers.

The selection artifact binds the experiment, selected candidate, alpha-evidence hash, candidate counts, ranked eligible IDs, selected metrics, timestamp, and selection hash. A `candidate_selection` lineage event is written before terminal data is opened.

This ranking policy is deterministic, but it is not a formal multiple-testing correction. Candidate-count/search lineage is intentionally preserved so a later statistical selection-bias layer can operate on the actual search history.

## One-shot terminal evaluation

`run_terminal_holdout()` is allowed only after selection and only when the experiment has no prior terminal event/evidence.

The runner:

1. reloads the exact committed terminal time range from canonical feed cache;
2. requires the source manifest to match the pre-search commitment;
3. takes the exact strategy name/config, warmup, fees and hold-time controls from the selected alpha replay attestation;
4. replays terminal rows through the canonical compiled Rust path;
5. stores raw terminal trade returns and their hash;
6. recomputes terminal performance metrics;
7. applies the terminal policy;
8. writes one `terminal_holdout` lineage event parented to the sealed selection;
9. hashes the complete evidence artifact;
10. immediately source-reverifies the artifact before persistence.

Default terminal policy:

```text
minimum completed trades      5
positive total return         required
minimum profit factor         1.00
maximum drawdown              25%
```

These are final-test admission floors, not an instruction to optimize on the terminal window. If the selected candidate fails, the experiment becomes `terminal_failed`; changing thresholds or choosing a replacement candidate requires a new experiment with a newly committed terminal window.

## Verification contract

`verify_terminal_holdout_evidence()` independently checks:

- schema/method/selection-policy versions;
- final evidence hash;
- terminal-result hash;
- raw-return hash;
- normalized strategy config/hash;
- recomputed terminal metrics and pass/fail reasons;
- exact terminal feed-cache manifest;
- regenerated compiled-Rust returns;
- complete append-only lineage chain;
- experiment/selection/terminal event hashes and bindings;
- exactly one terminal event for the experiment;
- selection -> terminal parent relationship.

Reverification is allowed and expected. Replaying the already committed selected candidate solely to verify the persisted evidence is not a new search trial and does not create another terminal lineage event.

## Challenger promotion binding

`ChallengerRegistry.evaluate()` preserves the existing alpha/replay diagnostics first. A challenger that fails canonical alpha/replay validation is rejected for those existing reasons before terminal evidence is considered.

If alpha/replay would otherwise approve, promotion eligibility additionally requires terminal evidence that:

- verifies against source and append-only lineage;
- passed its committed terminal policy;
- matches the challenger ID;
- matches the exact challenger config;
- matches the candidate source SHA;
- binds the exact alpha-evidence hash used by the promotion gate.

Missing terminal evidence fails closed with `terminal_holdout_evidence_required`.

An approved challenger persists both complete evidence artifacts and both hashes. `promote()` independently re-verifies alpha replay and terminal source/lineage again before writing canary state. Canary config, evaluation lineage and promotion lineage carry both evidence hashes.

Pre-terminal challenger approvals are therefore insufficient for new canary promotion. They must be reevaluated through the terminal-holdout path.

## Trust boundary and remaining limitations

This slice materially reduces repeated-final-test leakage, but it is not proof against a malicious researcher with unrestricted access to the underlying feed cache or filesystem.

Remaining scientific trust work includes:

- formal multiple-testing / selection-bias correction across the recorded candidate search;
- a trusted runner or access-control boundary that prevents research agents from reading terminal rows out-of-band;
- signed/build-attested execution identity and externally immutable evidence storage;
- block/bootstrap methods for serially correlated returns;
- regime/session/asset concentration diagnostics;
- broader typed configurable replay beyond currently supported configured strategy paths.

The legacy `scripts/backtest_framework/promote.py` helper is a separate config-generation utility, not challenger/capital-promotion authority. It can currently emit a caller-requested LIVE-labeled YAML and must be quarantined/hardened before it is ever treated as a runtime path. Do not use it to enable live trading.

## Safety invariant

Passing a terminal holdout authorizes, at most, the existing supervised canary metadata path. It does not certify real-capital execution. Live trading remains blocked by the execution boundary and requires separate shadow-live calibration, adapter/runtime certification, operational acceptance and explicit human authorization.