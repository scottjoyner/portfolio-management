// Tests for the economic decision classifier replay.
//
// The load-bearing theme: a backtest that cannot fail is worse than no backtest,
// because it produces confident numbers. So the controls are tested as controls,
// the fold generator is pinned against the Python implementation it mirrors, and
// the headline finding is asserted so it cannot silently invert.

import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

import {
  buildAttestation,
  buildDatasetManifest,
  controls,
  DEFAULT_COST_MODEL,
  generateWalkForwardSplits,
  runReplay,
  runReplayTier,
  scoreReplay,
  stableHash,
  REPLAY_ATTESTATION_TYPE,
} from '../scripts/run-decision-classifier-replay.mjs';

const CONFORMANCE = JSON.parse(
  readFileSync(new URL('./fixtures/decision-replay-conformance.json', import.meta.url), 'utf8'),
);

// ── fold generator conformance ──────────────────────────────────────────────
// scripts/run-decision-classifier-replay.mjs reimplements the walk-forward split
// layout from scripts/alpha_validation.py in Node, because the decision engine is
// JavaScript. Two divergent implementations already exist in this repo -- the two
// economicDecisionEngine forecast paths disagree on one divisor and duplicate
// every helper constant -- so the second one is pinned against the first by
// fixtures generated from Python rather than trusted.

test('walk-forward splits match the Python implementation exactly', () => {
  assert.ok(CONFORMANCE.folds.length >= 20, 'conformance fixtures should cover many shapes');
  for (const testCase of CONFORMANCE.folds) {
    const actual = generateWalkForwardSplits(testCase.n, {
      trainSize: testCase.trainSize,
      testSize: testCase.testSize,
      purgeSize: testCase.purgeSize,
      embargoSize: testCase.embargoSize,
    });
    const mapped = actual.map(fold => ({
      train_start: fold.trainStart, train_end: fold.trainEnd,
      purge_start: fold.purgeStart, purge_end: fold.purgeEnd,
      embargo_start: fold.embargoStart, embargo_end: fold.embargoEnd,
      test_start: fold.testStart, test_end: fold.testEnd,
    }));
    assert.deepEqual(mapped, testCase.folds,
      `folds diverged for n=${testCase.n} train=${testCase.trainSize} test=${testCase.testSize} purge=${testCase.purgeSize} embargo=${testCase.embargoSize}`);
  }
});

test('the purge and embargo gaps are actually excluded from train and test', () => {
  const folds = generateWalkForwardSplits(1000, { trainSize: 200, testSize: 200, purgeSize: 10, embargoSize: 5 });
  assert.ok(folds.length);
  for (const fold of folds) {
    // No overlap: train ends before purge, purge before embargo, embargo before
    // test. A fold that leaked embargo bars into training is the classic
    // look-ahead bug, and it inflates every number downstream.
    assert.ok(fold.trainEnd <= fold.purgeStart);
    assert.ok(fold.purgeEnd <= fold.embargoStart);
    assert.ok(fold.embargoEnd <= fold.testStart);
    assert.ok(fold.purgeEnd - fold.purgeStart === 10);
    assert.ok(fold.embargoEnd - fold.embargoStart === 5);
  }
});

test('degenerate fold configurations return nothing rather than throwing', () => {
  assert.deepEqual(generateWalkForwardSplits(0, { trainSize: 10, testSize: 10 }), []);
  assert.deepEqual(generateWalkForwardSplits(100, { trainSize: 200, testSize: 50 }), []);
  assert.deepEqual(generateWalkForwardSplits(100, { testSize: 50 }), []);
  assert.deepEqual(generateWalkForwardSplits(100, { trainSize: 10 }), []);
  assert.throws(() => generateWalkForwardSplits(100, { trainSize: 10, testSize: 10, purgeSize: -1 }), /non-negative/);
});

// ── canonical hashing ───────────────────────────────────────────────────────

test('stableHash matches alpha_validation.stable_hash for the same payloads', () => {
  CONFORMANCE.hash_payloads.forEach((payload, index) => {
    assert.equal(stableHash(payload), CONFORMANCE.hashes[index],
      `hash diverged for payload ${index}`);
  });
});

test('stableHash is key-order independent but content sensitive', () => {
  assert.equal(stableHash({ a: 1, b: 2 }), stableHash({ b: 2, a: 1 }));
  assert.notEqual(stableHash({ a: 1 }), stableHash({ a: 2 }));
  assert.notEqual(stableHash({ a: [1, 2] }), stableHash({ a: [2, 1] }), 'array order is data');
});

// ── dataset attestation ─────────────────────────────────────────────────────

const SAMPLE_ROWS = [
  { t: 1_700_000_000, open: 1, high: 2, low: 0.5, close: 1.5, volume: 10 },
  { t: 1_700_003_600, open: 1.5, high: 2.5, low: 1, close: 2, volume: 12 },
  { t: 1_700_007_200, open: 2, high: 2.6, low: 1.8, close: 1.9, volume: 9 },
];

test('the dataset manifest binds the rows, not just the bounds', () => {
  const manifest = buildDatasetManifest({ symbol: 'BTC-USD', granularity: 3600, rows: SAMPLE_ROWS });
  assert.equal(manifest.row_count, 3);
  assert.equal(manifest.kind, 'coinbase_candles');
  assert.ok(manifest.rows_hash && manifest.dataset_hash);
  // Same start and end, different values: a manifest that only hashed the bounds
  // would collide, and two different datasets could then produce interchangeable
  // evidence.
  const tampered = buildDatasetManifest({
    symbol: 'BTC-USD', granularity: 3600,
    rows: [{ ...SAMPLE_ROWS[0], close: 1.6 }, SAMPLE_ROWS[1], SAMPLE_ROWS[2]],
  });
  assert.notEqual(manifest.rows_hash, tampered.rows_hash);
  assert.notEqual(manifest.dataset_hash, tampered.dataset_hash);
});

test('the attestation hash covers fold boundaries and cost assumptions', () => {
  const manifest = buildDatasetManifest({ symbol: 'BTC-USD', granularity: 3600, rows: SAMPLE_ROWS });
  const folds = generateWalkForwardSplits(3, { trainSize: 1, testSize: 1 });
  const base = buildAttestation({ manifest, folds, costModel: DEFAULT_COST_MODEL, config: { lookbackBars: 24 } });
  assert.equal(base.attestation_type, REPLAY_ATTESTATION_TYPE);

  const differentFolds = buildAttestation({
    manifest, folds: generateWalkForwardSplits(3, { trainSize: 1, testSize: 1, purgeSize: 1 }),
    costModel: DEFAULT_COST_MODEL, config: { lookbackBars: 24 },
  });
  assert.notEqual(base.attestation_hash, differentFolds.attestation_hash,
    'rescoring the same candles on different folds must not produce the same attestation');

  const differentCosts = buildAttestation({
    manifest, folds, costModel: { ...DEFAULT_COST_MODEL, takerFeeRate: 0.01 }, config: { lookbackBars: 24 },
  });
  assert.notEqual(base.attestation_hash, differentCosts.attestation_hash,
    'the cost assumption decides every permission, so it must be inside the hash');

  assert.equal(base.attestation_hash, buildAttestation({ manifest, folds, costModel: DEFAULT_COST_MODEL, config: { lookbackBars: 24 } }).attestation_hash,
    'and it must be reproducible');
});

// ── synthetic rows for replay tests ─────────────────────────────────────────
// Real numbers, hand-built so the arithmetic is checkable by eye. Not read from
// the candle cache: these tests assert replay behaviour, not market behaviour.

function replayableRows(count = 300) {
  const rows = [];
  let price = 100;
  for (let index = 0; index < count; index += 1) {
    // Deterministic alternating drift, so both up and down outcomes occur.
    price *= 1 + (index % 7 < 4 ? 0.004 : -0.005);
    const high = price * 1.003;
    const low = price * 0.997;
    rows.push({
      t: 1_700_000_000 + index * 3600,
      open: price, high, low, close: price, volume: 100 + index,
      symbol: 'BTC-USD',
    });
  }
  return rows;
}

const BASE_OPTIONS = { lookbackBars: 24, horizonBars: 1, granularityMinutes: 60, notionalUsd: 1000 };

test('a historical replay is not blocked for cost-snapshot staleness', () => {
  // The trap this test exists for: ingestExecutionCostSnapshot defaults validUntil
  // to now + 30s, and evaluateEconomicDecision requires
  // executionCost.validUntil >= now. A replay that forgets to set it has 100% of
  // its decisions blocked for staleness, and reports a gate that refuses
  // everything for reasons that have nothing to do with economics.
  const rows = replayableRows();
  const { results } = runReplay({ rows, options: { ...BASE_OPTIONS, costModel: { ...DEFAULT_COST_MODEL, takerFeeRate: 0, makerFeeRate: 0, spreadMultiplier: 0 } } });
  assert.ok(results.length > 100);
  const errored = results.filter(row => row.errors);
  assert.deepEqual(errored, [], 'no window should error');
  assert.ok(
    results.every(row => !row.blockers.includes('execution_cost_snapshot_stale')),
    'historical windows must not be blocked for staleness',
  );
  assert.ok(results.every(row => !row.blockers.includes('forecast_stale_or_invalid')),
    'nor for forecast staleness');
});

test('costs are derived from the bar, not defaulted to zero', () => {
  const rows = replayableRows();
  const { results } = runReplay({ rows, options: { ...BASE_OPTIONS, notionalUsd: 1000 } });
  const withSpread = results.filter(row => !row.errors);
  assert.ok(withSpread.length);
  for (const row of withSpread.slice(0, 5)) {
    // 0.6% taker on 1000 USD is 6 USD before any spread.
    assert.ok(row.totalExecutionCostUsd >= 6, `cost too low: ${row.totalExecutionCostUsd}`);
    assert.ok(row.spreadBps > 0, 'spread must come from the bar range');
  }
});

// ── the headline finding, asserted ──────────────────────────────────────────

test('the gate refuses every window because expected move is smaller than costs', () => {
  // This is the result the replay exists to establish, so it is pinned. If a
  // future change makes the gate permissive at realistic fees, that is worth a
  // deliberate look, not a silent pass.
  const rows = replayableRows(400);
  const { results } = runReplay({ rows, options: { ...BASE_OPTIONS, notionalUsd: 1000 } });
  const score = scoreReplay(results);
  assert.ok(score.windows > 100);
  assert.equal(score.gate.permitted.n, 0, 'at 0.6% taker the gate should permit nothing');
  assert.ok(score.economics.costToEdgeRatio > 1,
    'expected move must be shown to be smaller than the cost of executing it');
  assert.ok(score.economics.meanExecutionCostUsd > score.economics.meanPredictedEdgeUsd);
});

test('the gate has two independent blockers, and each alone refuses every window', () => {
  // netExecutableEdgeUsd subtracts execution costs AND an uncertainty reserve of
  // 0.25 * notional * sigma. Zeroing only one still permits nothing, so neither
  // term is merely the reason the other is high. Both are individually sufficient,
  // and that is what makes the result above actionable rather than just "it says
  // no".
  const rows = replayableRows(400);
  const zeroCost = { ...DEFAULT_COST_MODEL, takerFeeRate: 0, makerFeeRate: 0, spreadMultiplier: 0 };

  const freeButReserved = scoreReplay(runReplay({
    rows, options: { ...BASE_OPTIONS, costModel: zeroCost },
  }).results);
  assert.equal(freeButReserved.gate.permitted.n, 0, 'the uncertainty reserve alone should still refuse everything');

  const reservedButFree = scoreReplay(runReplayTier({
    rows, options: { ...BASE_OPTIONS }, body: { uncertaintyReserveUsd: 0 },
  }).results);
  assert.equal(reservedButFree.gate.permitted.n, 0, 'costs alone should still refuse everything');

  const neither = scoreReplay(runReplayTier({
    rows, options: { ...BASE_OPTIONS, costModel: zeroCost }, body: { uncertaintyReserveUsd: 0 },
  }).results);
  assert.ok(neither.gate.permitted.n > neither.windows * 0.4,
    `with both blockers removed the gate should admit most windows, got ${neither.gate.permitted.n}/${neither.windows}`);
});

test('windows the gate admits once unblocked still lose money', () => {
  // With no fee and no reserve, permission tracks the forecast's expected return
  // rather than the realized move -- and those windows are net negative. This is
  // the forecast's anti-signal surviving every excuse: it is not unprofitable
  // because of costs, it is unprofitable on direction.
  const rows = replayableRows(400);
  const score = scoreReplay(runReplayTier({
    rows,
    options: { ...BASE_OPTIONS, costModel: { ...DEFAULT_COST_MODEL, takerFeeRate: 0, makerFeeRate: 0, spreadMultiplier: 0 } },
    body: { uncertaintyReserveUsd: 0 },
  }).results);
  assert.ok(score.gate.permitted.n > 0);
  assert.ok(score.gate.permitted.meanNetReturnUsd < 0,
    `permitted windows averaged ${score.gate.permitted.meanNetReturnUsd}, expected negative`);
});

// ── controls ────────────────────────────────────────────────────────────────

test('controls pass: the oracle profits and random does not', () => {
  const rows = replayableRows(400);
  const result = controls({ rows, options: { ...BASE_OPTIONS } });
  assert.equal(result.passed, true, `controls failed: ${JSON.stringify(result.checks)}`);
  assert.ok(result.oracle.score.gate.permitted.n > 0, 'oracle must permit something');
  assert.ok(result.oracle.permittedMean > 0, 'oracle must profit');
  assert.ok(
    result.random.permittedMean <= result.oracle.permittedMean,
    'a random edge predictor must not beat an oracle',
  );
});

test('the forecast is scored against the always-50% reference', () => {
  const rows = replayableRows(400);
  const { results } = runReplay({ rows, options: { ...BASE_OPTIONS } });
  const score = scoreReplay(results);
  assert.equal(score.coinFlip.brierScore, 0.25);
  assert.equal(score.coinFlip.directionalAccuracy, 0.5);
  // The comparison is reported as a signed delta so "worse than a coin flip" is
  // visible without the reader doing the subtraction.
  assert.ok(Math.abs(score.forecast.always50BrierDelta - (score.forecast.brierScore - 0.25)) < 1e-12);
});

// ── the tier classifier is descriptive, not causal ──────────────────────────

test('every decision tier yields an identical gate outcome', () => {
  // selectedTier is telemetry. Swapping the classifier's output changes the label
  // and nothing else: no tier variant alters permission or P&L by even a cent.
  // That is worth pinning, because a future change that gives a tier real effect
  // would change what the system acts on, and should not happen by accident.
  const rows = replayableRows(400);
  const variants = [
    {},
    { localModelAvailable: true },
    { requestRemoteModel: true, expectedDecisionImprovementUsd: 50, probabilityDecisionChanges: 0.9 },
    { requestRemoteModel: true, expectedDecisionImprovementUsd: 500, probabilityDecisionChanges: 0.9 },
  ];
  const summaries = variants.map(body => {
    const { results } = runReplayTier({ rows, options: { ...BASE_OPTIONS }, body });
    const score = scoreReplay(results);
    return { tiers: Object.keys(score.tiers).sort(), permitted: score.gate.permitted.n, net: score.gate.refused.meanNetReturnUsd };
  });
  for (const summary of summaries) {
    assert.equal(summary.permitted, summaries[0].permitted);
    assert.equal(summary.net, summaries[0].net);
  }
  // And the classifier genuinely does fire, so the test above is not vacuous.
  assert.ok(summaries[0].tiers.includes('deterministic'));
  assert.ok(summaries[1].tiers.includes('local_model'), 'localModelAvailable should select local_model');
});

// ── out-of-sample discipline ────────────────────────────────────────────────

test('the replay evaluates only windows inside a fold test range', () => {
  // The single most consequential thing a walk-forward harness can get wrong.
  // Nothing else here would notice: scoring, attestation and the controls all keep
  // working perfectly while in-sample windows are quietly mixed into
  // "out-of-sample" results, and the numbers get better rather than obviously
  // broken.
  const rows = replayableRows(400);
  const folds = generateWalkForwardSplits(rows.length, {
    trainSize: 120, testSize: 60, purgeSize: 2, embargoSize: 2,
  });
  assert.ok(folds.length >= 2, 'fixture needs several folds');

  const { results } = runReplay({ rows, options: { ...BASE_OPTIONS, folds } });
  assert.ok(results.length > 0);

  const inside = index => folds.some(fold => index >= fold.testStart && index < fold.testEnd);
  for (const row of results) {
    const index = rows.findIndex(candidate => candidate.t === row.t);
    assert.ok(index >= 0, 'every scored row must come from the dataset');
    assert.ok(inside(index),
      `window at index ${index} is outside every test range: training data leaked into the OOS result`);
  }

  // And the count must be bounded by the test ranges, not the whole dataset.
  const testBars = folds.reduce((total, fold) => total + (fold.testEnd - fold.testStart), 0);
  assert.ok(results.length <= testBars,
    `scored ${results.length} windows but the folds only expose ${testBars} test bars`);
});

test('without folds the replay covers the whole dataset, which is why folds are required', () => {
  const rows = replayableRows(200);
  const foldless = runReplay({ rows, options: { ...BASE_OPTIONS } }).results;
  const folds = generateWalkForwardSplits(rows.length, { trainSize: 60, testSize: 30 });
  const folded = runReplay({ rows, options: { ...BASE_OPTIONS, folds } }).results;
  assert.ok(foldless.length > folded.length,
    'a foldless run scores more windows; if these were equal the fold filter would be inert');
});

// ── scoring arithmetic ──────────────────────────────────────────────────────

test('scoring groups and deltas are arithmetically correct', () => {
  const synthetic = [
    { netReturnUsd: 10, grossReturnUsd: 12, directionCorrect: true, brierScore: 0.1, executionAllowed: true, predictedEdgeUsd: 5, executionCostsUsd: 2, netExecutableEdgeUsd: 3, spreadBps: 1, expectedVolatilityBps: 10 },
    { netReturnUsd: -4, grossReturnUsd: -2, directionCorrect: false, brierScore: 0.4, executionAllowed: true, predictedEdgeUsd: 5, executionCostsUsd: 2, netExecutableEdgeUsd: 3, spreadBps: 1, expectedVolatilityBps: 10 },
    { netReturnUsd: -6, grossReturnUsd: -8, directionCorrect: false, brierScore: 0.25, executionAllowed: false, predictedEdgeUsd: 1, executionCostsUsd: 2, netExecutableEdgeUsd: -1, spreadBps: 1, expectedVolatilityBps: 10 },
  ].map((row, index) => ({ ...row, regime: 'low_volatility_range', selectedTier: 'deterministic', t: index }));

  const score = scoreReplay(synthetic);
  assert.equal(score.windows, 3);
  assert.equal(score.forecast.brierScore, (0.1 + 0.4 + 0.25) / 3);
  assert.equal(score.gate.permitted.n, 2);
  assert.equal(score.gate.permitted.meanNetReturnUsd, 3);
  assert.equal(score.gate.permitted.winRate, 0.5);
  assert.equal(score.gate.refused.meanNetReturnUsd, -6);
  // Permitted minus refused is the gate's whole value proposition.
  assert.equal(score.gate.permittedEdgeSpreadUsd, 9);
  // mean cost 2 divided by mean edge 11/3.
  assert.ok(Math.abs(score.economics.costToEdgeRatio - 2 / (11 / 3)) < 1e-12);
  assert.equal(score.economics.windowsWhereEdgeExceedsCost, 2);
  assert.equal(score.economics.meanUncertaintyReserveUsd, null,
    'a missing field must produce null, never NaN, so it cannot read as a finding');
});
