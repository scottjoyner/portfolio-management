// Conformance of the Node statistics against scripts/selection_bias.py, and the
// behaviour of the signal existence audit.
//
// The replication is the load-bearing part. A backtest that reports "no edge" is
// only meaningful if its test for edge is the same test the repo already trusts.
// These fixtures were generated from the Python module, and the Node
// implementation is pinned against them -- for the same reason the walk-forward
// splits are: this repo already carries two implementations that disagree.

import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

import {
  assessCandidateSignificance,
  binomialUpperTailHalf,
  CANDIDATES,
  DEFAULT_POLICY,
  exactNonoverlappingBlockSignTest,
  exactOneSidedSignTest,
  perTrialAlpha,
  predict,
  reserveSweep,
  runAudit,
} from '../scripts/run-signal-existence-audit.mjs';
import { generateWalkForwardSplits } from '../scripts/run-decision-classifier-replay.mjs';

const CONFORMANCE = JSON.parse(
  readFileSync(new URL('./fixtures/selection-bias-conformance.json', import.meta.url), 'utf8'),
);

const close = (a, b, tolerance = 1e-12) => Math.abs(a - b) <= tolerance;

// ── binomial tail ───────────────────────────────────────────────────────────

test('the binomial upper tail matches Python over a wide grid', () => {
  assert.ok(CONFORMANCE.tails.length > 200, 'grid should be dense');
  for (const point of CONFORMANCE.tails) {
    const expected = Number(point.p);
    const actual = binomialUpperTailHalf(point.n, point.k);
    // Python writes these with repr(), which is exact for doubles, so the values
    // round-trip through JSON without loss.
    assert.ok(close(actual, expected, 1e-15),
      `P[Bin(${point.n},0.5) >= ${point.k}] should be ${expected}, got ${actual}`);
  }
});

test('binomial tail edge cases', () => {
  assert.equal(binomialUpperTailHalf(10, 0), 1.0);
  assert.equal(binomialUpperTailHalf(10, 10), 2 ** -10);
  // Symmetry: P[X>=k] == 1 - P[X<=k-1], and P[X<=k-1] == P[X>=n-k+1].
  // The first version of this asserted P[X>=k] == 1 - P[X>=n-k], which is off by
  // one: P[X>=1] is 1 - P[X=0], not 1 - P[X>=9].
  for (const n of [10, 21, 40]) {
    for (let k = 1; k <= n; k += 1) {
      assert.ok(close(binomialUpperTailHalf(n, k), 1 - binomialUpperTailHalf(n, n - k + 1), 1e-12),
        `symmetry broken at n=${n} k=${k}`);
    }
  }
  assert.throws(() => binomialUpperTailHalf(5, 6));
  assert.throws(() => binomialUpperTailHalf(-1, 0));
});

// ── sign tests ──────────────────────────────────────────────────────────────

test('the one-sided sign test matches Python on randomized cases', () => {
  for (const testCase of CONFORMANCE.cases) {
    const actual = exactOneSidedSignTest(testCase.returns);
    assert.equal(actual.positiveTrades, testCase.sign.positive_trades);
    assert.equal(actual.negativeTrades, testCase.sign.negative_trades);
    assert.equal(actual.zeroTrades, testCase.sign.zero_trades);
    assert.equal(actual.nonzeroTrades, testCase.sign.nonzero_trades);
    assert.ok(close(actual.rawPValue, Number(testCase.sign.raw_p_value), 1e-15),
      `p-value diverged: ${actual.rawPValue} vs ${testCase.sign.raw_p_value}`);
  }
});

test('the non-overlapping block sign test matches Python', () => {
  for (const testCase of CONFORMANCE.cases) {
    const actual = exactNonoverlappingBlockSignTest(testCase.folds, 5);
    assert.equal(actual.positiveBlocks, testCase.block.positive_blocks, `n=${testCase.returns.length}`);
    assert.equal(actual.negativeBlocks, testCase.block.negative_blocks);
    assert.equal(actual.zeroBlocks, testCase.block.zero_blocks);
    assert.equal(actual.nonzeroBlocks, testCase.block.nonzero_blocks);
    assert.equal(actual.totalBlocks, testCase.block.total_blocks);
    assert.ok(close(actual.rawPValue, Number(testCase.block.raw_p_value), 1e-15));
  }
});

test('candidate significance matches Python, including verdicts and reasons', () => {
  for (const testCase of CONFORMANCE.cases) {
    const actual = assessCandidateSignificance(testCase.folds, DEFAULT_POLICY);
    assert.equal(actual.passed, testCase.passed, `verdict diverged for ${testCase.returns.length} returns`);
    assert.deepEqual(actual.reasons.slice().sort(), testCase.reasons.slice().sort());
    assert.ok(close(actual.adjustedPValue, Number(testCase.adjusted), 1e-15));
    // Both components separately. The headline is a max(), and the block term is
    // more conservative in most fixtures, so asserting only the max let a removed
    // Bonferroni multiplier on the marginal term pass unnoticed.
    assert.ok(close(actual.marginalAdjustedPValue, Number(testCase.marginal_adjusted), 1e-15),
      `marginal adjusted p diverged`);
    assert.ok(close(actual.dependence.adjustedPValue, Number(testCase.block_adjusted), 1e-15),
      `block adjusted p diverged`);
  }
});

test('blocks never cross fold boundaries', () => {
  // The block test is the dependence guard. If blocks spanned folds, a single
  // lucky fold would vote in several blocks and the correction would silently
  // overstate confidence.
  const folds = [[1, 1, 1], [-1, -1, -1], [1, 1, 1]];
  // 3 returns per fold, block_size 5 -> one block per fold, never merged.
  const result = exactNonoverlappingBlockSignTest(folds, 5);
  assert.equal(result.totalBlocks, 3);
  assert.equal(result.positiveBlocks, 2);
  assert.equal(result.negativeBlocks, 1);
});

test('a block that ties votes zero rather than positive', () => {
  const result = exactNonoverlappingBlockSignTest([[1, -1, 1, -1]], 4);
  assert.equal(result.zeroBlocks, 1);
  assert.equal(result.positiveBlocks, 0);
  assert.equal(result.nonzeroBlocks, 0);
});

test('per-trial alpha is familywise divided by the budget', () => {
  assert.equal(perTrialAlpha(), 0.05 / 20);
  assert.equal(perTrialAlpha({ ...DEFAULT_POLICY, maxCandidateTrials: 10 }), 0.005);
});

// ── the audit's own guarantees ──────────────────────────────────────────────

test('the candidate family fits inside the pre-committed budget', () => {
  assert.ok(CANDIDATES.length <= DEFAULT_POLICY.maxCandidateTrials,
    `family of ${CANDIDATES.length} exceeds the committed budget of ${DEFAULT_POLICY.maxCandidateTrials}`);
  const ids = CANDIDATES.map(c => c.id);
  assert.equal(new Set(ids).size, ids.length, 'candidate ids must be unique');
  const controls = CANDIDATES.filter(c => c.kind === 'control').map(c => c.id).sort();
  assert.deepEqual(controls,
    ['control_always_up', 'control_coin_flip', 'control_oracle_sign', 'control_random_seeded'],
    'the four controls are what make the null and the positive legible');
});

test('an over-budget family is refused rather than silently tested', () => {
  // Guards the multiplicity commitment: raising the candidate count without
  // raising the budget would quietly weaken every correction.
  const rows = Array.from({ length: 200 }, (_, index) => ({
    t: 1_700_000_000 + index * 3600, open: 100, high: 101, low: 99, close: 100, volume: 1, symbol: 'BTC-USD',
  }));
  const folds = generateWalkForwardSplits(rows.length, { trainSize: 40, testSize: 20 });
  assert.throws(() => runAudit({
    rows, folds, options: { policy: { ...DEFAULT_POLICY, maxCandidateTrials: 3 } },
  }), /exceeds the pre-committed budget/);
});

// A seeded random walk: unpredictable by construction, and not adversarial. An
// earlier version of this fixture used a deterministic up/down cycle, which was
// trivially predictable -- momentum and reversion both found it, and the audit
// was correctly reporting significance. That was a broken null, not a broken audit.
function randomWalkRows(count, { drift = 0, volatility = 0.004, seed = 20261006 } = {}) {
  const rows = [];
  let price = 100;
  let state = seed >>> 0;
  const random = () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  for (let index = 0; index < count; index += 1) {
    const shock = (random() - 0.5) * 2 * volatility;
    price *= 1 + drift + shock;
    rows.push({
      t: 1_700_000_000 + index * 3600,
      open: price, high: price * 1.002, low: price * 0.998, close: price, volume: 100, symbol: 'BTC-USD',
    });
  }
  return rows;
}

test('controls behave as required when costs are zeroed, and nothing survives', () => {
  // The headline result, pinned. The data is a seeded random walk with no
  // exploitable structure, which is the honest setting for a null: if a control
  // passed here the audit would be broken, and if a signal passed here the audit
  // would be manufacturing significance from noise.
  const rows = randomWalkRows(600);
  const folds = generateWalkForwardSplits(rows.length, { trainSize: 150, testSize: 80, purgeSize: 1, embargoSize: 1 });
  const options = {
    lookbackBars: 24, horizonBars: 1, granularityMinutes: 60, notionalUsd: 1000,
    costModel: { takerFeeRate: 0, makerFeeRate: 0, spreadMultiplier: 0 },
  };
  const audit = runAudit({ rows, folds, options });
  assert.equal(audit.controlsPassed, true,
    `controls misbehaved: ${JSON.stringify(audit.controls.filter(c => c.kind === 'control').map(c => ({ id: c.id, passed: c.significance.passed, reasons: c.significance.reasons })))}`);

  const oracle = audit.results.find(row => row.id === 'control_oracle_sign');
  assert.equal(oracle.significance.passed, true, 'the oracle must be significant with costs removed');
  assert.equal(oracle.directionalAccuracy, 1);

  for (const row of audit.results.filter(candidate => candidate.kind !== 'control')) {
    assert.equal(row.significance.passed, false,
      `${row.id} claimed significance on data with no structure -- the audit is manufacturing results`);
  }
});

test('the oracle is directionally perfect yet unprofitable once fees apply', () => {
  // Worth its own test because it is the most counterintuitive fact the audit
  // produces, and it is why pass 2 must not be used to judge the statistics.
  // A tight random walk: direction is knowable to the oracle, magnitude is not,
  // and it is smaller than a 0.6% fee.
  const rows = randomWalkRows(600, { volatility: 0.0005 });
  const folds = generateWalkForwardSplits(rows.length, { trainSize: 150, testSize: 80 });
  const options = { lookbackBars: 24, horizonBars: 1, granularityMinutes: 60, notionalUsd: 1000,
    costModel: { takerFeeRate: 0.006, makerFeeRate: 0.004, spreadMultiplier: 1 } };
  const audit = runAudit({ rows, folds, options });
  const oracle = audit.results.find(row => row.id === 'control_oracle_sign');
  assert.equal(oracle.directionalAccuracy, 1, 'the oracle knows every direction');
  assert.ok(oracle.meanNetReturnUsd < 0,
    `perfect direction should still lose to a 0.6% fee, got ${oracle.meanNetReturnUsd}`);
  assert.equal(oracle.significance.passed, false,
    'and therefore must fail the economic pass -- which is why pass 2 cannot judge the statistics');
});

// ── prediction shapes ───────────────────────────────────────────────────────

test('every candidate produces a probability in range', () => {
  const rows = Array.from({ length: 120 }, (_, index) => ({
    t: 1_700_000_000 + index * 3600,
    open: 100 + index * 0.01, high: 100.5 + index * 0.01, low: 99.5 + index * 0.01,
    close: 100 + index * 0.01, volume: 10, symbol: 'BTC-USD',
  }));
  const random = () => 0.5;
  for (const candidate of CANDIDATES) {
    const value = predict(candidate, {
      rows, index: 100, lookbackBars: 24, horizonBars: 1, granularityMinutes: 60, random, sentiment: null,
    });
    assert.ok(value > 0 && value < 1, `${candidate.id} produced ${value}, outside (0,1)`);
  }
});

test('a short history degrades to neutral rather than throwing', () => {
  const rows = [
    { t: 1_700_000_000, open: 100, high: 101, low: 99, close: 100, volume: 1, symbol: 'BTC-USD' },
    { t: 1_700_003_600, open: 100, high: 101, low: 99, close: 101, volume: 1, symbol: 'BTC-USD' },
    { t: 1_700_007_200, open: 101, high: 102, low: 100, close: 100.5, volume: 1, symbol: 'BTC-USD' },
  ];
  // Three closes is two returns, which cannot support a z-score. The candidate
  // must abstain rather than speak from arithmetic on one observation.
  const momentum = CANDIDATES.find(candidate => candidate.id === 'momentum_12');
  assert.equal(predict(momentum, {
    rows, index: 2, lookbackBars: 24, horizonBars: 1, granularityMinutes: 60, random: () => 0.5, sentiment: null,
  }), 0.5);
  // And with enough history it does speak.
  const longer = randomWalkRows(80);
  const value = predict(momentum, {
    rows: longer, index: 79, lookbackBars: 24, horizonBars: 1, granularityMinutes: 60, random: () => 0.5, sentiment: null,
  });
  assert.ok(value !== 0.5, 'a signal with 24 bars of history should produce a reading');
});

// ── reserve sweep ───────────────────────────────────────────────────────────

test('the reserve sweep reports no fraction that permits a profitable set', () => {
  const rows = randomWalkRows(600, { volatility: 0.0005 });
  const folds = generateWalkForwardSplits(rows.length, { trainSize: 150, testSize: 80 });
  const sweep = reserveSweep({
    rows, folds,
    options: { lookbackBars: 24, horizonBars: 1, granularityMinutes: 60, notionalUsd: 1000 },
  });
  assert.ok(sweep.length >= 7, 'sweep should cover a range of reserve fractions');
  assert.ok(sweep.every(row => row.uncertaintyReserveFraction >= 0));
  // A zero reserve must be the most permissive point; if it is not, the sweep is
  // not varying what it claims to vary.
  const zero = sweep.find(row => row.uncertaintyReserveFraction === 0);
  assert.ok(zero, 'the sweep must include fraction 0');
  for (const row of sweep) {
    if (row.uncertaintyReserveFraction > 0) {
      assert.ok(row.permitted <= zero.permitted,
        `fraction ${row.uncertaintyReserveFraction} permitted ${row.permitted}, more than zero's ${zero.permitted}`);
    }
    assert.ok(row.significance == null || row.significance.passed === false,
      'no reserve fraction should manufacture significance here');
  }
});
