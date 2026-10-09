#!/usr/bin/env node
// Does this data contain any exploitable structure at all?
//
// WHY THIS COMES FIRST. The replay showed the gate refusing every trade and the
// forecast scoring worse than always answering 50%. The obvious next move is to
// build a better forecast. That is backwards: a replacement model trained on a
// series with no predictable structure will look excellent in-sample and fail
// out-of-sample, and this repo already carries that failure twice -- two divergent
// forecast implementations, and weight sets the docs themselves call "a
// forecasting hypothesis rather than established edge". So before proposing any
// replacement, measure whether there is an edge to replace it with.
//
// MULTIPLICITY IS THE POINT. Testing twenty signals and reporting the best one
// guarantees a winner at 5% family-wise. So the candidate family is declared in
// CANDIDATES below and frozen before any result is computed, and every candidate
// is charged one slot whether it passes or fails -- including the controls.
//
// The statistics replicate scripts/selection_bias.py exactly: a one-sided exact
// sign test over OOS trade signs, plus a non-overlapping block sign test that
// gives each chronological block within a fold one direction vote. Bonferroni
// across the family. tests/selection-bias-node.test.mjs pins the replication
// against fixtures generated from the Python module.
//
// ADVISORY ONLY. No orders, no broker, no operator store. In-memory state only.

import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import process from 'node:process';

import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';
import { buildPriceForecast } from '../packages/economics/src/economicDecisionEngine.mjs';
import { recordSentimentAugmentedForecast } from '../packages/economics/src/sentimentForecast.mjs';
import { generateWalkForwardSplits, DEFAULT_COST_MODEL } from './run-decision-classifier-replay.mjs';

export const AUDIT_SCHEMA_VERSION = 1;

// ── policy, matching scripts/selection_bias.py SearchMultiplicityPolicy ─────
export const DEFAULT_POLICY = {
  maxCandidateTrials: 20,
  familywiseAlpha: 0.05,
  minNonzeroTrades: 10,
  dependenceBlockSize: 5,
  minNonzeroBlocks: 5,
};

export function perTrialAlpha(policy = DEFAULT_POLICY) {
  return policy.familywiseAlpha / policy.maxCandidateTrials;
}

// P[Binomial(n, 0.5) >= k] without forming 2**n in a double.
export function binomialUpperTailHalf(n, k) {
  if (n < 0 || k < 0 || k > n) throw new Error('invalid binomial tail bounds');
  if (k === 0) return 1.0;
  const denominator = 1n << BigInt(n);
  if (k <= Math.floor(n / 2)) {
    let coefficient = 1n;
    let lowerSum = coefficient;
    for (let j = 0; j < k - 1; j += 1) {
      coefficient = (coefficient * BigInt(n - j)) / BigInt(j + 1);
      lowerSum += coefficient;
    }
    return 1.0 - Number(lowerSum) / Number(denominator);
  }
  let coefficient = 1n;
  for (let j = 1; j <= k; j += 1) coefficient = (coefficient * BigInt(n - k + j)) / BigInt(j);
  let upperSum = coefficient;
  for (let j = k; j < n; j += 1) {
    coefficient = (coefficient * BigInt(n - j)) / BigInt(j + 1);
    upperSum += coefficient;
  }
  return Number(upperSum) / Number(denominator);
}

export function exactOneSidedSignTest(returns) {
  let positive = 0;
  let negative = 0;
  let zero = 0;
  for (const raw of returns) {
    const value = Number(raw);
    if (!Number.isFinite(value)) throw new Error('sign-test returns must be finite');
    if (value > 0) positive += 1;
    else if (value < 0) negative += 1;
    else zero += 1;
  }
  const effective = positive + negative;
  return {
    positiveTrades: positive,
    negativeTrades: negative,
    zeroTrades: zero,
    nonzeroTrades: effective,
    rawPValue: effective === 0 ? 1.0 : binomialUpperTailHalf(effective, positive),
  };
}

export function exactNonoverlappingBlockSignTest(foldReturns, blockSize) {
  if (blockSize < 2) throw new Error('block_size must be at least 2');
  const blockVotes = [];
  let totalBlocks = 0;
  for (const fold of foldReturns) {
    for (let start = 0; start < fold.length; start += blockSize) {
      const block = fold.slice(start, start + blockSize);
      if (!block.length) continue;
      totalBlocks += 1;
      const positives = block.filter(value => Number(value) > 0).length;
      const negatives = block.filter(value => Number(value) < 0).length;
      blockVotes.push(positives > negatives ? 1 : negatives > positives ? -1 : 0);
    }
  }
  const sign = exactOneSidedSignTest(blockVotes);
  return {
    method: 'nonoverlapping_fold_block_sign_v1',
    blockSize,
    totalBlocks,
    positiveBlocks: sign.positiveTrades,
    negativeBlocks: sign.negativeTrades,
    zeroBlocks: sign.zeroTrades,
    nonzeroBlocks: sign.nonzeroTrades,
    rawPValue: sign.rawPValue,
  };
}

/** Mirrors assess_candidate_significance at selection_bias.py:299-352. */
export function assessCandidateSignificance(foldReturns, policy = DEFAULT_POLICY) {
  const flattened = foldReturns.flat();
  const sign = exactOneSidedSignTest(flattened);
  const block = exactNonoverlappingBlockSignTest(foldReturns, policy.dependenceBlockSize);
  const marginalAdjusted = Math.min(1.0, sign.rawPValue * policy.maxCandidateTrials);
  const blockAdjusted = Math.min(1.0, block.rawPValue * policy.maxCandidateTrials);
  const alpha = perTrialAlpha(policy);
  const reasons = [];
  if (sign.nonzeroTrades < policy.minNonzeroTrades) reasons.push('multiple_testing_insufficient_nonzero_trades');
  if (sign.rawPValue > alpha) reasons.push('multiple_testing_not_familywise_significant');
  if (block.nonzeroBlocks < policy.minNonzeroBlocks) reasons.push('dependence_insufficient_nonzero_blocks');
  if (block.rawPValue > alpha) reasons.push('dependence_not_familywise_significant');
  return {
    ...sign,
    marginalAdjustedPValue: marginalAdjusted,
    dependence: { ...block, adjustedPValue: blockAdjusted },
    adjustedPValue: Math.max(marginalAdjusted, blockAdjusted),
    passed: reasons.length === 0,
    reasons,
  };
}

// ── the pre-committed candidate family ──────────────────────────────────────
// Declared before any result exists. Every entry consumes a multiplicity slot
// whether it passes or fails, including the four controls. Controls first so a
// reader sees what "no edge" and "obvious edge" look like in this format before
// reading any candidate verdict.
export const CANDIDATES = [
  { id: 'control_coin_flip', kind: 'control', note: 'always 0.5 -- the reference. must FAIL' },
  { id: 'control_always_up', kind: 'control', note: 'always long -- degenerate. must FAIL' },
  { id: 'control_random_seeded', kind: 'control', note: 'seeded noise. must FAIL' },
  { id: 'control_oracle_sign', kind: 'control', note: 'sign of the realized move. must PASS' },
  { id: 'naive_drift_1', kind: 'signal', lookback: 1, mode: 'momentum' },
  { id: 'momentum_3', kind: 'signal', lookback: 3, mode: 'momentum' },
  { id: 'momentum_6', kind: 'signal', lookback: 6, mode: 'momentum' },
  { id: 'momentum_12', kind: 'signal', lookback: 12, mode: 'momentum' },
  { id: 'momentum_24', kind: 'signal', lookback: 24, mode: 'momentum' },
  { id: 'mean_reversion_6', kind: 'signal', lookback: 6, mode: 'reversion' },
  { id: 'mean_reversion_12', kind: 'signal', lookback: 12, mode: 'reversion' },
  { id: 'mean_reversion_24', kind: 'signal', lookback: 24, mode: 'reversion' },
  { id: 'ensemble_current', kind: 'forecast', note: 'the deployed deterministic-price-ensemble-v1' },
  { id: 'ensemble_augmented', kind: 'forecast_augmented', note: 'plus a sentiment reading' },
  { id: 'ensemble_momentum_only', kind: 'signal', lookback: 5, mode: 'momentum' },
  { id: 'ensemble_reversion_only', kind: 'signal', lookback: 20, mode: 'reversion' },
];

function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function stdev(values) {
  if (values.length < 2) return 0;
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  return Math.sqrt(values.reduce((acc, value) => acc + (value - mean) ** 2, 0) / (values.length - 1));
}

const clamp01 = value => Math.max(0.001, Math.min(0.999, value));
// Minimum returns before a momentum/reversion z-score is allowed to speak.
const MIN_SIGNAL_RETURNS = 8;

/** Probability the candidate assigns to an up move, for one window. */
export function predict(candidate, { rows, index, lookbackBars, horizonBars, granularityMinutes, sentiment, random }) {
  const slice = rows.slice(Math.max(0, index - lookbackBars), index + 1);
  const closes = slice.map(row => Number(row.close));
  const actual = rows[index + horizonBars];

  if (candidate.id === 'control_coin_flip') return 0.5;
  if (candidate.id === 'control_always_up') return 0.9;
  if (candidate.id === 'control_random_seeded') return 0.05 + random() * 0.9;
  if (candidate.id === 'control_oracle_sign') {
    if (!actual) return 0.5;
    return Number(actual.close) > Number(rows[index].close) ? 0.9 : 0.1;
  }

  if (candidate.kind === 'forecast' || candidate.kind === 'forecast_augmented') {
    const state = createInitialOperatorState(new Date(rows[index].t * 1000).toISOString());
    const asOf = new Date(rows[index].t * 1000).toISOString();
    const forecast = buildPriceForecast(state, {
      symbol: rows[index].symbol,
      observations: slice.map(row => ({ price: Number(row.close), timestamp: new Date(row.t * 1000).toISOString() })),
      horizonMinutes: granularityMinutes * horizonBars,
      observationIntervalMinutes: granularityMinutes,
      maxDataAgeSeconds: 86_400,
    }, asOf)?.priceForecast;
    if (!forecast) return 0.5;
    if (candidate.kind === 'forecast_augmented' && sentiment) {
      const blended = recordSentimentAugmentedForecast(state, forecast, sentiment);
      return blended.priceForecast?.probabilityUp ?? 0.5;
    }
    return forecast.probabilityUp;
  }

  // Signal candidates: a z-score of the last move against its own recent
  // distribution, mapped to a probability. Momentum takes the sign as-is;
  // reversion inverts it. Volatility scales the magnitude so a confident reading
  // in a quiet tape does not claim the same conviction as a violent one.
  const returns = [];
  for (let i = 1; i < closes.length; i += 1) returns.push(Math.log(closes[i] / closes[i - 1]));
  // A z-score needs a distribution. With two closes there is one return and
  // sigma is either zero or a single point, so any "reading" is arithmetic on
  // noise. Degrade to neutral instead, which is the honest answer and keeps a
  // short slice of the dataset from being scored as a confident signal.
  if (returns.length < MIN_SIGNAL_RETURNS) return 0.5;
  const last = returns[returns.length - 1];
  const sigma = stdev(returns.slice(-Math.min(12, returns.length))) || 1e-9;
  const z = Math.max(-3, Math.min(3, last / sigma));
  const signed = candidate.mode === 'reversion' ? -z : z;
  return clamp01(0.5 + signed * 0.12);
}

// ── audit ───────────────────────────────────────────────────────────────────

export function auditCandidate(candidate, { rows, folds, options }) {
  const lookbackBars = Math.max(candidate.lookback ?? options.lookbackBars, 3);
  const horizonBars = options.horizonBars;
  const granularityMinutes = options.granularityMinutes;
  const notionalUsd = options.notionalUsd;
  const costModel = { ...DEFAULT_COST_MODEL, ...(options.costModel || {}) };
  const random = mulberry32(0xC0FFEE);
  const sentimentByT = options.sentimentByT || null;

  const foldReturns = folds.map(() => []);
  const foldBrier = folds.map(() => []);
  const foldDirection = folds.map(() => []);
  let taken = 0;
  let considered = 0;

  for (const [foldIndex, fold] of folds.entries()) {
    for (let index = fold.testStart; index < fold.testEnd; index += 1) {
      if (index < lookbackBars) continue;
      const actual = rows[index + horizonBars];
      if (!actual) continue;
      considered += 1;
      const probabilityUp = predict(candidate, {
        rows, index, lookbackBars, horizonBars, granularityMinutes,
        sentiment: sentimentByT ? sentimentByT.get(rows[index].t) : null,
        random,
      });
      const actualUp = Number(actual.close) > Number(rows[index].close) ? 1 : 0;
      foldBrier[foldIndex].push((probabilityUp - actualUp) ** 2);
      foldDirection[foldIndex].push((probabilityUp >= 0.5) === Boolean(actualUp) ? 1 : 0);
      if (probabilityUp < 0.5) continue; // flat: no trade, no return

      const grossUsd = (Number(actual.close) - Number(rows[index].close)) / Number(rows[index].close) * notionalUsd;
      const spreadBps = Math.max(0, ((Number(rows[index].high) - Number(rows[index].low)) / Number(rows[index].close)) * 10_000) * (costModel.spreadMultiplier ?? 1);
      const costUsd = notionalUsd * costModel.takerFeeRate + notionalUsd * spreadBps / 20_000;
      foldReturns[foldIndex].push(grossUsd - costUsd);
      taken += 1;
    }
  }

  const assessment = assessCandidateSignificance(foldReturns, options.policy || DEFAULT_POLICY);
  const mean = values => (values.length ? values.reduce((a, b) => a + b, 0) / values.length : null);
  return {
    id: candidate.id,
    kind: candidate.kind,
    note: candidate.note || null,
    lookback: lookbackBars,
    windowsConsidered: considered,
    tradesTaken: taken,
    brierScore: mean(foldBrier.flat()),
    directionalAccuracy: mean(foldDirection.flat()),
    meanNetReturnUsd: mean(foldReturns.flat()),
    totalNetReturnUsd: foldReturns.flat().reduce((a, b) => a + b, 0),
    significance: assessment,
  };
}

const COST_FREE = { takerFeeRate: 0, makerFeeRate: 0, spreadMultiplier: 0 };

export function runAudit({ rows, folds, options = {}, sentimentByT = null }) {
  const policy = options.policy || DEFAULT_POLICY;
  if (CANDIDATES.length > policy.maxCandidateTrials) {
    throw new Error(`candidate family of ${CANDIDATES.length} exceeds the pre-committed budget of ${policy.maxCandidateTrials}`);
  }
  const merged = { ...options, policy, sentimentByT };
  const results = CANDIDATES.map(candidate => auditCandidate(candidate, { rows, folds, options: merged }));
  const controls = results.filter(row => row.kind === 'control');
  return {
    results,
    controls,
    controlsPassed: controls.every(row => {
      if (row.id === 'control_oracle_sign') return row.significance.passed;
      return !row.significance.passed;
    }),
  };
}

// ── reserve calibration ─────────────────────────────────────────────────────
// The other half of the question. Lowering uncertaintyReserveFraction to 0 admits
// trades; the replay showed those trades lose money. This asks the narrower,
// answerable question: at what fraction does the gate start permitting, and is
// there ANY fraction at which the permitted set is profitable out of sample.

export function reserveSweep({ rows, folds, options }) {
  const out = [];
  for (const uncertaintyReserveFraction of [0, 0.05, 0.1, 0.15, 0.25, 0.5, 1.0]) {
    const rowsOut = [];
    for (const fold of folds) {
      const returns = [];
      for (let index = fold.testStart; index < fold.testEnd; index += 1) {
        if (index < options.lookbackBars) continue;
        const actual = rows[index + horizonOf(options)];
        if (!actual) continue;
        const asOf = new Date(rows[index].t * 1000).toISOString();
        const state = createInitialOperatorState(asOf);
        const slice = rows.slice(index - options.lookbackBars, index + 1);
        const forecast = buildPriceForecast(state, {
          symbol: rows[index].symbol,
          observations: slice.map(row => ({ price: Number(row.close), timestamp: new Date(row.t * 1000).toISOString() })),
          horizonMinutes: options.granularityMinutes * options.horizonBars,
          observationIntervalMinutes: options.granularityMinutes,
          maxDataAgeSeconds: 86_400,
        }, asOf)?.priceForecast;
        if (!forecast) continue;
        const notionalUsd = options.notionalUsd;
        const reserve = notionalUsd * forecast.expectedVolatilityBps / 10_000 * uncertaintyReserveFraction;
        const edge = notionalUsd * forecast.expectedReturnBps / 10_000;
        const spreadBps = Math.max(0, ((Number(rows[index].high) - Number(rows[index].low)) / Number(rows[index].close)) * 10_000);
        const cost = notionalUsd * DEFAULT_COST_MODEL.takerFeeRate + notionalUsd * spreadBps / 20_000;
        if (edge - cost - reserve <= 0) continue;
        const grossUsd = (Number(actual.close) - Number(rows[index].close)) / Number(rows[index].close) * notionalUsd;
        returns.push(grossUsd - cost);
      }
      rowsOut.push(returns);
    }
    const flat = rowsOut.flat();
    out.push({
      uncertaintyReserveFraction,
      permitted: flat.length,
      meanNetReturnUsd: flat.length ? flat.reduce((a, b) => a + b, 0) / flat.length : null,
      totalNetReturnUsd: flat.reduce((a, b) => a + b, 0),
      winRate: flat.length ? flat.filter(value => value > 0).length / flat.length : null,
      significance: flat.length ? assessCandidateSignificance(rowsOut, options.policy || DEFAULT_POLICY) : null,
    });
  }
  return out;
}

function horizonOf(options) {
  return options.horizonBars;
}

function fmt(value, digits = 2) {
  return value == null || !Number.isFinite(value) ? '     n/a' : Number(value).toFixed(digits);
}
function pct(value) {
  return value == null ? '  n/a' : `${(value * 100).toFixed(2)}%`;
}

function loadSentiment(path) {
  if (!path) return null;
  const rows = readFileSync(path, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
  // The recorded series is daily. Map each reading onto the last bar at or
  // before its publication, which is the only way to use it without look-ahead.
  const byT = new Map();
  for (const row of rows) byT.set(Math.trunc(row.t), row);
  return byT;
}

function main() {
  const argv = process.argv.slice(2);
  const flag = (name, fallback) => {
    const at = argv.indexOf(`--${name}`);
    return at >= 0 && argv[at + 1] ? argv[at + 1] : fallback;
  };

  const symbol = flag('symbol', 'BTC-USD');
  const candlesPath = flag('candles', 'data/sentiment_harness/candles/BTC-USD-1h.jsonl');
  const rows = readFileSync(candlesPath, 'utf8').trim().split('\n').filter(Boolean)
    .map(line => JSON.parse(line)).map(row => ({ ...row, symbol }));
  if (rows.length < 300) {
    process.stderr.write(JSON.stringify({ error: 'dataset_too_small', rows: rows.length }) + '\n');
    process.exitCode = 2;
    return;
  }

  const options = {
    lookbackBars: 24,
    horizonBars: 1,
    granularityMinutes: 60,
    notionalUsd: 1000,
    costModel: DEFAULT_COST_MODEL,
  };
  const folds = generateWalkForwardSplits(rows.length, { trainSize: 400, testSize: 481, purgeSize: 1, embargoSize: 1 });
  if (!folds.length) {
    process.stderr.write(JSON.stringify({ error: 'no_folds' }) + '\n');
    process.exitCode = 2;
    return;
  }

  const sentimentByT = loadSentiment(flag('sentiment', null));
  console.log(`  dataset    ${rows.length} bars, ${folds.length} walk-forward folds`);
  console.log(`  family     ${CANDIDATES.length} candidates pre-committed, budget ${DEFAULT_POLICY.maxCandidateTrials}, per-trial alpha ${perTrialAlpha(DEFAULT_POLICY)}`);

  // TWO PASSES, because these are two different questions and conflating them
  // breaks the controls. The oracle is 100% directionally correct yet loses money
  // at 0.6% taker, because a typical 1h up-move is smaller than the fee. Judging
  // the statistics on cost-inclusive returns makes the oracle look like a failure
  // and hides whether the machinery works at all. So: pass 1 answers "does any
  // signal predict direction?", pass 2 answers "does any signal make money?".
  // expectControls: whether "oracle passes, others fail" is the required shape.
  // Pass 2 does not have it -- at realistic costs the oracle SHOULD fail, and
  // reporting that as a broken audit would be exactly backwards.
  const printPass = (title, auditResult, expectControls) => {
    console.log(`\n  === ${title} ===`);
    console.log('    candidate                   trades   Brier     dir     meanNet   adj-p    verdict');
    for (const row of auditResult.results) {
      const verdict = row.kind === 'control'
        ? (row.significance.passed ? (row.id === 'control_oracle_sign' ? 'ok' : 'UNEXPECTED PASS') : (row.id === 'control_oracle_sign' ? 'UNEXPECTED FAIL' : 'ok'))
        : (row.significance.passed ? 'SIGNIFICANT' : 'no edge');
      console.log(`    ${row.id.padEnd(27)} ${String(row.tradesTaken).padStart(5)}`
        + `  ${fmt(row.brierScore, 6)}  ${pct(row.directionalAccuracy).padStart(7)}`
        + `  ${fmt(row.meanNetReturnUsd).padStart(8)}`
        + `  ${fmt(row.significance.adjustedPValue, 4).padStart(7)}   ${verdict}`);
    }
    const signals = auditResult.results.filter(row => row.kind !== 'control');
    const winners = signals.filter(row => row.significance.passed);
    if (expectControls) {
      console.log(`  controls   ${auditResult.controlsPassed ? 'PASSED' : 'FAILED -- the audit itself is unreliable'}`);
    } else {
      const oracle = auditResult.results.find(row => row.id === 'control_oracle_sign');
      console.log(`  note       the oracle is 100% directionally correct and still reports`
        + ` meanNet ${fmt(oracle?.meanNetReturnUsd)} -- fees exceed a typical up-move.`);
      console.log(`             It is expected to fail here; only pass 1 validates the statistics.`);
    }
    console.log(`  verdict    ${winners.length === 0
      ? `no candidate survived multiplicity correction across ${signals.length} signals`
      : `${winners.length} candidate(s) survived: ${winners.map(r => r.id).join(', ')}`}`);
    return winners;
  };

  const costFreeAudit = runAudit({ rows, folds, options: { ...options, costModel: COST_FREE }, sentimentByT });
  const costFreeWinners = printPass('pass 1: direction only (costs zeroed, controls must behave)', costFreeAudit, true);

  const realisticAudit = runAudit({ rows, folds, options, sentimentByT });
  const realisticWinners = printPass(`pass 2: economics (${(DEFAULT_COST_MODEL.takerFeeRate * 100).toFixed(2)}% taker + measured spread)`, realisticAudit, false);

  if (!costFreeAudit.controlsPassed) {
    process.stderr.write(JSON.stringify({ error: 'control_failure', pass: 'cost_free', note: 'audit controls did not behave as required' }) + '\n');
    process.exitCode = 1;
    return;
  }

  console.log('\n  === uncertainty reserve calibration ===');
  const sweep = reserveSweep({ rows, folds, options });
  console.log('    fraction   permitted   meanNet USD   win      totalNet     verdict');
  for (const row of sweep) {
    const verdict = row.significance?.passed ? 'SIGNIFICANT' : (row.permitted ? 'not significant' : 'permits nothing');
    console.log(`    ${String(row.uncertaintyReserveFraction).padStart(8)}`
      + `  ${String(row.permitted).padStart(9)}`
      + `   ${fmt(row.meanNetReturnUsd).padStart(10)}`
      + `  ${pct(row.winRate).padStart(7)}`
      + `  ${fmt(row.totalNetReturnUsd).padStart(11)}   ${verdict}`);
  }
  const profitable = sweep.filter(row => row.permitted > 0 && (row.significance?.passed));
  console.log(`\n  ${profitable.length === 0
    ? 'no reserve fraction produced a significant profitable permitted set'
    : `profitable at: ${profitable.map(r => r.uncertaintyReserveFraction).join(', ')}`}`);

  console.log('\n  === summary ===');
  console.log(`  direction  ${costFreeWinners.length ? `${costFreeWinners.length} signal(s) predict direction OOS` : 'no signal predicts direction OOS'}`);
  console.log(`  economics  ${realisticWinners.length ? `${realisticWinners.length} signal(s) make money after costs` : 'no signal makes money after costs'}`);

  const outDir = flag('out', join('scripts', 'experiments', 'signal_existence_audit'));
  mkdirSync(outDir, { recursive: true });
  writeFileSync(join(outDir, 'scorecard.json'), `${JSON.stringify({
    schema_version: AUDIT_SCHEMA_VERSION,
    policy: DEFAULT_POLICY,
    candidate_count: CANDIDATES.length,
    candidates: CANDIDATES,
    folds: folds.length,
    audit_cost_free: costFreeAudit.results,
    audit_realistic_costs: realisticAudit.results,
    controls_passed: costFreeAudit.controlsPassed,
    direction_winners: costFreeWinners.map(row => row.id),
    economics_winners: realisticWinners.map(row => row.id),
    reserve_sweep: sweep,
    generated_at: new Date().toISOString(),
  }, null, 2)}\n`);
  console.log(`\n  scorecard  ${outDir}/scorecard.json`);
}

if (import.meta.url === `file://${process.argv[1]}`) main();
