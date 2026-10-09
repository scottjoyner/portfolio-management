#!/usr/bin/env node
// Offline walk-forward harness: does a Grok sentiment reading add anything to the
// deterministic price ensemble?
//
// THE POINT IS NOT THE NUMBER, IT IS THE CONTROLS. A calibration harness that can
// only report that a new signal "helped" is worthless, because adding noise to a
// probability will improve some windows by chance and the improvement will be
// indistinguishable from skill in a report. So this runs four sentiment series
// through identical windows and requires the ordering to come out right:
//
//   oracle   sentiment = sign of the realized forward return   MUST help
//   random   sentiment independent of price                   MUST NOT help
//   inverted sentiment = -sign of the realized forward return  MUST hurt
//   real     a recorded Grok series, if one is supplied
//
// If `oracle` does not beat the baseline, the harness is broken and no other
// number it prints means anything. That is checked explicitly at the end, and the
// process exits non-zero when the self-test fails, so a broken harness cannot be
// mistaken for a negative result about sentiment.
//
// ADVISORY ONLY. Nothing here writes to the operator state or reaches the trader.
// Both arms are scored through the real recordForecastOutcome/summarize path so the
// numbers are produced by the same code that would score a live forecast.

import { readFileSync } from 'node:fs';
import process from 'node:process';

import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';
import {
  buildPriceForecast,
  recordForecastOutcome,
} from '../packages/economics/src/economicDecisionEngine.mjs';
import {
  recordSentimentAugmentedForecast,
  SENTIMENT_AUGMENTED_VERSION,
} from '../packages/economics/src/sentimentForecast.mjs';

const BASELINE_VERSION = 'deterministic-price-ensemble-v1';

// Deterministic PRNG so a control run is reproducible; Math.random would make the
// negative control irreproducible and therefore unfalsifiable.
function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function loadCandles(path) {
  return readFileSync(path, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
}

function forwardReturnBps(candles, index, horizonBars) {
  const target = candles[index + horizonBars];
  if (!target) return null;
  const now = candles[index].close;
  if (!(now > 0)) return null;
  return (target.close / now - 1) * 10_000;
}

/**
 * Build a sentiment series keyed by candle index.
 *
 * The oracle and inverted controls read the forward return on purpose. They are
 * labelled as oracles precisely because they are not a strategy -- they exist only
 * to prove the scoring direction is right, and any headline result must come from
 * the `real` series or from `random` failing to help.
 */
function buildControlSeries(candles, mode, horizonBars, seed) {
  const random = mulberry32(seed);
  const series = new Map();
  for (let index = 0; index < candles.length; index += 1) {
    const bps = forwardReturnBps(candles, index, horizonBars);
    if (bps == null) continue;
    if (mode === 'random') {
      series.set(index, { score: random() * 2 - 1, confidence: 0.6 + random() * 0.4, horizonMinutes: 60, rationale: 'random control' });
      continue;
    }
    const sign = Math.sign(bps);
    if (sign === 0) continue;
    const score = mode === 'oracle' ? sign : -sign;
    // Confidence varies with magnitude so the blend is not exercised only at its
    // ceiling; a real model is rarely maximally sure.
    const confidence = Math.min(0.95, 0.3 + Math.abs(bps) / 200);
    series.set(index, { score, confidence, horizonMinutes: 60, rationale: `${mode} control` });
  }
  return series;
}

function summarize(outcomes) {
  if (!outcomes.length) return { samples: 0, brierScore: null, directionalAccuracy: null, p10P90Coverage: null, meanAbsoluteErrorPct: null };
  const sum = values => values.reduce((acc, value) => acc + value, 0);
  return {
    samples: outcomes.length,
    brierScore: sum(outcomes.map(o => o.brierScore)) / outcomes.length,
    directionalAccuracy: sum(outcomes.map(o => (o.directionCorrect ? 1 : 0))) / outcomes.length,
    p10P90Coverage: sum(outcomes.map(o => (o.insideP10P90 ? 1 : 0))) / outcomes.length,
    meanAbsoluteErrorPct: sum(outcomes.map(o => o.absoluteErrorPct)) / outcomes.length,
  };
}

// Always-50% has a Brier score of exactly 0.25 and directional accuracy of exactly
// 50%, whatever the data. Reporting it beside the model turns "0.2556" from a
// number into a verdict: a calibrated-but-uninformative forecast scores 0.25, so
// anything above that is worse than refusing to answer. Without this row a reader
// has to know the constant to notice.
const COIN_FLIP = { samples: 0, brierScore: 0.25, directionalAccuracy: 0.5, p10P90Coverage: null, meanAbsoluteErrorPct: null };

export function coinFlipReference() {
  return { ...COIN_FLIP };
}

function runArm(candles, sentimentSeries, options) {
  const { lookbackBars, horizonBars, granularityMinutes, maxTiltSigma } = options;
  const state = createInitialOperatorState();
  const outcomes = { [BASELINE_VERSION]: [], [SENTIMENT_AUGMENTED_VERSION]: [] };
  // Paired per window. Comparing a mean over every window against a mean over
  // only the windows that happened to have a reading is not a comparison -- the
  // two arms would differ by sample composition, not by sentiment. The controls
  // hid this because they populate every window.
  const paired = [];
  const byRegime = {};
  let skippedNoSentiment = 0;
  let skippedNoOutcome = 0;

  for (let index = lookbackBars; index < candles.length - horizonBars; index += 1) {
    const slice = candles.slice(index - lookbackBars, index + 1);
    const observations = slice.map(row => ({ price: row.close, timestamp: row.timestamp, volume: row.volume }));
    // `now` is the candle's own timestamp, so the engine's staleness guard stays
    // active and meaningful instead of rejecting every historical window.
    const asOf = slice.at(-1).timestamp;

    const baselineResult = buildPriceForecast(state, {
      symbol: candles[index].symbol || 'BTC-USD',
      observations,
      horizonMinutes: granularityMinutes * horizonBars,
      observationIntervalMinutes: granularityMinutes,
      sentimentOptions: { maxTiltSigma },
    }, asOf);
    if (!baselineResult?.priceForecast) continue;
    const baseline = baselineResult.priceForecast;

    const sentiment = sentimentSeries.get(index) || null;
    if (!sentiment) skippedNoSentiment += 1;
    let augmented = null;
    if (sentiment) {
      const blended = recordSentimentAugmentedForecast(state, baseline, sentiment, { maxTiltSigma });
      if (!blended.errors) augmented = blended.priceForecast;
    }

    const actualPrice = candles[index + horizonBars].close;
    const windowOutcomes = {};
    for (const forecast of [baseline, augmented].filter(Boolean)) {
      const recorded = recordForecastOutcome(state, { forecastId: forecast.id, actualPrice }, asOf);
      const outcome = recorded.forecastOutcome;
      if (!outcome) {
        skippedNoOutcome += 1;
        continue;
      }
      outcomes[forecast.modelVersion] ||= [];
      outcomes[forecast.modelVersion].push(outcome);
      windowOutcomes[forecast.modelVersion] = outcome;
      byRegime[outcome.regime] ||= { [BASELINE_VERSION]: [], [SENTIMENT_AUGMENTED_VERSION]: [] };
      byRegime[outcome.regime][forecast.modelVersion]?.push(outcome);
    }
    const base = windowOutcomes[BASELINE_VERSION];
    const aug = windowOutcomes[SENTIMENT_AUGMENTED_VERSION];
    if (base && aug) paired.push({ baseline: base, augmented: aug });
  }
  return { outcomes, paired, byRegime, skippedNoSentiment, skippedNoOutcome };
}

function delta(base, augmented, key, lowerIsBetter) {
  if (base[key] == null || augmented[key] == null) return null;
  const raw = augmented[key] - base[key];
  const improved = lowerIsBetter ? raw < 0 : raw > 0;
  return { raw, improved };
}

// Restrict both arms to the windows where both exist, so the headline numbers
// describe the same windows in each column.
function summarizePaired(paired) {
  return {
    baseline: summarize(paired.map(p => p.baseline)),
    augmented: summarize(paired.map(p => p.augmented)),
    pairs: paired.length,
  };
}

export function runHarness({ candles, sentimentSeries, options = {} }) {
  const base = runArm(candles, sentimentSeries, options);
  const paired = summarizePaired(base.paired);
  const baseSummary = summarize(base.outcomes[BASELINE_VERSION] || []);
  const augSummary = summarize(base.outcomes[SENTIMENT_AUGMENTED_VERSION] || []);
  return {
    coinFlip: coinFlipReference(),
    // The comparable pair: identical windows on both sides.
    paired,
    pairedComparison: {
      brier: {
        raw: (paired.augmented.brierScore ?? 0) - (paired.baseline.brierScore ?? 0),
        improved: paired.augmented.brierScore != null && paired.augmented.brierScore < paired.baseline.brierScore,
      },
      directionalAccuracy: {
        raw: (paired.augmented.directionalAccuracy ?? 0) - (paired.baseline.directionalAccuracy ?? 0),
        improved: paired.augmented.directionalAccuracy != null && paired.augmented.directionalAccuracy > paired.baseline.directionalAccuracy,
      },
    },
    baseline: baseSummary,
    augmented: augSummary,
    comparison: {
      brier: delta(baseSummary, augSummary, 'brierScore', true),
      directionalAccuracy: delta(baseSummary, augSummary, 'directionalAccuracy', false),
      p10P90Coverage: { raw: (augSummary.p10P90Coverage ?? 0) - (baseSummary.p10P90Coverage ?? 0), improved: null },
    },
    skippedNoSentiment: base.skippedNoSentiment,
    skippedNoOutcome: base.skippedNoOutcome,
    byRegime: Object.fromEntries(Object.entries(base.byRegime).map(([regime, arms]) => [regime, {
      baseline: summarize(arms[BASELINE_VERSION] || []),
      augmented: summarize(arms[SENTIMENT_AUGMENTED_VERSION] || []),
    }])),
  };
}

// The self-test. A harness that cannot detect a perfect oracle cannot be trusted
// to report that sentiment does not help.
export function harnessSelfTest(candles, options) {
  const control = (mode, seed) => runHarness({
    candles,
    sentimentSeries: buildControlSeries(candles, mode, options.horizonBars, seed),
    options,
  });
  const oracle = control('oracle', 1);
  const random = control('random', 2);
  const inverted = control('inverted', 3);
  const oracleBrier = oracle.comparison.brier;
  const randomBrier = random.comparison.brier;
  const invertedBrier = inverted.comparison.brier;
  return {
    oracle,
    random,
    inverted,
    passed: Boolean(oracleBrier?.improved) && (!randomBrier?.improved || randomBrier.raw > oracleBrier.raw),
    checks: {
      oracleImprovesBrier: Boolean(oracleBrier?.improved),
      randomDoesNotBeatOracle: randomBrier == null || randomBrier.raw > oracleBrier.raw,
      invertedIsWorseThanRandom: invertedBrier == null || randomBrier == null || invertedBrier.raw > randomBrier.raw,
    },
  };
}

function pct(value) {
  return value == null ? '   n/a' : `${(value * 100).toFixed(2)}%`;
}

function describe(label, result) {
  const { baseline: b, augmented: a, comparison: c, coinFlip: f } = result;
  const verdict = b.brierScore == null ? ''
    : b.brierScore > f.brierScore ? 'WORSE than always-50%'
      : b.brierScore >= f.brierScore - 0.005 ? 'no better than always-50%'
        : 'better than always-50%';
  console.log(`\n  ${label}`);
  console.log(`    samples      baseline ${String(b.samples).padStart(5)}   augmented ${String(a.samples).padStart(5)}`);
  console.log(`    reference    always-50%: Brier 0.250000, directional 50.00%  -> baseline is ${verdict}`);
  console.log(`    Brier        ${b.brierScore?.toFixed(6)} -> ${a.brierScore?.toFixed(6)}   delta ${c.brier ? c.brier.raw.toFixed(6) : 'n/a'} ${c.brier?.improved ? '(better)' : c.brier && '(worse)'}`);
  console.log(`    directional  ${pct(b.directionalAccuracy)} -> ${pct(a.directionalAccuracy)}   delta ${c.directionalAccuracy ? (c.directionalAccuracy.raw * 100).toFixed(2) + 'pp' : 'n/a'}`);
  console.log(`    p10-p90 hit  ${pct(b.p10P90Coverage)} -> ${pct(a.p10P90Coverage)}`);
  console.log(`    MAE %        ${b.meanAbsoluteErrorPct?.toFixed(6)} -> ${a.meanAbsoluteErrorPct?.toFixed(6)}`);
}

function loadRealSeries(path) {
  if (!path) return null;
  const rows = readFileSync(path, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
  const byTimestamp = new Map(rows.map(row => [row.timestamp, row]));
  return byTimestamp;
}

function main() {
  const args = Object.fromEntries(
    process.argv.slice(2).join(' ').split('--').filter(Boolean)
      .map(chunk => chunk.trim().split(/\s+/))
      .map(([key, ...rest]) => [key, rest.join(' ') || true]),
  );
  const candlesPath = args.candles || 'data/sentiment_harness/candles/BTC-USD-1h.jsonl';
  const candles = loadCandles(candlesPath).map(row => ({ ...row, symbol: args.symbol || 'BTC-USD' }));
  if (candles.length < 100) {
    console.error(`refusing to run: only ${candles.length} candles at ${candlesPath}`);
    process.exit(2);
  }

  const granularityMinutes = Number(args.granularity || 60);
  const options = {
    lookbackBars: Number(args.lookback || 24),
    horizonBars: Number(args.horizon || 1),
    granularityMinutes,
    maxTiltSigma: Number(args.maxTiltSigma || 0.35),
  };

  console.log(`  candles ${candles.length} from ${candlesPath}`);
  console.log(`  window  lookback=${options.lookbackBars} bars, horizon=${options.horizonBars} bars, maxTiltSigma=${options.maxTiltSigma}`);

  console.log('\n  === harness self-test (controls, not evidence) ===');
  const selfTest = harnessSelfTest(candles, options);
  describe('oracle control  (sentiment = sign of realized return)', selfTest.oracle);
  describe('random control  (sentiment independent of price)', selfTest.random);
  describe('inverted control(sentiment = negated realized return)', selfTest.inverted);
  console.log(`\n  self-test: ${selfTest.passed ? 'PASSED' : 'FAILED'} ${JSON.stringify(selfTest.checks)}`);
  if (!selfTest.passed) {
    console.error('\n  The harness failed its own controls, so any other number above is meaningless.');
    process.exit(1);
  }

  const realByTimestamp = loadRealSeries(args.sentiment);
  if (!realByTimestamp) {
    console.log('\n  no --sentiment file supplied; run the oracle/random controls above to validate,');
    console.log('  then pass a recorded Grok series via --sentiment data/sentiment_harness/sentiment/<symbol>.jsonl');
    return;
  }

  const horizonMinutes = granularityMinutes * options.horizonBars;
  const series = new Map();
  for (let index = options.lookbackBars; index < candles.length - options.horizonBars; index += 1) {
    const row = realByTimestamp.get(candles[index].timestamp);
    if (!row) continue;
    if (Number(row.horizonMinutes) !== horizonMinutes) continue;
    series.set(index, row);
  }
  console.log(`\n  real sentiment readings matched: ${series.size} of ${candles.length - options.lookbackBars - options.horizonBars} windows`);
  const real = runHarness({ candles, sentimentSeries: series, options });
  describe('real recorded sentiment', real);
  describe('paired (identical windows, the actual verdict)', { baseline: real.paired.baseline, augmented: real.paired.augmented, comparison: real.pairedComparison, coinFlip: real.coinFlip });
  console.log(`\n  paired windows: ${real.paired.pairs}`);
  console.log('\n  by regime (unpaired regime means; read the paired block for the verdict):');
  for (const [regime, arms] of Object.entries(real.byRegime)) {
    if (arms.baseline.samples < 5) continue;
    console.log(`    ${regime.padEnd(24)} n=${String(arms.baseline.samples).padStart(4)}  Brier ${arms.baseline.brierScore.toFixed(6)} -> ${arms.augmented.brierScore?.toFixed(6)}  dir ${pct(arms.baseline.directionalAccuracy)} -> ${pct(arms.augmented.directionalAccuracy)}`);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) main();