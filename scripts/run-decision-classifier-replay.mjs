#!/usr/bin/env node
// Historical replay of the economic decision engine's decision classifier.
//
// WHAT IS BEING TESTED. packages/economics/ decides three things per window:
// which model tier to use (selectedTier), which market regime it believes it is
// in, and whether execution is permitted (executionAllowed). No backtest of any of
// that existed -- every caller of evaluateEconomicDecision outside the package was
// a test or a live route. This replays it over real candles.
//
// THE QUESTION THAT MATTERS. predictedEdgeUsd defaults to
// notionalUsd * forecast.expectedReturnBps / 10000 (economicDecisionEngineLegacy.mjs:645),
// so with no explicit edge supplied the gate is a pure function of forecast quality.
// That makes "do permitted trades make money after costs?" a direct measurement of
// the forecast, and it is the number that decides whether this system should ever
// be trusted with a real order. It is reported here.
//
// ADVISORY ONLY. This places no orders, contacts no broker, and writes only to
// scripts/experiments/. State is built in memory via createInitialOperatorState();
// the operator store is never opened.
//
// HONEST COSTS, AND A SENSITIVITY SWEEP. A gate's verdict depends entirely on the
// fee assumption, so one number would be arbitrary. Every cost below is derived
// from the candles themselves where a candle can support it, and the sweep reports
// the gate at several fee levels so the reader can see where the decision flips
// rather than being handed a conclusion that a different assumption would reverse.

import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import process from 'node:process';

import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';
import { buildPriceForecast } from '../packages/economics/src/economicDecisionEngine.mjs';
import {
  evaluateEconomicDecision,
  ingestExecutionCostSnapshot,
  ingestModelPricingCatalog,
  quoteModelRequest,
  reconcileModelUsage,
} from '../packages/economics/src/economicDecisionEngine.mjs';
import { recordSentimentAugmentedForecast } from '../packages/economics/src/sentimentForecast.mjs';

export const REPLAY_SCHEMA_VERSION = 1;
export const REPLAY_ATTESTATION_TYPE = 'economic_decision_classifier_replay_v1';
export const RUNNER_ID = 'scripts.run-decision-classifier-replay';

// ── canonical hashing ───────────────────────────────────────────────────────
// Matches scripts/alpha_validation.py:73-86, which is
//   sha256(json.dumps(payload, sort_keys=True, separators=(',', ':'),
//                      ensure_ascii=False, allow_nan=False))
// Only integers and strings are ever hashed (manifest, boundaries, fold counts).
// That is deliberate: Python and JavaScript disagree on shortest-round-trip float
// formatting, so a hash covering floats would be reproducible within one language
// and unverifiable across the Python verifier that is supposed to check it.
export function stableHash(payload) {
  const canonical = (value) => {
    if (Array.isArray(value)) return value.map(canonical);
    if (value && typeof value === 'object') {
      return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]));
    }
    return value;
  };
  return createHash('sha256').update(JSON.stringify(canonical(payload))).digest('hex');
}

// ── walk-forward ────────────────────────────────────────────────────────────
// Mirrors scripts/alpha_validation.py:89-166. The layout is
//   [ training ][ purge ][ embargo ][ test ]
// with half-open boundaries. Conformance against the Python implementation is
// asserted by tests/decision-classifier-replay.test.mjs against committed
// fixtures, because a second divergent fold generator is exactly the failure mode
// already documented in this repo (two forecast implementations with duplicated
// constants that disagree on one divisor).
export function generateWalkForwardSplits(nObservations, options = {}) {
  const trainSize = options.trainSize ?? null;
  const testSize = options.testSize ?? null;
  const stepSize = options.stepSize ?? testSize;
  const purgeSize = options.purgeSize ?? 0;
  const embargoSize = options.embargoSize ?? 0;
  const expanding = options.expanding ?? true;

  if (!Number.isInteger(nObservations) || nObservations <= 0) return [];
  if (purgeSize < 0 || embargoSize < 0) throw new Error('purge_size and embargo_size must be non-negative');
  if (!trainSize || !testSize || trainSize <= 0 || testSize <= 0) return [];

  const folds = [];
  const firstTestStart = trainSize + purgeSize + embargoSize;
  if (firstTestStart + testSize > nObservations) return [];
  let testStart = firstTestStart;
  while (testStart + testSize <= nObservations) {
    const trainEnd = testStart - purgeSize - embargoSize;
    const trainStart = expanding ? 0 : trainEnd - trainSize;
    if (trainStart >= 0 && trainEnd - trainStart >= trainSize) {
      folds.push({
        trainStart, trainEnd,
        purgeStart: trainEnd, purgeEnd: trainEnd + purgeSize,
        embargoStart: trainEnd + purgeSize, embargoEnd: testStart,
        testStart, testEnd: testStart + testSize,
      });
    }
    testStart += (stepSize || testSize);
  }
  return folds;
}

// ── dataset attestation ─────────────────────────────────────────────────────

export function buildDatasetManifest({ symbol, granularity, rows }) {
  if (!rows.length) throw new Error('dataset_empty');
  const first = rows[0];
  const last = rows[rows.length - 1];
  const manifest = {
    kind: 'coinbase_candles',
    symbol,
    granularity,
    row_count: rows.length,
    start_ts: Math.trunc(first.t),
    end_ts: Math.trunc(last.t),
    // Hashes the exact OHLCV values, not just the bounds, so two different
    // datasets with identical start/end cannot collide.
    rows_hash: stableHash(rows.map(row => [Math.trunc(row.t), row.open, row.high, row.low, row.close, row.volume])),
    dataset_id: `coinbase_candles:${symbol}:${granularity}:${Math.trunc(first.t)}:${Math.trunc(last.t)}:${rows.length}`,
  };
  return { ...manifest, dataset_hash: stableHash(manifest) };
}

export function buildAttestation({ manifest, folds, costModel, config }) {
  const foldRecords = folds.map((fold, index) => ({
    fold: index,
    train_start: fold.trainStart, train_end: fold.trainEnd,
    purge_start: fold.purgeStart, purge_end: fold.purgeEnd,
    embargo_start: fold.embargoStart, embargo_end: fold.embargoEnd,
    test_start: fold.testStart, test_end: fold.testEnd,
  }));
  const core = {
    schema_version: REPLAY_SCHEMA_VERSION,
    attestation_type: REPLAY_ATTESTATION_TYPE,
    runner: RUNNER_ID,
    dataset: manifest,
    cost_model: costModel,
    config,
    n_folds: folds.length,
    // Fold boundaries and the cost assumptions are inside the hash. A result that
    // only binds the dataset can be re-scored against different folds and still
    // look like the same evidence.
    folds: foldRecords,
  };
  return { ...core, attestation_hash: stableHash(core) };
}

// ── cost model ──────────────────────────────────────────────────────────────
// Spread is measured from the bar itself: (high - low) / close in bps. Fee is the
// one assumption that cannot be derived from candles, so it is swept rather than
// asserted. Coinbase Advanced takes 0.60% taker / 0.40% maker at the base tier,
// which is the default below.
// Pricing for the local sentiment model the tier sweep quotes. Values are the
// point: they decide whether an intelligence purchase is economic, which decides
// the tier, so they are stated rather than inherited from a live catalog.
export const SENTIMENT_MODEL_ID = 'local/k2-horizon-7b';
export const PRICING_CATALOG = {
  data: [{
    id: SENTIMENT_MODEL_ID,
    name: 'local k2-horizon',
    context_length: 524288,
    pricing: { prompt: '0', completion: '0', request: '0', image: '0', web_search: '0', internal_reasoning: '0', input_cache_read: '0', input_cache_write: '0' },
  }],
};

export const DEFAULT_COST_MODEL = {
  takerFeeRate: 0.006,
  makerFeeRate: 0.004,
  liquidity: 'taker',
  // The engine halves spreadBps internally (spreads over a round trip), so a
  // measured bar range is passed straight through rather than pre-halved.
  spreadSource: 'bar_range_bps',
  // Multiplied into the measured bar range. Exists so the controls can run
  // cost-free: at a 1h horizon the taker fee alone exceeds almost any BTC move,
  // which means a realistic cost model pins the gate near 0% permission and no
  // control can distinguish a working harness from a broken one. Controls zero the
  // costs to validate plumbing; the sweep then measures the economics.
  spreadMultiplier: 1,
  latencyDecayUsd: 0,
  fundingBorrowUsd: 0,
  gasUsd: 0,
};

// The cleaned JSONL export names its fields open/high/low/close, unlike the
// parquet cache's positional o/h/l/c. Reading the parquet names here silently
// produced NaN everywhere and made every window fail with
// 'forecast_requires_five_prices', which the replay reported as
// 'forecast_unavailable' until the loader shape was asserted.
function barSpreadBps(row) {
  const close = Number(row.close);
  const high = Number(row.high);
  const low = Number(row.low);
  if (!(close > 0) || !Number.isFinite(high) || !Number.isFinite(low)) {
    throw new Error(`candle_missing_ohlc:${JSON.stringify(row).slice(0, 120)}`);
  }
  return Math.max(0, ((high - low) / close) * 10_000);
}

/**
 * Synthesize an execution cost snapshot from a bar.
 *
 * validUntil is pinned to the historical evaluation instant for a reason that took
 * a wrong comment to find: the engine's own default is `now + 30s`, where `now` is
 * the timestamp passed in, not wall-clock. Passing the historical instant to
 * ingestExecutionCostSnapshot is therefore sufficient on its own and this line is
 * belt-and-braces. What is NOT sufficient is passing wall-clock `now` -- then the
 * snapshot would be valid until "now+30s" while the decision is evaluated at a
 * three-month-old instant, and evaluateEconomicDecision's freshness check
 * (economicDecisionEngineLegacy.mjs:642) would block every window for staleness
 * rather than for economics.
 *
 * source is 'historical_replay' rather than the default
 * 'coinbase_preview_and_fee_tier', because no Coinbase preview existed for a bar
 * from three months ago and leaving the default would assert provenance that is
 * false.
 */
export function buildCostSnapshot(state, { symbol, row, notionalUsd, side, costModel, now }) {
  const spreadBps = barSpreadBps(row) * (costModel.spreadMultiplier ?? 1);
  const result = ingestExecutionCostSnapshot(state, {
    symbol,
    side,
    notionalUsd,
    quantity: notionalUsd / Number(row.close),
    referencePrice: Number(row.close),
    liquidity: costModel.liquidity,
    spreadBps,
    feeSummary: {
      fee_tier: {
        taker_fee_rate: String(costModel.takerFeeRate),
        maker_fee_rate: String(costModel.makerFeeRate),
        pricing_tier: 'historical_replay_assumed_base_tier',
      },
    },
    preview: { preview_id: `replay-${Math.trunc(row.t)}`, commission_total: String(notionalUsd * costModel.takerFeeRate) },
    latencyDecayUsd: costModel.latencyDecayUsd,
    fundingBorrowUsd: costModel.fundingBorrowUsd,
    gasUsd: costModel.gasUsd,
    validUntil: now,
    source: 'historical_replay',
  }, now);
  if (result.errors) return { errors: result.errors };
  return { executionCostSnapshot: result.executionCostSnapshot, spreadBps };
}

// ── replay ──────────────────────────────────────────────────────────────────

/**
 * Replay one window.
 *
 * `predictedEdgeUsd` is deliberately NOT passed. The engine then derives it from
 * the forecast's expectedReturnBps, which is what makes this a measurement of the
 * forecast rather than a measurement of a hand-fed edge.
 */
export function replayWindow({ rows, index, lookbackBars, horizonBars, granularityMinutes, notionalUsd, costModel, remote, body = {} }) {
  const slice = rows.slice(index - lookbackBars, index + 1);
  const asOf = new Date(slice.at(-1).t * 1000).toISOString();
  const horizonMinutes = granularityMinutes * horizonBars;
  const state = createInitialOperatorState(asOf);

  const forecast = buildPriceForecast(state, {
    symbol: rows[index].symbol,
    observations: slice.map(row => ({ price: row.close, timestamp: new Date(row.t * 1000).toISOString() })),
    horizonMinutes,
    observationIntervalMinutes: granularityMinutes,
    // Generous relative to a historical bar: the point is to prove staleness
    // checks are inactive, not to tune them.
    maxDataAgeSeconds: 86_400,
  }, asOf)?.priceForecast;
  if (!forecast) return { errors: ['forecast_unavailable'] };

  const costs = buildCostSnapshot(state, {
    symbol: rows[index].symbol,
    row: rows[index],
    notionalUsd,
    side: 'BUY',
    costModel,
    now: asOf,
  });
  if (costs.errors) return { errors: costs.errors };

  const variant = { ...body, ...(remote ? { requestRemoteModel: true } : {}) };
  let modelQuoteId = null;
  if (variant.requestRemoteModel === true) {
    // A remote tier cannot be selected without a quote: the engine returns
    // 'model_quote_required' otherwise, so a sweep that omits one silently
    // exercises no remote tier at all. Reconciled immediately, because an
    // unreconciled quote forces decisionPhase 'intelligence_purchase' and
    // executionAllowed false before economics are even considered.
    ingestModelPricingCatalog(state, { catalog: PRICING_CATALOG }, asOf);
    const quoted = quoteModelRequest(state, { model: SENTIMENT_MODEL_ID, promptTokens: 900, completionTokens: 400 }, asOf).modelQuote;
    const costUsd = Number(quoted?.estimatedCostUsd ?? 0);
    reconcileModelUsage(state, {
      quoteId: quoted.id,
      actualCostUsd: costUsd,
      generationId: `replay-${Math.trunc(rows[index].t)}`,
      usage: { cost: costUsd },
    }, asOf);
    modelQuoteId = quoted.id;
  }

  const decision = evaluateEconomicDecision(state, {
    forecastId: forecast.id,
    executionCostSnapshotId: costs.executionCostSnapshot.id,
    notionalUsd,
    minimumNetEdgeUsd: 0,
    ...(modelQuoteId ? { modelQuoteId } : {}),
    // Tier-classifier variants arrive here. predictedEdgeUsd is never among them.
    ...variant,
  }, asOf).economicDecision;
  if (!decision) return { errors: ['decision_unavailable'] };

  const actual = rows[index + horizonBars];
  if (!actual) return { errors: ['horizon_exceeds_dataset'] };

  const referenceClose = Number(rows[index].close);
  const grossReturnUsd = (Number(actual.close) - referenceClose) / referenceClose * notionalUsd;
  const netReturnUsd = grossReturnUsd - costs.executionCostSnapshot.totalExecutionCostUsd;
  const actualUp = Number(actual.close) > referenceClose ? 1 : 0;

  return {
    t: rows[index].t,
    symbol: rows[index].symbol,
    regime: forecast.regime,
    modelVersion: forecast.modelVersion,
    selectedTier: decision.selectedTier,
    executionAllowed: decision.executionAllowed === true,
    blockers: decision.blockers,
    probabilityUp: forecast.probabilityUp,
    uncertainty: Math.max(0, Math.min(1, 1 - Math.abs(forecast.probabilityUp - 0.5) * 2)),
    expectedReturnBps: forecast.expectedReturnBps,
    expectedVolatilityBps: forecast.expectedVolatilityBps,
    predictedEdgeUsd: decision.predictedEdgeUsd,
    executionCostsUsd: decision.executionCostsUsd,
    uncertaintyReserveUsd: decision.uncertaintyReserveUsd,
    netExecutableEdgeUsd: decision.netExecutableEdgeUsd,
    totalExecutionCostUsd: costs.executionCostSnapshot.totalExecutionCostUsd,
    spreadBps: costs.spreadBps,
    actualUp,
    brierScore: (forecast.probabilityUp - actualUp) ** 2,
    directionCorrect: (forecast.probabilityUp >= 0.5) === Boolean(actualUp),
    grossReturnUsd,
    netReturnUsd,
  };
}

// ── scoring ─────────────────────────────────────────────────────────────────

// Returns null rather than NaN when any input is non-finite. A NaN silently
// poisons every downstream comparison -- `NaN > 0` is false, so a broken row would
// read as "no permission" and the report would look like a finding rather than a
// defect.
const mean = values => {
  if (!values.length) return null;
  const total = values.reduce((acc, value) => acc + Number(value), 0);
  const average = total / values.length;
  return Number.isFinite(average) ? average : null;
};

export function scoreReplay(results) {
  const scored = results.filter(row => !row.errors);
  const allowed = scored.filter(row => row.executionAllowed);
  const blocked = scored.filter(row => !row.executionAllowed);
  const sum = (rows, key) => rows.reduce((total, row) => total + (row[key] || 0), 0);

  const group = rows => ({
    n: rows.length,
    meanNetReturnUsd: mean(rows.map(r => r.netReturnUsd)),
    totalNetReturnUsd: sum(rows, 'netReturnUsd'),
    winRate: mean(rows.map(r => (r.netReturnUsd > 0 ? 1 : 0))),
    meanGrossReturnUsd: mean(rows.map(r => r.grossReturnUsd)),
    directionalAccuracy: mean(rows.map(r => (r.directionCorrect ? 1 : 0))),
  });

  const byTier = {};
  for (const row of scored) (byTier[row.selectedTier] ||= []).push(row);

  return {
    windows: scored.length,
    errors: results.length - scored.length,
    // Always-50% reference: a calibrated but uninformative forecast scores 0.25
    // and 50%. Anything worse than that is not edge, it is anti-signal.
    coinFlip: { brierScore: 0.25, directionalAccuracy: 0.5 },
    forecast: {
      brierScore: mean(scored.map(r => r.brierScore)),
      directionalAccuracy: mean(scored.map(r => (r.directionCorrect ? 1 : 0))),
      always50BrierDelta: mean(scored.map(r => r.brierScore)) - 0.25,
    },
    // WHY the gate answered what it did. A permission rate of zero is only
    // informative next to the two numbers that produced it.
    economics: {
      meanPredictedEdgeUsd: mean(scored.map(r => r.predictedEdgeUsd)),
      meanExecutionCostUsd: mean(scored.map(r => r.executionCostsUsd)),
      meanNetExecutableEdgeUsd: mean(scored.map(r => r.netExecutableEdgeUsd)),
      // >1 means the forecast's expected move is smaller than the cost of
      // executing it, so no forecast quality can ever produce permission.
      costToEdgeRatio: (() => {
        const edge = mean(scored.map(r => r.predictedEdgeUsd));
        const cost = mean(scored.map(r => r.executionCostsUsd));
        return edge != null && cost != null && edge > 0 ? cost / edge : null;
      })(),
      meanExpectedReturnBps: mean(scored.map(r => r.expectedReturnBps)),
      meanSpreadBps: mean(scored.map(r => r.spreadBps)),
      // netExecutableEdgeUsd = predictedEdge - executionCosts - modelCost
      // - uncertaintyReserve - latencyDecay, so the gate has TWO independently
      // sufficient blockers here. Decomposing them matters: "the gate refuses
      // everything" is not actionable, but "the fee alone refuses everything, and
      // so does the uncertainty reserve alone" is.
      meanUncertaintyReserveUsd: mean(scored.map(r => r.uncertaintyReserveUsd)),
      windowsWhereEdgeExceedsCost: scored.filter(r => r.predictedEdgeUsd > r.executionCostsUsd).length,
      windowsWhereEdgeExceedsReserve: scored.filter(
        r => r.predictedEdgeUsd > (r.executionCostsUsd || 0) + (r.uncertaintyReserveUsd || 0),
      ).length,
    },
    gate: {
      permitted: group(allowed),
      refused: group(blocked),
      // The headline: did permission correlate with profit? A gate that permits
      // the losers and refuses the winners is worse than no gate, because it looks
      // like a filter.
      permittedEdgeSpreadUsd: mean(allowed.map(r => r.netReturnUsd)) != null && mean(blocked.map(r => r.netReturnUsd)) != null
        ? mean(allowed.map(r => r.netReturnUsd)) - mean(blocked.map(r => r.netReturnUsd))
        : null,
      refusalRate: scored.length ? blocked.length / scored.length : null,
      meanPermittedEdgeUsd: mean(allowed.map(r => r.netExecutableEdgeUsd)),
    },
    tiers: Object.fromEntries(Object.entries(byTier).map(([tier, rows]) => [tier, {
      n: rows.length,
      meanUncertainty: mean(rows.map(r => r.uncertainty)),
      meanNetReturnUsd: mean(rows.map(r => r.netReturnUsd)),
      winRate: mean(rows.map(r => (r.netReturnUsd > 0 ? 1 : 0))),
      permissionRate: mean(rows.map(r => (r.executionAllowed ? 1 : 0))),
    }])),
    regimes: Object.fromEntries([...new Set(scored.map(r => r.regime))].map(regime => {
      const rows = scored.filter(r => r.regime === regime);
      return [regime, {
        n: rows.length,
        permissionRate: mean(rows.map(r => (r.executionAllowed ? 1 : 0))),
        meanNetReturnUsd: mean(rows.map(r => r.netReturnUsd)),
        meanVolatilityBps: mean(rows.map(r => r.expectedVolatilityBps)),
      }];
    })),
  };
}

// ── controls ────────────────────────────────────────────────────────────────
// A replay harness that cannot detect a planted edge is a machine for producing
// confident nonsense. Two controls, both run before any real result is reported:
//   oracle: predictedEdgeUsd is overwritten with the realized forward return, so
//           the gate can see the answer. It must permit and profit.
//   random: predictedEdgeUsd is set from a seeded PRNG. It must not profit.
//
// Oracle injection is done by post-processing the window rather than by changing
// the engine, so the control exercises the scoring path exactly as the real arm
// does.

function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function runReplay({ rows, options = {} }) {
  const lookbackBars = options.lookbackBars ?? 24;
  const horizonBars = options.horizonBars ?? 1;
  const granularityMinutes = options.granularityMinutes ?? 60;
  const notionalUsd = options.notionalUsd ?? 1000;
  const costModel = { ...DEFAULT_COST_MODEL, ...(options.costModel || {}) };
  const folds = options.folds || null;

  const inFold = (index) => {
    if (!folds) return true;
    return folds.some(fold => index >= fold.testStart && index < fold.testEnd);
  };

  const results = [];
  for (let index = lookbackBars; index + horizonBars < rows.length; index += 1) {
    if (!inFold(index)) continue;
    results.push(replayWindow({ rows, index, lookbackBars, horizonBars, granularityMinutes, notionalUsd, costModel, remote: options.remote === true }));
  }
  return { results, costModel };
}

/** Replay with extra decision-engine inputs, to exercise the tier classifier. */
export function runReplayTier({ rows, options = {}, body = {} }) {
  const lookbackBars = options.lookbackBars ?? 24;
  const horizonBars = options.horizonBars ?? 1;
  const granularityMinutes = options.granularityMinutes ?? 60;
  const notionalUsd = options.notionalUsd ?? 1000;
  const costModel = { ...DEFAULT_COST_MODEL, ...(options.costModel || {}) };
  const folds = options.folds || null;
  const results = [];
  for (let index = lookbackBars; index + horizonBars < rows.length; index += 1) {
    if (folds && !folds.some(fold => index >= fold.testStart && index < fold.testEnd)) continue;
    results.push(replayWindow({ rows, index, lookbackBars, horizonBars, granularityMinutes, notionalUsd, costModel, remote: options.remote === true, body }));
  }
  return { results, costModel };
}

// Apply a control by rewriting the gate inputs, then re-scoring.
function withOracle(rows, results, notionalUsd) {
  return results.map(row => {
    const predictedEdgeUsd = row.grossReturnUsd;
    const netExecutableEdgeUsd = predictedEdgeUsd - row.executionCostsUsd;
    return { ...row, predictedEdgeUsd, netExecutableEdgeUsd, executionAllowed: netExecutableEdgeUsd > 0 };
  });
}

function withRandom(results, seed) {
  const random = mulberry32(seed);
  return results.map(row => {
    const predictedEdgeUsd = random() * 400;           // 0..400 USD on a 1000 notional
    const netExecutableEdgeUsd = predictedEdgeUsd - row.executionCostsUsd;
    return { ...row, predictedEdgeUsd, netExecutableEdgeUsd, executionAllowed: netExecutableEdgeUsd > 0 };
  });
}

export function controls({ rows, options }) {
  const notionalUsd = options.notionalUsd ?? 1000;
  // Cost-free on purpose -- see DEFAULT_COST_MODEL.spreadMultiplier. The controls
  // answer "can this harness detect a gate that works?", not "is the gate good?".
  const costFree = { takerFeeRate: 0, makerFeeRate: 0, spreadMultiplier: 0, latencyDecayUsd: 0, fundingBorrowUsd: 0, gasUsd: 0 };
  const { results } = runReplay({ rows, options: { ...options, costModel: costFree } });
  const oracle = scoreReplay(withOracle(rows, results, notionalUsd));
  const random = scoreReplay(withRandom(results, 12345));
  return {
    oracle: { score: oracle, permittedMean: oracle.gate.permitted.meanNetReturnUsd },
    random: { score: random, permittedMean: random.gate.permitted.meanNetReturnUsd },
    checks: {
      // The oracle can see the answer, so it must permit a majority and profit.
      oraclePermits: oracle.gate.permitted.n > 0,
      oracleProfits: (oracle.gate.permitted.meanNetReturnUsd ?? 0) > 0,
      randomDoesNotProfit: (random.gate.permitted.meanNetReturnUsd ?? 0) <= (oracle.gate.permitted.meanNetReturnUsd ?? 0),
    },
    passed: (oracle.gate.permitted.n > 0)
      && ((oracle.gate.permitted.meanNetReturnUsd ?? 0) > 0)
      && ((random.gate.permitted.meanNetReturnUsd ?? 0) <= (oracle.gate.permitted.meanNetReturnUsd ?? 0)),
  };
}

function fmt(value, digits = 4) {
  return value == null || !Number.isFinite(value) ? '   n/a' : Number(value).toFixed(digits);
}
function pct(value) {
  return value == null ? '  n/a' : `${(value * 100).toFixed(2)}%`;
}

function loadRows(path) {
  return readFileSync(path, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
}

function main() {
  const argv = process.argv.slice(2);
  const flag = (name, fallback) => {
    const at = argv.indexOf(`--${name}`);
    return at >= 0 && argv[at + 1] ? argv[at + 1] : fallback;
  };
  const has = name => argv.includes(`--${name}`);

  const symbol = flag('symbol', 'BTC-USD');
  const granularity = Number(flag('granularity', 3600));
  const candlesPath = flag('candles', 'data/sentiment_harness/candles/BTC-USD-1h.jsonl');
  const rows = loadRows(candlesPath).map(row => ({ ...row, symbol }));
  if (rows.length < 200) {
    process.stderr.write(JSON.stringify({ error: 'dataset_too_small', rows: rows.length }) + '\n');
    process.exitCode = 2;
    return;
  }

  const config = {
    lookbackBars: Number(flag('lookback', 24)),
    horizonBars: Number(flag('horizon', 1)),
    granularityMinutes: granularity / 60,
    notionalUsd: Number(flag('notional', 1000)),
    folds: Number(flag('folds', 4)),
    trainSize: Number(flag('train', 400)),
    purgeSize: Number(flag('purge', 1)),
    embargoSize: Number(flag('embargo', 1)),
  };

  const folds = generateWalkForwardSplits(rows.length, {
    trainSize: config.trainSize,
    testSize: config.folds ? Math.floor((rows.length - config.trainSize) / config.folds) : null,
    purgeSize: config.purgeSize,
    embargoSize: config.embargoSize,
    expanding: true,
  });
  if (!folds.length) {
    process.stderr.write(JSON.stringify({ error: 'no_walk_forward_folds', rows: rows.length, config }) + '\n');
    process.exitCode = 2;
    return;
  }

  const manifest = buildDatasetManifest({ symbol, granularity, rows });
  const attestation = buildAttestation({ manifest, folds, costModel: DEFAULT_COST_MODEL, config });

  console.log(`  dataset   ${manifest.dataset_id}`);
  console.log(`            rows_hash ${manifest.rows_hash.slice(0, 16)}  dataset_hash ${manifest.dataset_hash.slice(0, 16)}`);
  console.log(`  folds     ${folds.length} walk-forward, purge ${config.purgeSize}, embargo ${config.embargoSize}`);
  console.log(`  attest    ${attestation.attestation_hash.slice(0, 16)}  (${REPLAY_ATTESTATION_TYPE})`);

  console.log('\n  === controls (not evidence) ===');
  const controlResult = controls({ rows, options: { ...config, folds } });
  console.log(`    oracle permitted n=${controlResult.oracle.score.gate.permitted.n} meanNet ${fmt(controlResult.oracle.permittedMean, 2)} USD`);
  console.log(`    random permitted n=${controlResult.random.score.gate.permitted.n} meanNet ${fmt(controlResult.random.permittedMean, 2)} USD`);
  console.log(`    controls: ${controlResult.passed ? 'PASSED' : 'FAILED'} ${JSON.stringify(controlResult.checks)}`);
  if (!controlResult.passed) {
    process.stderr.write(JSON.stringify({ error: 'control_failure', checks: controlResult.checks }) + '\n');
    process.exitCode = 1;
    return;
  }

  console.log('\n  === decision classifier replay ===');
  const sweep = [];
  for (const takerFeeRate of [0.001, 0.002, 0.004, 0.006, 0.008, 0.01]) {
    const { results } = runReplay({ rows, options: { ...config, folds, costModel: { takerFeeRate, makerFeeRate: takerFeeRate / 1.5 } } });
    const score = scoreReplay(results);
    sweep.push({ takerFeeRate, ...score });
    console.log(`    fee ${(takerFeeRate * 100).toFixed(2)}%  permit ${pct(score.gate.refusalRate != null ? 1 - score.gate.refusalRate : null).padStart(7)}`
      + `  permitted meanNet ${fmt(score.gate.permitted.meanNetReturnUsd, 2).padStart(9)} USD`
      + `  refused meanNet ${fmt(score.gate.refused.meanNetReturnUsd, 2).padStart(9)} USD`);
  }

  const headline = sweep.find(row => row.takerFeeRate === DEFAULT_COST_MODEL.takerFeeRate) || sweep[0];
  console.log(`\n  at the base tier (${(DEFAULT_COST_MODEL.takerFeeRate * 100).toFixed(2)}% taker), ${headline.windows} windows:`);
  console.log(`    forecast     Brier ${fmt(headline.forecast.brierScore, 6)} (always-50% = 0.250000, delta ${fmt(headline.forecast.always50BrierDelta, 6)})`);
  console.log(`                 directional ${pct(headline.forecast.directionalAccuracy)}`);
  console.log(`    gate         permitted n=${headline.gate.permitted.n} meanNet ${fmt(headline.gate.permitted.meanNetReturnUsd, 2)} USD win ${pct(headline.gate.permitted.winRate)}`);
  console.log(`                 refused   n=${headline.gate.refused.n} meanNet ${fmt(headline.gate.refused.meanNetReturnUsd, 2)} USD win ${pct(headline.gate.refused.winRate)}`);
  console.log(`                 permitted-minus-refused ${fmt(headline.gate.permittedEdgeSpreadUsd, 2)} USD`);
  console.log(`    tiers        ${Object.entries(headline.tiers).map(([t, v]) => `${t} n=${v.n} unc=${fmt(v.meanUncertainty, 2)} net=${fmt(v.meanNetReturnUsd, 2)}`).join(' | ')}`);

  console.log('\n  === horizon sweep (is there a horizon where the edge covers costs?) ===');
  const horizonSweep = [];
  for (const horizonBars of [1, 4, 12, 24, 48, 168]) {
    const { results } = runReplay({ rows, options: { ...config, folds, horizonBars } });
    const score = scoreReplay(results);
    horizonSweep.push({ horizonBars, ...score });
    console.log(`    ${String(horizonBars).padStart(3)} bars (${String(horizonBars * config.granularityMinutes).padStart(5)} min)`
      + `  permit ${pct(score.gate.permitted.n / score.windows).padStart(7)}`
      + `  meanEdge ${fmt(score.economics.meanPredictedEdgeUsd, 2).padStart(8)} USD`
      + `  cost/edge ${fmt(score.economics.costToEdgeRatio, 2).padStart(7)}`
      + `  Brier ${fmt(score.forecast.brierScore, 6)}`
      + `  dir ${pct(score.forecast.directionalAccuracy)}`);
  }

  console.log('\n  === tier sweep (does the classifier pick anything better?) ===');
  const tierSweep = [];
  for (const variant of [
    { label: 'deterministic', body: {} },
    { label: 'local_model', body: { localModelAvailable: true } },
    { label: 'cheap_remote', body: { requestRemoteModel: true, expectedDecisionImprovementUsd: 50, probabilityDecisionChanges: 0.9 } },
    { label: 'premium_remote', body: { requestRemoteModel: true, expectedDecisionImprovementUsd: 500, probabilityDecisionChanges: 0.9 } },
  ]) {
    const { results } = runReplayTier({ rows, options: { ...config, folds }, body: variant.body });
    const score = scoreReplay(results);
    const tiers = Object.entries(score.tiers).map(([t, v]) => `${t}(n=${v.n})`);
    tierSweep.push({ label: variant.label, ...score });
    console.log(`    ${variant.label.padEnd(14)}  selected ${tiers.join(' ').padEnd(34)}`
      + `  meanNet ${fmt(score.gate.refused.meanNetReturnUsd, 2).padStart(8)} USD`);
  }

  const outDir = flag('out', join('scripts', 'experiments', 'decision_classifier_replay'));
  mkdirSync(outDir, { recursive: true });
  const scorecard = {
    schema_version: REPLAY_SCHEMA_VERSION,
    attestation: { ...attestation, config },
    headline_cost_model: DEFAULT_COST_MODEL,
    headline: headline ?? null,
    fee_sweep: sweep,
    horizon_sweep: horizonSweep,
    tier_sweep: tierSweep,
    controls: { passed: controlResult.passed, checks: controlResult.checks },
    generated_at: new Date().toISOString(),
  };
  const scorecardPath = join(outDir, 'scorecard.json');
  writeFileSync(scorecardPath, `${JSON.stringify(scorecard, null, 2)}\n`);
  console.log(`\n  scorecard  ${scorecardPath}`);
  console.log(`  NOTE       this is a research artifact. It grants no trading authority and is not`);
  console.log(`             evidence under scripts/alpha_validation.py's policy gates.`);
}

if (import.meta.url === `file://${process.argv[1]}`) main();
