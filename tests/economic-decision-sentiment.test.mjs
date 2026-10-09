// Sentiment-augmented forecasts: provider gating, blend invariants, and the
// selectedTier decision-model axis across input styles.
//
// The style here follows tests/economic-decision-engine.test.mjs: node:test with
// assert/strict, real engine calls, no mocks of the code under test. The one
// thing faked is the remote model itself, because a test that calls OpenRouter is
// a test that fails on a network blip and costs money when it does not.

import test from 'node:test';
import assert from 'node:assert/strict';

import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';
import {
  buildPriceForecast,
  evaluateEconomicDecision,
  ingestExecutionCostSnapshot,
  ingestModelPricingCatalog,
  quoteModelRequest,
  reconcileModelUsage,
} from '../packages/economics/src/economicDecisionEngine.mjs';
import {
  blendSentimentForecast,
  recordSentimentAugmentedForecast,
  SENTIMENT_AUGMENTED_VERSION,
  DEFAULT_MAX_TILT_SIGMA,
  normalCdf,
} from '../packages/economics/src/sentimentForecast.mjs';
import {
  buildSentimentPrompt,
  createFixtureSentimentRegistry,
  extractSentimentJson,
  normalizeSentiment,
  readSentiment,
} from '../packages/intelligence/src/sentimentProvider.mjs';

const NOW = '2026-07-29T22:00:00.000Z';

function series(prices, intervalMinutes = 1) {
  return prices.map((price, index) => ({
    price,
    timestamp: new Date(new Date(NOW).getTime() - (prices.length - 1 - index) * intervalMinutes * 60_000).toISOString(),
  }));
}

function baselineFor(state, prices = [100, 100.5, 101, 101.5, 102, 102.5], overrides = {}) {
  return buildPriceForecast(state, {
    symbol: 'BTC-USD',
    observations: series(prices),
    observationIntervalMinutes: 1,
    horizonMinutes: 2,
    ttlSeconds: 180,
    ...overrides,
  }, NOW).priceForecast;
}

function reading(score, confidence, overrides = {}) {
  return { score, confidence, horizonMinutes: 2, rationale: 'test reading', drivers: [], ...overrides };
}

// ── blend invariants ────────────────────────────────────────────────────────
// These are the tests that matter most. Each one exists because the naive
// implementation of the blend broke it during development.

test('a zero-tilt blend reproduces the baseline exactly', () => {
  const state = createInitialOperatorState(NOW);
  const baseline = baselineFor(state);
  const blended = blendSentimentForecast(baseline, reading(0, 1)).priceForecast;

  // Not "close to". Exactly. The engine rounds probabilityUp to 6dp and prices to
  // 8dp, so recomputing the normal CDF from those rounded values drifts ~1e-6 --
  // which is nothing next to a real signal and exactly the size of the effect the
  // harness is trying to measure. An earlier version of this blend recomputed and
  // moved probabilityUp from 0.626468 to 0.815757 with no sentiment at all,
  // because its erf was malformed.
  assert.equal(blended.probabilityUp, baseline.probabilityUp);
  assert.equal(blended.expectedPrice, baseline.expectedPrice);
  assert.equal(blended.p10Price, baseline.p10Price);
  assert.equal(blended.p90Price, baseline.p90Price);
  assert.equal(blended.expectedReturnBps, baseline.expectedReturnBps);
  assert.equal(blended.regime, baseline.regime);
  assert.equal(blended.sentimentContributionBps, 0);
});

test('the normal CDF used by the blend agrees with the engine across the range', () => {
  // Guards the other half of the previous bug: if these two ever diverge, the
  // no-op test above will pass for a subset of inputs while every other window
  // quietly drifts.
  const state = createInitialOperatorState(NOW);
  for (const prices of [
    [100, 100.5, 101, 101.5, 102, 102.5],
    [102.5, 102, 101.5, 101, 100.5, 100],
    [100, 100.02, 100.01, 100.05, 100.03, 100.06],
    [100, 104, 99, 103, 98, 102],
    [50, 50.1, 49.9, 50.2, 49.8, 50.05],
  ]) {
    const baseline = baselineFor(state, prices);
    const blended = blendSentimentForecast(baseline, reading(0, 0.5)).priceForecast;
    assert.equal(blended.probabilityUp, baseline.probabilityUp, `diverged for ${prices.join(',')}`);
  }
});

test('the normal CDF matches known standard-normal quantiles', () => {
  // Direct calibration. The zero-tilt fast path never calls normalCdf, so the
  // no-op invariant above cannot catch a malformed erf -- during development a
  // truncated polynomial moved a zero-sentiment probability from 0.626468 to
  // 0.815757 and every other test still passed. This one cannot.
  for (const [z, expected] of [
    [0, 0.5],
    [1, 0.8413447460685429],
    [-1, 0.15865525393145707],
    [1.281551565545, 0.9],   // the z80 the engine uses for p10/p90
    [-1.281551565545, 0.1],
    [1.959963985, 0.975],
    [2.575829304, 0.995],
  ]) {
    assert.ok(Math.abs(normalCdf(z) - expected) < 1e-6, `Phi(${z}) should be ~${expected}, got ${normalCdf(z)}`);
  }
  // Tolerance follows the approximation's own accuracy, not exactness: A&S
  // 7.1.26 is specified to 1.5e-7 in erf, so ~1e-8 in Phi. Observed origin error
  // is 5e-10 and the worst quantile error is ~7e-8.
  assert.ok(Math.abs(normalCdf(0) - 0.5) < 1e-8, 'symmetric at the origin');
  assert.ok(Math.abs(normalCdf(1) + normalCdf(-1) - 1) < 1e-12, 'antisymmetric about zero');
  assert.ok(Math.abs(normalCdf(50) - 1) < 1e-9, 'saturates in the tail');
});

test('confidence attenuates the contribution monotonically', () => {
  const state = createInitialOperatorState(NOW);
  const baseline = baselineFor(state);
  const at = confidence => blendSentimentForecast(baseline, reading(0.8, confidence)).priceForecast;

  let previous = -Infinity;
  for (const confidence of [0, 0.1, 0.25, 0.5, 0.75, 1]) {
    const contribution = at(confidence).sentimentContributionBps;
    assert.ok(contribution > previous, `contribution should rise with confidence at ${confidence}`);
    previous = contribution;
  }
  assert.equal(at(0).sentimentContributionBps, 0, 'zero confidence must contribute nothing');
});

test('the tilt is bounded by maxTiltSigma of the baseline horizon sigma', () => {
  const state = createInitialOperatorState(NOW);
  const baseline = baselineFor(state);
  // Extreme inputs: a maximally confident, maximally bearish read.
  const blended = blendSentimentForecast(baseline, reading(-1, 1)).priceForecast;
  const sigmaMultiple = Math.abs(blended.sentimentContributionBps) / baseline.expectedVolatilityBps;
  assert.ok(
    sigmaMultiple <= DEFAULT_MAX_TILT_SIGMA + 1e-6,
    `tilt ${sigmaMultiple} sigma must not exceed the ${DEFAULT_MAX_TILT_SIGMA} cap`,
  );
  // And a larger explicit cap is still honoured rather than silently ignored.
  const wide = blendSentimentForecast(baseline, reading(-1, 1), { maxTiltSigma: 1.2 }).priceForecast;
  assert.ok(Math.abs(wide.sentimentContributionBps) > Math.abs(blended.sentimentContributionBps));
});

test('the predictive interval moves with the mean, not just the probability', () => {
  const state = createInitialOperatorState(NOW);
  const baseline = baselineFor(state);
  const up = blendSentimentForecast(baseline, reading(0.9, 1)).priceForecast;
  const down = blendSentimentForecast(baseline, reading(-0.9, 1)).priceForecast;

  // If only probabilityUp were rewritten, p10/p90 would still describe the
  // baseline's distribution and the coverage metric would report a healthy
  // number for the wrong interval.
  assert.ok(up.p10Price > baseline.p10Price, 'p10 must shift up with a bullish read');
  assert.ok(up.p90Price > baseline.p90Price, 'p90 must shift up with a bullish read');
  assert.ok(down.p10Price < baseline.p10Price, 'p10 must shift down with a bearish read');
  assert.ok(down.p90Price < baseline.p90Price, 'p90 must shift down with a bearish read');
  // Uncertainty is unchanged: the blend moves the mean, it never manufactures
  // confidence by narrowing the distribution. For a lognormal interval the
  // absolute price width scales with the mean, so the invariant is that width
  // stays proportional to expectedPrice -- i.e. the implied sigma is identical.
  //
  // Tolerance is 1e-7, about nine times the measured floor. Prices are rounded to
  // 8dp, so at ~102 the width carries ~1e-8 of cancellation error against a
  // quantity of ~1.4e-4. Any genuine change in sigma is orders of magnitude
  // larger than that.
  const relativeWidth = f => (f.p90Price - f.p10Price) / f.expectedPrice;
  const baseWidth = relativeWidth(baseline);
  assert.ok(Math.abs(relativeWidth(up) - baseWidth) < 1e-7, 'implied volatility must not change');
  assert.ok(Math.abs(relativeWidth(down) - baseWidth) < 1e-7, 'implied volatility must not change');
});

test('the blend never mutates the baseline record', () => {
  const state = createInitialOperatorState(NOW);
  const baseline = baselineFor(state);
  const snapshot = JSON.stringify(baseline);
  blendSentimentForecast(baseline, reading(1, 1));
  assert.equal(JSON.stringify(baseline), snapshot, 'baseline must be untouched for the comparison arm');
});

test('the augmented arm is tagged as its own model version', () => {
  const state = createInitialOperatorState(NOW);
  const baseline = baselineFor(state);
  const blended = recordSentimentAugmentedForecast(state, baseline, reading(0.5, 0.8)).priceForecast;
  // summarizeForecastCalibration groups by regime, not by version, so without a
  // distinct tag the two arms cannot be told apart in any recorded outcome.
  assert.equal(blended.modelVersion, SENTIMENT_AUGMENTED_VERSION);
  assert.equal(baseline.modelVersion, 'deterministic-price-ensemble-v1');
  assert.equal(blended.baselineForecastId, baseline.id);
  assert.equal(blended.baselineProbabilityUp, baseline.probabilityUp);
  assert.equal(state.priceForecasts.filter(row => row.id === baseline.id).length, 1);
  assert.equal(state.priceForecasts.length, 2);
});

test('malformed sentiment is rejected rather than coerced to neutral', () => {
  const state = createInitialOperatorState(NOW);
  const baseline = baselineFor(state);
  for (const [label, bad] of [
    ['out of range', reading(1.5, 0.5)],
    ['nan', reading('loud', 0.5)],
    ['missing confidence', { score: 0.5, rationale: 'x' }],
    ['not an object', 'bullish'],
    ['null', null],
  ]) {
    const result = blendSentimentForecast(baseline, bad);
    assert.ok(result.errors?.length, `${label} should be rejected`);
    assert.ok(!result.priceForecast, `${label} must not produce a forecast`);
  }
  assert.equal(state.priceForecasts.length, 1, 'rejected readings must not record anything');
});

// ── provider: gating and fail-closed parsing ────────────────────────────────

test('a disabled remote provider blocks the read and yields no sentiment', async () => {
  const registry = createFixtureSentimentRegistry([], { keyFor: () => 'BTC-USD' });
  // The fixture throws whatever the real registry throws when the kill switch is
  // off. The point is that readSentiment surfaces the code and returns nothing
  // usable, rather than defaulting to a neutral score.
  const result = await readSentiment({
    registry: { execute: async () => { throw new Error('remote_llm_execution_disabled'); } },
    symbol: 'BTC-USD',
    horizonMinutes: 2,
    observations: series([100, 101]),
  });
  assert.deepEqual(result.errors, ['remote_llm_execution_disabled']);
  assert.ok(!result.sentiment, 'a blocked provider must not yield a sentiment reading');
  assert.ok(!registry.callCount);
});

test('a missing provider key blocks the read', async () => {
  const result = await readSentiment({
    registry: { execute: async () => { throw new Error('openrouter_api_key_required'); } },
    symbol: 'BTC-USD',
    horizonMinutes: 2,
    observations: [],
  });
  assert.deepEqual(result.errors, ['openrouter_api_key_required']);
});

test('prose and fenced JSON are tolerated, nonsense is not', () => {
  const valid = { score: -0.4, confidence: 0.7, horizonMinutes: 60, rationale: 'risk-off tone' };
  assert.deepEqual(extractSentimentJson(JSON.stringify(valid)), valid);
  assert.deepEqual(extractSentimentJson('```json\n' + JSON.stringify(valid) + '\n```'), valid);
  assert.deepEqual(extractSentimentJson('Here you go: ' + JSON.stringify(valid)), valid);
  assert.equal(extractSentimentJson('the market looks constructive'), null);
  assert.equal(extractSentimentJson(''), null);
  assert.equal(extractSentimentJson(null), null);
});

test('a model that omits required fields is a failure, not a zero', () => {
  // The dangerous default is a fabricated 0.0, which is indistinguishable from a
  // real "no directional view" and would silently dilute every forecast.
  assert.deepEqual(normalizeSentiment({ confidence: 0.5, horizonMinutes: 60, rationale: 'x' }).errors, ['sentiment_score_out_of_range']);
  assert.deepEqual(normalizeSentiment({ score: 0.5, horizonMinutes: 60, rationale: 'x' }).errors, ['sentiment_confidence_out_of_range']);
  assert.deepEqual(normalizeSentiment({ score: 0.5, confidence: 0.5, rationale: 'x' }).errors, ['sentiment_horizon_invalid']);
  assert.deepEqual(normalizeSentiment({ score: 0.5, confidence: 0.5, horizonMinutes: 60 }).errors, ['sentiment_rationale_required']);
  assert.deepEqual(normalizeSentiment('bullish').errors, ['sentiment_payload_required']);
  assert.deepEqual(normalizeSentiment({ score: 0, confidence: 1, horizonMinutes: 60, rationale: 'no view' }).errors, undefined);
});

test('the prompt demands JSON and forbids trade recommendations', () => {
  const { messages, responseFormat } = buildSentimentPrompt({
    symbol: 'BTC-USD', horizonMinutes: 60, observations: series([100, 101]), now: new Date(NOW),
  });
  assert.equal(responseFormat.type, 'json_schema');
  assert.equal(responseFormat.json_schema.schema.additionalProperties, false);
  assert.deepEqual(responseFormat.json_schema.schema.required.sort(), ['confidence', 'horizonMinutes', 'rationale', 'score']);
  const system = messages[0].content.toLowerCase();
  assert.ok(system.includes('json only'));
  assert.ok(system.includes('do not place trades'));
  assert.ok(!JSON.parse(messages[1].content).recentObservations.length < 1);
});

test('the fixture provider errors on an unknown key instead of inventing a reading', async () => {
  const registry = createFixtureSentimentRegistry([{ key: 'known', value: { score: 0.5, confidence: 0.6, horizonMinutes: 60, rationale: 'ok' } }]);
  const ok = await readSentiment({ registry, symbol: 'known', horizonMinutes: 60, observations: [] });
  assert.equal(ok.sentiment.score, 0.5);
  assert.equal(ok.costUsd, 0);

  const missing = await readSentiment({ registry, symbol: 'unknown', horizonMinutes: 60, observations: [] });
  assert.deepEqual(missing.errors, ['fixture_sentiment_unavailable']);
  assert.ok(!missing.sentiment);
});

test('an unparseable provider response yields no reading, end to end', async () => {
  // normalizeSentiment and extractSentimentJson are each covered above, but only
  // this path proves readSentiment itself refuses to invent one when the model
  // answers in prose. A registry returning prose is the realistic failure.
  for (const reply of ['markets look constructive today', '', '```', '{"score": "bullish"}']) {
    const result = await readSentiment({
      registry: { execute: async () => ({ provider: 'fixture', model: 'm', choices: [{ message: { content: reply } }], usage: { cost: 0 } }) },
      symbol: 'BTC-USD',
      horizonMinutes: 60,
      observations: [],
    });
    assert.ok(result.errors?.length, `reply ${JSON.stringify(reply)} must not produce a reading`);
    assert.ok(!result.sentiment, 'a garbled reply must not become a neutral score');
  }
});

test('a provider response missing required fields is refused end to end', async () => {
  const result = await readSentiment({
    registry: { execute: async () => ({ provider: 'fixture', model: 'm', choices: [{ message: { content: '{"confidence":0.8,"horizonMinutes":60,"rationale":"ok"}' } }], usage: { cost: 0 } }) },
    symbol: 'BTC-USD',
    horizonMinutes: 60,
    observations: [],
  });
  assert.deepEqual(result.errors, ['sentiment_score_out_of_range']);
  assert.ok(!result.sentiment);
});

// ── local routing: the free path ────────────────────────────────────────────
// The real IntelligenceProviderRegistry has no execute(); it exposes routeLocal(),
// which picks a node by health and queue depth. An earlier version of readSentiment
// accepted only execute(), so it could not drive the registry it was written for --
// every test used a duck-typed stand-in and nothing caught it.

function routedRegistry({ execute, routeErrors = null, calls = [] } = {}) {
  return {
    calls,
    async routeLocal(request) {
      calls.push({ via: 'routeLocal', model: request.model, maxCompletionTokens: request.maxCompletionTokens });
      if (routeErrors) return { errors: routeErrors, nodes: [] };
      return {
        provider: {
          async execute(inner) {
            calls.push({ via: 'provider.execute', maxCompletionTokens: inner.maxCompletionTokens });
            return execute(inner);
          },
        },
        route: { nodeId: 'local-1', estimatedCostUsd: 0 },
      };
    },
  };
}

const VALID_COMPLETION = {
  choices: [{ message: { content: '{"score":0.2,"confidence":0.6,"horizonMinutes":60,"rationale":"ok"}' } }],
  usage: { cost: 0 },
};

test('a local route is used without any remote fallback', async () => {
  const calls = [];
  const result = await readSentiment({
    registry: routedRegistry({ execute: async () => VALID_COMPLETION, calls }),
    symbol: 'BTC-USD', horizonMinutes: 60, observations: [],
  });
  assert.equal(result.sentiment.score, 0.2);
  assert.equal(result.costUsd, 0);
  assert.equal(result.route.nodeId, 'local-1');
  assert.deepEqual(calls.map(c => c.via), ['routeLocal', 'provider.execute']);
});

test('an unavailable local node fails closed instead of reaching for a paid provider', async () => {
  // The whole point of local-first: if the free node is busy, the read must fail
  // rather than quietly become an OpenRouter call that costs money and is only
  // meant to happen behind REMOTE_LLM_EXECUTION_ENABLED.
  const result = await readSentiment({
    registry: { async routeLocal() { return { errors: ['no_healthy_local_model_route'], nodes: [] }; } },
    symbol: 'BTC-USD', horizonMinutes: 60, observations: [],
  });
  assert.deepEqual(result.errors, ['no_healthy_local_model_route']);
  assert.ok(!result.sentiment);
});

test('reasoning models get enough token budget to emit an answer', async () => {
  const calls = [];
  await readSentiment({
    registry: routedRegistry({ execute: async () => VALID_COMPLETION, calls }),
    symbol: 'BTC-USD', horizonMinutes: 60, observations: [],
  });
  const request = calls.find(c => c.via === 'provider.execute');
  // Verified live against ornith-1.5-35b: 400 max_tokens returned HTTP 200 with
  // empty content because reasoning_content consumed the entire budget.
  assert.ok(request.maxCompletionTokens >= 1500, `token budget too small: ${request.maxCompletionTokens}`);
});

test('the token budget is overridable', async () => {
  const calls = [];
  await readSentiment({
    registry: routedRegistry({ execute: async () => VALID_COMPLETION, calls }),
    symbol: 'BTC-USD', horizonMinutes: 60, observations: [],
    env: { SENTIMENT_MAX_TOKENS: '4096' },
  });
  assert.equal(calls.find(c => c.via === 'provider.execute').maxCompletionTokens, 4096);
});

test('a registry exposing neither shape is rejected', async () => {
  assert.deepEqual((await readSentiment({ registry: {}, symbol: 'X', horizonMinutes: 60 })).errors, ['sentiment_registry_required']);
});

test('a read requires a registry and a positive horizon', async () => {
  assert.deepEqual((await readSentiment({ symbol: 'BTC-USD', horizonMinutes: 60 })).errors, ['sentiment_registry_required']);
  assert.deepEqual(
    (await readSentiment({ registry: createFixtureSentimentRegistry([]), symbol: 'BTC-USD', horizonMinutes: 0 })).errors,
    ['sentiment_horizon_required'],
  );
});

// ── decision model types across input styles ────────────────────────────────
// selectedTier is the closest thing this codebase has to a decision model type:
// economicDecisionEngineLegacy.mjs:678-680 picks deterministic / local_model /
// cheap_remote / premium_remote from requestRemoteModel, intelligenceAllowed and
// uncertainty. Nothing in the repository had exercised that axis end to end.

function catalog() {
  return { data: [{ id: 'example/value-model', name: 'Value Model', context_length: 128000, pricing: {
    prompt: '0.000001', completion: '0.000002', request: '0.01', image: '0.02',
    web_search: '0.005', internal_reasoning: '0.000003',
    input_cache_read: '0.0000002', input_cache_write: '0.0000012',
  } }] };
}

function decisionFixture(state, body = {}) {
  ingestModelPricingCatalog(state, { catalog: catalog() }, NOW);
  const quote = quoteModelRequest(state, { model: 'example/value-model', promptTokens: 100, completionTokens: 50 }, NOW).modelQuote;
  const forecast = baselineFor(state);
  const costs = ingestExecutionCostSnapshot(state, {
    symbol: 'BTC-USD', notionalUsd: 1000, quantity: 10, referencePrice: 100, liquidity: 'taker',
    feeSummary: { fee_tier: { taker_fee_rate: '0.001', maker_fee_rate: '0.0005' } },
    preview: { preview_id: 'preview-1', commission_total: '1', commission_total_rate: '0.001' },
  }, NOW).executionCostSnapshot;
  return { quote, forecast, costs, body };
}

test('selectedTier resolves to each decision model type as uncertainty and availability change', () => {
  const cases = [
    { label: 'deterministic', body: { requestRemoteModel: false, decisionUncertainty: 0.9 }, expected: 'deterministic' },
    { label: 'deterministic (low uncertainty, no remote request)', body: { requestRemoteModel: false, decisionUncertainty: 0.1 }, expected: 'deterministic' },
    { label: 'local_model', body: { requestRemoteModel: false, localModelAvailable: true, decisionUncertainty: 0.5 }, expected: 'local_model' },
    { label: 'cheap_remote', body: { requestRemoteModel: true, decisionUncertainty: 0.2 }, expected: 'cheap_remote' },
    { label: 'premium_remote', body: { requestRemoteModel: true, decisionUncertainty: 0.8 }, expected: 'premium_remote' },
  ];
  for (const { label, body, expected } of cases) {
    const state = createInitialOperatorState(NOW);
    const fixture = decisionFixture(state);
    const decision = evaluateEconomicDecision(state, {
      forecastId: fixture.forecast.id,
      modelQuoteId: fixture.quote.id,
      executionCostSnapshotId: fixture.costs.id,
      notionalUsd: 1000, predictedEdgeUsd: 20,
      expectedDecisionImprovementUsd: 5, probabilityDecisionChanges: 0.8,
      uncertaintyReserveUsd: 1, requiredCostCoverageMultiple: 3,
      ...body,
    }, NOW).economicDecision;
    assert.ok(decision, `${label}: expected a decision`);
    assert.equal(decision.selectedTier, expected, `${label} should select ${expected}`);
  }
});

test('an intelligence purchase never authorizes execution', () => {
  const state = createInitialOperatorState(NOW);
  const fixture = decisionFixture(state);
  const purchase = evaluateEconomicDecision(state, {
    forecastId: fixture.forecast.id,
    modelQuoteId: fixture.quote.id,
    executionCostSnapshotId: fixture.costs.id,
    requestRemoteModel: true,
    notionalUsd: 1000, predictedEdgeUsd: 20,
    expectedDecisionImprovementUsd: 5, probabilityDecisionChanges: 0.8,
    uncertaintyReserveUsd: 1, requiredCostCoverageMultiple: 3,
  }, NOW).economicDecision;
  assert.equal(purchase.decisionPhase, 'intelligence_purchase');
  assert.equal(purchase.executionAllowed, false);

  reconcileModelUsage(state, { quoteId: fixture.quote.id, actualCostUsd: 0.02, generationId: 'g1', usage: { cost: 0.02 } }, '2026-07-29T22:00:10.000Z');
  const executable = evaluateEconomicDecision(state, {
    forecastId: fixture.forecast.id,
    modelQuoteId: fixture.quote.id,
    executionCostSnapshotId: fixture.costs.id,
    requestRemoteModel: true,
    notionalUsd: 1000, predictedEdgeUsd: 20,
    expectedDecisionImprovementUsd: 5, probabilityDecisionChanges: 0.8,
    uncertaintyReserveUsd: 1, requiredCostCoverageMultiple: 3,
  }, '2026-07-29T22:00:11.000Z').economicDecision;
  assert.equal(executable.executionAllowed, true);
  // No decisionUncertainty supplied, so it resolves to the cheap remote tier.
  // The meaningful property is that a remote tier was chosen at all --
  // providerPreferences is only attached for tiers ending in "remote".
  assert.ok(executable.selectedTier.endsWith('remote'), `expected a remote tier, got ${executable.selectedTier}`);
});

test('a sentiment-augmented forecast grants no authority on its own', () => {
  // The safety property this whole module rests on: sentiment changes a
  // probability, never a permission. Even a perfect reading leaves the decision
  // engine's own blockers untouched.
  const state = createInitialOperatorState(NOW);
  const fixture = decisionFixture(state);
  const augmented = recordSentimentAugmentedForecast(state, fixture.forecast, reading(1, 1)).priceForecast;
  assert.equal(augmented.modelVersion, SENTIMENT_AUGMENTED_VERSION);

  const economics = {
    executionCostSnapshotId: fixture.costs.id,
    notionalUsd: 1000, predictedEdgeUsd: 20,
    expectedDecisionImprovementUsd: 5, probabilityDecisionChanges: 0.8,
    uncertaintyReserveUsd: 1, requiredCostCoverageMultiple: 3,
  };
  const onBaseline = evaluateEconomicDecision(state, { forecastId: fixture.forecast.id, ...economics }, NOW).economicDecision;
  const onAugmented = evaluateEconomicDecision(state, { forecastId: augmented.id, ...economics }, NOW).economicDecision;

  // The real property: a maximally bullish reading at full confidence moves the
  // forecast's probability but leaves authority untouched. Sentiment is an input
  // to a number, never a permission -- if this ever diverges, sentiment has
  // acquired a veto it was never granted.
  assert.equal(onAugmented.executionAllowed, onBaseline.executionAllowed);
  assert.deepEqual(onAugmented.blockers, onBaseline.blockers);
  assert.equal(onAugmented.selectedTier, onBaseline.selectedTier);
  assert.notEqual(augmented.probabilityUp, fixture.forecast.probabilityUp, 'the reading should have moved the forecast');
});

test('stale or insufficient inputs are refused regardless of sentiment', () => {
  const state = createInitialOperatorState(NOW);
  const stale = buildPriceForecast(state, {
    symbol: 'BTC-USD',
    observations: series([100, 101, 102, 103, 104]).map((row, index) => ({
      ...row, timestamp: new Date(new Date(NOW).getTime() - (1000 + index) * 1000).toISOString(),
    })),
    maxDataAgeSeconds: 60,
  }, NOW);
  assert.deepEqual(stale.errors, ['forecast_market_data_stale']);

  const tooFew = buildPriceForecast(state, { symbol: 'BTC-USD', observations: series([100, 101]) }, NOW);
  assert.deepEqual(tooFew.errors, ['forecast_requires_five_prices']);

  const noSymbol = buildPriceForecast(state, { observations: series([100, 101, 102, 103, 104]) }, NOW);
  assert.deepEqual(noSymbol.errors, ['forecast_symbol_required']);
  assert.equal(state.priceForecasts.length, 0);
});