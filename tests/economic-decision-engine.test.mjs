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
  recordAgentAttribution,
  recordForecastOutcome,
  summarizeAgentAttribution,
  summarizeForecastCalibration,
} from '../packages/economics/src/economicDecisionEngine.mjs';

const NOW = '2026-07-29T22:00:00.000Z';

function catalog() {
  return {
    data: [{
      id: 'example/value-model',
      name: 'Value Model',
      context_length: 128000,
      pricing: {
        prompt: '0.000001',
        completion: '0.000002',
        request: '0.01',
        image: '0.02',
        web_search: '0.005',
        internal_reasoning: '0.000003',
        input_cache_read: '0.0000002',
        input_cache_write: '0.0000012',
      },
    }],
  };
}

test('quotes remote model requests from versioned component pricing', () => {
  const state = createInitialOperatorState(NOW);
  const ingested = ingestModelPricingCatalog(state, { catalog: catalog() }, NOW);
  assert.equal(ingested.pricingSnapshot.modelCount, 1);

  const result = quoteModelRequest(state, {
    provider: 'openrouter',
    model: 'example/value-model',
    promptTokens: 1000,
    completionTokens: 100,
    reasoningTokens: 50,
    cacheReadTokens: 500,
    cacheWriteTokens: 200,
    webSearchRequests: 2,
    imageUnits: 1,
  }, NOW);

  assert.equal(result.modelQuote.status, 'quoted');
  assert.equal(result.modelQuote.estimatedCostUsd, 0.04169);
  assert.equal(result.modelQuote.providerPreferences.sort, 'price');
  assert.equal(result.modelQuote.providerPreferences.max_price.prompt, 1);
  assert.equal(result.modelQuote.providerPreferences.max_price.completion, 2);
});

test('remote quote fails closed when pricing is stale or missing', () => {
  const state = createInitialOperatorState(NOW);
  ingestModelPricingCatalog(state, { catalog: catalog() }, '2026-07-27T22:00:00.000Z');
  const result = quoteModelRequest(state, {
    model: 'example/value-model',
    promptTokens: 10,
    maxPricingAgeSeconds: 60,
  }, NOW);
  assert.deepEqual(result.errors, ['model_pricing_snapshot_stale']);
});

test('provider usage becomes the authoritative reconciled cost', () => {
  const state = createInitialOperatorState(NOW);
  ingestModelPricingCatalog(state, { catalog: catalog() }, NOW);
  const quote = quoteModelRequest(state, { model: 'example/value-model', promptTokens: 1000, completionTokens: 100 }, NOW).modelQuote;
  const result = reconcileModelUsage(state, {
    quoteId: quote.id,
    generationId: 'gen-123',
    usage: {
      prompt_tokens: 900,
      completion_tokens: 120,
      cost: 0.019,
      prompt_tokens_details: { cached_tokens: 400, cache_write_tokens: 100 },
      completion_tokens_details: { reasoning_tokens: 30 },
      cost_details: { upstream_inference_cost: 0.016 },
    },
  }, NOW);
  assert.equal(result.modelUsage.actualCostUsd, 0.019);
  assert.equal(result.modelUsage.generationId, 'gen-123');
  assert.equal(result.modelUsage.cacheReadTokensActual, 400);
  assert.equal(result.modelUsage.reasoningTokensActual, 30);
});

test('builds an expiring probabilistic forecast instead of a fixed price target', () => {
  const state = createInitialOperatorState(NOW);
  const observations = [100, 100.2, 100.4, 100.8, 101.1, 101.5, 101.8].map((price, index) => ({
    price,
    timestamp: new Date(new Date(NOW).getTime() - (6 - index) * 60_000).toISOString(),
  }));
  const result = buildPriceForecast(state, {
    symbol: 'BTC-USD',
    observations,
    horizonMinutes: 5,
    observationIntervalMinutes: 1,
    orderBookImbalance: 0.2,
    spreadBps: 4,
    ttlSeconds: 60,
  }, NOW);
  const forecast = result.priceForecast;
  assert.equal(forecast.status, 'valid');
  assert.equal(forecast.symbol, 'BTC-USD');
  assert.ok(forecast.expectedPrice > forecast.currentPrice);
  assert.ok(forecast.p10Price < forecast.p50Price);
  assert.ok(forecast.p90Price > forecast.p50Price);
  assert.ok(forecast.probabilityUp > 0.5);
  assert.equal(forecast.modelVersion, 'deterministic-price-ensemble-v1');
});

test('stale market observations block forecast creation', () => {
  const state = createInitialOperatorState(NOW);
  const observations = [100, 101, 102, 103, 104].map((price, index) => ({
    price,
    timestamp: new Date(new Date(NOW).getTime() - (1000 + index) * 1000).toISOString(),
  }));
  const result = buildPriceForecast(state, { symbol: 'BTC-USD', observations, maxDataAgeSeconds: 60 }, NOW);
  assert.deepEqual(result.errors, ['forecast_market_data_stale']);
});

test('economic gate combines forecast edge, venue costs, model cost and uncertainty', () => {
  const state = createInitialOperatorState(NOW);
  ingestModelPricingCatalog(state, { catalog: catalog() }, NOW);
  const quote = quoteModelRequest(state, { model: 'example/value-model', promptTokens: 100, completionTokens: 50 }, NOW).modelQuote;
  const forecast = buildPriceForecast(state, {
    symbol: 'BTC-USD',
    prices: [100, 100.5, 101, 101.5, 102, 102.5],
    observationIntervalMinutes: 1,
    horizonMinutes: 2,
    ttlSeconds: 120,
  }, NOW).priceForecast;
  const costs = ingestExecutionCostSnapshot(state, {
    symbol: 'BTC-USD',
    notionalUsd: 1000,
    liquidity: 'taker',
    feeSummary: { fee_tier: { taker_fee_rate: '0.001', maker_fee_rate: '0.0005' } },
    preview: { commission_total: '1', estimatedSlippage: 0.5, validUntil: '2026-07-29T22:02:00.000Z' },
    spreadBps: 2,
  }, NOW).executionCostSnapshot;

  const allowed = evaluateEconomicDecision(state, {
    forecastId: forecast.id,
    modelQuoteId: quote.id,
    executionCostSnapshotId: costs.id,
    requestRemoteModel: true,
    notionalUsd: 1000,
    predictedEdgeUsd: 20,
    expectedDecisionImprovementUsd: 5,
    probabilityDecisionChanges: 0.8,
    uncertaintyReserveUsd: 1,
    requiredCostCoverageMultiple: 3,
  }, NOW).economicDecision;
  assert.equal(allowed.intelligenceAllowed, true);
  assert.equal(allowed.executionAllowed, true);
  assert.equal(allowed.selectedTier, 'cheap_remote');
  assert.ok(allowed.netExecutableEdgeUsd > 0);

  const blocked = evaluateEconomicDecision(state, {
    forecastId: forecast.id,
    modelQuoteId: quote.id,
    executionCostSnapshotId: costs.id,
    requestRemoteModel: true,
    notionalUsd: 1000,
    predictedEdgeUsd: 2,
    expectedDecisionImprovementUsd: 0.01,
    probabilityDecisionChanges: 0.2,
    uncertaintyReserveUsd: 1,
  }, NOW).economicDecision;
  assert.equal(blocked.intelligenceAllowed, false);
  assert.equal(blocked.executionAllowed, false);
  assert.ok(blocked.blockers.includes('intelligence_purchase_not_economic'));
});

test('forecast calibration and counterfactual attribution are measurable', () => {
  const state = createInitialOperatorState(NOW);
  const forecast = buildPriceForecast(state, {
    symbol: 'ETH-USD',
    prices: [100, 101, 102, 103, 104],
    observationIntervalMinutes: 1,
    horizonMinutes: 1,
  }, NOW).priceForecast;
  recordForecastOutcome(state, { forecastId: forecast.id, actualPrice: 105 }, NOW);
  const calibration = summarizeForecastCalibration(state);
  assert.equal(calibration.samples, 1);
  assert.equal(calibration.directionalAccuracy, 1);

  recordAgentAttribution(state, {
    botAction: 'hold',
    agentAction: 'buy',
    finalAction: 'buy',
    realizedPnlUsd: 12,
    counterfactualPnlUsd: 2,
    agentCostUsd: 1,
  }, NOW);
  const attribution = summarizeAgentAttribution(state);
  assert.equal(attribution.changedDecisions, 1);
  assert.equal(attribution.incrementalPnlUsd, 10);
  assert.equal(attribution.incrementalRoi, 10);
  assert.equal(attribution.agentOverrideWinRate, 1);
});
