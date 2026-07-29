import test from 'node:test';
import assert from 'node:assert/strict';

import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';
import {
  buildPriceForecast,
  evaluateEconomicDecision,
  ingestExecutionCostSnapshot,
  ingestModelPricingCatalog,
  matureForecastOutcomes,
  queueOrRecordSettlementAttribution,
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

function forecastAndCosts(state) {
  const forecast = buildPriceForecast(state, {
    symbol: 'BTC-USD',
    observations: [100, 100.5, 101, 101.5, 102, 102.5].map((price, index) => ({
      price,
      timestamp: new Date(new Date(NOW).getTime() - (5 - index) * 60_000).toISOString(),
    })),
    observationIntervalMinutes: 1,
    horizonMinutes: 2,
    ttlSeconds: 180,
  }, NOW).priceForecast;
  const costs = ingestExecutionCostSnapshot(state, {
    symbol: 'BTC-USD',
    notionalUsd: 1000,
    quantity: 10,
    referencePrice: 100,
    liquidity: 'taker',
    feeSummary: { fee_tier: { taker_fee_rate: '0.001', maker_fee_rate: '0.0005' } },
    preview: {
      preview_id: 'preview-1',
      commission_total: '1',
      estimatedSlippage: 0.5,
      commission_detail_total: { total_commission: '1' },
      validUntil: '2026-07-29T22:03:00.000Z',
    },
    spreadBps: 2,
  }, NOW).executionCostSnapshot;
  return { forecast, costs };
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
  assert.equal(result.modelQuote.authoritativeCostUsd, 0.04169);
  assert.equal(result.modelQuote.costSource, 'pre_call_estimate');
  assert.equal(result.modelQuote.providerPreferences.sort, 'price');
  assert.equal(result.modelQuote.providerPreferences.max_price.prompt, 1);
  assert.equal(result.modelQuote.providerPreferences.max_price.completion, 2);
  assert.equal(result.modelQuote.providerPreferences.data_collection, 'deny');
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

test('provider usage becomes authoritative, idempotent, and invalidates pre-call execution decisions', () => {
  const state = createInitialOperatorState(NOW);
  ingestModelPricingCatalog(state, { catalog: catalog() }, NOW);
  const quote = quoteModelRequest(state, { model: 'example/value-model', promptTokens: 1000, completionTokens: 100 }, NOW).modelQuote;
  const { forecast, costs } = forecastAndCosts(state);
  const decision = evaluateEconomicDecision(state, {
    forecastId: forecast.id,
    modelQuoteId: quote.id,
    executionCostSnapshotId: costs.id,
    requestRemoteModel: true,
    predictedEdgeUsd: 20,
    expectedDecisionImprovementUsd: 5,
    probabilityDecisionChanges: 0.8,
    uncertaintyReserveUsd: 1,
  }, NOW).economicDecision;
  assert.equal(decision.intelligenceAllowed, true);
  assert.equal(decision.executionAllowed, false);
  assert.ok(decision.blockers.includes('model_usage_not_reconciled'));

  const reconciledAt = '2026-07-29T22:00:10.000Z';
  const payload = {
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
  };
  const result = reconcileModelUsage(state, payload, reconciledAt);
  assert.equal(result.modelUsage.actualCostUsd, 0.019);
  assert.equal(result.modelUsage.authoritativeCostUsd, 0.019);
  assert.equal(result.modelUsage.costSource, 'provider_reported_actual');
  assert.equal(result.modelUsage.generationId, 'gen-123');
  assert.equal(result.modelUsage.cacheReadTokensActual, 400);
  assert.equal(result.modelUsage.reasoningTokensActual, 30);
  assert.equal(decision.executionAllowed, false);
  assert.equal(decision.supersededByReconciliation, true);
  assert.ok(decision.blockers.includes('economic_decision_requires_post_reconciliation_refresh'));

  const idempotent = reconcileModelUsage(state, payload, '2026-07-29T22:00:11.000Z');
  assert.equal(idempotent.idempotent, true);
  const conflict = reconcileModelUsage(state, { ...payload, actualCostUsd: 0.021 }, '2026-07-29T22:00:12.000Z');
  assert.deepEqual(conflict.errors, ['model_usage_reconciliation_conflict']);
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
  assert.equal(forecast.targetObservedAt, '2026-07-29T22:05:00.000Z');
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

test('economic gate requires actual reconciled model cost before execution', () => {
  const state = createInitialOperatorState(NOW);
  ingestModelPricingCatalog(state, { catalog: catalog() }, NOW);
  const quote = quoteModelRequest(state, { model: 'example/value-model', promptTokens: 100, completionTokens: 50 }, NOW).modelQuote;
  const { forecast, costs } = forecastAndCosts(state);

  const purchase = evaluateEconomicDecision(state, {
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
  assert.equal(purchase.intelligenceAllowed, true);
  assert.equal(purchase.executionAllowed, false);
  assert.equal(purchase.decisionPhase, 'intelligence_purchase');

  reconcileModelUsage(state, { quoteId: quote.id, actualCostUsd: 0.02, generationId: 'gen-1', usage: { cost: 0.02 } }, '2026-07-29T22:00:10.000Z');
  const executable = evaluateEconomicDecision(state, {
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
  }, '2026-07-29T22:00:11.000Z').economicDecision;
  assert.equal(executable.modelCostUsd, 0.02);
  assert.equal(executable.modelCostSource, 'provider_reported_actual');
  assert.equal(executable.modelUsageReconciled, true);
  assert.equal(executable.executionAllowed, true);
  assert.equal(executable.decisionPhase, 'execution');

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
  }, '2026-07-29T22:00:12.000Z').economicDecision;
  assert.equal(blocked.intelligenceAllowed, false);
  assert.equal(blocked.executionAllowed, false);
  assert.ok(blocked.blockers.includes('intelligence_purchase_not_economic'));
});

test('forecast outcomes mature from post-horizon market evidence exactly once', () => {
  const state = createInitialOperatorState(NOW);
  const forecast = buildPriceForecast(state, {
    symbol: 'ETH-USD',
    observations: [100, 101, 102, 103, 104].map((price, index) => ({
      price,
      timestamp: new Date(new Date(NOW).getTime() - (4 - index) * 60_000).toISOString(),
    })),
    observationIntervalMinutes: 1,
    horizonMinutes: 1,
  }, NOW).priceForecast;
  state.marketDataSnapshots.push({
    id: 'md-after-horizon',
    symbol: 'ETH-USD',
    mid: 105,
    timestamp: '2026-07-29T22:01:05.000Z',
  });
  const first = matureForecastOutcomes(state, '2026-07-29T22:01:06.000Z');
  assert.equal(first.forecastOutcomes.length, 1);
  assert.equal(first.forecastOutcomes[0].forecastId, forecast.id);
  const second = matureForecastOutcomes(state, '2026-07-29T22:02:00.000Z');
  assert.equal(second.forecastOutcomes.length, 0);
  assert.equal(state.forecastOutcomes.length, 1);
});

test('settlement attribution is recorded only with real counterfactual evidence', () => {
  const state = createInitialOperatorState(NOW);
  state.opportunities.push({ id: 'opp-1', recommendation: 'buy', economicDecisionId: 'decision-1' });
  state.economicDecisions.push({ id: 'decision-1', modelQuoteId: 'quote-1' });
  state.modelUsageLedger.push({ id: 'quote-1', status: 'reconciled', actualCostUsd: 1 });
  const execution = {
    id: 'exec-1',
    opportunityId: 'opp-1',
    status: 'filled',
    side: 'buy',
    counterfactualPnlUsd: 2,
    fills: [{ id: 'fill-1', settlementStatus: 'settled', realizedPnlUsd: 12 }],
  };
  const recorded = queueOrRecordSettlementAttribution(state, execution, NOW);
  assert.equal(recorded.agentAttribution.incrementalValueUsd, 10);
  assert.equal(recorded.agentAttribution.agentCostUsd, 1);
  const idempotent = queueOrRecordSettlementAttribution(state, execution, NOW);
  assert.equal(idempotent.reason, 'attribution_exists');

  const pendingExecution = {
    id: 'exec-2',
    opportunityId: 'opp-1',
    status: 'filled',
    fills: [{ id: 'fill-2', settlementStatus: 'settled', realizedPnlUsd: 4 }],
  };
  const pending = queueOrRecordSettlementAttribution(state, pendingExecution, NOW);
  assert.ok(pending.attributionPending.blockers.includes('counterfactual_pnl_required'));
  assert.equal(state.economicAttributionQueue.length, 1);
});

test('forecast calibration and counterfactual attribution are measurable', () => {
  const state = createInitialOperatorState(NOW);
  const forecast = buildPriceForecast(state, {
    symbol: 'ETH-USD',
    observations: [100, 101, 102, 103, 104].map((price, index) => ({
      price,
      timestamp: new Date(new Date(NOW).getTime() - (4 - index) * 60_000).toISOString(),
    })),
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
