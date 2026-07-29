import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';

import { handleRequest, createInitialState } from '../apps/api/src/server.p1.mjs';
import { MemoryOperatorStore } from '../packages/storage/src/operatorStore.mjs';

function request(path, method = 'GET', body = null) {
  const stream = new Readable({ read() {} });
  stream.method = method;
  stream.url = path;
  stream.headers = body ? { 'content-type': 'application/json' } : {};
  if (body) stream.push(JSON.stringify(body));
  stream.push(null);
  return stream;
}

async function call(store, path, method = 'GET', body = null, now = new Date(), options = {}) {
  const out = await handleRequest(request(path, method, body), {
    store,
    now,
    env: { OPERATOR_AUTH_REQUIRED: 'false', MODE: 'mock', ...(options.env || {}) },
    fetchImpl: options.fetchImpl,
  });
  return { ...out, data: JSON.parse(out.body) };
}

function economicEvidence(now) {
  const expiresAt = new Date(now.getTime() + 60_000).toISOString();
  return {
    forecast: {
      id: 'forecast-0001',
      status: 'valid',
      symbol: 'BTC-USD',
      currentPrice: 100,
      expectedReturnBps: 200,
      expectedVolatilityBps: 10,
      probabilityUp: 0.7,
      expiresAt,
    },
    executionCost: {
      id: 'execution-cost-0001',
      symbol: 'BTC-USD',
      notionalUsd: 1000,
      totalExecutionCostUsd: 1,
      validUntil: expiresAt,
    },
  };
}

test('pricing catalog refresh and dashboard are available through the guarded API', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const refreshed = await call(store, '/api/economics/model-pricing/refresh', 'POST', {
    catalog: {
      data: [{
        id: 'example/value-model',
        pricing: { prompt: '0.000001', completion: '0.000002', request: '0' },
      }],
    },
  });
  assert.equal(refreshed.status, 201);
  assert.equal(refreshed.data.pricingSnapshot.modelCount, 1);

  const dashboard = await call(store, '/api/economics/dashboard');
  assert.equal(dashboard.status, 200);
  assert.equal(dashboard.data.pricing.latestSnapshot.modelCount, 1);
});

test('remote research is blocked without an approved value-of-information decision', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const out = await call(store, '/api/agents/jobs', 'POST', {
    localOrRemote: 'remote',
    model: 'example/value-model',
    promptTokens: 100,
    completionTokens: 20,
  });
  assert.equal(out.status, 409);
  assert.equal(out.data.error, 'remote_intelligence_purchase_blocked');
  assert.ok(out.data.errors.includes('model_quote_required'));
  assert.ok(out.data.errors.includes('economic_decision_required'));
});

test('approved intelligence purchase injects the authoritative model quote into the cost ledger', async () => {
  const now = new Date();
  const { forecast, executionCost } = economicEvidence(now);
  const state = createInitialState(now.toISOString());
  state.modelUsageLedger = [{
    id: 'model-quote-0001',
    status: 'quoted',
    provider: 'openrouter',
    model: 'example/value-model',
    localOrRemote: 'remote',
    pricingSnapshotId: 'pricing-0001',
    estimatedCostUsd: 0.025,
    requestedAt: now.toISOString(),
  }];
  state.priceForecasts = [forecast];
  state.executionCostSnapshots = [executionCost];
  state.economicDecisions = [{
    id: 'economic-decision-0001',
    modelQuoteId: 'model-quote-0001',
    forecastId: forecast.id,
    executionCostSnapshotId: executionCost.id,
    intelligenceAllowed: true,
    executionAllowed: false,
    createdAt: now.toISOString(),
  }];
  const store = new MemoryOperatorStore(state);

  const out = await call(store, '/api/agents/jobs', 'POST', {
    localOrRemote: 'remote',
    modelQuoteId: 'model-quote-0001',
    economicDecisionId: 'economic-decision-0001',
    promptTokens: 100,
    completionTokens: 20,
    totalTokens: 120,
    marketScope: 'BTC-USD',
  }, now);

  assert.equal(out.status, 201);
  assert.equal(out.data.job.model, 'example/value-model');
  assert.equal(out.data.ledger.remoteApiCost, 0.025);
});

test('guarded OpenRouter execution reconciles actual cost and invalidates the pre-call decision', async () => {
  const now = new Date();
  const { forecast, executionCost } = economicEvidence(now);
  const state = createInitialState(now.toISOString());
  state.modelUsageLedger = [{
    id: 'quote-001',
    status: 'quoted',
    provider: 'openrouter',
    model: 'example/value-model',
    localOrRemote: 'remote',
    pricingSnapshotId: 'pricing-001',
    estimatedCostUsd: 0.01,
    authoritativeCostUsd: 0.01,
    requestedAt: now.toISOString(),
    completionTokens: 100,
    providerPreferences: { sort: 'price', max_price: { prompt: 1, completion: 2 }, data_collection: 'deny' },
  }];
  state.priceForecasts = [forecast];
  state.executionCostSnapshots = [executionCost];
  state.economicDecisions = [{
    id: 'decision-001',
    modelQuoteId: 'quote-001',
    forecastId: forecast.id,
    executionCostSnapshotId: executionCost.id,
    intelligenceAllowed: true,
    executionAllowed: false,
    blockers: ['model_usage_not_reconciled'],
    createdAt: now.toISOString(),
  }];
  state.researchJobs = [{
    id: 'job-001',
    agentId: 'market-research-agent',
    model: 'example/value-model',
    provider: 'openrouter',
    localOrRemote: 'remote',
    status: 'queued',
    startedAt: now.toISOString(),
  }];
  state.agentCostLedger = [{
    id: 'cost-001',
    agentId: 'market-research-agent',
    jobId: 'job-001',
    model: 'example/value-model',
    provider: 'openrouter',
    localOrRemote: 'remote',
    remoteApiCost: 0.01,
    localComputeCost: 0,
    createdAt: now.toISOString(),
  }];
  const store = new MemoryOperatorStore(state);
  let capturedRequest = null;
  const fetchImpl = async (url, init) => {
    capturedRequest = { url, init, body: JSON.parse(init.body) };
    return {
      ok: true,
      status: 200,
      async json() {
        return {
          id: 'gen-001',
          model: 'example/value-model',
          choices: [{ finish_reason: 'stop', message: { role: 'assistant', content: '{"recommendation":"hold"}' } }],
          usage: {
            prompt_tokens: 90,
            completion_tokens: 15,
            cost: 0.018,
            prompt_tokens_details: { cached_tokens: 40 },
            completion_tokens_details: { reasoning_tokens: 5 },
          },
        };
      },
    };
  };

  const out = await call(store, '/api/economics/intelligence/execute', 'POST', {
    modelQuoteId: 'quote-001',
    economicDecisionId: 'decision-001',
    researchJobId: 'job-001',
    messages: [{ role: 'user', content: 'Evaluate the supplied structured market evidence.' }],
  }, now, { env: { OPENROUTER_API_KEY: 'test-key' }, fetchImpl });

  assert.equal(out.status, 200);
  assert.equal(out.data.modelUsage.actualCostUsd, 0.018);
  assert.equal(out.data.economicDecisionRefreshRequired, true);
  assert.equal(capturedRequest.body.usage.include, true);
  assert.equal(capturedRequest.body.provider.max_price.prompt, 1);
  assert.equal(capturedRequest.body.provider.data_collection, 'deny');

  const persisted = await store.load();
  assert.equal(persisted.modelUsageLedger[0].status, 'reconciled');
  assert.equal(persisted.modelUsageLedger[0].actualCostUsd, 0.018);
  assert.equal(persisted.agentCostLedger[0].remoteApiCost, 0.018);
  assert.equal(persisted.agentCostLedger[0].costSource, 'provider_reported_actual');
  assert.equal(persisted.researchJobs[0].status, 'completed');
  assert.equal(persisted.researchJobs[0].generationId, 'gen-001');
  assert.equal(persisted.economicDecisions[0].executionAllowed, false);
  assert.equal(persisted.economicDecisions[0].supersededByReconciliation, true);
});

test('paid-agent opportunity approval fails closed without a fresh executable decision', async () => {
  const now = new Date();
  const state = createInitialState(now.toISOString());
  state.researchJobs = [{ id: 'job-001', localOrRemote: 'remote', status: 'completed' }];
  state.opportunities = [{
    id: 'opp-001',
    researchJobId: 'job-001',
    symbol: 'BTC-USD',
    venue: 'coinbase',
    status: 'needs_review',
    approvalStatus: 'needs_review',
  }];
  const store = new MemoryOperatorStore(state);

  const out = await call(store, '/api/opportunities/opp-001/approve', 'POST', { reviewer: 'operator' }, now);
  assert.equal(out.status, 409);
  assert.equal(out.data.error, 'paid_agent_execution_blocked');
  assert.ok(out.data.errors.includes('economic_decision_required'));
});

test('reconciled fresh economics allow paid-agent opportunity approval', async () => {
  const now = new Date();
  const expiresAt = new Date(now.getTime() + 60_000).toISOString();
  const reconciledAt = new Date(now.getTime() - 5000).toISOString();
  const decisionAt = new Date(now.getTime() - 1000).toISOString();
  const state = createInitialState(now.toISOString());
  state.researchJobs = [{ id: 'job-001', localOrRemote: 'remote', status: 'completed' }];
  state.modelUsageLedger = [{ id: 'quote-001', status: 'reconciled', actualCostUsd: 0.02, reconciledAt }];
  state.priceForecasts = [{ id: 'forecast-001', status: 'valid', currentPrice: 100, expiresAt }];
  state.executionCostSnapshots = [{ id: 'cost-001', validUntil: expiresAt }];
  state.economicDecisions = [{
    id: 'decision-001',
    modelQuoteId: 'quote-001',
    forecastId: 'forecast-001',
    executionCostSnapshotId: 'cost-001',
    executionAllowed: true,
    intelligenceAllowed: true,
    modelUsageReconciled: true,
    netExecutableEdgeUsd: 4.5,
    createdAt: decisionAt,
  }];
  state.opportunities = [{
    id: 'opp-001',
    researchJobId: 'job-001',
    economicDecisionId: 'decision-001',
    modelQuoteId: 'quote-001',
    forecastId: 'forecast-001',
    executionCostSnapshotId: 'cost-001',
    symbol: 'BTC-USD',
    venue: 'coinbase',
    recommendation: 'long',
    totalMoneyRisked: 100,
    positionSizing: { recommendedSize: 100 },
    status: 'needs_review',
    approvalStatus: 'needs_review',
  }];
  const store = new MemoryOperatorStore(state);

  const out = await call(store, '/api/opportunities/opp-001/approve', 'POST', { reviewer: 'operator' }, now);
  assert.equal(out.status, 200);
  assert.equal(out.data.opportunity.status, 'approved');
  assert.equal(out.data.execution.opportunityId, 'opp-001');
});
