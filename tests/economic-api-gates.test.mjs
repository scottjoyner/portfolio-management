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

async function call(store, path, method = 'GET', body = null, now = new Date()) {
  const out = await handleRequest(request(path, method, body), {
    store,
    now,
    env: { OPERATOR_AUTH_REQUIRED: 'false', MODE: 'mock' },
  });
  return { ...out, data: JSON.parse(out.body) };
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
  const expiresAt = new Date(now.getTime() + 60_000).toISOString();
  const state = createInitialState(now.toISOString());
  state.modelUsageLedger = [{
    id: 'model-quote-0001',
    status: 'quoted',
    provider: 'openrouter',
    model: 'example/value-model',
    pricingSnapshotId: 'pricing-0001',
    estimatedCostUsd: 0.025,
    requestedAt: now.toISOString(),
  }];
  state.priceForecasts = [{ id: 'forecast-0001', status: 'valid', expiresAt }];
  state.executionCostSnapshots = [{ id: 'execution-cost-0001', validUntil: expiresAt }];
  state.economicDecisions = [{
    id: 'economic-decision-0001',
    modelQuoteId: 'model-quote-0001',
    forecastId: 'forecast-0001',
    executionCostSnapshotId: 'execution-cost-0001',
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
  state.modelUsageLedger = [{ id: 'quote-001', status: 'reconciled', reconciledAt }];
  state.priceForecasts = [{ id: 'forecast-001', status: 'valid', currentPrice: 100, expiresAt }];
  state.executionCostSnapshots = [{ id: 'cost-001', validUntil: expiresAt }];
  state.economicDecisions = [{
    id: 'decision-001',
    modelQuoteId: 'quote-001',
    forecastId: 'forecast-001',
    executionCostSnapshotId: 'cost-001',
    executionAllowed: true,
    intelligenceAllowed: true,
    netExecutableEdgeUsd: 4.5,
    createdAt: decisionAt,
  }];
  state.opportunities = [{
    id: 'opp-001',
    researchJobId: 'job-001',
    economicDecisionId: 'decision-001',
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
