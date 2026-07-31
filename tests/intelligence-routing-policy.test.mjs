import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { readFileSync } from 'node:fs';

import { handleRequest, createInitialState } from '../apps/api/src/server.p1.mjs';
import {
  evaluateRemoteIntelligencePolicy,
  intelligenceRoutingPolicyView,
  normalizeIntelligenceRoutingPolicy,
  validateIntelligenceRoutingPolicy,
} from '../apps/api/src/intelligencePolicy.mjs';
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

async function call(store, path, method = 'GET', body = null, options = {}) {
  const out = await handleRequest(request(path, method, body), {
    store,
    now: options.now || new Date('2026-07-31T07:00:00.000Z'),
    env: {
      OPERATOR_AUTH_REQUIRED: 'false',
      MODE: 'mock',
      REMOTE_LLM_EXECUTION_ENABLED: 'true',
      OPENROUTER_API_KEY: 'test-openrouter-key',
      ...(options.env || {}),
    },
    fetchImpl: options.fetchImpl,
  });
  return { ...out, data: String(out.headers?.['content-type'] || '').includes('application/json') ? JSON.parse(out.body) : null };
}

function openRouterCatalog() {
  return {
    data: [{
      id: 'example/value-model',
      pricing: { prompt: '0.000001', completion: '0.000002', request: '0' },
    }],
  };
}

test('routing policy defaults fail closed even when OpenRouter is deployed', () => {
  const state = createInitialState();
  const view = intelligenceRoutingPolicyView(state, {
    REMOTE_LLM_EXECUTION_ENABLED: 'true',
    OPENROUTER_API_KEY: 'configured',
    LOCAL_LLM_ENDPOINTS: 'http://xwing:8080/v1',
  }, new Date('2026-07-31T07:00:00.000Z'));
  assert.equal(view.policy.mode, 'local_only');
  assert.equal(view.capabilities.openRouterAvailable, true);
  assert.equal(view.effective.remoteAllowed, false);
  assert.equal(view.effective.localAllowed, true);
});

test('policy validation rejects unsafe or contradictory limits', () => {
  assert.deepEqual(validateIntelligenceRoutingPolicy({
    mode: 'unknown',
    remoteSpendCapUsdPerDay: 1,
    remoteSpendCapUsdPerRequest: 2,
    minimumRemoteValueCoverage: 0,
  }).errors, [
    'intelligence_routing_mode_invalid',
    'remote_request_cap_exceeds_daily_cap',
    'minimum_remote_value_coverage_invalid',
  ]);
  const normalized = normalizeIntelligenceRoutingPolicy({
    mode: 'openrouter_allowed',
    remoteSpendCapUsdPerDay: 2,
    remoteSpendCapUsdPerRequest: 5,
    minimumRemoteValueCoverage: 4,
  });
  assert.equal(normalized.remoteSpendCapUsdPerRequest, 2);
});

test('economic auto-selection requires value coverage and respects daily commitments', () => {
  const state = createInitialState('2026-07-31T07:00:00.000Z');
  state.config.intelligenceRoutingPolicy = {
    mode: 'economic_auto',
    remoteSpendCapUsdPerDay: 1,
    remoteSpendCapUsdPerRequest: 0.5,
    minimumRemoteValueCoverage: 3,
    fallbackToLocalOnRemoteBlock: true,
  };
  state.modelUsageLedger = [{
    id: 'quote-existing',
    localOrRemote: 'remote',
    status: 'reconciled',
    actualCostUsd: 0.7,
    requestedAt: '2026-07-31T06:00:00.000Z',
  }];
  const env = { REMOTE_LLM_EXECUTION_ENABLED: 'true', OPENROUTER_API_KEY: 'configured' };

  const lowValue = evaluateRemoteIntelligencePolicy(state, {
    estimatedCostUsd: 0.1,
    expectedDecisionImprovementUsd: 0.2,
  }, env, new Date('2026-07-31T07:00:00.000Z'));
  assert.equal(lowValue.allowed, false);
  assert.ok(lowValue.blockers.includes('remote_value_coverage_below_policy'));

  const overBudget = evaluateRemoteIntelligencePolicy(state, {
    estimatedCostUsd: 0.4,
    expectedDecisionImprovementUsd: 2,
  }, env, new Date('2026-07-31T07:00:00.000Z'));
  assert.equal(overBudget.allowed, false);
  assert.ok(overBudget.blockers.includes('remote_daily_spend_cap_exceeded'));

  const allowed = evaluateRemoteIntelligencePolicy(state, {
    estimatedCostUsd: 0.2,
    expectedDecisionImprovementUsd: 0.8,
  }, env, new Date('2026-07-31T07:00:00.000Z'));
  assert.equal(allowed.allowed, true);
  assert.equal(allowed.valueCoverage, 4);
});

test('operator can read and persist the routing knob through the guarded API', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const initial = await call(store, '/api/economics/intelligence/policy');
  assert.equal(initial.status, 200);
  assert.equal(initial.data.policy.mode, 'local_only');

  const saved = await call(store, '/api/economics/intelligence/policy', 'PUT', {
    mode: 'economic_auto',
    remoteSpendCapUsdPerDay: 3,
    remoteSpendCapUsdPerRequest: 0.5,
    minimumRemoteValueCoverage: 4,
    fallbackToLocalOnRemoteBlock: true,
  });
  assert.equal(saved.status, 200);
  assert.equal(saved.data.policy.mode, 'economic_auto');
  assert.equal(saved.data.policy.remoteSpendCapUsdPerDay, 3);

  const persisted = await store.load();
  assert.equal(persisted.config.intelligenceRoutingPolicy.mode, 'economic_auto');
  assert.equal(persisted.config.intelligenceRoutingPolicy.minimumRemoteValueCoverage, 4);
});

test('local-only policy blocks a remote quote before it can become executable', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const refreshed = await call(store, '/api/economics/model-pricing/refresh', 'POST', { catalog: openRouterCatalog() });
  assert.equal(refreshed.status, 201);

  const quoted = await call(store, '/api/economics/model-quotes', 'POST', {
    localOrRemote: 'remote',
    provider: 'openrouter',
    model: 'example/value-model',
    promptTokens: 100,
    completionTokens: 20,
  });
  assert.equal(quoted.status, 409);
  assert.equal(quoted.data.error, 'remote_intelligence_policy_blocked');
  assert.ok(quoted.data.errors.includes('intelligence_policy_local_only'));

  const state = await store.load();
  assert.equal(state.modelUsageLedger[0].status, 'policy_blocked');
});

test('OpenRouter-eligible mode permits a capped quote but still records the policy decision', async () => {
  const state = createInitialState();
  state.config.intelligenceRoutingPolicy = {
    mode: 'openrouter_allowed',
    remoteSpendCapUsdPerDay: 2,
    remoteSpendCapUsdPerRequest: 0.25,
    minimumRemoteValueCoverage: 3,
    fallbackToLocalOnRemoteBlock: true,
  };
  const store = new MemoryOperatorStore(state);
  await call(store, '/api/economics/model-pricing/refresh', 'POST', { catalog: openRouterCatalog() });

  const quoted = await call(store, '/api/economics/model-quotes', 'POST', {
    localOrRemote: 'remote',
    provider: 'openrouter',
    model: 'example/value-model',
    promptTokens: 100,
    completionTokens: 20,
  });
  assert.equal(quoted.status, 201);
  assert.equal(quoted.data.routingDecision.selected, 'remote');
  assert.equal(quoted.data.modelQuote.routingPolicyDecision.allowed, true);
});

test('served console loads the routing control and exposes its static module', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const page = await call(store, '/');
  assert.equal(page.status, 200);
  assert.match(page.body, /\/ui\/intelligence-policy\.js/);

  const asset = await call(store, '/ui/intelligence-policy.js');
  assert.equal(asset.status, 200);
  assert.match(asset.body, /Local fleet versus OpenRouter/);
  assert.match(asset.body, /\/api\/economics\/intelligence\/policy/);
});

test('UI source preserves the three operator-selectable modes and hard budget fields', () => {
  const source = readFileSync('apps/web/src/intelligence-policy.js', 'utf8');
  for (const token of [
    'local_only',
    'economic_auto',
    'openrouter_allowed',
    'remoteSpendCapUsdPerDay',
    'remoteSpendCapUsdPerRequest',
    'minimumRemoteValueCoverage',
    'fallbackToLocalOnRemoteBlock',
  ]) assert.match(source, new RegExp(token));
  assert.match(source, /never stores the OpenRouter key/i);
  assert.match(source, /never bypasses forecast/i);
});
