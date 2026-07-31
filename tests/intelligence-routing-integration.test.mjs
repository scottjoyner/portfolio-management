import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';

import { handleRequest, createInitialState } from '../apps/api/src/server.p1.mjs';
import { resetIntelligenceProviderRegistry } from '../apps/api/src/intelligenceExecution.mjs';
import { MemoryOperatorStore } from '../packages/storage/src/operatorStore.mjs';

function request(path, body) {
  const stream = new Readable({ read() {} });
  stream.method = 'POST';
  stream.url = path;
  stream.headers = { 'content-type': 'application/json' };
  stream.push(JSON.stringify(body));
  stream.push(null);
  return stream;
}

async function post(store, path, body, env = {}, fetchImpl = globalThis.fetch) {
  const response = await handleRequest(request(path, body), {
    store,
    now: new Date('2026-07-31T07:00:00.000Z'),
    env: {
      OPERATOR_AUTH_REQUIRED: 'false',
      MODE: 'mock',
      REMOTE_LLM_EXECUTION_ENABLED: 'true',
      OPENROUTER_API_KEY: 'test-openrouter-key',
      ...env,
    },
    fetchImpl,
  });
  return { ...response, data: JSON.parse(response.body) };
}

function localFleet() {
  return JSON.stringify([{
    id: 'x1-370',
    name: 'x1-370',
    kind: 'lmstudio',
    baseUrl: 'http://x1-370.lan:1234/v1',
    models: ['qwen-local'],
    priority: 20,
    maxConcurrent: 1,
    prefillTokensPerSecond: 100,
    decodeTokensPerSecond: 25,
    estimatedWatts: 110,
    electricityRatePerKwh: 0.14,
    hardwareDepreciationPerHour: 0.18,
    contextLength: 65536,
  }]);
}

async function localHealth(url) {
  if (String(url).endsWith('/models')) {
    return {
      ok: true,
      status: 200,
      async json() { return { data: [{ id: 'qwen-local' }] }; },
    };
  }
  throw new Error(`unexpected_fetch:${url}`);
}

test('automatic routing falls back to the local fleet when remote quoting is unavailable', async () => {
  resetIntelligenceProviderRegistry();
  const state = createInitialState();
  state.config.intelligenceRoutingPolicy = {
    mode: 'economic_auto',
    remoteSpendCapUsdPerDay: 2,
    remoteSpendCapUsdPerRequest: 0.25,
    minimumRemoteValueCoverage: 3,
    fallbackToLocalOnRemoteBlock: true,
  };
  const result = await post(new MemoryOperatorStore(state), '/api/economics/model-quotes', {
    localOrRemote: 'auto',
    localModel: 'qwen-local',
    remoteModel: 'example/value-model',
    promptTokens: 100,
    completionTokens: 20,
    expectedDecisionImprovementUsd: 1,
  }, {
    REMOTE_LLM_EXECUTION_ENABLED: 'false',
    OPENROUTER_API_KEY: '',
    LOCAL_LLM_NODES_JSON: localFleet(),
  }, localHealth);

  assert.equal(result.status, 201);
  assert.equal(result.data.modelQuote.localOrRemote, 'local');
  assert.equal(result.data.modelQuote.localNodeId, 'x1-370');
  assert.equal(result.data.routingDecision.selected, 'local');
  assert.equal(result.data.routingDecision.reason, 'remote_quote_unavailable');
  assert.ok(result.data.routingDecision.remoteBlockers.includes('remote_llm_execution_disabled'));
});

test('research job inherits the selected quote route when locality is omitted', async () => {
  const state = createInitialState('2026-07-31T07:00:00.000Z');
  state.config.intelligenceRoutingPolicy = {
    mode: 'openrouter_allowed',
    remoteSpendCapUsdPerDay: 2,
    remoteSpendCapUsdPerRequest: 0.25,
    minimumRemoteValueCoverage: 3,
    fallbackToLocalOnRemoteBlock: true,
  };
  state.modelUsageLedger = [{
    id: 'quote-selected-remote',
    status: 'quoted',
    provider: 'openrouter',
    model: 'example/value-model',
    localOrRemote: 'remote',
    pricingSnapshotId: 'pricing-1',
    estimatedCostUsd: 0.02,
    requestedAt: '2026-07-31T07:00:00.000Z',
  }];
  state.economicDecisions = [{
    id: 'decision-selected-remote',
    modelQuoteId: 'quote-selected-remote',
    intelligenceAllowed: true,
    executionAllowed: false,
    createdAt: '2026-07-31T07:00:00.000Z',
  }];

  const result = await post(new MemoryOperatorStore(state), '/api/agents/jobs', {
    modelQuoteId: 'quote-selected-remote',
    economicDecisionId: 'decision-selected-remote',
    promptTokens: 100,
    completionTokens: 20,
    totalTokens: 120,
    marketScope: 'BTC-USD',
  });

  assert.equal(result.status, 201);
  assert.equal(result.data.job.localOrRemote, 'remote');
  assert.equal(result.data.job.provider, 'openrouter');
  assert.equal(result.data.job.model, 'example/value-model');
});
