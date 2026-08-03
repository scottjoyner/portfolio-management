import test from 'node:test';
import assert from 'node:assert/strict';

import {
  discoverLocalIntelligenceNodes,
  executeEconomicIntelligence,
  quoteLocalIntelligence,
  resetIntelligenceProviderRegistry,
} from '../apps/api/src/intelligenceExecution.mjs';
import { createInitialOperatorState, MemoryOperatorStore } from '../packages/storage/src/operatorStore.mjs';

const NOW = '2026-07-30T19:30:00.000Z';

function localEnv(overrides = {}) {
  return {
    LOCAL_LLM_EXECUTION_REQUIRED: 'true',
    REMOTE_LLM_EXECUTION_ENABLED: 'false',
    LOCAL_LLM_NODES_JSON: JSON.stringify([
      {
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
      },
      {
        id: 'xwing',
        name: 'xwing',
        kind: 'llama.cpp',
        baseUrl: 'http://xwing.lan:8080/v1',
        models: ['qwen-local'],
        priority: 10,
        maxConcurrent: 1,
        prefillTokensPerSecond: 90,
        decodeTokensPerSecond: 22,
        estimatedWatts: 125,
        electricityRatePerKwh: 0.14,
        hardwareDepreciationPerHour: 0.2,
        contextLength: 65536,
      },
    ]),
    ...overrides,
  };
}

function fakeLocalFetch({ completionDelayMs = 0 } = {}) {
  const calls = [];
  const fetchImpl = async (url, init = {}) => {
    calls.push({ url: String(url), init });
    if (String(url).endsWith('/models')) {
      return {
        ok: true,
        status: 200,
        async json() { return { data: [{ id: 'qwen-local' }] }; },
      };
    }
    if (String(url).endsWith('/chat/completions')) {
      if (completionDelayMs) await new Promise(resolve => setTimeout(resolve, completionDelayMs));
      return {
        ok: true,
        status: 200,
        async json() {
          return {
            id: 'local-generation-1',
            model: 'qwen-local',
            choices: [{ finish_reason: 'stop', message: { role: 'assistant', content: '{"recommendation":"hold"}' } }],
            usage: { prompt_tokens: 90, completion_tokens: 12, total_tokens: 102 },
            timings: { prompt_per_second: 120, predicted_per_second: 24 },
          };
        },
      };
    }
    throw new Error(`unexpected_fetch:${url}`);
  };
  fetchImpl.calls = calls;
  return fetchImpl;
}

class MutationAwareStore extends MemoryOperatorStore {
  constructor(state) {
    super(state);
    this.insideMutation = false;
  }

  async mutate(mutator) {
    this.insideMutation = true;
    try {
      return await super.mutate(mutator);
    } finally {
      this.insideMutation = false;
    }
  }
}

test('fleet discovery reports configured LM Studio and llama.cpp nodes', async () => {
  resetIntelligenceProviderRegistry();
  const fetchImpl = fakeLocalFetch();
  const result = await discoverLocalIntelligenceNodes({ env: localEnv(), fetchImpl });
  assert.equal(result.status, 200);
  assert.equal(result.body.ok, true);
  assert.equal(result.body.nodes.length, 2);
  assert.deepEqual(result.body.nodes.map(row => row.nodeId).sort(), ['x1-370', 'xwing']);
  assert.ok(result.body.nodes.every(row => row.models.includes('qwen-local')));
});

test('local quote selects the highest-value healthy node and records routing economics', async () => {
  resetIntelligenceProviderRegistry();
  const fetchImpl = fakeLocalFetch();
  const store = new MemoryOperatorStore(createInitialOperatorState(NOW));
  const result = await quoteLocalIntelligence({
    store,
    env: localEnv(),
    fetchImpl,
    now: NOW,
    body: {
      localOrRemote: 'local',
      model: 'qwen-local',
      promptTokens: 1000,
      completionTokens: 100,
      purpose: 'market-review',
    },
  });

  assert.equal(result.status, 201);
  assert.equal(result.body.route.nodeId, 'x1-370');
  assert.equal(result.body.modelQuote.localOrRemote, 'local');
  assert.equal(result.body.modelQuote.localNodeId, 'x1-370');
  assert.equal(result.body.modelQuote.provider, 'lmstudio');
  assert.ok(result.body.modelQuote.estimatedCostUsd > 0);
  assert.equal(result.body.modelQuote.estimatedPrefillSeconds, 10);
  assert.equal(result.body.modelQuote.estimatedDecodeSeconds, 4);
});

test('local execution occurs outside the store mutation and reconciles measured cost', async () => {
  resetIntelligenceProviderRegistry();
  const store = new MutationAwareStore(createInitialOperatorState(NOW));
  let providerCallInsideMutation = null;
  const baseFetch = fakeLocalFetch({ completionDelayMs: 5 });
  const fetchImpl = async (url, init) => {
    if (String(url).endsWith('/chat/completions')) providerCallInsideMutation = store.insideMutation;
    return baseFetch(url, init);
  };

  const quoteResult = await quoteLocalIntelligence({
    store,
    env: localEnv(),
    fetchImpl,
    now: NOW,
    body: {
      localOrRemote: 'local',
      model: 'qwen-local',
      promptTokens: 100,
      completionTokens: 20,
      researchJobId: 'job-local-1',
    },
  });
  const quote = quoteResult.body.modelQuote;
  const state = await store.load();
  state.economicDecisions.push({
    id: 'decision-local-1',
    modelQuoteId: quote.id,
    intelligenceAllowed: true,
    executionAllowed: false,
    blockers: ['model_usage_not_reconciled'],
    createdAt: NOW,
  });
  state.researchJobs.push({
    id: 'job-local-1',
    agentId: 'market-research-agent',
    provider: 'lmstudio',
    model: 'qwen-local',
    localOrRemote: 'local',
    status: 'queued',
    startedAt: NOW,
  });
  state.agentCostLedger.push({
    id: 'cost-local-1',
    agentId: 'market-research-agent',
    jobId: 'job-local-1',
    provider: 'lmstudio',
    model: 'qwen-local',
    localOrRemote: 'local',
    remoteApiCost: 0,
    localComputeCost: quote.estimatedCostUsd,
    createdAt: NOW,
  });

  const result = await executeEconomicIntelligence({
    store,
    env: localEnv(),
    fetchImpl,
    now: NOW,
    body: {
      modelQuoteId: quote.id,
      economicDecisionId: 'decision-local-1',
      researchJobId: 'job-local-1',
      messages: [{ role: 'user', content: 'Evaluate structured market evidence.' }],
    },
  });

  assert.equal(result.status, 200);
  assert.equal(providerCallInsideMutation, false);
  assert.equal(result.body.modelResponse.nodeId, 'x1-370');
  assert.equal(result.body.modelUsage.status, 'reconciled');
  assert.equal(result.body.modelUsage.costSource, 'provider_reported_actual');
  assert.equal(result.body.modelUsage.localNodeId, 'x1-370');
  assert.ok(result.body.modelUsage.runtimeSecondsActual >= 0);
  assert.equal(result.body.modelUsage.prefillTokensPerSecondActual, 120);
  assert.equal(result.body.modelUsage.decodeTokensPerSecondActual, 24);

  const persisted = await store.load();
  assert.equal(persisted.agentCostLedger[0].remoteApiCost, 0);
  assert.equal(persisted.agentCostLedger[0].localComputeCost, result.body.modelUsage.actualCostUsd);
  assert.equal(persisted.researchJobs[0].status, 'completed');
  assert.equal(persisted.economicDecisions[0].supersededByReconciliation, true);
});

test('missing requested model fails closed instead of silently choosing another model', async () => {
  resetIntelligenceProviderRegistry();
  const store = new MemoryOperatorStore(createInitialOperatorState(NOW));
  const result = await quoteLocalIntelligence({
    store,
    env: localEnv(),
    fetchImpl: fakeLocalFetch(),
    now: NOW,
    body: {
      localOrRemote: 'local',
      model: 'model-not-loaded',
      promptTokens: 100,
      completionTokens: 20,
    },
  });
  assert.equal(result.status, 503);
  assert.deepEqual(result.body.errors, ['no_healthy_local_model_route']);
  assert.equal((await store.load()).modelUsageLedger.length, 0);
});

test('remote provider remains disabled unless explicitly enabled', async () => {
  resetIntelligenceProviderRegistry();
  const state = createInitialOperatorState(NOW);
  state.modelUsageLedger.push({
    id: 'remote-quote-1',
    status: 'quoted',
    provider: 'openrouter',
    model: 'remote-model',
    localOrRemote: 'remote',
    estimatedCostUsd: 0.01,
    completionTokens: 10,
    requestedAt: NOW,
  });
  state.economicDecisions.push({
    id: 'remote-decision-1',
    modelQuoteId: 'remote-quote-1',
    intelligenceAllowed: true,
    executionAllowed: false,
    createdAt: NOW,
  });
  const store = new MemoryOperatorStore(state);
  const result = await executeEconomicIntelligence({
    store,
    env: localEnv(),
    fetchImpl: fakeLocalFetch(),
    now: NOW,
    body: {
      modelQuoteId: 'remote-quote-1',
      economicDecisionId: 'remote-decision-1',
      messages: [{ role: 'user', content: 'test' }],
    },
  });
  assert.equal(result.status, 409);
  assert.deepEqual(result.body.errors, ['remote_llm_execution_disabled']);
});
