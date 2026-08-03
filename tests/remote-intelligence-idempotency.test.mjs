import test from 'node:test';
import assert from 'node:assert/strict';

import {
  executeEconomicIntelligence,
  resetIntelligenceProviderRegistry,
} from '../apps/api/src/intelligenceExecution.mjs';
import { recoverStaleModelCalls } from '../apps/api/src/modelCallRecovery.mjs';
import {
  applyPendingOpenRouterReconciliations,
  preparePendingOpenRouterReconciliations,
} from '../apps/api/src/openRouterUsageReconciliation.mjs';
import { createInitialOperatorState, MemoryOperatorStore } from '../packages/storage/src/operatorStore.mjs';

const NOW = '2026-07-31T20:00:00.000Z';

function remoteEnv() {
  return {
    LOCAL_LLM_EXECUTION_REQUIRED: 'false',
    REMOTE_LLM_EXECUTION_ENABLED: 'true',
    OPENROUTER_API_KEY: 'test-key',
  };
}

function remoteState() {
  const state = createInitialOperatorState(NOW);
  state.modelUsageLedger.push({
    id: 'remote-quote-uncertain',
    status: 'quoted',
    provider: 'openrouter',
    model: 'example/remote-model',
    localOrRemote: 'remote',
    estimatedCostUsd: 0.02,
    completionTokens: 20,
    requestedAt: NOW,
    researchJobId: 'remote-job-uncertain',
  });
  state.economicDecisions.push({
    id: 'remote-decision-uncertain',
    modelQuoteId: 'remote-quote-uncertain',
    intelligenceAllowed: true,
    executionAllowed: false,
    createdAt: NOW,
  });
  state.researchJobs.push({
    id: 'remote-job-uncertain',
    modelQuoteId: 'remote-quote-uncertain',
    localOrRemote: 'remote',
    status: 'queued',
    createdAt: NOW,
  });
  state.agentCostLedger.push({
    id: 'remote-cost-uncertain',
    jobId: 'remote-job-uncertain',
    modelQuoteId: 'remote-quote-uncertain',
    localOrRemote: 'remote',
    createdAt: NOW,
  });
  return state;
}

function requestBody() {
  return {
    modelQuoteId: 'remote-quote-uncertain',
    economicDecisionId: 'remote-decision-uncertain',
    researchJobId: 'remote-job-uncertain',
    messages: [{ role: 'user', content: 'Evaluate this bounded test request.' }],
  };
}

function jsonResponse(payload, status = 200, headers = {}) {
  const normalized = new Map(Object.entries(headers).map(([key, value]) => [key.toLowerCase(), value]));
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: key => normalized.get(String(key).toLowerCase()) || null },
    async json() { return payload; },
  };
}

test('an uncertain remote provider outcome becomes usage_pending and cannot be executed twice', async () => {
  resetIntelligenceProviderRegistry();
  const store = new MemoryOperatorStore(remoteState());
  let remoteCalls = 0;
  const fetchImpl = async () => {
    remoteCalls += 1;
    throw new Error('simulated_connection_reset_after_request');
  };

  const first = await executeEconomicIntelligence({
    store,
    env: remoteEnv(),
    fetchImpl,
    now: NOW,
    body: requestBody(),
  });

  assert.equal(first.status, 409);
  assert.equal(first.body.requiresManualReconciliation, true);
  assert.equal(remoteCalls, 1);

  const afterFirst = await store.load();
  const quote = afterFirst.modelUsageLedger.find(row => row.id === 'remote-quote-uncertain');
  const job = afterFirst.researchJobs.find(row => row.id === 'remote-job-uncertain');
  const cost = afterFirst.agentCostLedger.find(row => row.id === 'remote-cost-uncertain');
  assert.equal(quote.status, 'usage_pending');
  assert.equal(quote.retryable, false);
  assert.equal(quote.uncertainProviderOutcome, true);
  assert.equal(quote.requiresManualReconciliation, true);
  assert.equal(job.retryable, false);
  assert.equal(cost.recoveryStatus, 'usage_pending_manual_reconciliation_required');

  const second = await executeEconomicIntelligence({
    store,
    env: remoteEnv(),
    fetchImpl,
    now: '2026-07-31T20:01:00.000Z',
    body: requestBody(),
  });

  assert.equal(second.status, 409);
  assert.deepEqual(second.body.errors, ['model_usage_pending_reconciliation']);
  assert.equal(remoteCalls, 1, 'a usage-pending quote must not produce a second paid provider call');
});

test('an accepted generation ID in the response body is reconciled without replaying the paid request', async () => {
  resetIntelligenceProviderRegistry();
  const store = new MemoryOperatorStore(remoteState());
  let chatCalls = 0;
  let usageCalls = 0;
  let usageReady = false;
  const fetchImpl = async (url, init = {}) => {
    if (String(init.method || 'GET').toUpperCase() === 'POST') {
      chatCalls += 1;
      return jsonResponse({
        id: 'generation-from-body-1',
        model: 'example/remote-model',
        choices: [{ finish_reason: 'stop', message: { role: 'assistant', content: 'bounded result' } }],
        usage: { prompt_tokens: 12, completion_tokens: 4 },
      });
    }
    usageCalls += 1;
    return usageReady
      ? jsonResponse({ data: { tokens_prompt: 12, tokens_completion: 4, total_cost: 0.0042 } })
      : jsonResponse({ data: { tokens_prompt: 12, tokens_completion: 4 } });
  };

  const first = await executeEconomicIntelligence({
    store,
    env: remoteEnv(),
    fetchImpl,
    now: NOW,
    body: requestBody(),
  });

  assert.equal(first.status, 409);
  assert.equal(first.body.generationId, 'generation-from-body-1');
  assert.equal(first.body.automaticReconciliationScheduled, true);
  assert.equal(first.body.requiresManualReconciliation, false);
  assert.equal(chatCalls, 1);
  assert.equal(usageCalls, 1);

  const pendingState = await store.load();
  const pendingQuote = pendingState.modelUsageLedger.find(row => row.id === 'remote-quote-uncertain');
  assert.equal(pendingQuote.status, 'usage_pending');
  assert.equal(pendingQuote.generationId, 'generation-from-body-1');
  assert.equal(pendingQuote.reconciliationStatus, 'pending');
  assert.equal(pendingQuote.requiresManualReconciliation, false);

  const blocked = await executeEconomicIntelligence({
    store,
    env: remoteEnv(),
    fetchImpl,
    now: '2026-07-31T20:01:00.000Z',
    body: requestBody(),
  });
  assert.equal(blocked.status, 409);
  assert.deepEqual(blocked.body.errors, ['model_usage_pending_reconciliation']);
  assert.equal(chatCalls, 1, 'the accepted generation must never be submitted again');

  usageReady = true;
  const prepared = await preparePendingOpenRouterReconciliations({
    state: pendingState,
    env: remoteEnv(),
    fetchImpl,
    now: new Date('2099-01-01T00:00:00.000Z'),
  });
  assert.equal(prepared.attempted, 1);
  const applied = applyPendingOpenRouterReconciliations(
    pendingState,
    prepared,
    remoteEnv(),
    new Date('2099-01-01T00:00:01.000Z'),
  );
  assert.equal(applied.reconciled, 1);
  assert.equal(chatCalls, 1);
  assert.equal(usageCalls, 2);
  assert.equal(pendingQuote.status, 'reconciled');
  assert.equal(pendingQuote.actualCostUsd, 0.0042);
  assert.equal(pendingQuote.reconciliationStatus, 'reconciled');
  assert.equal(pendingState.researchJobs[0].status, 'completed');
});

test('stale remote calls require reconciliation while stale local calls require a new quote', () => {
  const state = createInitialOperatorState(NOW);
  state.modelUsageLedger = [
    {
      id: 'remote-running',
      status: 'running',
      localOrRemote: 'remote',
      startedAt: '2026-07-31T19:00:00.000Z',
    },
    {
      id: 'local-running',
      status: 'running',
      localOrRemote: 'local',
      localNodeId: 'x1-370',
      startedAt: '2026-07-31T19:00:00.000Z',
    },
  ];

  const report = recoverStaleModelCalls(state, {
    now: NOW,
    staleSeconds: 300,
  });

  assert.equal(report.recoveredQuoteCount, 2);
  assert.equal(state.modelUsageLedger[0].status, 'usage_pending');
  assert.equal(state.modelUsageLedger[0].retryable, false);
  assert.equal(state.modelUsageLedger[0].requiresManualReconciliation, true);
  assert.equal(state.modelUsageLedger[1].status, 'failed');
  assert.equal(state.modelUsageLedger[1].retryable, true);
  assert.equal(state.modelUsageLedger[1].requiresRequote, true);
});
