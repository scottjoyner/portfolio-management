import test from 'node:test';
import assert from 'node:assert/strict';

import {
  executeEconomicIntelligence,
  resetIntelligenceProviderRegistry,
} from '../apps/api/src/intelligenceExecution.mjs';
import {
  applyPendingOpenRouterReconciliations,
  preparePendingOpenRouterReconciliations,
} from '../apps/api/src/openRouterUsageReconciliation.mjs';
import { createInitialOperatorState, MemoryOperatorStore } from '../packages/storage/src/operatorStore.mjs';

const NOW = '2026-07-31T20:00:00.000Z';
const CHAT_URL = 'https://openrouter.test/api/v1/chat/completions';
const GENERATION_URL = 'https://openrouter.test/api/v1/generation';

function env(overrides = {}) {
  return {
    REMOTE_LLM_EXECUTION_ENABLED: 'true',
    OPENROUTER_API_KEY: 'test-openrouter-key',
    OPENROUTER_CHAT_URL: CHAT_URL,
    OPENROUTER_GENERATION_URL: GENERATION_URL,
    OPENROUTER_RECONCILIATION_RETRY_MS: '1000',
    OPENROUTER_RECONCILIATION_MAX_ATTEMPTS: '3',
    ...overrides,
  };
}

function remoteState() {
  const state = createInitialOperatorState(NOW);
  state.modelUsageLedger.push({
    id: 'quote-remote-1',
    status: 'quoted',
    provider: 'openrouter',
    model: 'example/value-model',
    localOrRemote: 'remote',
    estimatedCostUsd: 0.02,
    authoritativeCostUsd: 0.02,
    completionTokens: 20,
    requestedAt: NOW,
  });
  state.economicDecisions.push({
    id: 'decision-remote-1',
    modelQuoteId: 'quote-remote-1',
    intelligenceAllowed: true,
    executionAllowed: false,
    createdAt: NOW,
  });
  state.researchJobs.push({
    id: 'job-remote-1',
    modelQuoteId: 'quote-remote-1',
    economicDecisionId: 'decision-remote-1',
    localOrRemote: 'remote',
    provider: 'openrouter',
    model: 'example/value-model',
    status: 'queued',
    requestedAt: NOW,
  });
  state.agentCostLedger.push({
    id: 'cost-remote-1',
    jobId: 'job-remote-1',
    modelQuoteId: 'quote-remote-1',
    economicDecisionId: 'decision-remote-1',
    remoteApiCost: 0.02,
    localComputeCost: 0,
    costSource: 'pre_call_estimate',
    createdAt: NOW,
  });
  return state;
}

function execute(store, fetchImpl) {
  return executeEconomicIntelligence({
    store,
    env: env(),
    fetchImpl,
    now: NOW,
    body: {
      modelQuoteId: 'quote-remote-1',
      economicDecisionId: 'decision-remote-1',
      researchJobId: 'job-remote-1',
      messages: [{ role: 'user', content: 'Evaluate bounded market evidence.' }],
    },
  });
}

function headers(generationId = null) {
  return {
    get(name) {
      return String(name).toLowerCase() === 'x-generation-id' ? generationId : null;
    },
  };
}

test('confirmed HTTP request failure restores the quote without creating usage_pending', async () => {
  resetIntelligenceProviderRegistry();
  const store = new MemoryOperatorStore(remoteState());
  let posts = 0;
  const fetchImpl = async (url, init = {}) => {
    if (String(url) === CHAT_URL && init.method === 'POST') {
      posts += 1;
      return {
        ok: false,
        status: 429,
        headers: headers(),
        async json() { return { error: { message: 'rate_limit_exceeded' } }; },
      };
    }
    throw new Error(`unexpected_fetch:${url}`);
  };

  const result = await execute(store, fetchImpl);
  assert.equal(result.status, 503);
  assert.equal(result.body.providerOutcome, 'not_started');
  assert.equal(result.body.retryable, true);
  assert.equal(posts, 1);

  const state = await store.load();
  assert.equal(state.modelUsageLedger[0].status, 'quoted');
  assert.equal(state.modelUsageLedger[0].retryable, true);
  assert.equal(state.modelUsageLedger[0].providerAttemptNumber, 1);
  assert.equal(state.researchJobs[0].status, 'queued');
  assert.equal(state.agentCostLedger[0].recoveryStatus, 'provider_not_started_retryable');
});

test('known generation with missing usage is quarantined and never posts twice', async () => {
  resetIntelligenceProviderRegistry();
  const store = new MemoryOperatorStore(remoteState());
  let posts = 0;
  let generationReads = 0;
  const fetchImpl = async (url, init = {}) => {
    if (String(url) === CHAT_URL && init.method === 'POST') {
      posts += 1;
      return {
        ok: true,
        status: 200,
        headers: headers('gen-known-1'),
        async json() {
          return {
            id: 'gen-known-1',
            model: 'example/value-model',
            choices: [{ finish_reason: 'stop', message: { role: 'assistant', content: '{}' } }],
            usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
          };
        },
      };
    }
    if (String(url).startsWith(`${GENERATION_URL}?id=`)) {
      generationReads += 1;
      return { ok: false, status: 503, headers: headers(), async json() { return {}; } };
    }
    throw new Error(`unexpected_fetch:${url}`);
  };

  const first = await execute(store, fetchImpl);
  assert.equal(first.status, 409);
  assert.equal(first.body.providerOutcome, 'generation_usage_pending');
  assert.equal(first.body.generationId, 'gen-known-1');
  assert.equal(first.body.automaticReconciliationScheduled, true);
  assert.equal(posts, 1);
  assert.equal(generationReads, 1);

  const second = await execute(store, fetchImpl);
  assert.equal(second.status, 409);
  assert.ok(second.body.errors.includes('model_usage_pending_reconciliation'));
  assert.equal(posts, 1, 'a pending generation must never issue a second billable POST');

  const state = await store.load();
  assert.equal(state.modelUsageLedger[0].status, 'usage_pending');
  assert.equal(state.modelUsageLedger[0].generationId, 'gen-known-1');
  assert.equal(state.modelUsageLedger[0].requiresManualReconciliation, false);
  assert.equal(state.modelUsageLedger[0].reconciliationStatus, 'pending');
});

test('transport-uncertain remote attempt is manual-only and never retried automatically', async () => {
  resetIntelligenceProviderRegistry();
  const store = new MemoryOperatorStore(remoteState());
  let posts = 0;
  const fetchImpl = async (url, init = {}) => {
    if (String(url) === CHAT_URL && init.method === 'POST') {
      posts += 1;
      throw new Error('socket_closed_after_write');
    }
    throw new Error(`unexpected_fetch:${url}`);
  };

  const first = await execute(store, fetchImpl);
  assert.equal(first.status, 409);
  assert.equal(first.body.providerOutcome, 'uncertain');
  assert.equal(first.body.requiresManualReconciliation, true);
  assert.equal(posts, 1);

  const second = await execute(store, fetchImpl);
  assert.equal(second.status, 409);
  assert.equal(posts, 1);

  const state = await store.load();
  assert.equal(state.modelUsageLedger[0].status, 'usage_pending');
  assert.equal(state.modelUsageLedger[0].generationId, null);
  assert.equal(state.modelUsageLedger[0].requiresManualReconciliation, true);
  assert.equal(state.modelUsageLedger[0].reconciliationStatus, 'manual_required');
});

test('known generation usage is reconciled idempotently by maintenance metadata lookup', async () => {
  const state = remoteState();
  const quote = state.modelUsageLedger[0];
  quote.status = 'usage_pending';
  quote.generationId = 'gen-reconcile-1';
  quote.reconciliationStatus = 'pending';
  quote.nextReconciliationAt = NOW;
  quote.requiresManualReconciliation = false;

  let generationReads = 0;
  const fetchImpl = async url => {
    generationReads += 1;
    assert.match(String(url), /id=gen-reconcile-1/);
    return {
      ok: true,
      status: 200,
      async json() {
        return {
          data: {
            id: 'gen-reconcile-1',
            tokens_prompt: 120,
            tokens_completion: 30,
            native_tokens_reasoning: 10,
            native_tokens_cached: 5,
            total_cost: 0.0125,
            upstream_inference_cost: 0.01,
            provider_name: 'Example Provider',
            request_id: 'req-1',
          },
        };
      },
    };
  };

  const prepared = await preparePendingOpenRouterReconciliations({
    state,
    env: env(),
    fetchImpl,
    now: new Date(NOW),
  });
  assert.equal(prepared.attempted, 1);
  assert.equal(generationReads, 1);

  const applied = applyPendingOpenRouterReconciliations(state, prepared, env(), new Date(NOW));
  assert.equal(applied.reconciled, 1);
  assert.equal(quote.status, 'reconciled');
  assert.equal(quote.actualCostUsd, 0.0125);
  assert.equal(quote.promptTokensActual, 120);
  assert.equal(quote.completionTokensActual, 30);
  assert.equal(state.researchJobs[0].status, 'completed');
  assert.equal(state.agentCostLedger[0].remoteApiCost, 0.0125);
  assert.equal(state.agentCostLedger[0].recoveryStatus, 'reconciled');
});

test('generation metadata failures back off and eventually require manual reconciliation', async () => {
  const state = remoteState();
  const quote = state.modelUsageLedger[0];
  quote.status = 'usage_pending';
  quote.generationId = 'gen-missing-1';
  quote.reconciliationStatus = 'pending';
  quote.nextReconciliationAt = NOW;
  quote.requiresManualReconciliation = false;

  const fetchImpl = async () => ({
    ok: false,
    status: 404,
    async json() { return { error: { message: 'generation_not_found_yet' } }; },
  });

  const prepared = await preparePendingOpenRouterReconciliations({ state, env: env(), fetchImpl, now: new Date(NOW) });
  const scheduled = applyPendingOpenRouterReconciliations(state, prepared, env(), new Date(NOW));
  assert.equal(scheduled.retryScheduled, 1);
  assert.equal(quote.reconciliationAttempts, 1);
  assert.equal(quote.reconciliationStatus, 'retry_scheduled');
  assert.ok(new Date(quote.nextReconciliationAt) > new Date(NOW));
  assert.equal(quote.requiresManualReconciliation, false);

  quote.nextReconciliationAt = '2026-07-31T20:00:01.000Z';
  quote.reconciliationAttempts = 2;
  const finalPrepared = await preparePendingOpenRouterReconciliations({
    state,
    env: env({ OPENROUTER_RECONCILIATION_MAX_ATTEMPTS: '3' }),
    fetchImpl,
    now: new Date('2026-07-31T20:00:02.000Z'),
  });
  const exhausted = applyPendingOpenRouterReconciliations(
    state,
    finalPrepared,
    env({ OPENROUTER_RECONCILIATION_MAX_ATTEMPTS: '3' }),
    new Date('2026-07-31T20:00:02.000Z'),
  );
  assert.equal(exhausted.exhausted, 1);
  assert.equal(quote.reconciliationStatus, 'exhausted');
  assert.equal(quote.requiresManualReconciliation, true);
  assert.equal(quote.nextReconciliationAt, null);
  assert.equal(state.agentCostLedger[0].recoveryStatus, 'usage_pending_manual_reconciliation_required');
});
