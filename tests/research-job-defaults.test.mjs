import test from 'node:test';
import assert from 'node:assert/strict';

import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';
import { createResearchJob } from '../apps/api/src/opportunityFlows.mjs';

test('research jobs default to local queued execution with durable lineage', () => {
  const state = createInitialOperatorState('2026-07-30T20:00:00.000Z');
  const result = createResearchJob(state, {
    model: 'qwen-local',
    promptTokens: 1200,
    completionTokens: 300,
    modelQuoteId: 'model-quote-001',
    economicDecisionId: 'decision-001',
    pricingSnapshotId: 'pricing-local-001',
    localNodeId: 'x1-370',
    localNodeName: 'x1-370',
  }, '2026-07-30T20:01:00.000Z');

  assert.equal(result.errors, undefined);
  assert.equal(result.job.localOrRemote, 'local');
  assert.equal(result.job.provider, 'local');
  assert.equal(result.job.status, 'queued');
  assert.equal(result.job.queuedAt, '2026-07-30T20:01:00.000Z');
  assert.equal(result.job.startedAt, null);
  assert.equal(result.job.completedAt, null);
  assert.equal(result.job.modelQuoteId, 'model-quote-001');
  assert.equal(result.job.economicDecisionId, 'decision-001');
  assert.equal(result.job.localNodeId, 'x1-370');
  assert.ok(result.ledger.localComputeCost > 0);
  assert.equal(result.ledger.remoteApiCost, 0);
  assert.equal(result.ledger.costSource, 'pre_call_estimate');
  assert.equal(state.audit.at(-1).payload.localOrRemote, 'local');
  assert.equal(state.audit.at(-1).payload.status, 'queued');
});

test('remote completed research remains explicit and budget-gated', () => {
  const state = createInitialOperatorState('2026-07-30T20:00:00.000Z');
  const blocked = createResearchJob(state, {
    localOrRemote: 'remote',
    status: 'completed',
    model: 'paid-model',
    totalTokens: 1000,
    remoteApiCost: 12,
  });
  assert.ok(blocked.errors.includes('research_budget_approval_required'));

  const allowed = createResearchJob(state, {
    localOrRemote: 'remote',
    status: 'completed',
    model: 'paid-model',
    totalTokens: 1000,
    remoteApiCost: 12,
    systemBudgetOverride: true,
  }, '2026-07-30T20:02:00.000Z');
  assert.equal(allowed.job.localOrRemote, 'remote');
  assert.equal(allowed.job.status, 'completed');
  assert.equal(allowed.job.startedAt, '2026-07-30T20:02:00.000Z');
  assert.equal(allowed.job.completedAt, '2026-07-30T20:02:00.000Z');
  assert.equal(allowed.ledger.localComputeCost, 0);
  assert.equal(allowed.ledger.remoteApiCost, 12);
});
