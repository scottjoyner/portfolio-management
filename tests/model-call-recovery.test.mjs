import test from 'node:test';
import assert from 'node:assert/strict';

import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';
import { recoverStaleModelCalls } from '../apps/api/src/modelCallRecovery.mjs';

test('stale running quotes and linked jobs fail closed and become retryable', () => {
  const state = createInitialOperatorState('2026-07-30T20:00:00.000Z');
  state.modelUsageLedger = [{
    id: 'quote-001',
    status: 'running',
    localOrRemote: 'local',
    localNodeId: 'x1-370',
    researchJobId: 'job-001',
    startedAt: '2026-07-30T20:00:00.000Z',
  }];
  state.researchJobs = [{
    id: 'job-001',
    modelQuoteId: 'quote-001',
    localOrRemote: 'local',
    status: 'running',
    startedAt: '2026-07-30T20:00:00.000Z',
  }];
  state.agentCostLedger = [{ id: 'cost-001', jobId: 'job-001', modelQuoteId: 'quote-001' }];

  const report = recoverStaleModelCalls(state, {
    now: '2026-07-30T20:10:01.000Z',
    staleSeconds: 600,
  });

  assert.equal(report.recoveredQuoteCount, 1);
  assert.equal(report.recoveredOrphanJobCount, 0);
  assert.equal(state.modelUsageLedger[0].status, 'failed');
  assert.equal(state.modelUsageLedger[0].failureReason, 'stale_model_call_recovered');
  assert.equal(state.modelUsageLedger[0].requiresRequote, true);
  assert.equal(state.researchJobs[0].status, 'failed');
  assert.equal(state.researchJobs[0].retryable, true);
  assert.equal(state.agentCostLedger[0].recoveryStatus, 'failed_before_cost_reconciliation');
  assert.equal(state.audit.at(-1).action, 'stale_model_calls_recovered');
});

test('fresh running calls and usage-pending reconciliation are not changed', () => {
  const state = createInitialOperatorState('2026-07-30T20:00:00.000Z');
  state.modelUsageLedger = [
    { id: 'quote-fresh', status: 'running', localOrRemote: 'remote', startedAt: '2026-07-30T20:09:30.000Z' },
    { id: 'quote-pending', status: 'usage_pending', localOrRemote: 'remote', startedAt: '2026-07-30T19:00:00.000Z' },
  ];

  const report = recoverStaleModelCalls(state, {
    now: '2026-07-30T20:10:00.000Z',
    staleSeconds: 60,
  });

  assert.equal(report.recoveredQuoteCount, 0);
  assert.equal(state.modelUsageLedger[0].status, 'running');
  assert.equal(state.modelUsageLedger[1].status, 'usage_pending');
  assert.equal(state.audit.length, 0);
});

test('orphan running research jobs are recovered without inventing model usage', () => {
  const state = createInitialOperatorState('2026-07-30T20:00:00.000Z');
  state.researchJobs = [{
    id: 'job-orphan',
    localOrRemote: 'remote',
    status: 'running',
    startedAt: '2026-07-30T19:00:00.000Z',
  }];

  const report = recoverStaleModelCalls(state, {
    now: '2026-07-30T20:10:00.000Z',
    staleSeconds: 300,
  });

  assert.equal(report.recoveredQuoteCount, 0);
  assert.equal(report.recoveredOrphanJobCount, 1);
  assert.equal(state.researchJobs[0].status, 'failed');
  assert.equal(state.researchJobs[0].requiresRequote, false);
  assert.equal(state.modelUsageLedger.length, 0);
});
