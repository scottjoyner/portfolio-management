import test from 'node:test';
import assert from 'node:assert/strict';
import { handleOperatorRoute, persistAuditEvents } from '../apps/api/src/operatorRouter.mjs';
import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';

function makeStore({ targeted = false } = {}) {
  const state = createInitialOperatorState();
  const calls = [];
  return {
    state,
    calls,
    async load() {
      calls.push({ method: 'load' });
      return state;
    },
    async mutate(fn) {
      calls.push({ method: 'mutate' });
      const result = await fn(state);
      calls.push({ method: 'save' });
      return result;
    },
    getStatus() {
      return { targetedProductMutations: targeted };
    },
    async upsertOpportunityBundle(bundle) {
      calls.push({ method: 'upsertOpportunityBundle', bundle });
    },
    async query(sql, params = []) {
      calls.push({ method: 'query', sql, params });
      return { rowCount: 1, rows: [] };
    }
  };
}

async function jsonBody(body) {
  return body;
}

test('product-only routes use targeted fast path when store supports it', async () => {
  const store = makeStore({ targeted: true });
  const result = await handleOperatorRoute({
    method: 'POST',
    pathname: '/api/agents/budget-approvals',
    state: store.state,
    store,
    readJsonBody: () => jsonBody({ agentId: 'market-research-agent', projectedCost: 12, projectedTokens: 60000, requestedBy: 'test' })
  });

  assert.equal(result.status, 201);
  assert.ok(result.body.budgetApproval.id);
  assert.deepEqual(store.calls.map(call => call.method).slice(0, 3), ['load', 'upsertOpportunityBundle', 'query']);
  assert.ok(!store.calls.some(call => call.method === 'mutate'));
  assert.ok(!store.calls.some(call => call.method === 'save'));
  const auditInsert = store.calls.find(call => call.method === 'query' && /INSERT INTO audit_events/.test(call.sql));
  assert.ok(auditInsert, 'fast path must persist audit events');
});

test('product-only routes fall back to full mutate when store does not support fast path', async () => {
  const store = makeStore({ targeted: false });
  const result = await handleOperatorRoute({
    method: 'POST',
    pathname: '/api/agents/budget-approvals',
    state: store.state,
    store,
    readJsonBody: () => jsonBody({ agentId: 'market-research-agent', projectedCost: 12, projectedTokens: 60000, requestedBy: 'test' })
  });

  assert.equal(result.status, 201);
  assert.ok(store.calls.some(call => call.method === 'mutate'));
  assert.ok(store.calls.some(call => call.method === 'save'));
});

test('targeted fast path does not persist validation failures', async () => {
  const store = makeStore({ targeted: true });
  const result = await handleOperatorRoute({
    method: 'POST',
    pathname: '/api/agents/budget-approvals',
    state: store.state,
    store,
    readJsonBody: () => jsonBody({ agentId: 'market-research-agent', projectedCost: 0, projectedTokens: 0 })
  });

  assert.equal(result.status, 400);
  assert.ok(result.body.errors.includes('projected_cost_required'));
  assert.ok(!store.calls.some(call => call.method === 'upsertOpportunityBundle'));
  assert.ok(!store.calls.some(call => call.method === 'query' && /audit_events/.test(call.sql)));
});

test('persistAuditEvents uses store upsertAuditEvents when available', async () => {
  const calls = [];
  const store = {
    async upsertAuditEvents(events) {
      calls.push(events);
    }
  };
  await persistAuditEvents(store, [{ id: 'audit-1', action: 'x' }]);
  assert.equal(calls.length, 1);
  assert.equal(calls[0][0].id, 'audit-1');
});
