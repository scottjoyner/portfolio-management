import test from 'node:test';
import assert from 'node:assert/strict';
import { startServer } from '../apps/api/src/server.p1.mjs';
import { MemoryOperatorStore, createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';

async function withServer(fn) {
  const store = new MemoryOperatorStore(createInitialOperatorState());
  const server = startServer(0, { store, env: { NODE_ENV: 'development', OPERATOR_AUTH_REQUIRED: 'false', CSRF_REQUIRED: 'false' } });
  await new Promise(resolve => server.once('listening', resolve));
  const { port } = server.address();
  const baseUrl = `http://127.0.0.1:${port}`;
  try {
    return await fn({ baseUrl, store });
  } finally {
    await new Promise(resolve => server.close(resolve));
  }
}

async function request(baseUrl, path, options = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    headers: { 'content-type': 'application/json', ...(options.headers || {}) },
    ...options,
    body: options.body ? JSON.stringify(options.body) : undefined
  });
  const text = await response.text();
  let body = text;
  try { body = JSON.parse(text); } catch {}
  return { response, body };
}

test('operator UI assets are served over HTTP', async () => {
  await withServer(async ({ baseUrl }) => {
    const home = await request(baseUrl, '/');
    assert.equal(home.response.status, 200);
    assert.match(home.body, /Trading Bot Command Center/);
    assert.match(home.body, /id="opportunities"/);
    assert.match(home.body, /id="polymarket"/);

    const app = await request(baseUrl, '/ui/app.js');
    assert.equal(app.response.status, 200);
    assert.match(app.body, /netExpectedValue/);

    const data = await request(baseUrl, '/ui/dashboard-data.js');
    assert.equal(data.response.status, 200);
    assert.match(data.body, /agentCostSummary/);
  });
});

test('full paper operator workflow runs over HTTP', async () => {
  await withServer(async ({ baseUrl }) => {
    const summary = await request(baseUrl, '/api/operator/summary');
    assert.equal(summary.response.status, 200);
    assert.ok(summary.body.counts.strategies >= 1);

    const templates = await request(baseUrl, '/api/strategy-templates');
    assert.equal(templates.response.status, 200);
    const template = templates.body.templates[0];
    assert.ok(template.id);

    const createdStrategy = await request(baseUrl, '/api/strategies/from-template', {
      method: 'POST',
      body: { templateId: template.id, name: 'E2E Template Strategy' }
    });
    assert.equal(createdStrategy.response.status, 201);
    assert.equal(createdStrategy.body.ok, true);
    const strategyId = createdStrategy.body.strategy.id;

    const backtest = await request(baseUrl, '/api/backtests/run', {
      method: 'POST',
      body: { strategyId, initialCapitalUsd: 100000, feeBps: 5, slippageBps: 10 }
    });
    assert.equal(backtest.response.status, 201);
    assert.equal(backtest.body.backtest.status, 'completed');

    const approval = await request(baseUrl, '/api/approvals/request', {
      method: 'POST',
      body: { strategyId, tier: 'canary' }
    });
    assert.equal(approval.response.status, 201);
    assert.equal(approval.body.approval.status, 'pending_review');

    const decision = await request(baseUrl, `/api/approvals/${approval.body.approval.id}/decision`, {
      method: 'POST',
      body: { status: 'approved', reviewer: 'e2e-test', reason: 'covered by e2e paper flow' }
    });
    assert.equal(decision.response.status, 200);
    assert.equal(decision.body.approval.status, 'approved');

    const paper = await request(baseUrl, '/api/paper-executions', {
      method: 'POST',
      body: { strategyId, accountId: 'acct-paper-primary' }
    });
    assert.equal(paper.response.status, 201);
    assert.equal(paper.body.execution.status, 'running');

    const signal = await request(baseUrl, `/api/paper-executions/${paper.body.execution.id}/signal`, {
      method: 'POST',
      body: { signal: { symbol: 'BTC-USD', side: 'buy', quantity: 0.1, price: 50000, feeBps: 5, slippageBps: 10 } }
    });
    assert.equal(signal.response.status, 200);
    assert.equal(signal.body.fill.status, 'filled');
    assert.equal(signal.body.reconciliation.status, 'ok');

    const positions = await request(baseUrl, '/api/positions');
    assert.equal(positions.response.status, 200);
    assert.ok(positions.body.positions.some(position => position.symbol === 'BTC-USD'));

    const audit = await request(baseUrl, '/api/audit');
    assert.equal(audit.response.status, 200);
    assert.ok(audit.body.audit.some(event => event.action === 'paper_signal_filled'));

    const liveBlocked = await request(baseUrl, '/api/execution/live/orders', { method: 'POST', body: { side: 'buy' } });
    assert.equal(liveBlocked.response.status, 403);
    assert.equal(liveBlocked.body.error, 'live_execution_disabled');

    const metrics = await request(baseUrl, '/metrics');
    assert.equal(metrics.response.status, 200);
    assert.equal(typeof metrics.body.strategies_total, 'number');

    const prom = await request(baseUrl, '/metrics.prom');
    assert.equal(prom.response.status, 200);
    assert.match(prom.body, /portfolio_requests_total/);
  });
});
