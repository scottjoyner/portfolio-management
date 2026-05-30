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

test('operator UI assets are served over HTTP and use API-backed dashboard data', async () => {
  await withServer(async ({ baseUrl }) => {
    const home = await request(baseUrl, '/');
    assert.equal(home.response.status, 200);
    assert.match(home.body, /Trading Bot Command Center/);
    assert.match(home.body, /id="opportunities"/);
    assert.match(home.body, /id="polymarket"/);
    assert.match(home.body, /id="connectors"/);

    const app = await request(baseUrl, '/ui/app.js');
    assert.equal(app.response.status, 200);
    assert.match(app.body, /netExpectedValue/);
    assert.match(app.body, /\/api\/opportunity-dashboard/);
    assert.match(app.body, /\/api\/connectors\/market-data\/ingest/);
    assert.match(app.body, /\/api\/opportunities\/generate-from-connectors/);
    assert.doesNotMatch(app.body, /dashboard-data\.js/);
  });
});

test('connector ingest records status, run history, snapshots, and review opportunities', async () => {
  await withServer(async ({ baseUrl }) => {
    const initialStatus = await request(baseUrl, '/api/connectors/status');
    assert.equal(initialStatus.response.status, 200);
    assert.equal(initialStatus.body.connectors.length, 0);
    assert.equal(initialStatus.body.lastRun, null);

    const ingest = await request(baseUrl, '/api/connectors/market-data/ingest', { method: 'POST' });
    assert.equal(ingest.response.status, 200);
    assert.equal(ingest.body.run.status, 'completed');
    assert.ok(ingest.body.snapshots.length >= 3);
    assert.ok(ingest.body.health.connectors.length >= 2);

    const runsAfterIngest = await request(baseUrl, '/api/connectors/runs');
    assert.equal(runsAfterIngest.response.status, 200);
    assert.equal(runsAfterIngest.body.runs.length, 1);
    assert.equal(runsAfterIngest.body.runs[0].kind, 'market_data_ingest');

    const generated = await request(baseUrl, '/api/opportunities/generate-from-connectors', { method: 'POST' });
    assert.equal(generated.response.status, 201);
    assert.equal(generated.body.run.kind, 'opportunity_generation');
    assert.ok(generated.body.opportunities.length >= 3);
    assert.ok(generated.body.opportunities.some(opp => opp.venue === 'polymarket-watch'));
    assert.ok(generated.body.opportunities.every(opp => Number.isFinite(opp.netExpectedValue)));

    const dashboard = await request(baseUrl, '/api/opportunity-dashboard');
    assert.equal(dashboard.response.status, 200);
    assert.ok(dashboard.body.marketDataSnapshots.length >= 3);
    assert.ok(dashboard.body.connectorRuns.length >= 3);
    assert.ok(dashboard.body.connectorHealth.connectors.length >= 2);
    assert.ok(dashboard.body.researchJobs.length >= generated.body.opportunities.length);
    assert.ok(dashboard.body.riskBreakdowns.length >= generated.body.opportunities.length);

    const secondRun = await request(baseUrl, '/api/opportunities/generate-from-connectors', { method: 'POST' });
    assert.equal(secondRun.response.status, 201);
    assert.equal(secondRun.body.opportunities.length, 0);
  });
});

test('opportunity validation and agent budget guardrails reject unsafe inputs', async () => {
  await withServer(async ({ baseUrl }) => {
    const invalidOpportunity = await request(baseUrl, '/api/opportunities', {
      method: 'POST',
      body: {
        marketType: 'crypto_spot',
        venue: 'coinbase-paper',
        title: 'Invalid risk candidate',
        winProbability: 0.9,
        lossProbability: 0.9,
        totalMoneyRisked: 100,
        maxLoss: 250,
        potentialUpside: 50
      }
    });
    assert.equal(invalidOpportunity.response.status, 400);
    assert.ok(invalidOpportunity.body.errors.includes('max_loss_exceeds_total_money_risked'));
    assert.ok(invalidOpportunity.body.errors.includes('win_loss_probability_sum_invalid'));

    const runawayJob = await request(baseUrl, '/api/agents/jobs', {
      method: 'POST',
      body: {
        agentId: 'market-research-agent',
        model: 'expensive-remote-model',
        localOrRemote: 'remote',
        promptTokens: 250000,
        completionTokens: 250000,
        totalTokens: 500000,
        costPerMillionTokens: 200,
        marketScope: 'PREDICTION:DEMO'
      }
    });
    assert.equal(runawayJob.response.status, 400);
    assert.ok(runawayJob.body.errors.includes('per_job_token_limit_exceeded'));
    assert.ok(runawayJob.body.errors.includes('daily_cost_limit_exceeded'));
    assert.ok(runawayJob.body.errors.includes('research_budget_approval_required'));

    const overrideJob = await request(baseUrl, '/api/agents/jobs', {
      method: 'POST',
      body: {
        agentId: 'market-research-agent',
        model: 'approved-review-model',
        localOrRemote: 'remote',
        promptTokens: 1000,
        completionTokens: 500,
        totalTokens: 1500,
        remoteApiCost: 12,
        approvedBudgetOverride: true,
        marketScope: 'PREDICTION:DEMO'
      }
    });
    assert.equal(overrideJob.response.status, 201);
    assert.equal(overrideJob.body.job.agentId, 'market-research-agent');
  });
});

test('opportunity, risk, agent, and cost workflow runs over HTTP', async () => {
  await withServer(async ({ baseUrl }) => {
    const job = await request(baseUrl, '/api/agents/jobs', {
      method: 'POST',
      body: { agentId: 'e2e-research-agent', model: 'local-qwen', localOrRemote: 'local', promptTokens: 9000, completionTokens: 3000, totalTokens: 12000, runtimeSeconds: 120, estimatedWatts: 300, hardwareDepreciationPerHour: 0.4, marketScope: 'ETH-USD' }
    });
    assert.equal(job.response.status, 201);
    assert.equal(job.body.job.agentId, 'e2e-research-agent');
    assert.ok(job.body.ledger.localComputeCost > 0);

    const created = await request(baseUrl, '/api/opportunities', {
      method: 'POST',
      body: {
        researchJobId: job.body.job.id,
        marketType: 'crypto_spot',
        venue: 'coinbase-paper',
        symbol: 'ETH-USD',
        title: 'E2E ETH opportunity',
        recommendation: 'paper_review',
        confidenceScore: 0.62,
        winProbability: 0.56,
        grossExpectedValue: 100,
        totalMoneyRisked: 1000,
        maxLoss: 250,
        potentialUpside: 440,
        liquidityScore: 80,
        dataFreshnessScore: 90,
        estimatedFees: 4,
        estimatedSlippage: 6,
        agentResearchCost: 3,
        modelInferenceCost: 2
      }
    });
    assert.equal(created.response.status, 201);
    assert.equal(created.body.opportunity.netExpectedValue, 85);
    assert.equal(created.body.opportunity.riskBreakdownId, created.body.riskBreakdown.id);

    const dashboard = await request(baseUrl, '/api/opportunity-dashboard');
    assert.equal(dashboard.response.status, 200);
    assert.ok(dashboard.body.opportunities.some(opp => opp.id === created.body.opportunity.id));
    assert.ok(dashboard.body.riskBreakdowns.some(risk => risk.id === created.body.riskBreakdown.id));
    assert.ok(dashboard.body.agentCostLedger.some(row => row.jobId === job.body.job.id));

    const detail = await request(baseUrl, `/api/opportunities/${created.body.opportunity.id}`);
    assert.equal(detail.response.status, 200);
    assert.equal(detail.body.opportunity.title, 'E2E ETH opportunity');
    assert.ok(detail.body.riskBreakdown.aggregateScore >= 0);

    const approved = await request(baseUrl, `/api/opportunities/${created.body.opportunity.id}/approve`, { method: 'POST', body: { reviewer: 'e2e', reason: 'paper review accepted' } });
    assert.equal(approved.response.status, 200);
    assert.equal(approved.body.opportunity.status, 'approved');

    const moreResearch = await request(baseUrl, `/api/opportunities/${created.body.opportunity.id}/request-research`, { method: 'POST', body: { localOrRemote: 'local', model: 'local-review', totalTokens: 8000, runtimeSeconds: 60, approvedBudgetOverride: true } });
    assert.equal(moreResearch.response.status, 200);
    assert.equal(moreResearch.body.opportunity.status, 'research_requested');
    assert.ok(moreResearch.body.ledger.localComputeCost >= 0);

    const costs = await request(baseUrl, '/api/agents/costs');
    assert.equal(costs.response.status, 200);
    assert.ok(costs.body.summary.spentTodayUsd > 0);
    assert.match(costs.body.summary.localCostFormula, /runtime_hours/);

    const poly = await request(baseUrl, '/api/polymarket/opportunities');
    assert.equal(poly.response.status, 200);
    assert.ok(Array.isArray(poly.body.opportunities));

    const audit = await request(baseUrl, '/api/audit');
    assert.equal(audit.response.status, 200);
    assert.ok(audit.body.audit.some(event => event.action === 'opportunity_created'));
    assert.ok(audit.body.audit.some(event => event.action === 'opportunity_approved'));
    assert.ok(audit.body.audit.some(event => event.action === 'opportunity_research_requested'));
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

    const createdStrategy = await request(baseUrl, '/api/strategies/from-template', { method: 'POST', body: { templateId: template.id, name: 'E2E Template Strategy' } });
    assert.equal(createdStrategy.response.status, 201);
    assert.equal(createdStrategy.body.ok, true);
    const strategyId = createdStrategy.body.strategy.id;

    const backtest = await request(baseUrl, '/api/backtests/run', { method: 'POST', body: { strategyId, initialCapitalUsd: 100000, feeBps: 5, slippageBps: 10 } });
    assert.equal(backtest.response.status, 201);
    assert.equal(backtest.body.backtest.status, 'completed');

    const approval = await request(baseUrl, '/api/approvals/request', { method: 'POST', body: { strategyId, tier: 'canary' } });
    assert.equal(approval.response.status, 201);
    assert.equal(approval.body.approval.status, 'pending_review');

    const decision = await request(baseUrl, `/api/approvals/${approval.body.approval.id}/decision`, { method: 'POST', body: { status: 'approved', reviewer: 'e2e-test', reason: 'covered by e2e paper flow' } });
    assert.equal(decision.response.status, 200);
    assert.equal(decision.body.approval.status, 'approved');

    const paper = await request(baseUrl, '/api/paper-executions', { method: 'POST', body: { strategyId, accountId: 'acct-paper-primary' } });
    assert.equal(paper.response.status, 201);
    assert.equal(paper.body.execution.status, 'running');

    const signal = await request(baseUrl, `/api/paper-executions/${paper.body.execution.id}/signal`, { method: 'POST', body: { signal: { symbol: 'BTC-USD', side: 'buy', quantity: 0.1, price: 50000, feeBps: 5, slippageBps: 10 } } });
    assert.equal(signal.response.status, 200);
    assert.equal(signal.body.fill.status, 'filled');
    assert.equal(signal.body.reconciliation.status, 'ok');

    const positions = await request(baseUrl, '/api/positions');
    assert.equal(positions.response.status, 200);
    assert.ok(positions.body.positions.some(position => position.symbol === 'BTC-USD'));

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
