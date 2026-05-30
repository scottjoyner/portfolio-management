import { createBacktest, createStrategyFromTemplate, cloneStrategyVersion, updateStrategyStatus, requestApproval, decideApproval, startPaperExecution, stopPaperExecution } from './operatorFlows.mjs';
import { nextId } from '../../../packages/storage/src/operatorStore.mjs';
import { executePaperSignal } from '../../../packages/execution/src/paperEngine.mjs';
import { createOpportunity, createResearchJob, decideBudgetApproval, decideOpportunity, ensureOpportunityState, requestBudgetApproval, summarizeAgentCosts } from './opportunityFlows.mjs';
import { generateOpportunitiesFromConnectors, ingestConnectorSnapshots } from './opportunityGenerator.mjs';

export function routeMatch(pathname, pattern) {
  const pathParts = pathname.split('/').filter(Boolean);
  const patternParts = pattern.split('/').filter(Boolean);
  if (pathParts.length !== patternParts.length) return null;
  const params = {};
  for (let i = 0; i < patternParts.length; i += 1) {
    const patternPart = patternParts[i];
    if (patternPart.startsWith(':')) params[patternPart.slice(1)] = decodeURIComponent(pathParts[i]);
    else if (patternPart !== pathParts[i]) return null;
  }
  return params;
}

async function persistOne(store, method, value) {
  if (value && typeof store[method] === 'function') await store[method](value);
}

async function persistMany(store, method, values = []) {
  if (!Array.isArray(values) || !values.length || typeof store[method] !== 'function') return;
  await store[method](values);
}

export async function persistRouteArtifacts(store, result = {}) {
  if (!result || result.errors?.length) return;

  if (typeof store.upsertOpportunityBundle === 'function') {
    const bundle = {
      marketDataSnapshots: result.snapshots || result.marketDataSnapshots || [],
      budgetApprovals: [result.budgetApproval].filter(Boolean),
      researchJobs: [result.job, ...(result.jobs || [])].filter(Boolean),
      opportunities: [result.opportunity, ...(result.opportunities || [])].filter(Boolean),
      riskBreakdowns: [result.riskBreakdown, ...(result.riskBreakdowns || [])].filter(Boolean),
      agentCostLedger: [result.ledger, ...(result.ledgers || [])].filter(Boolean)
    };
    const hasBundleRecords = Object.values(bundle).some(records => records.length);
    if (hasBundleRecords) {
      await store.upsertOpportunityBundle(bundle);
      return;
    }
  }

  await persistMany(store, 'upsertMarketDataSnapshots', result.snapshots || result.marketDataSnapshots || []);
  await persistOne(store, 'upsertBudgetApproval', result.budgetApproval);
  await persistOne(store, 'upsertResearchJob', result.job);
  await persistOne(store, 'upsertAgentCost', result.ledger);
  await persistOne(store, 'upsertOpportunity', result.opportunity);
  await persistOne(store, 'upsertRiskBreakdown', result.riskBreakdown);

  for (const job of result.jobs || []) await persistOne(store, 'upsertResearchJob', job);
  for (const ledger of result.ledgers || []) await persistOne(store, 'upsertAgentCost', ledger);
  for (const opportunity of result.opportunities || []) await persistOne(store, 'upsertOpportunity', opportunity);
  for (const riskBreakdown of result.riskBreakdowns || []) await persistOne(store, 'upsertRiskBreakdown', riskBreakdown);
}

function supportsTargetedProductFastPath(store) {
  if (store?.targetedProductMutations === true) return true;
  if (typeof store?.getStatus !== 'function') return false;
  const status = store.getStatus();
  return Boolean(status?.targetedProductMutations && typeof store.load === 'function' && typeof store.upsertOpportunityBundle === 'function');
}

export async function persistAuditEvents(store, events = []) {
  if (!Array.isArray(events) || !events.length) return;
  if (typeof store.upsertAuditEvents === 'function') {
    await store.upsertAuditEvents(events);
    return;
  }
  if (typeof store.query !== 'function') return;
  for (const event of events) {
    await store.query(
      'INSERT INTO audit_events (id, action, actor, at, details, payload_json) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (id) DO UPDATE SET action = EXCLUDED.action, actor = EXCLUDED.actor, at = EXCLUDED.at, details = EXCLUDED.details, payload_json = EXCLUDED.payload_json',
      [event.id, event.action, event.actor || 'system', event.at || new Date().toISOString(), event.details || null, JSON.stringify(event.payload || {})]
    );
  }
}

async function mutateTargetedProduct(store, fn) {
  const state = await store.load();
  const auditStart = Array.isArray(state.audit) ? state.audit.length : 0;
  const result = await fn(state);
  if (result?.errors?.length) {
    const missing = result.errors.some(error => error.endsWith('_not_found'));
    return { status: missing ? 404 : 400, body: { ok: false, errors: result.errors } };
  }
  await persistRouteArtifacts(store, result);
  await persistAuditEvents(store, (state.audit || []).slice(auditStart));
  store.state = state;
  return { status: 200, body: { ok: true, ...result } };
}

async function mutate(store, fn, options = {}) {
  if (options.productOnly && supportsTargetedProductFastPath(store)) {
    return mutateTargetedProduct(store, fn);
  }
  const result = await store.mutate(async state => fn(state));
  if (result?.errors?.length) {
    const missing = result.errors.some(error => error.endsWith('_not_found'));
    return { status: missing ? 404 : 400, body: { ok: false, errors: result.errors } };
  }
  await persistRouteArtifacts(store, result);
  return { status: 200, body: { ok: true, ...result } };
}

function opportunityDashboard(state) {
  ensureOpportunityState(state);
  return {
    opportunities: state.opportunities,
    riskBreakdowns: state.riskBreakdowns,
    researchJobs: state.researchJobs,
    agentBudgets: state.agentBudgets,
    budgetApprovals: state.budgetApprovals,
    agentCostLedger: state.agentCostLedger,
    marketDataSnapshots: state.marketDataSnapshots,
    agentCostSummary: summarizeAgentCosts(state)
  };
}

export async function handleOperatorRoute({ method, pathname, state, store, readJsonBody }) {
  ensureOpportunityState(state);

  if (method === 'GET' && pathname === '/api/accounts') return { status: 200, body: { accounts: state.accounts } };
  if (method === 'GET' && pathname === '/api/instruments') return { status: 200, body: { instruments: state.instruments } };
  if (method === 'GET' && pathname === '/api/strategy-templates') return { status: 200, body: { templates: state.strategyTemplates } };
  if (method === 'GET' && pathname === '/api/opportunity-dashboard') return { status: 200, body: opportunityDashboard(state) };
  if (method === 'GET' && pathname === '/api/opportunities') return { status: 200, body: { opportunities: state.opportunities } };
  if (method === 'GET' && pathname === '/api/risk-breakdowns') return { status: 200, body: { riskBreakdowns: state.riskBreakdowns } };
  if (method === 'GET' && pathname === '/api/agents/jobs') return { status: 200, body: { jobs: state.researchJobs } };
  if (method === 'GET' && pathname === '/api/agents/budgets') return { status: 200, body: { budgets: state.agentBudgets } };
  if (method === 'GET' && pathname === '/api/agents/budget-approvals') return { status: 200, body: { budgetApprovals: state.budgetApprovals } };
  if (method === 'GET' && pathname === '/api/agents/costs') return { status: 200, body: { costs: state.agentCostLedger, summary: summarizeAgentCosts(state) } };
  if (method === 'GET' && pathname === '/api/market-data/snapshots') return { status: 200, body: { snapshots: state.marketDataSnapshots } };
  if (method === 'GET' && pathname === '/api/polymarket/opportunities') return { status: 200, body: { opportunities: state.opportunities.filter(o => o.venue?.includes('polymarket') || o.marketType === 'prediction_market') } };

  if (method === 'POST' && pathname === '/api/connectors/market-data/ingest') {
    const result = await mutate(store, current => ingestConnectorSnapshots(current), { productOnly: true });
    return result;
  }

  if (method === 'POST' && pathname === '/api/opportunities/generate-from-connectors') {
    const result = await mutate(store, current => generateOpportunitiesFromConnectors(current), { productOnly: true });
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  if (method === 'POST' && pathname === '/api/agents/budget-approvals') {
    const body = await readJsonBody();
    const result = await mutate(store, current => requestBudgetApproval(current, body), { productOnly: true });
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  const budgetDecision = routeMatch(pathname, '/api/agents/budget-approvals/:id/decision');
  if (method === 'POST' && budgetDecision) {
    const body = await readJsonBody();
    return mutate(store, current => decideBudgetApproval(current, budgetDecision.id, body), { productOnly: true });
  }

  const opportunityDetail = routeMatch(pathname, '/api/opportunities/:id');
  if (method === 'GET' && opportunityDetail) {
    const opportunity = state.opportunities.find(o => o.id === opportunityDetail.id);
    if (!opportunity) return { status: 404, body: { ok: false, errors: ['opportunity_not_found'] } };
    return { status: 200, body: { opportunity, riskBreakdown: state.riskBreakdowns.find(r => r.id === opportunity.riskBreakdownId) || null } };
  }

  if (method === 'POST' && pathname === '/api/agents/jobs') {
    const body = await readJsonBody();
    const result = await mutate(store, current => createResearchJob(current, body), { productOnly: true });
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  if (method === 'POST' && pathname === '/api/opportunities') {
    const body = await readJsonBody();
    const result = await mutate(store, current => createOpportunity(current, body), { productOnly: true });
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  for (const decisionStatus of ['approve', 'reject', 'defer']) {
    const decision = routeMatch(pathname, `/api/opportunities/:id/${decisionStatus}`);
    if (method === 'POST' && decision) {
      const body = await readJsonBody();
      const status = decisionStatus === 'approve' ? 'approved' : decisionStatus === 'reject' ? 'rejected' : 'deferred';
      return mutate(store, current => decideOpportunity(current, decision.id, { ...body, status }), { productOnly: true });
    }
  }

  const requestResearch = routeMatch(pathname, '/api/opportunities/:id/request-research');
  if (method === 'POST' && requestResearch) {
    const body = await readJsonBody();
    return mutate(store, current => {
      const opportunity = current.opportunities.find(o => o.id === requestResearch.id);
      if (!opportunity) return { errors: ['opportunity_not_found'] };
      const research = createResearchJob(current, { ...body, agentId: body.agentId || opportunity.sourceAgentId, marketScope: opportunity.marketSlug || opportunity.symbol || opportunity.title });
      if (research.errors) return research;
      const { job, ledger } = research;
      opportunity.status = 'research_requested';
      opportunity.updatedAt = new Date().toISOString();
      current.audit.push({ id: nextId('audit', current.audit), action: 'opportunity_research_requested', actor: job.agentId, at: new Date().toISOString(), details: opportunity.id, payload: { jobId: job.id, costLedgerId: ledger.id, budgetApprovalId: job.budgetApprovalId } });
      return { opportunity, job, ledger };
    }, { productOnly: true });
  }

  const clone = routeMatch(pathname, '/api/strategies/:id/clone');
  if (method === 'POST' && clone) {
    const body = await readJsonBody();
    const result = await mutate(store, current => cloneStrategyVersion(current, clone.id, body));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  const strategyStatus = routeMatch(pathname, '/api/strategies/:id/status');
  if (method === 'POST' && strategyStatus) {
    const body = await readJsonBody();
    return mutate(store, current => updateStrategyStatus(current, strategyStatus.id, body.status));
  }

  const backtestReport = routeMatch(pathname, '/api/backtests/:id/report');
  if (method === 'GET' && backtestReport) {
    const backtest = state.backtests.find(run => run.id === backtestReport.id);
    if (!backtest) return { status: 404, body: { ok: false, errors: ['backtest_not_found'] } };
    return { status: 200, body: { report: backtest.report || {}, metrics: backtest.metrics, assumptions: backtest.assumptions, trades: backtest.trades, equityCurve: backtest.equityCurve } };
  }

  const approvalDecision = routeMatch(pathname, '/api/approvals/:id/decision');
  if (method === 'POST' && approvalDecision) {
    const body = await readJsonBody();
    return mutate(store, current => decideApproval(current, approvalDecision.id, body));
  }

  if (method === 'GET' && pathname === '/api/paper-executions') return { status: 200, body: { executions: state.paperExecutions } };

  if (method === 'POST' && pathname === '/api/paper-executions') {
    const body = await readJsonBody();
    const result = await mutate(store, current => startPaperExecution(current, body));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  const stopPaper = routeMatch(pathname, '/api/paper-executions/:id/stop');
  if (method === 'POST' && stopPaper) {
    const body = await readJsonBody();
    return mutate(store, current => stopPaperExecution(current, stopPaper.id, body));
  }

  const signalPaper = routeMatch(pathname, '/api/paper-executions/:id/signal');
  if (method === 'POST' && signalPaper) {
    const body = await readJsonBody();
    return mutate(store, current => {
      const result = executePaperSignal(current, signalPaper.id, body.signal || body, body.quote || {});
      if (!result.ok) return { errors: result.errors };
      current.audit.push({ id: nextId('audit', current.audit), action: 'paper_signal_filled', actor: 'operator', at: new Date().toISOString(), details: result.fill.id, payload: { executionId: signalPaper.id, symbol: result.fill.symbol } });
      return result;
    });
  }

  if (method === 'POST' && pathname === '/api/strategies/from-template') {
    const body = await readJsonBody();
    const result = await mutate(store, current => createStrategyFromTemplate(current, body));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  if (method === 'POST' && pathname === '/api/backtests/run') {
    const body = await readJsonBody();
    const result = await mutate(store, current => createBacktest(current, body));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  if (method === 'POST' && pathname === '/api/approvals/request') {
    const body = await readJsonBody();
    const result = await mutate(store, current => requestApproval(current, body));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  if (method === 'POST' && pathname === '/api/kill-switch/stop-paper') {
    const result = await mutate(store, current => {
      const now = new Date().toISOString();
      for (const execution of current.paperExecutions) {
        if (execution.status === 'running') {
          execution.status = 'stopped';
          execution.stoppedAt = now;
          execution.stopReason = 'operator_stop_all';
        }
      }
      current.audit.push({ id: nextId('audit', current.audit), action: 'paper_execution_stop_all', actor: 'operator', at: now, details: 'operator_stop_all' });
      return { executions: current.paperExecutions };
    });
    return result;
  }

  return null;
}
