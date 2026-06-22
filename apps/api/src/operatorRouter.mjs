import { createBacktest, createStrategyFromTemplate, cloneStrategyVersion, updateStrategyStatus, requestApproval, decideApproval, startPaperExecution, stopPaperExecution } from './operatorFlows.mjs';
import { nextId } from '../../../packages/storage/src/operatorStore.mjs';
import { executePaperSignal } from '../../../packages/execution/src/paperEngine.mjs';
import { ExecutionReconciler } from '../../../packages/execution/src/reconciliation.mjs';
import { SettlementTracker } from '../../../packages/execution/src/settlement.mjs';
import { createOpportunity, createResearchJob, decideBudgetApproval, decideOpportunity, ensureOpportunityState, requestBudgetApproval, summarizeAgentCosts } from './opportunityFlows.mjs';
import { fetchPredictionMarketSnapshots, generateOpportunitiesFromArbitrage, generateOpportunitiesFromConnectors, generateOpportunitiesFromPredictionMarkets, generateOpportunitiesFromStrategySignals, ingestConnectorSnapshots } from './opportunityGenerator.mjs';

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

function normalizeTradePlan(input = {}) {
  const tradePlan = input.tradePlan && typeof input.tradePlan === 'object' ? input.tradePlan : {};
  const entryPrice = Number(input.entryPrice ?? tradePlan.entry_price ?? input.price ?? input.orders?.[0]?.price ?? 0);
  const takeProfitPrice = Number(input.takeProfitPrice ?? tradePlan.take_profit_price ?? input.orders?.[0]?.takeProfitPrice ?? 0);
  const stopLossPrice = Number(input.stopLossPrice ?? tradePlan.stop_loss_price ?? input.orders?.[0]?.stopLossPrice ?? 0);
  const executionPurpose = input.executionPurpose ?? tradePlan.execution_purpose ?? null;
  const tradeIntent = input.tradeIntent ?? tradePlan.plan_type ?? null;
  const positionSide = input.positionSide ?? tradePlan.position_side ?? null;

  if (!Number.isFinite(entryPrice) || entryPrice <= 0) return { errors: ['entry_price_required'] };
  if (!Number.isFinite(takeProfitPrice) || takeProfitPrice <= 0) return { errors: ['take_profit_price_required'] };
  if (!Number.isFinite(stopLossPrice) || stopLossPrice <= 0) return { errors: ['stop_loss_price_required'] };

  const normalizedPlan = {
    ...tradePlan,
    entry_price: entryPrice,
    take_profit_price: takeProfitPrice,
    stop_loss_price: stopLossPrice,
    execution_purpose: executionPurpose,
    plan_type: tradeIntent,
    position_side: positionSide,
  };

  const normalizeOrder = order => ({
    ...order,
    price: Number(order.price ?? entryPrice),
    takeProfitPrice,
    stopLossPrice,
    tradePlan: {
      ...normalizedPlan,
      ...(order.tradePlan && typeof order.tradePlan === 'object' ? order.tradePlan : {}),
    },
  });

  return {
    ...input,
    entryPrice,
    takeProfitPrice,
    stopLossPrice,
    executionPurpose,
    tradeIntent,
    positionSide,
    tradePlan: normalizedPlan,
    orders: Array.isArray(input.orders) && input.orders.length ? input.orders.map(normalizeOrder) : [normalizeOrder({
      id: `ord-${Date.now()}`,
      side: String(input.side || 'buy').toLowerCase() === 'sell' ? 'sell' : 'buy',
      symbol: input.symbol,
      quantity: Number(input.quantity || 0),
      orderType: 'market',
      timeInForce: 'GTC',
    })],
  };
}

export async function persistRouteArtifacts(store, result = {}) {
  if (!result || result.errors?.length) return;
  
  if (result.budgetApproval && typeof store.upsertBudgetApproval === 'function') await store.upsertBudgetApproval(result.budgetApproval);
  if (result.job && typeof store.upsertResearchJob === 'function') await store.upsertResearchJob(result.job);
  if (result.ledger && typeof store.upsertAgentCost === 'function') await store.upsertAgentCost(result.ledger);
  if (result.opportunity && typeof store.upsertOpportunity === 'function') await store.upsertOpportunity(result.opportunity);
  if (result.riskBreakdown && typeof store.upsertRiskBreakdown === 'function') await store.upsertRiskBreakdown(result.riskBreakdown);
  if (Array.isArray(result.jobs) && typeof store.upsertResearchJob === 'function') for (const job of result.jobs) await store.upsertResearchJob(job);
  if (Array.isArray(result.ledgers) && typeof store.upsertAgentCost === 'function') for (const ledger of result.ledgers) await store.upsertAgentCost(ledger);
  if (Array.isArray(result.opportunities) && typeof store.upsertOpportunity === 'function') for (const opportunity of result.opportunities) await store.upsertOpportunity(opportunity);
  if (Array.isArray(result.riskBreakdowns) && typeof store.upsertRiskBreakdown === 'function') for (const riskBreakdown of result.riskBreakdowns) await store.upsertRiskBreakdown(riskBreakdown);

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

async function mutate(store, fn) {
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
  if (method === 'GET' && pathname === '/api/execution/strategy-signals') {
    const { runStrategySignalScanner } = await import('./opportunityGenerator.mjs');
    try {
      return { status: 200, body: { ok: true, ...runStrategySignalScanner() } };
    } catch (error) {
      return { status: 503, body: { ok: false, error: String(error) } };
    }
  }
  if (method === 'GET' && pathname === '/api/risk-breakdowns') return { status: 200, body: { riskBreakdowns: state.riskBreakdowns } };
  if (method === 'GET' && pathname === '/api/agents/jobs') return { status: 200, body: { jobs: state.researchJobs } };
  if (method === 'GET' && pathname === '/api/agents/budgets') return { status: 200, body: { budgets: state.agentBudgets } };
  if (method === 'GET' && pathname === '/api/agents/budget-approvals') return { status: 200, body: { budgetApprovals: state.budgetApprovals } };
  if (method === 'GET' && pathname === '/api/agents/costs') return { status: 200, body: { costs: state.agentCostLedger, summary: summarizeAgentCosts(state) } };
  if (method === 'GET' && pathname === '/api/market-data/snapshots') return { status: 200, body: { snapshots: state.marketDataSnapshots } };
  if (method === 'GET' && pathname === '/api/polymarket/opportunities') return { status: 200, body: { opportunities: state.opportunities.filter(o => o.venue?.includes('polymarket') || o.marketType === 'prediction_market') } };
  if (method === 'GET' && pathname === '/api/prediction-markets/scan') {
    try {
      const result = await fetchPredictionMarketSnapshots();
      return { status: 200, body: { ok: true, ...result } };
    } catch (error) {
      return { status: 503, body: { ok: false, error: String(error) } };
    }
  }

  if (method === 'POST' && pathname === '/api/connectors/market-data/ingest') {
    const result = await mutate(store, current => ingestConnectorSnapshots(current));
    return result;
  }

  if (method === 'POST' && pathname === '/api/opportunities/generate-from-connectors') {
    const result = await mutate(store, current => generateOpportunitiesFromConnectors(current));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  if (method === 'POST' && pathname === '/api/opportunities/generate-from-strategies') {
    const body = await readJsonBody();
    const result = await mutate(store, current => generateOpportunitiesFromStrategySignals(current, body));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  if (method === 'POST' && pathname === '/api/opportunities/generate-from-prediction-markets') {
    const body = await readJsonBody();
    const result = await mutate(store, current => generateOpportunitiesFromPredictionMarkets(current, body));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  if (method === 'POST' && pathname === '/api/opportunities/generate-from-arbitrage') {
    const body = await readJsonBody();
    const result = await mutate(store, current => generateOpportunitiesFromArbitrage(current, body));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  if (method === 'POST' && pathname === '/api/agents/budget-approvals') {
    const body = await readJsonBody();
    const result = await mutate(store, current => requestBudgetApproval(current, body));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  const budgetDecision = routeMatch(pathname, '/api/agents/budget-approvals/:id/decision');
  if (method === 'POST' && budgetDecision) {
    const body = await readJsonBody();
    return mutate(store, current => decideBudgetApproval(current, budgetDecision.id, body));
  }

  const opportunityDetail = routeMatch(pathname, '/api/opportunities/:id');
  if (method === 'GET' && opportunityDetail) {
    const opportunity = state.opportunities.find(o => o.id === opportunityDetail.id);
    if (!opportunity) return { status: 404, body: { ok: false, errors: ['opportunity_not_found'] } };
    return { status: 200, body: { opportunity, riskBreakdown: state.riskBreakdowns.find(r => r.id === opportunity.riskBreakdownId) || null } };
  }

  if (method === 'POST' && pathname === '/api/agents/jobs') {
    const body = await readJsonBody();
    const result = await mutate(store, current => createResearchJob(current, body));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  if (method === 'POST' && pathname === '/api/opportunities') {
    const body = await readJsonBody();
    const result = await mutate(store, current => createOpportunity(current, body));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  for (const decisionStatus of ['approve', 'reject', 'defer']) {
    const decision = routeMatch(pathname, `/api/opportunities/:id/${decisionStatus}`);
    if (method === 'POST' && decision) {
      const body = await readJsonBody();
      const status = decisionStatus === 'approve' ? 'approved' : decisionStatus === 'reject' ? 'rejected' : 'deferred';
      const result = await mutate(store, current => decideOpportunity(current, decision.id, { ...body, status }));
      // Register any newly created execution with the execution engine singleton
      if (status === 'approved' && result.body?.execution) {
        const exec = result.body.execution;
        const engine = handleOperatorRoute._execEngine;
        if (engine) {
          const enriched = {
            id: exec.id,
            strategyId: exec.strategyId || 'opportunity-driver',
            opportunityId: exec.opportunityId,
            accountId: exec.accountId || 'paper',
            mode: 'paper',
            status: 'draft',
            orders: exec.orders || [{
              id: `ord-${Date.now()}`,
              side: exec.side || 'buy',
              symbol: exec.symbol,
              quantity: exec.quantity || 0,
              price: exec.price || 0,
              orderType: 'market',
              timeInForce: 'GTC',
            }],
            fills: exec.fills || [],
            confidenceScore: exec.confidenceScore || 0.5,
            convictionWeight: exec.convictionWeight || 0.5,
            riskDecision: exec.riskDecision || { approved: true },
            startedAt: exec.startedAt || new Date().toISOString(),
            lastHeartbeatAt: exec.lastHeartbeatAt || new Date().toISOString(),
          };
          engine.executions.set(exec.id, enriched);
        }
      }
      return result;
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
    });
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

  // === Unified Execution Engine Routes (singleton engine) ===
  if (!handleOperatorRoute._execEngine) {
    handleOperatorRoute._execEngine = new (await import('../../../packages/execution/src/executionEngine.mjs')).default();
  }
  const execEngine = handleOperatorRoute._execEngine;

  if (method === 'POST' && pathname === '/api/execution/plan') {
    const body = await readJsonBody();
    const normalized = normalizeTradePlan(body);
    if (normalized.errors) return { status: 400, body: { ok: false, errors: normalized.errors } };
    return { status: 200, body: { ok: true, ...await execEngine.plan(normalized) } };
  }

  if (method === 'POST' && pathname === '/api/execution/execute') {
    const body = await readJsonBody();
    const normalized = normalizeTradePlan(body);
    if (normalized.errors) return { status: 400, body: { ok: false, errors: normalized.errors } };
    const result = await execEngine.execute(normalized);
    let preview = null;
    if (result.execution?.orders?.length) {
      const order = result.execution.orders[0];
      const venue = order.venue || 'coinbase';
      try {
        const { getDefaultRegistry } = await import('../../../packages/adapters/src/adapterRegistry.mjs');
        const registry = getDefaultRegistry();
        const adapter = registry.getAdapterForVenue(venue);
        if (adapter?.previewOrder) {
          preview = await adapter.previewOrder(order);
        }
      } catch { /* preview not available */ }
    }
    if (preview?.ok && result.execution) {
      result.execution.preview = preview.preview;
    }
    return mutate(store, async current => {
      if (result.execution) {
        current.executions = current.executions || [];
        const idx = current.executions.findIndex(e => e.id === result.execution.id);
        if (idx >= 0) current.executions[idx] = result.execution;
        else current.executions.push(result.execution);
      }
      if (result.ok && result.execution) {
        current.audit.push({ id: nextId('audit', current.audit), action: 'execution_submitted', actor: 'operator', at: new Date().toISOString(), details: result.execution.id, payload: { strategyId: result.execution.strategyId, status: result.execution.status, confidenceScore: result.execution.confidenceScore } });
      }
      return result;
    });
  }

  const executionApprove = routeMatch(pathname, '/api/execution/:id/approve');
  if (method === 'POST' && executionApprove) {
    const result = await execEngine.approve(executionApprove.id);
    return mutate(store, async current => {
      if (result.execution) {
        current.executions = current.executions || [];
        const idx = current.executions.findIndex(e => e.id === result.execution.id);
        if (idx >= 0) current.executions[idx] = result.execution;
      }
      if (result.ok) {
        current.audit.push({ id: nextId('audit', current.audit), action: 'execution_approved', actor: 'operator', at: new Date().toISOString(), details: result.execution.id });
      }
      return result;
    });
  }

  const executionReject = routeMatch(pathname, '/api/execution/:id/reject');
  if (method === 'POST' && executionReject) {
    const body = await readJsonBody();
    const result = await execEngine.reject(executionReject.id, body.reason);
    return mutate(store, async current => {
      if (result.execution) {
        current.executions = current.executions || [];
        const idx = current.executions.findIndex(e => e.id === result.execution.id);
        if (idx >= 0) current.executions[idx] = result.execution;
      }
      current.audit.push({ id: nextId('audit', current.audit), action: 'execution_rejected', actor: 'operator', at: new Date().toISOString(), details: result.execution?.id || executionReject.id, payload: { reason: body.reason } });
      return { execution: result.execution };
    });
  }

  const executionCancel = routeMatch(pathname, '/api/execution/:id/cancel');
  if (method === 'POST' && executionCancel) {
    const result = await execEngine.cancel(executionCancel.id);
    return mutate(store, async current => {
      if (result.execution) {
        current.executions = current.executions || [];
        const idx = current.executions.findIndex(e => e.id === result.execution.id);
        if (idx >= 0) current.executions[idx] = result.execution;
      }
      if (result.ok) {
        current.audit.push({ id: nextId('audit', current.audit), action: 'execution_cancelled', actor: 'operator', at: new Date().toISOString(), details: result.execution.id });
      }
      return result;
    });
  }

  if (method === 'GET' && pathname === '/api/executions') {
    const executions = execEngine.listExecutions();
    return { status: 200, body: { ok: true, executions } };
  }

  const executionDetail = routeMatch(pathname, '/api/executions/:id');
  if (method === 'GET' && executionDetail) {
    const exec = execEngine.getExecution(executionDetail.id);
    if (!exec) return { status: 404, body: { ok: false, errors: ['execution_not_found'] } };
    const events = execEngine.getEvents(executionDetail.id);
    return { status: 200, body: { ok: true, execution: exec, events } };
  }

  const executionEvents = routeMatch(pathname, '/api/executions/:id/events');
  if (method === 'GET' && executionEvents) {
    const events = execEngine.getEvents(executionEvents.id);
    return { status: 200, body: { ok: true, events } };
  }

  if (method === 'GET' && pathname === '/api/execution/events') {
    const events = execEngine.getAllEvents();
    return { status: 200, body: { ok: true, events } };
  }

  // === Config Routes ===
  if (method === 'GET' && pathname === '/api/config') {
    return { status: 200, body: { ok: true, config: state.config || {} } };
  }

  if (method === 'POST' && pathname === '/api/config') {
    const body = await readJsonBody();
    return mutate(store, async current => {
      current.config = { ...(current.config || {}), ...body, updatedAt: new Date().toISOString() };
      current.audit.push({ id: nextId('audit', current.audit), action: 'config_updated', actor: 'operator', at: new Date().toISOString(), details: Object.keys(body).join(', '), payload: { keys: Object.keys(body) } });
      return { config: current.config };
    });
  }

  if (method === 'GET' && pathname === '/api/execution/adapters') {
    const { getDefaultRegistry } = await import('../../../packages/adapters/src/adapterRegistry.mjs');
    const registry = getDefaultRegistry();
    return { status: 200, body: { ok: true, adapters: registry.listAdapters() } };
  }

  // === Live Quotes (fast poll — single CLI call) ===
  if (method === 'GET' && pathname === '/api/market-data/live-quotes') {
    try {
      const { fetchQuotes } = await import('../../../packages/execution/src/paperSweeper.mjs');
      const quotes = await fetchQuotes();
      const symbols = ['BTC-USD', 'ETH-USD', 'SOL-USD', 'XRP-USD', 'ADA-USD', 'DOGE-USD', 'LINK-USD', 'AVAX-USD', 'DOT-USD', 'MATIC-USD', 'UNI-USD', 'AAVE-USD'];
      const filtered = {};
      for (const s of symbols) {
        const q = quotes[s];
        if (q) filtered[s] = { bid: q.bid, ask: q.ask, mid: q.mid, spreadBps: q.spreadBps };
      }
      return { status: 200, body: { ok: true, quotes: filtered, ts: Date.now() } };
    } catch (e) {
      return { status: 500, body: { ok: false, error: String(e.message || e) } };
    }
  }

  // === Coinbase Sync Route ===
  if (method === 'POST' && pathname === '/api/coinbase/sync') {
    const { getDefaultRegistry } = await import('../../../packages/adapters/src/adapterRegistry.mjs');
    const registry = getDefaultRegistry();
    const coinbase = registry.getAdapterForVenue('coinbase');
    if (!coinbase || typeof coinbase.syncOperatorState !== 'function') {
      return { status: 400, body: { ok: false, errors: ['coinbase_adapter_not_available'] } };
    }

    return mutate(store, async current => {
      const coinbaseData = await coinbase.syncOperatorState();

      if (coinbaseData.accounts.length) {
        current.accounts = coinbaseData.accounts;
      }

      if (coinbaseData.positions.length) {
        const existingIds = new Set(current.positions.map(p => p.symbol));
        for (const pos of coinbaseData.positions) {
          const idx = current.positions.findIndex(p => p.symbol === pos.symbol);
          if (idx >= 0) current.positions[idx] = { ...current.positions[idx], ...pos };
          else if (!existingIds.has(pos.symbol)) current.positions.push(pos);
        }
      }

      if (coinbaseData.quotes.length) {
        const now = new Date().toISOString();
        for (const q of coinbaseData.quotes) {
          const idx = current.marketDataSnapshots.findIndex(m => m.symbol === q.symbol && m.venue === 'coinbase');
          const snapshot = {
            id: `md-coinbase-${q.symbol.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`,
            symbol: q.symbol,
            venue: 'coinbase',
            assetClass: 'crypto',
            bid: q.bid,
            ask: q.ask,
            spreadBps: q.spreadBps,
            volume24h: 0,
            liquidityScore: Math.min(90, Math.round(100 - q.spreadBps / 2)),
            volatilityScore: 50,
            status: 'connected',
            timestamp: now,
            source: 'coinbase-bridge'
          };
          if (idx >= 0) current.marketDataSnapshots[idx] = snapshot;
          else current.marketDataSnapshots.push(snapshot);
        }
      }

      current.audit.push({ id: nextId('audit', current.audit), action: 'coinbase_synced', actor: 'coinbase-bridge', at: new Date().toISOString(), details: `${coinbaseData.accounts.length} accounts, ${coinbaseData.positions.length} positions, ${coinbaseData.quotes.length} quotes`, payload: { accountCount: coinbaseData.accounts.length, positionCount: coinbaseData.positions.length, quoteCount: coinbaseData.quotes.length } });

      return { ...coinbaseData };
    });
  }

  // === Graph-Alpha-Bot Signal Routes ===
  if (method === 'GET' && pathname === '/api/execution/graph-signals') {
    const { fetchGraphSignals } = await import('./opportunityGenerator.mjs');
    const signals = await fetchGraphSignals();
    const engines = execEngine.listExecutions().filter(e => e.tags?.source?.startsWith('graph-alpha'));
    return { status: 200, body: { ok: true, signals, signalCount: signals.length, linkedExecutions: engines.length } };
  }

  if (method === 'POST' && pathname === '/api/execution/graph-signals/ingest') {
    const { generateOpportunitiesFromGraphSignals } = await import('./opportunityGenerator.mjs');
    return mutate(store, async current => {
      const result = await generateOpportunitiesFromGraphSignals(current);
      const count = result.opportunities.length;
      if (count > 0) {
        current.audit.push({ id: nextId('audit', current.audit), action: 'graph_signals_ingested', actor: 'graph-alpha-bot', at: new Date().toISOString(), details: `${count} opportunities created from ${result.signals.length} signals`, payload: { signalCount: result.signals.length, opportunityCount: count } });
      }
      return result;
    });
  }

  // === Reconciliation & Settlement Routes ===
  const executionReconcile = routeMatch(pathname, '/api/execution/:id/reconcile');
  if (method === 'POST' && executionReconcile) {
    const exec = execEngine.getExecution(executionReconcile.id);
    if (!exec) return { status: 404, body: { ok: false, errors: ['execution_not_found'] } };

    const reconciler = new ExecutionReconciler();
    const report = reconciler.reconcile(exec);
    const auditEvent = reconciler.toAuditEvent(exec.id, report);

    return mutate(store, async current => {
      current.audit.push({ id: nextId('audit', current.audit), ...auditEvent });
      return { ok: true, reconciliation: report };
    });
  }

  const executionSettle = routeMatch(pathname, '/api/execution/:id/settle/:fillId');
  if (method === 'POST' && executionSettle) {
    const exec = execEngine.getExecution(executionSettle.id);
    if (!exec) return { status: 404, body: { ok: false, errors: ['execution_not_found'] } };

    const fill = (exec.fills || []).find(f => f.id === executionSettle.fillId);
    if (!fill) return { status: 404, body: { ok: false, errors: ['fill_not_found'] } };

    const tracker = new SettlementTracker();
    tracker.trackFill(fill);
    const result = tracker.settleFill(fill.id);

    return mutate(store, async current => {
      if (result.ok) {
        fill.settlementStatus = 'settled';
        current.audit.push({ id: nextId('audit', current.audit), action: 'fill_settled', actor: 'operator', at: new Date().toISOString(), details: fill.id, payload: { executionId: exec.id, fillId: fill.id } });
      }
      return { ok: result.ok, fill, settlement: result };
    });
  }

  const executionRetrySettlement = routeMatch(pathname, '/api/execution/:id/retry-settlement');
  if (method === 'POST' && executionRetrySettlement) {
    const exec = execEngine.getExecution(executionRetrySettlement.id);
    if (!exec) return { status: 404, body: { ok: false, errors: ['execution_not_found'] } };

    const tracker = new SettlementTracker();
    for (const fill of exec.fills || []) tracker.trackFill(fill);

    const results = [];
    for (const fill of exec.fills || []) {
      if (fill.settlementStatus === 'pending' || fill.settlementStatus === 'failed') {
        const r = tracker.settleFill(fill.id);
        if (r.ok) fill.settlementStatus = 'settled';
        results.push({ fillId: fill.id, ...r });
      }
    }

    const settledCount = results.filter(r => r.ok).length;
    return mutate(store, async current => {
      if (settledCount > 0) {
        current.audit.push({ id: nextId('audit', current.audit), action: 'settlement_retry_completed', actor: 'operator', at: new Date().toISOString(), details: `${settledCount}/${results.length} fills settled`, payload: { executionId: exec.id, results } });
      }
      return { ok: true, settlementResults: results };
    });
  }

  function normalizeKalshiMarkets(markets) {
    if (!Array.isArray(markets)) return [];
    return markets.map(m => {
      const title = (() => {
        const rawTitle = m.title || eventTitle || m.ticker || '';
        const isMulti = m.market_type === 'multivariate' || rawTitle.includes(',');
        if (isMulti) {
          const legs = rawTitle.split(',').filter(Boolean).map(s => s.trim());
          const unique = [...new Set(legs.map(l => l.replace(/^(yes|no)\s+/i, '').trim()))];
          if (unique.length >= 3) {
            return `${unique[0]} +${unique.length - 1} more`;
          }
          return unique.join(', ');
        }
        return rawTitle;
      })();
      const yesBid = Number(parseFloat(m.yes_bid_dollars ?? m.yes_bid ?? 0));
      const yesAsk = Number(parseFloat(m.yes_ask_dollars ?? m.yes_ask ?? 0));
      const noBid = Number(parseFloat(m.no_bid_dollars ?? m.no_bid ?? 0));
      const noAsk = Number(parseFloat(m.no_ask_dollars ?? m.no_ask ?? 0));
      return {
        id: m.ticker || m.id,
        ticker: m.ticker,
        event_ticker: m.event_ticker,
        title,
        raw_title: m.title,
        status: m.status,
        close_date: m.close_time || m.close_date,
        yes_bid: yesBid,
        yes_ask: yesAsk,
        no_bid: noBid,
        no_ask: noAsk,
        volume: Number(m.volume_fp || m.volume || 0),
        tick_size: m.tick_size || 1,
        has_liquidity: yesBid > 0 || yesAsk > 0,
      };
    });
  }

  // === Kalshi Data Routes ===
  if (method === 'GET' && pathname === '/api/kalshi/markets') {
    const { KalshiClient } = await import('../../../packages/kalshi/src/client.ts').catch(() => ({ KalshiClient: null }));
    if (!KalshiClient) return { status: 503, body: { ok: false, error: 'kalshi_client_unavailable' } };
    const client = new KalshiClient(state.config?.kalshiEmail, state.config?.kalshiPassword);
    const marketResult = await client.listMarkets({ limit: 20 }).catch(() => null);
    const raw = Array.isArray(marketResult) ? marketResult : marketResult?.markets || [];
    return { status: 200, body: { ok: true, markets: normalizeKalshiMarkets(raw) } };
  }

  if (method === 'GET' && pathname === '/api/kalshi/balance') {
    const { KalshiClient } = await import('../../../packages/kalshi/src/client.ts').catch(() => ({ KalshiClient: null }));
    if (!KalshiClient) return { status: 503, body: { ok: false, error: 'kalshi_client_unavailable' } };
    const client = new KalshiClient(state.config?.kalshiEmail, state.config?.kalshiPassword);
    const balance = await client.getBalance().catch(() => null);
    return { status: 200, body: { ok: true, balance: balance || { error: 'not_available' } } };
  }

  // === Polymarket Data Routes ===
  if (method === 'GET' && pathname === '/api/polymarket/markets') {
    const { PolymarketClient } = await import('../../../packages/polymarket/src/client.ts').catch(() => ({ PolymarketClient: null }));
    if (!PolymarketClient) return { status: 503, body: { ok: false, error: 'polymarket_client_unavailable' } };
    const client = new PolymarketClient();
    const markets = await client.listMarkets({ limit: 20 }).catch(() => null);
    return { status: 200, body: { ok: true, markets: Array.isArray(markets) ? markets : [] } };
  }

  const polymarketOrderbookMatch = routeMatch(pathname, '/api/polymarket/orderbook/:marketId');
  if (method === 'GET' && polymarketOrderbookMatch) {
    const { PolymarketClient } = await import('../../../packages/polymarket/src/client.ts').catch(() => ({ PolymarketClient: null }));
    if (!PolymarketClient) return { status: 503, body: { ok: false, error: 'polymarket_client_unavailable' } };
    const client = new PolymarketClient();
    const orderbook = await client.getOrderBook(polymarketOrderbookMatch.marketId).catch(() => null);
    return { status: 200, body: { ok: true, orderbook: orderbook || {} } };
  }

  if (method === 'GET' && pathname === '/api/polymarket/balance') {
    const { PolymarketClient } = await import('../../../packages/polymarket/src/client.ts').catch(() => ({ PolymarketClient: null }));
    if (!PolymarketClient) return { status: 503, body: { ok: false, error: 'polymarket_client_unavailable' } };
    const client = new PolymarketClient();
    const balance = await client.getBalance().catch(() => null);
    return { status: 200, body: { ok: true, balance: balance || { error: 'not_available' } } };
  }

  // === Arbitrage Scanner ===
  async function runArbitrageScan(scanConfig) {
    const { scanForArbitrage } = await import('../../../packages/arbitrage/src/arbitrageScanner.mjs');
    return scanForArbitrage(scanConfig);
  }

  if (method === 'POST' && pathname === '/api/arbitrage/scan') {
    const opportunities = await runArbitrageScan({
      kalshiEmail: state.config?.kalshiEmail,
      kalshiPassword: state.config?.kalshiPassword,
      matchOptions: state.config?.arbitrageMatchOptions || {},
    });
    handleOperatorRoute._arbitrageCache = { ts: new Date().toISOString(), opportunities };
    if (opportunities.length) {
      state.audit.push({
        id: nextId('audit', state.audit),
        action: 'arbitrage_scan_complete',
        actor: 'arbitrage-scanner',
        at: new Date().toISOString(),
        details: `Found ${opportunities.length} arbitrage opportunity(ies)`,
        payload: { count: opportunities.length, topEdgeBps: opportunities[0]?.edgeBps },
      });
    }
    return { status: 200, body: { ok: true, count: opportunities.length, opportunities } };
  }

  if (method === 'GET' && pathname === '/api/arbitrage/opportunities') {
    const cache = handleOperatorRoute._arbitrageCache;
    if (cache) return { status: 200, body: { ok: true, opportunities: cache.opportunities, lastScanAt: cache.ts, cached: true } };
    const opportunities = await runArbitrageScan({
      kalshiEmail: state.config?.kalshiEmail,
      kalshiPassword: state.config?.kalshiPassword,
    });
    handleOperatorRoute._arbitrageCache = { ts: new Date().toISOString(), opportunities };
    return { status: 200, body: { ok: true, opportunities, lastScanAt: null, cached: false } };
  }

  if (method === 'POST' && pathname === '/api/arbitrage/opportunities/persist') {
    const cache = handleOperatorRoute._arbitrageCache;
    if (!cache || !cache.opportunities.length) return { status: 400, body: { ok: false, errors: ['no_arbitrage_opportunities_to_persist'] } };
    const results = [];
    for (const arb of cache.opportunities) {
      const body = {
        sourceAgentId: 'arbitrage-scanner',
        marketType: 'prediction_market',
        venue: `${arb.kalshiMarket.venue}+${arb.polymarketMarket.venue}`,
        title: arb.title,
        symbol: arb.kalshiMarket.id,
        marketSlug: arb.polymarketMarket.conditionId || arb.polymarketMarket.id,
        tradeIntent: 'arbitrage',
        executionPurpose: 'two_leg_cross_venue',
        positionSide: 'flat',
        tradePlan: {
          plan_type: 'arbitrage',
          execution_purpose: 'two_leg_cross_venue',
          expectedProfitUsd: arb.expectedProfitUsd,
          edgeBps: arb.edgeBps,
          payoutPerShare: arb.bestStrategy.payout,
          costPerShare: arb.bestStrategy.totalCost,
          legs: arb.bestStrategy.legs,
          venuePair: [arb.kalshiMarket.venue, arb.polymarketMarket.venue],
        },
        recommendation: 'review',
        confidenceScore: arb.confidenceScore,
        expectedValue: arb.profitPerShare * arb.size,
        grossExpectedValue: arb.returnPct / 100 * arb.size,
        totalMoneyRisked: arb.totalCostPerShare * arb.size,
        maxLoss: arb.totalCostPerShare * arb.size,
        potentialUpside: arb.profitPerShare * arb.size,
        liquidityScore: arb.liquidityScore,
        estimatedFees: arb.size * 0.002,
        estimatedSlippage: arb.size * 0.001,
        notes: `Arbitrage: ${arb.bestStrategy.label} (${arb.edgeBps} bps edge)`,
        evidence: [{
          type: 'arbitrage_scan',
          kalshiMarket: { id: arb.kalshiMarket.id, title: arb.kalshiMarket.title },
          polymarketMarket: { id: arb.polymarketMarket.conditionId || arb.polymarketMarket.id, title: arb.polymarketMarket.question || arb.polymarketMarket.title },
          bestStrategy: { ...arb.bestStrategy },
          similarity: arb.similarity,
          expectedProfitUsd: arb.expectedProfitUsd,
        }],
        status: 'needs_review',
      };
      const result = await mutate(store, current => createOpportunity(current, body));
      if (result.body?.opportunity) results.push(result.body.opportunity);
    }
    return { status: 200, body: { ok: true, count: results.length, opportunities: results } };
  }

  // === Paper Trading Sweeper ===
  if (method === 'POST' && pathname === '/api/paper/sweep') {
    try {
      const { runSweep } = await import('../../../packages/execution/src/paperSweeper.mjs');
      const body = await readJsonBody();
      const positions = state.positions?.filter(p => p.status === 'open') || [];
      const result = await runSweep({
        maxMarkets: body.maxMarkets || 100,
        strategies: body.strategies,
        positions,
        fetchOrderBooks: body.fetchOrderBooks || false,
      });
      if (state.accounts) {
        const account = state.accounts.find(a => a.id === 'acct-paper-primary');
        if (account && result.signals?.length) {
          const { executePaperSignal } = await import('../../../packages/execution/src/paperEngine.mjs');
          for (const s of result.signals.slice(0, 10)) {
            let exec = state.paperExecutions?.find(e => e.status === 'running' && e.strategyId === s.strategy);
            if (!exec) continue;
            const signal = { id: `sig-${Date.now()}`, strategyId: s.strategy, symbol: s.productId, side: s.action, quantity: s.quantity, price: s.price, confidence: s.confidence, feeBps: 15, slippageBps: 10, createdAt: s.timestamp };
            const quote = { price: s.price, bid: s.price * 0.999, ask: s.price * 1.001 };
            const paperResult = executePaperSignal(state, exec.id, signal, quote);
            if (paperResult.ok) {
              state.audit.push({ id: nextId('audit', state.audit), action: 'paper_signal_executed', actor: 'paper-sweeper', at: new Date().toISOString(), details: `${s.strategy} ${s.action} ${s.productId} @ ${s.price}`, payload: { strategy: s.strategy, productId: s.productId, action: s.action, price: s.price, confidence: s.confidence, reason: s.reason } });
            }
          }
        }
      }
      return { status: 200, body: { ok: true, ...result } };
    } catch (e) {
      return { status: 500, body: { ok: false, error: String(e.message || e) } };
    }
  }

  if (method === 'GET' && pathname === '/api/paper/sweep/history') {
    const { sweepHistory, computeSweepPnL } = await import('../../../packages/execution/src/paperSweeper.mjs');
    const pnl = computeSweepPnL();
    return { status: 200, body: { ok: true, history: sweepHistory.slice(-50), pnl } };
  }

  // === Aggregated Activity Feed ===
  if (method === 'GET' && pathname === '/api/activity-feed') {
    const execEngine = handleOperatorRoute._execEngine;
    const execEvents = execEngine ? execEngine.getAllEvents() : [];
    const audit = state.audit || [];
    const opps = state.opportunities || [];
    const recentOpps = opps
      .filter(o => o.updatedAt || o.createdAt)
      .map(o => ({
        id: `opp-${o.id}`,
        type: 'opportunity',
        action: o.approvalStatus === 'approved' ? 'opportunity_approved' : o.approvalStatus === 'rejected' ? 'opportunity_rejected' : 'opportunity_created',
        timestamp: o.updatedAt || o.createdAt,
        actor: o.sourceAgentId || 'system',
        details: `${o.title || o.symbol}: ${o.approvalStatus || 'pending'}`
      }));
    const feed = [];
    for (const entry of (audit || []).slice(-50)) {
      feed.push({ id: entry.id, type: 'audit', action: entry.action, timestamp: entry.at, actor: entry.actor, details: entry.details, payload: entry.payload });
    }
    for (const event of execEvents.slice(-50)) {
      feed.push({ id: event.id, type: 'execution', action: event.type || event.action || 'event', timestamp: event.timestamp, actor: event.actor || 'engine', details: event.executionId || event.details || '', payload: event });
    }
    feed.push(...recentOpps.slice(-20));
    feed.sort((a, b) => new Date(b.timestamp || 0).getTime() - new Date(a.timestamp || 0).getTime());
    return { status: 200, body: { ok: true, feed: feed.slice(0, 100) } };
  }

  return null;
}
