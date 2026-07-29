import http from 'node:http';
import { handleRequest as handleBaseRequest } from './server.mjs';
import { createInitialOperatorState } from '../../../packages/storage/src/operatorStore.mjs';
import { createOperatorStore } from '../../../packages/storage/src/operatorStoreFactory.mjs';
import { handleOperatorRoute } from './operatorRouter.mjs';
import { handleEconomicRoute, isEconomicRoute } from './economicRouter.mjs';
import { authResponse, authStatus, authorizeRoute, requestId } from './auth.mjs';
import { csrfStatus, preflightResponse, securityResponse, withSecurityHeaders } from './security.mjs';
import { assertRuntimeEnv, validateRuntimeEnv } from '../../../packages/config/src/runtimeEnv.mjs';
import { logRequest, recordResponse, renderPrometheusMetrics } from './metrics.mjs';
import { verifyAuditIntegrity } from '../../../packages/audit/src/integrity.mjs';
import { buildSystemTruth } from './systemTruth.mjs';
import { buildCompetitionSnapshot } from './competitionSnapshot.mjs';

const JSON_TYPE = 'application/json; charset=utf-8';

export const createInitialState = createInitialOperatorState;

function json(status, body, headers = {}) {
  return { status, headers: { 'content-type': JSON_TYPE, ...headers }, body: JSON.stringify(body, null, 2) };
}

function text(status, body, contentType = 'text/plain; charset=utf-8') {
  return { status, headers: { 'content-type': contentType }, body };
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', chunk => {
      data += chunk;
      if (data.length > 1_000_000) {
        reject(new Error('request_body_too_large'));
        req.destroy();
      }
    });
    req.on('end', () => {
      if (!data) return resolve({});
      try { resolve(JSON.parse(data)); } catch { reject(new Error('invalid_json')); }
    });
    req.on('error', reject);
  });
}

function storeStatus(store) {
  return typeof store.getStatus === 'function' ? store.getStatus() : { kind: 'unknown', durable: false };
}

async function loadState(store) {
  try {
    return { state: await store.load(), error: null };
  } catch (error) {
    return { state: createInitialOperatorState(), error };
  }
}

function isP1Route(pathname) {
  return isEconomicRoute(pathname)
    || pathname === '/api/system-truth'
    || pathname === '/api/competition'
    || pathname === '/api/positions'
    || pathname === '/api/accounts'
    || pathname === '/api/instruments'
    || pathname === '/api/strategy-templates'
    || pathname === '/api/paper-executions'
    || pathname === '/api/strategies/from-template'
    || pathname === '/api/backtests/run'
    || pathname === '/api/approvals/request'
    || pathname === '/api/kill-switch/stop-paper'
    || pathname === '/api/audit/verify'
    || pathname === '/api/opportunity-dashboard'
    || pathname === '/api/opportunities'
    || pathname === '/api/risk-breakdowns'
    || pathname === '/api/agents/jobs'
    || pathname === '/api/agents/budgets'
    || pathname === '/api/agents/budget-approvals'
    || pathname === '/api/agents/costs'
    || pathname === '/api/market-data/snapshots'
    || pathname === '/api/connectors/market-data/ingest'
    || pathname === '/api/opportunities/generate-from-connectors'
    || pathname === '/api/opportunities/generate-from-strategies'
    || pathname === '/api/opportunities/generate-from-prediction-markets'
    || pathname === '/api/opportunities/generate-from-arbitrage'
    || pathname === '/api/polymarket/opportunities'
    || /^\/api\/agents\/budget-approvals\/[^/]+\/decision$/.test(pathname)
    || /^\/api\/opportunities\/[^/]+$/.test(pathname)
    || /^\/api\/opportunities\/[^/]+\/(approve|reject|defer|request-research)$/.test(pathname)
    || /^\/api\/strategies\/[^/]+\/(clone|status)$/.test(pathname)
    || /^\/api\/backtests\/[^/]+\/report$/.test(pathname)
    || /^\/api\/approvals\/[^/]+\/decision$/.test(pathname)
    || /^\/api\/paper-executions\/[^/]+\/(stop|signal)$/.test(pathname)
    || pathname === '/api/execution/plan'
    || pathname === '/api/execution/execute'
    || pathname === '/api/executions'
    || pathname === '/api/execution/adapters'
    || pathname === '/api/execution/events'
    || /^\/api\/executions\/[^/]+$/.test(pathname)
    || /^\/api\/executions\/[^/]+\/events$/.test(pathname)
    || /^\/api\/execution\/[^/]+\/(approve|reject|cancel)$/.test(pathname)
    || pathname === '/api/config'
    || pathname === '/api/coinbase/sync'
    || pathname === '/api/execution/graph-signals'
    || pathname === '/api/execution/strategy-signals'
    || /^\/api\/execution\/[^/]+\/(reconcile|settle|retry-settlement)$/.test(pathname)
    || /^\/api\/execution\/graph-signals\/ingest$/.test(pathname)
    || pathname === '/api/kalshi/markets'
    || pathname === '/api/kalshi/balance'
    || pathname === '/api/prediction-markets/scan'
    || pathname === '/api/polymarket/markets'
    || pathname === '/api/polymarket/balance'
    || /^\/api\/polymarket\/orderbook\/[^/]+$/.test(pathname)
    || pathname === '/api/activity-feed'
    || pathname === '/api/arbitrage/scan'
    || pathname === '/api/arbitrage/opportunities'
    || pathname === '/api/arbitrage/opportunities/persist'
    || pathname === '/api/paper/sweep'
    || pathname === '/api/paper/sweep/history'
    || pathname === '/api/market-data/live-quotes'
    || pathname === '/api/secrets'
    || pathname === '/api/secrets/auto-rotate/config'
    || pathname === '/api/secrets/auto-rotate/run'
    || /^\/api\/secrets\/rotate\/[^/]+$/.test(pathname);
}

function makeSummary(state, store, runtime) {
  const execs = state.executions || [];
  const execFilled = execs.filter(e => e.status === 'filled').length;
  const execPending = execs.filter(e => e.status === 'draft' || e.status === 'submitted').length;
  const execFailed = execs.filter(e => e.status === 'failed').length;
  const execFills = execs.flatMap(e => e.fills || []);
  const execSettled = execFills.filter(f => f.settlementStatus === 'settled').length;
  const execPendingSettlement = execFills.filter(f => f.settlementStatus === 'pending' || !f.settlementStatus).length;

  return {
    counts: {
      accounts: state.accounts.length,
      instruments: state.instruments.length,
      templates: state.strategyTemplates.length,
      strategies: state.strategies.length,
      backtests: state.backtests.length,
      approvals: state.approvals.length,
      opportunities: state.opportunities?.length || 0,
      researchJobs: state.researchJobs?.length || 0,
      budgetApprovals: state.budgetApprovals?.length || 0,
      modelPricingSnapshots: state.modelPricingSnapshots?.length || 0,
      modelUsageQuotes: state.modelUsageLedger?.length || 0,
      priceForecasts: state.priceForecasts?.length || 0,
      economicDecisions: state.economicDecisions?.length || 0,
      agentAttributionRecords: state.agentAttributionRecords?.length || 0,
      paperExecutions: state.paperExecutions.length,
      executions: execs.length,
      executions_filled: execFilled,
      executions_pending: execPending,
      executions_failed: execFailed,
      settlement_fills: execFills.length,
      settlement_settled: execSettled,
      settlement_pending: execPendingSettlement,
      positions: state.positions.length,
      auditEvents: state.audit.length
    },
    killSwitch: state.killSwitch,
    storage: storeStatus(store),
    runtime,
    p0p1: {
      operatorProductLayer: true,
      strategyVersioning: true,
      approvalDecisions: true,
      backtestReports: true,
      paperExecution: true,
      opportunityReview: true,
      agentCostLedger: true,
      budgetApprovals: true,
      competitionConsole: true,
      economicDecisionEngine: true,
      liveTradingCertified: false
    }
  };
}

function productionPaperReadiness({ runtime, storage, audit }) {
  const blockers = [];
  if (!runtime.ok) blockers.push(...runtime.errors);
  if (storage.kind !== 'postgres-p1' && storage.kind !== 'postgres') blockers.push('postgres_storage_required');
  if (!storage.durable || !storage.sql) blockers.push('sql_durable_storage_required');
  if (!storage.migrations?.ok) blockers.push('postgres_migrations_not_ready');
  if (audit && !audit.ok) blockers.push(`audit_integrity_${audit.reason}`);
  if (runtime.safeSummary?.LIVE_TRADING === 'true') blockers.push('live_trading_must_be_false');
  return { ok: blockers.length === 0, productionPaperReady: blockers.length === 0, liveTradingCertified: false, blockers, storage, runtime, audit };
}

function paidAgentOpportunity(state, opportunity) {
  if (!opportunity) return false;
  const job = state.researchJobs?.find(row => row.id === opportunity.researchJobId);
  return job?.localOrRemote === 'remote'
    || Number(opportunity.modelInferenceCost || 0) > 0
    || Boolean(opportunity.modelQuoteId)
    || Boolean(opportunity.economicDecisionId && state.modelUsageLedger?.some(row => row.decisionId === opportunity.economicDecisionId));
}

function validateExecutableDecision(state, decisionId, now = new Date()) {
  const decision = state.economicDecisions?.find(row => row.id === decisionId);
  if (!decision) return { ok: false, errors: ['economic_decision_required'] };
  const errors = [];
  if (decision.executionAllowed !== true) errors.push('economic_decision_blocks_execution');
  const forecast = state.priceForecasts?.find(row => row.id === decision.forecastId);
  const executionCost = state.executionCostSnapshots?.find(row => row.id === decision.executionCostSnapshotId);
  const quote = decision.modelQuoteId ? state.modelUsageLedger?.find(row => row.id === decision.modelQuoteId) : null;
  if (!forecast || forecast.status !== 'valid' || new Date(forecast.expiresAt || 0) < now) errors.push('economic_forecast_stale');
  if (!executionCost || new Date(executionCost.validUntil || 0) < now) errors.push('economic_execution_cost_stale');
  if (quote) {
    if (quote.status !== 'reconciled') errors.push('model_usage_not_reconciled');
    if (quote.reconciledAt && new Date(decision.createdAt || 0) < new Date(quote.reconciledAt)) errors.push('economic_decision_requires_post_reconciliation_refresh');
  }
  return { ok: errors.length === 0, errors, decision, forecast, executionCost, quote };
}

function validateIntelligencePurchase(state, body, now = new Date()) {
  const quote = state.modelUsageLedger?.find(row => row.id === body.modelQuoteId);
  const decision = state.economicDecisions?.find(row => row.id === body.economicDecisionId);
  const errors = [];
  if (!quote) errors.push('model_quote_required');
  if (!decision) errors.push('economic_decision_required');
  if (decision && decision.intelligenceAllowed !== true) errors.push('intelligence_purchase_not_economic');
  if (decision && quote && decision.modelQuoteId !== quote.id) errors.push('economic_decision_model_quote_mismatch');
  const maxAgeSeconds = Number(state.config?.maximumModelPricingAgeSeconds || 86400);
  if (quote && new Date(quote.requestedAt || 0) < new Date(now.getTime() - maxAgeSeconds * 1000)) errors.push('model_quote_stale');
  if (decision) {
    const forecast = state.priceForecasts?.find(row => row.id === decision.forecastId);
    const executionCost = state.executionCostSnapshots?.find(row => row.id === decision.executionCostSnapshotId);
    if (!forecast || new Date(forecast.expiresAt || 0) < now) errors.push('economic_forecast_stale');
    if (!executionCost || new Date(executionCost.validUntil || 0) < now) errors.push('economic_execution_cost_stale');
  }
  return { ok: errors.length === 0, errors, quote, decision };
}

async function economicMutationGate({ method, pathname, state, readBody, now = new Date() }) {
  if (method !== 'POST' || state.config?.requireEconomicDecisionForRemoteAgent === false) return null;

  const researchRequest = pathname === '/api/agents/jobs' || /^\/api\/opportunities\/[^/]+\/request-research$/.test(pathname);
  if (researchRequest) {
    const body = await readBody();
    const isRemote = (body.localOrRemote || 'remote') === 'remote';
    if (isRemote) {
      const gate = validateIntelligencePurchase(state, body, now);
      if (!gate.ok) return { status: 409, body: { ok: false, error: 'remote_intelligence_purchase_blocked', errors: gate.errors } };
      body.remoteApiCost = gate.quote.estimatedCostUsd;
      body.provider = gate.quote.provider;
      body.model = gate.quote.model;
      body.pricingSnapshotId = gate.quote.pricingSnapshotId;
      body.modelQuoteId = gate.quote.id;
      body.economicDecisionId = gate.decision.id;
    }
  }

  const opportunityApproval = pathname.match(/^\/api\/opportunities\/([^/]+)\/approve$/);
  if (opportunityApproval) {
    const opportunity = state.opportunities?.find(row => row.id === decodeURIComponent(opportunityApproval[1]));
    if (paidAgentOpportunity(state, opportunity)) {
      const gate = validateExecutableDecision(state, opportunity?.economicDecisionId, now);
      if (!gate.ok) return { status: 409, body: { ok: false, error: 'paid_agent_execution_blocked', errors: gate.errors, opportunityId: opportunity?.id || null } };
    }
  }

  if (pathname === '/api/execution/execute') {
    const body = await readBody();
    const opportunity = body.opportunityId ? state.opportunities?.find(row => row.id === body.opportunityId) : null;
    const paid = paidAgentOpportunity(state, opportunity)
      || Boolean(body.modelQuoteId)
      || String(body.tags?.competitor || body.sourceAgentId || '').toLowerCase() === 'agent';
    if (paid) {
      const decisionId = body.economicDecisionId || opportunity?.economicDecisionId;
      const gate = validateExecutableDecision(state, decisionId, now);
      if (!gate.ok) return { status: 409, body: { ok: false, error: 'paid_agent_execution_blocked', errors: gate.errors, opportunityId: opportunity?.id || null } };
      body.economicDecisionId = gate.decision.id;
      body.modelQuoteId = gate.decision.modelQuoteId;
      body.forecastId = gate.decision.forecastId;
      body.executionCostSnapshotId = gate.decision.executionCostSnapshotId;
      body.netExecutableEdgeUsd = gate.decision.netExecutableEdgeUsd;
      body.providerPreferences = gate.decision.providerPreferences;
    }
  }
  return null;
}

async function dispatchRequest(req, options = {}) {
  const env = { ...process.env, ...(options.env || {}) };
  const runtime = validateRuntimeEnv(env);
  const id = requestId(req.headers || {});
  const url = new URL(req.url || '/', 'http://localhost');

  if ((req.method || 'GET') === 'OPTIONS') return preflightResponse(req, env);
  if ((req.method || 'GET') === 'GET' && url.pathname === '/metrics.prom') return withSecurityHeaders(text(200, renderPrometheusMetrics(), 'text/plain; version=0.0.4; charset=utf-8'), req, env);

  const csrf = csrfStatus(req, env);
  if (!csrf.ok) return securityResponse(csrf.status, csrf.error, id, req, env);

  const auth = authStatus(req, env);
  if (!auth.ok && !['/health', '/ready', '/ready/production-paper'].includes(url.pathname)) {
    return withSecurityHeaders(authResponse(auth.status, auth.error, id), req, env);
  }

  const authorization = authorizeRoute(auth, req, url.pathname);
  if (!authorization.ok && !['/health', '/ready', '/ready/production-paper'].includes(url.pathname)) {
    return withSecurityHeaders(authResponse(authorization.status, authorization.error, id), req, env);
  }

  const store = createOperatorStore(options);
  const method = req.method || 'GET';
  let bodyPromise = null;
  const readBody = () => {
    bodyPromise ||= readJsonBody(req);
    return bodyPromise;
  };

  if (method === 'GET' && url.pathname === '/ready/production-paper') {
    const { state, error } = await loadState(store);
    const audit = error ? { ok: false, reason: 'operator_store_unavailable' } : verifyAuditIntegrity(state.audit || []);
    const readiness = productionPaperReadiness({ runtime, storage: storeStatus(store), audit });
    return withSecurityHeaders(json(readiness.ok ? 200 : 503, { ...readiness, requestId: id }), req, env);
  }

  if (url.pathname === '/api/operator/summary' || isP1Route(url.pathname)) {
    const { state, error } = await loadState(store);
    if (error) return withSecurityHeaders(json(503, { ok: false, error: 'operator_store_unavailable', reason: error.message, storage: storeStatus(store), requestId: id }), req, env);

    if (method === 'GET' && url.pathname === '/api/operator/summary') {
      const base = await handleBaseRequest(req, { ...options, store });
      const body = JSON.parse(base.body);
      return withSecurityHeaders(json(200, { ...body, ...makeSummary(state, store, runtime), requestId: id, actor: auth.actor, role: auth.role }), req, env);
    }

    if (method === 'GET' && url.pathname === '/api/system-truth') {
      const truth = buildSystemTruth({ env, dataDir: options.dataDir, now: options.now });
      return withSecurityHeaders(json(200, truth), req, env);
    }

    if (method === 'GET' && url.pathname === '/api/competition') {
      const competition = buildCompetitionSnapshot({ state, dataDir: options.dataDir, now: options.now });
      return withSecurityHeaders(json(200, competition), req, env);
    }

    if (method === 'GET' && url.pathname === '/api/positions') {
      return withSecurityHeaders(json(200, {
        ok: true,
        positions: Array.isArray(state.positions) ? state.positions : [],
        accounts: Array.isArray(state.accounts) ? state.accounts : [],
        capitalInPlayUsd: state.capitalInPlayUsd ?? null,
        source: 'operator_store',
        requestId: id,
        actor: auth.actor,
        role: auth.role,
      }), req, env);
    }

    if (method === 'GET' && url.pathname === '/api/audit/verify') {
      const audit = verifyAuditIntegrity(state.audit || []);
      return withSecurityHeaders(json(audit.ok ? 200 : 409, { ...audit, requestId: id, actor: auth.actor, role: auth.role }), req, env);
    }

    const mutationGate = await economicMutationGate({ method, pathname: url.pathname, state, readBody, now: options.now ? new Date(options.now) : new Date() });
    if (mutationGate) return withSecurityHeaders(json(mutationGate.status, { ...mutationGate.body, requestId: id, actor: auth.actor, role: auth.role }), req, env);

    const economicRoute = await handleEconomicRoute({
      method,
      pathname: url.pathname,
      state,
      store,
      readJsonBody: readBody,
      env,
      fetchImpl: options.fetchImpl || globalThis.fetch,
    });
    if (economicRoute) return withSecurityHeaders(json(economicRoute.status, { ...economicRoute.body, requestId: id, actor: auth.actor, role: auth.role }), req, env);

    const route = await handleOperatorRoute({ method, pathname: url.pathname, state, store, readJsonBody: readBody });
    if (route) return withSecurityHeaders(json(route.status, { ...route.body, requestId: id, actor: auth.actor, role: auth.role }), req, env);
  }

  const out = await handleBaseRequest(req, { ...options, store });
  return withSecurityHeaders({ ...out, headers: { ...out.headers, 'x-request-id': id } }, req, env);
}

export async function handleRequest(req, options = {}) {
  const started = Date.now();
  const id = requestId(req.headers || {});
  const url = new URL(req.url || '/', 'http://localhost');
  const out = await dispatchRequest(req, options);
  recordResponse(out.status);
  logRequest({ requestId: id, method: req.method || 'GET', path: url.pathname, status: out.status, durationMs: Date.now() - started });
  return out;
}

export async function startServer(port = Number(process.env.PORT || 3000), options = {}) {
  assertRuntimeEnv({ ...process.env, ...(options.env || {}) });
  const store = createOperatorStore(options);
  const s = http.createServer(async (req, res) => {
    try {
      const out = await handleRequest(req, { ...options, store });
      res.writeHead(out.status, out.headers);
      res.end(out.body);
    } catch (error) {
      res.writeHead(500, { 'content-type': JSON_TYPE });
      res.end(JSON.stringify({ ok: false, error: error.message || 'internal_error' }, null, 2));
    }
  });
  s.listen(port);
  const { startAutoRotate } = await import('./secrets.mjs');
  startAutoRotate({
    getConfig: () => store.state?.config || {},
    mutate: fn => store.mutate(fn),
    intervalMs: Number(process.env.SECRET_AUTO_ROTATE_MS || 86_400_000),
  });
  return s;
}

if (process.argv[1] === new URL(import.meta.url).pathname) startServer();
