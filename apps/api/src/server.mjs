import http from 'node:http';
import { readFileSync, existsSync } from 'node:fs';
import { join, extname } from 'node:path';
import { validateModeConfig } from '../../../packages/config/src/mode.mjs';
import { createInitialOperatorState, nextId } from '../../../packages/storage/src/operatorStore.mjs';
import { createOperatorStore } from '../../../packages/storage/src/operatorStoreFactory.mjs';

const JSON_TYPE = 'application/json; charset=utf-8';
const UI_ROOT = new URL('../../web/src/', import.meta.url);

export const createInitialState = createInitialOperatorState;

function defaultEnv(overrides = {}) {
  return {
    MODE: process.env.MODE || 'mock',
    LIVE_TRADING: process.env.LIVE_TRADING || 'false',
    PAPER_TRADING: process.env.PAPER_TRADING || 'true',
    REQUIRE_RUNTIME_CONFIRMATION: process.env.REQUIRE_RUNTIME_CONFIRMATION || 'true',
    REQUIRE_MANUAL_APPROVAL: process.env.REQUIRE_MANUAL_APPROVAL || 'true',
    REQUIRE_APPROVED_MARKET_PAIR: process.env.REQUIRE_APPROVED_MARKET_PAIR || 'true',
    ALLOW_LIVE_SETTLEMENT_REDEMPTION: process.env.ALLOW_LIVE_SETTLEMENT_REDEMPTION || 'false',
    ALLOW_POLYMARKET_ORDER_SUBMISSION: process.env.ALLOW_POLYMARKET_ORDER_SUBMISSION || 'false',
    ...overrides
  };
}

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

function summarizeReadiness(modeConfig, state, store) {
  const storage = storeStatus(store);
  const blockers = [];
  if (!modeConfig.ok) blockers.push(...modeConfig.reasons);
  if (modeConfig.mode === 'live') blockers.push('live_mode_not_certified');
  if (state.killSwitch.enabled) blockers.push('kill_switch_enabled');
  blockers.push('ui_api_contract_only');
  if (!storage.durable) blockers.push('database_persistence_not_enabled');
  else if (!storage.sql) blockers.push('sql_database_migrations_pending');
  else if (!storage.migrations?.ok) blockers.push('sql_database_migrations_not_ready');
  blockers.push('real_execution_disabled');
  return { ok: false, mode: modeConfig.mode, productionReady: false, liveTradingCertified: false, storage, blockers };
}

function validateStrategyInput(input) {
  const errors = [];
  if (!input || typeof input !== 'object') errors.push('body_required');
  if (!input.name || typeof input.name !== 'string') errors.push('name_required');
  if (!input.parameters || typeof input.parameters !== 'object') errors.push('parameters_required');
  if (input.riskLevel && !['low', 'medium', 'high'].includes(input.riskLevel)) errors.push('invalid_risk_level');
  return errors;
}

function runDeterministicBacktest(strategy, body = {}) {
  const initialCapitalUsd = Number(body.initialCapitalUsd || 100000);
  const feeBps = Number(body.feeBps || 5);
  const slippageBps = Number(body.slippageBps || 10);
  const grossReturnPct = strategy.riskLevel === 'low' ? 1.4 : strategy.riskLevel === 'high' ? 5.8 : 3.2;
  const costDragPct = (feeBps + slippageBps) / 100;
  const totalReturnPct = Number((grossReturnPct - costDragPct).toFixed(2));
  const finalEquity = Math.round(initialCapitalUsd * (1 + totalReturnPct / 100));
  return {
    assumptions: { initialCapitalUsd, feeBps, slippageBps, dataSource: body.dataSource || 'demo-fixture' },
    metrics: { totalReturnPct, maxDrawdownPct: Number((Math.max(0.75, Math.abs(totalReturnPct) / 3)).toFixed(2)), sharpe: Number((0.8 + Math.max(0, totalReturnPct) / 10).toFixed(2)), totalTrades: strategy.riskLevel === 'low' ? 6 : 14, winRatePct: strategy.riskLevel === 'low' ? 61.11 : 57.14 },
    equityCurve: [initialCapitalUsd, Math.round(initialCapitalUsd * 1.005), Math.round(initialCapitalUsd * 0.998), Math.round(initialCapitalUsd * 1.017), finalEquity],
    trades: [
      { timestamp: '2026-01-02T10:00:00.000Z', symbol: strategy.parameters.symbol || 'DEMO', side: 'buy', quantity: 1, price: 100 },
      { timestamp: '2026-01-03T15:00:00.000Z', symbol: strategy.parameters.symbol || 'DEMO', side: 'sell', quantity: 1, price: Number((100 * (1 + totalReturnPct / 100)).toFixed(2)) }
    ]
  };
}

function serveUi(pathname) {
  const target = pathname === '/' ? 'index.html' : pathname.replace(/^\/ui\/?/, '') || 'index.html';
  const safeTarget = target.includes('..') ? 'index.html' : target;
  const fullPath = join(UI_ROOT.pathname, safeTarget);
  if (!existsSync(fullPath)) return text(404, 'not found');
  const type = extname(fullPath) === '.css' ? 'text/css; charset=utf-8' : extname(fullPath) === '.js' ? 'application/javascript; charset=utf-8' : 'text/html; charset=utf-8';
  return text(200, readFileSync(fullPath, 'utf8'), type);
}

async function loadStateOrHealthError(store) {
  try {
    return { state: await store.load(), error: null };
  } catch (error) {
    const fallback = createInitialOperatorState();
    return { state: fallback, error };
  }
}

export async function handleRequest(req, options = {}) {
  const store = createOperatorStore(options);
  const env = defaultEnv(options.env || {});
  const modeConfig = validateModeConfig(env);
  const url = new URL(req.url || '/', 'http://localhost');
  const method = req.method || 'GET';

  if (method === 'GET' && (url.pathname === '/' || url.pathname.startsWith('/ui'))) return serveUi(url.pathname);

  const { state, error: loadError } = await loadStateOrHealthError(store);

  if (method === 'GET' && url.pathname === '/health') {
    return json(loadError ? 503 : 200, { ok: !loadError, service: 'portfolio-management-api', mode: modeConfig.mode, liveTradingCertified: false, storage: storeStatus(store), error: loadError?.message });
  }

  if (method === 'GET' && url.pathname === '/ready') return json(503, summarizeReadiness(modeConfig, state, store));

  if (loadError) return json(503, { ok: false, error: 'operator_store_unavailable', reason: loadError.message, storage: storeStatus(store) });

  if (method === 'GET' && url.pathname === '/metrics') {
    return json(200, { strategies_total: state.strategies.length, backtests_total: state.backtests.length, approvals_pending: state.approvals.filter(a => a.status === 'pending_review').length, positions_open: state.positions.length, kill_switch_enabled: state.killSwitch.enabled ? 1 : 0, durable_storage_enabled: storeStatus(store).durable ? 1 : 0, sql_storage_enabled: storeStatus(store).sql ? 1 : 0, live_trading_certified: 0 });
  }

  if (method === 'GET' && url.pathname === '/api/operator/summary') {
    return json(200, { readiness: summarizeReadiness(modeConfig, state, store), counts: { strategies: state.strategies.length, backtests: state.backtests.length, approvals: state.approvals.length, positions: state.positions.length, auditEvents: state.audit.length }, killSwitch: state.killSwitch, storage: storeStatus(store) });
  }

  if (method === 'GET' && url.pathname === '/api/strategies') return json(200, { strategies: state.strategies });

  if (method === 'POST' && url.pathname === '/api/strategies') {
    const body = await readJsonBody(req);
    const errors = validateStrategyInput(body);
    if (errors.length) return json(400, { ok: false, errors });
    const strategy = await store.mutate(async current => {
      const now = new Date().toISOString();
      const next = { id: nextId('strategy', current.strategies), name: body.name, version: 1, status: 'draft', riskLevel: body.riskLevel || 'medium', parameters: body.parameters, createdAt: now, updatedAt: now };
      current.strategies.push(next);
      current.audit.push({ id: nextId('audit', current.audit), action: 'strategy_created', actor: 'operator', at: now, details: next.id });
      return next;
    });
    return json(201, { ok: true, strategy });
  }

  if (method === 'GET' && url.pathname === '/api/backtests') return json(200, { backtests: state.backtests });

  if (method === 'POST' && url.pathname === '/api/backtests') {
    const body = await readJsonBody(req);
    const created = await store.mutate(async current => {
      const strategy = current.strategies.find(s => s.id === body.strategyId);
      if (!strategy) return null;
      const now = new Date().toISOString();
      const result = runDeterministicBacktest(strategy, body);
      const backtest = { id: nextId('bt', current.backtests), strategyId: strategy.id, status: 'completed', startedAt: now, completedAt: now, ...result };
      current.backtests.push(backtest);
      current.audit.push({ id: nextId('audit', current.audit), action: 'backtest_completed', actor: 'operator', at: now, details: backtest.id });
      return backtest;
    });
    if (!created) return json(404, { ok: false, errors: ['strategy_not_found'] });
    return json(201, { ok: true, backtest: created });
  }

  if (method === 'GET' && url.pathname === '/api/approvals') return json(200, { approvals: state.approvals });

  if (method === 'POST' && url.pathname === '/api/approvals') {
    const body = await readJsonBody(req);
    const approval = await store.mutate(async current => {
      const strategy = current.strategies.find(s => s.id === body.strategyId);
      if (!strategy) return null;
      const hasBacktest = current.backtests.some(b => b.strategyId === strategy.id && b.status === 'completed');
      const now = new Date().toISOString();
      const next = { id: nextId('approval', current.approvals), strategyId: strategy.id, status: hasBacktest ? 'pending_review' : 'blocked', tier: body.tier || 'canary', reason: hasBacktest ? 'Ready for human review.' : 'Completed backtest evidence is required.', createdAt: now };
      current.approvals.push(next);
      current.audit.push({ id: nextId('audit', current.audit), action: 'approval_requested', actor: 'operator', at: now, details: next.id });
      return next;
    });
    if (!approval) return json(404, { ok: false, errors: ['strategy_not_found'] });
    return json(201, { ok: true, approval });
  }

  if (method === 'GET' && url.pathname === '/api/positions') return json(200, { positions: state.positions });
  if (method === 'GET' && url.pathname === '/api/audit') return json(200, { audit: state.audit });

  if (method === 'POST' && url.pathname === '/api/kill-switch') {
    const body = await readJsonBody(req);
    const killSwitch = await store.mutate(async current => {
      const now = new Date().toISOString();
      current.killSwitch = { enabled: body.enabled !== false, reason: body.reason || 'operator_request', updatedAt: now };
      current.audit.push({ id: nextId('audit', current.audit), action: current.killSwitch.enabled ? 'kill_switch_enabled' : 'kill_switch_disabled', actor: 'operator', at: now, details: current.killSwitch.reason });
      return current.killSwitch;
    });
    return json(200, { ok: true, killSwitch });
  }

  if (url.pathname.startsWith('/api/execution/live')) return json(403, { ok: false, error: 'live_execution_disabled', reason: 'Live trading is not certified in this implementation slice.' });
  return json(404, { ok: false, error: 'route_not_found', route: url.pathname });
}

export function startServer(port = Number(process.env.PORT || 3000), options = {}) {
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
  return s;
}

if (process.argv[1] === new URL(import.meta.url).pathname) startServer();
