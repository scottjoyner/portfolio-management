import http from 'node:http';
import { readFileSync, existsSync } from 'node:fs';
import { join, extname } from 'node:path';
import { validateModeConfig } from '../../../packages/config/src/mode.mjs';

const JSON_TYPE = 'application/json; charset=utf-8';
const UI_ROOT = new URL('../../web/src/', import.meta.url);

export function createInitialState() {
  return {
    strategies: [
      {
        id: 'strategy-ema-cross-v1',
        name: 'EMA Crossover',
        version: 1,
        status: 'draft',
        riskLevel: 'medium',
        parameters: { fastPeriod: 9, slowPeriod: 21, symbol: 'BTC-USD', timeframe: '1h' },
        createdAt: '2026-05-29T00:00:00.000Z',
        updatedAt: '2026-05-29T00:00:00.000Z'
      },
      {
        id: 'strategy-zscore-v1',
        name: 'Z-Score Mean Reversion',
        version: 1,
        status: 'draft',
        riskLevel: 'low',
        parameters: { lookback: 20, entryZ: -2, exitZ: 0, symbol: 'ETH-USD', timeframe: '1h' },
        createdAt: '2026-05-29T00:00:00.000Z',
        updatedAt: '2026-05-29T00:00:00.000Z'
      }
    ],
    backtests: [
      {
        id: 'bt-demo-001',
        strategyId: 'strategy-ema-cross-v1',
        status: 'completed',
        startedAt: '2026-05-29T00:00:00.000Z',
        completedAt: '2026-05-29T00:00:01.000Z',
        assumptions: { initialCapitalUsd: 100000, feeBps: 5, slippageBps: 10, dataSource: 'demo-fixture' },
        metrics: { totalReturnPct: 3.42, maxDrawdownPct: 1.18, sharpe: 1.12, totalTrades: 14, winRatePct: 57.14 },
        equityCurve: [100000, 100800, 100150, 101250, 103420],
        trades: [
          { timestamp: '2026-01-02T10:00:00.000Z', symbol: 'BTC-USD', side: 'buy', quantity: 0.1, price: 45000 },
          { timestamp: '2026-01-03T15:00:00.000Z', symbol: 'BTC-USD', side: 'sell', quantity: 0.1, price: 46539 }
        ]
      }
    ],
    approvals: [
      {
        id: 'approval-demo-001',
        strategyId: 'strategy-ema-cross-v1',
        status: 'pending_review',
        tier: 'canary',
        reason: 'Backtest evidence required before paper incubation.',
        createdAt: '2026-05-29T00:00:00.000Z'
      }
    ],
    positions: [],
    audit: [
      { id: 'audit-001', action: 'system_bootstrap', actor: 'system', at: '2026-05-29T00:00:00.000Z', details: 'Mock/paper operator surface initialized.' }
    ],
    killSwitch: { enabled: false, reason: null, updatedAt: null }
  };
}

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

function summarizeReadiness(modeConfig, state) {
  const blockers = [];
  if (!modeConfig.ok) blockers.push(...modeConfig.reasons);
  if (modeConfig.mode === 'live') blockers.push('live_mode_not_certified');
  if (state.killSwitch.enabled) blockers.push('kill_switch_enabled');
  blockers.push('ui_api_contract_only');
  blockers.push('database_persistence_not_enabled');
  blockers.push('real_execution_disabled');
  return {
    ok: blockers.length === 0,
    mode: modeConfig.mode,
    productionReady: false,
    liveTradingCertified: false,
    blockers
  };
}

function nextId(prefix, collection) {
  return `${prefix}-${String(collection.length + 1).padStart(3, '0')}`;
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
    metrics: {
      totalReturnPct,
      maxDrawdownPct: Number((Math.max(0.75, Math.abs(totalReturnPct) / 3)).toFixed(2)),
      sharpe: Number((0.8 + Math.max(0, totalReturnPct) / 10).toFixed(2)),
      totalTrades: strategy.riskLevel === 'low' ? 6 : 14,
      winRatePct: strategy.riskLevel === 'low' ? 61.11 : 57.14
    },
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

export async function handleRequest(req, options = {}) {
  const state = options.state || createInitialState();
  const env = defaultEnv(options.env || {});
  const modeConfig = validateModeConfig(env);
  const url = new URL(req.url || '/', 'http://localhost');
  const method = req.method || 'GET';

  if (method === 'GET' && (url.pathname === '/' || url.pathname.startsWith('/ui'))) {
    return serveUi(url.pathname);
  }

  if (method === 'GET' && url.pathname === '/health') {
    return json(200, { ok: true, service: 'portfolio-management-api', mode: modeConfig.mode, liveTradingCertified: false });
  }

  if (method === 'GET' && url.pathname === '/ready') {
    const readiness = summarizeReadiness(modeConfig, state);
    return json(readiness.ok ? 200 : 503, readiness);
  }

  if (method === 'GET' && url.pathname === '/metrics') {
    return json(200, {
      strategies_total: state.strategies.length,
      backtests_total: state.backtests.length,
      approvals_pending: state.approvals.filter(a => a.status === 'pending_review').length,
      positions_open: state.positions.length,
      kill_switch_enabled: state.killSwitch.enabled ? 1 : 0,
      live_trading_certified: 0
    });
  }

  if (method === 'GET' && url.pathname === '/api/operator/summary') {
    return json(200, {
      readiness: summarizeReadiness(modeConfig, state),
      counts: {
        strategies: state.strategies.length,
        backtests: state.backtests.length,
        approvals: state.approvals.length,
        positions: state.positions.length,
        auditEvents: state.audit.length
      },
      killSwitch: state.killSwitch
    });
  }

  if (method === 'GET' && url.pathname === '/api/strategies') return json(200, { strategies: state.strategies });

  if (method === 'POST' && url.pathname === '/api/strategies') {
    const body = await readJsonBody(req);
    const errors = validateStrategyInput(body);
    if (errors.length) return json(400, { ok: false, errors });
    const now = new Date().toISOString();
    const strategy = {
      id: nextId('strategy', state.strategies),
      name: body.name,
      version: 1,
      status: 'draft',
      riskLevel: body.riskLevel || 'medium',
      parameters: body.parameters,
      createdAt: now,
      updatedAt: now
    };
    state.strategies.push(strategy);
    state.audit.push({ id: nextId('audit', state.audit), action: 'strategy_created', actor: 'operator', at: now, details: strategy.id });
    return json(201, { ok: true, strategy });
  }

  if (method === 'GET' && url.pathname === '/api/backtests') return json(200, { backtests: state.backtests });

  if (method === 'POST' && url.pathname === '/api/backtests') {
    const body = await readJsonBody(req);
    const strategy = state.strategies.find(s => s.id === body.strategyId);
    if (!strategy) return json(404, { ok: false, errors: ['strategy_not_found'] });
    const now = new Date().toISOString();
    const result = runDeterministicBacktest(strategy, body);
    const backtest = { id: nextId('bt', state.backtests), strategyId: strategy.id, status: 'completed', startedAt: now, completedAt: now, ...result };
    state.backtests.push(backtest);
    state.audit.push({ id: nextId('audit', state.audit), action: 'backtest_completed', actor: 'operator', at: now, details: backtest.id });
    return json(201, { ok: true, backtest });
  }

  if (method === 'GET' && url.pathname === '/api/approvals') return json(200, { approvals: state.approvals });

  if (method === 'POST' && url.pathname === '/api/approvals') {
    const body = await readJsonBody(req);
    const strategy = state.strategies.find(s => s.id === body.strategyId);
    if (!strategy) return json(404, { ok: false, errors: ['strategy_not_found'] });
    const hasBacktest = state.backtests.some(b => b.strategyId === strategy.id && b.status === 'completed');
    const now = new Date().toISOString();
    const approval = {
      id: nextId('approval', state.approvals),
      strategyId: strategy.id,
      status: hasBacktest ? 'pending_review' : 'blocked',
      tier: body.tier || 'canary',
      reason: hasBacktest ? 'Ready for human review.' : 'Completed backtest evidence is required.',
      createdAt: now
    };
    state.approvals.push(approval);
    state.audit.push({ id: nextId('audit', state.audit), action: 'approval_requested', actor: 'operator', at: now, details: approval.id });
    return json(201, { ok: true, approval });
  }

  if (method === 'GET' && url.pathname === '/api/positions') return json(200, { positions: state.positions });
  if (method === 'GET' && url.pathname === '/api/audit') return json(200, { audit: state.audit });

  if (method === 'POST' && url.pathname === '/api/kill-switch') {
    const body = await readJsonBody(req);
    const now = new Date().toISOString();
    state.killSwitch = { enabled: body.enabled !== false, reason: body.reason || 'operator_request', updatedAt: now };
    state.audit.push({ id: nextId('audit', state.audit), action: state.killSwitch.enabled ? 'kill_switch_enabled' : 'kill_switch_disabled', actor: 'operator', at: now, details: state.killSwitch.reason });
    return json(200, { ok: true, killSwitch: state.killSwitch });
  }

  if (url.pathname.startsWith('/api/execution/live')) {
    return json(403, { ok: false, error: 'live_execution_disabled', reason: 'Live trading is not certified in this implementation slice.' });
  }

  return json(404, { ok: false, error: 'route_not_found', route: url.pathname });
}

export function startServer(port = Number(process.env.PORT || 3000), options = {}) {
  const state = options.state || createInitialState();
  const s = http.createServer(async (req, res) => {
    try {
      const out = await handleRequest(req, { ...options, state });
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

if (process.argv[1] === new URL(import.meta.url).pathname) {
  startServer();
}
