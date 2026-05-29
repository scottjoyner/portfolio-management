import http from 'node:http';
import { handleRequest as handleBaseRequest } from './server.mjs';
import { createInitialOperatorState } from '../../../packages/storage/src/operatorStore.mjs';
import { createOperatorStore } from '../../../packages/storage/src/operatorStoreFactory.mjs';
import { handleOperatorRoute } from './operatorRouter.mjs';

const JSON_TYPE = 'application/json; charset=utf-8';

function json(status, body, headers = {}) {
  return { status, headers: { 'content-type': JSON_TYPE, ...headers }, body: JSON.stringify(body, null, 2) };
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
  return pathname === '/api/accounts'
    || pathname === '/api/instruments'
    || pathname === '/api/strategy-templates'
    || pathname === '/api/paper-executions'
    || pathname === '/api/strategies/from-template'
    || pathname === '/api/backtests/run'
    || pathname === '/api/approvals/request'
    || pathname === '/api/kill-switch/stop-paper'
    || /^\/api\/strategies\/[^/]+\/(clone|status)$/.test(pathname)
    || /^\/api\/backtests\/[^/]+\/report$/.test(pathname)
    || /^\/api\/approvals\/[^/]+\/decision$/.test(pathname)
    || /^\/api\/paper-executions\/[^/]+\/stop$/.test(pathname);
}

function makeSummary(state, store) {
  return {
    counts: {
      accounts: state.accounts.length,
      instruments: state.instruments.length,
      templates: state.strategyTemplates.length,
      strategies: state.strategies.length,
      backtests: state.backtests.length,
      approvals: state.approvals.length,
      paperExecutions: state.paperExecutions.length,
      positions: state.positions.length,
      auditEvents: state.audit.length
    },
    killSwitch: state.killSwitch,
    storage: storeStatus(store),
    p0p1: {
      operatorProductLayer: true,
      strategyVersioning: true,
      approvalDecisions: true,
      backtestReports: true,
      paperExecution: true,
      liveTradingCertified: false
    }
  };
}

export async function handleRequest(req, options = {}) {
  const store = createOperatorStore(options);
  const url = new URL(req.url || '/', 'http://localhost');
  const method = req.method || 'GET';

  if (url.pathname === '/api/operator/summary' || isP1Route(url.pathname)) {
    const { state, error } = await loadState(store);
    if (error) return json(503, { ok: false, error: 'operator_store_unavailable', reason: error.message, storage: storeStatus(store) });

    if (method === 'GET' && url.pathname === '/api/operator/summary') {
      const base = await handleBaseRequest(req, { ...options, store });
      const body = JSON.parse(base.body);
      return json(200, { ...body, ...makeSummary(state, store) });
    }

    const route = await handleOperatorRoute({ method, pathname: url.pathname, state, store, readJsonBody: () => readJsonBody(req) });
    if (route) return json(route.status, route.body);
  }

  return handleBaseRequest(req, { ...options, store });
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
