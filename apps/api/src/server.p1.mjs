import http from 'node:http';
import { Readable } from 'node:stream';

import {
  createInitialState,
  handleRequest as legacyHandleRequest,
} from './server.p1Legacy.mjs';
import { assertRuntimeEnv } from '../../../packages/config/src/runtimeEnv.mjs';
import { createOperatorStore } from '../../../packages/storage/src/operatorStoreFactory.mjs';

const JSON_TYPE = 'application/json; charset=utf-8';

export { createInitialState };

function requestNow(options = {}) {
  const value = options.now instanceof Date ? options.now : options.now ? new Date(options.now) : new Date();
  return Number.isNaN(value.getTime()) ? new Date().toISOString() : value.toISOString();
}

function researchMutation(pathname, method) {
  return method === 'POST'
    && (pathname === '/api/agents/jobs' || /^\/api\/opportunities\/[^/]+\/request-research$/.test(pathname));
}

async function readBody(req) {
  let data = '';
  for await (const chunk of req) {
    data += chunk;
    if (data.length > 1_000_000) throw new Error('request_body_too_large');
  }
  if (!data.trim()) return {};
  try { return JSON.parse(data); } catch { throw new Error('invalid_json'); }
}

function replacementRequest(req, body, url = req.url) {
  const stream = new Readable({ read() {} });
  stream.method = req.method;
  stream.url = url;
  stream.headers = { ...(req.headers || {}), 'content-type': 'application/json' };
  stream.push(JSON.stringify(body));
  stream.push(null);
  return stream;
}

export async function handleRequest(req, options = {}) {
  const url = new URL(req.url || '/', 'http://localhost');
  const method = req.method || 'GET';

  if (method === 'GET' && url.pathname === '/metrics') {
    const alias = Object.create(req);
    alias.url = '/metrics.prom';
    return legacyHandleRequest(alias, options);
  }

  if (researchMutation(url.pathname, method)) {
    const body = await readBody(req);
    body.localOrRemote ||= 'local';
    body.status ||= 'queued';
    body.requestedAt ||= requestNow(options);
    return legacyHandleRequest(replacementRequest(req, body), options);
  }

  return legacyHandleRequest(req, options);
}

export async function startServer(port = Number(process.env.PORT || 3000), options = {}) {
  assertRuntimeEnv({ ...process.env, ...(options.env || {}) });
  const store = createOperatorStore(options);
  const server = http.createServer(async (req, res) => {
    try {
      const out = await handleRequest(req, { ...options, store });
      res.writeHead(out.status, out.headers);
      res.end(out.body);
    } catch (error) {
      res.writeHead(500, { 'content-type': JSON_TYPE });
      res.end(JSON.stringify({ ok: false, error: error.message || 'internal_error' }, null, 2));
    }
  });
  server.listen(port);
  const { startAutoRotate } = await import('./secrets.mjs');
  startAutoRotate({
    getConfig: () => store.state?.config || {},
    mutate: fn => store.mutate(fn),
    intervalMs: Number(process.env.SECRET_AUTO_ROTATE_MS || 86_400_000),
  });
  return server;
}

if (process.argv[1] === new URL(import.meta.url).pathname) startServer();
