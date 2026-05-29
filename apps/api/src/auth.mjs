const READ_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);

export function requestId(headers = {}) {
  return headers['x-request-id'] || headers['X-Request-Id'] || `req-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function tokenMap(env = process.env) {
  const entries = [];
  if (env.OPERATOR_AUTH_TOKEN) entries.push([env.OPERATOR_AUTH_TOKEN, { actor: 'operator', role: 'admin' }]);
  if (env.OPERATOR_ADMIN_TOKEN) entries.push([env.OPERATOR_ADMIN_TOKEN, { actor: 'admin-operator', role: 'admin' }]);
  if (env.OPERATOR_PAPER_TOKEN) entries.push([env.OPERATOR_PAPER_TOKEN, { actor: 'paper-operator', role: 'paper' }]);
  if (env.OPERATOR_READONLY_TOKEN) entries.push([env.OPERATOR_READONLY_TOKEN, { actor: 'readonly-operator', role: 'readonly' }]);
  return new Map(entries);
}

export function authStatus(req, env = process.env) {
  const mode = env.MODE || 'mock';
  const authRequired = env.OPERATOR_AUTH_REQUIRED === 'true' || mode === 'live';
  if (!authRequired) return { ok: true, actor: 'dev-operator', role: 'admin', mode: 'dev-bypass' };
  const tokens = tokenMap(env);
  const header = req.headers?.authorization || req.headers?.Authorization || '';
  const token = header.startsWith('Bearer ') ? header.slice('Bearer '.length) : '';
  if (!tokens.size) return { ok: false, status: 503, error: 'operator_auth_not_configured' };
  const match = tokens.get(token);
  if (!match) return { ok: false, status: 401, error: 'operator_auth_required' };
  return { ok: true, ...match, mode: 'bearer' };
}

export function authorizeRoute(auth, req, pathname) {
  if (!auth.ok) return auth;
  const method = req.method || 'GET';
  if (auth.role === 'admin') return { ok: true };
  if (auth.role === 'readonly') {
    return READ_METHODS.has(method) ? { ok: true } : { ok: false, status: 403, error: 'operator_role_forbidden' };
  }
  if (auth.role === 'paper') {
    const allowed = READ_METHODS.has(method)
      || pathname === '/api/backtests/run'
      || pathname === '/api/approvals/request'
      || pathname === '/api/paper-executions'
      || /^\/api\/paper-executions\/[^/]+\/(stop|signal)$/.test(pathname)
      || pathname === '/api/kill-switch/stop-paper';
    return allowed ? { ok: true } : { ok: false, status: 403, error: 'operator_role_forbidden' };
  }
  return { ok: false, status: 403, error: 'operator_role_forbidden' };
}

export function authResponse(status, error, id) {
  return {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', 'x-request-id': id },
    body: JSON.stringify({ ok: false, error, requestId: id }, null, 2)
  };
}
