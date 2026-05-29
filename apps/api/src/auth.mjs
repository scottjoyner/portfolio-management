export function requestId(headers = {}) {
  return headers['x-request-id'] || headers['X-Request-Id'] || `req-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export function authStatus(req, env = process.env) {
  const configuredToken = env.OPERATOR_AUTH_TOKEN;
  const mode = env.MODE || 'mock';
  const authRequired = env.OPERATOR_AUTH_REQUIRED === 'true' || mode === 'live';
  if (!authRequired) return { ok: true, actor: 'dev-operator', mode: 'dev-bypass' };
  const header = req.headers?.authorization || req.headers?.Authorization || '';
  const token = header.startsWith('Bearer ') ? header.slice('Bearer '.length) : '';
  if (!configuredToken) return { ok: false, status: 503, error: 'operator_auth_not_configured' };
  if (token !== configuredToken) return { ok: false, status: 401, error: 'operator_auth_required' };
  return { ok: true, actor: 'operator', mode: 'bearer' };
}

export function authResponse(status, error, id) {
  return {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', 'x-request-id': id },
    body: JSON.stringify({ ok: false, error, requestId: id }, null, 2)
  };
}
