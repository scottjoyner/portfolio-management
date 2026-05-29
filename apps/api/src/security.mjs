const SAFE_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);
const MUTATING_METHODS = new Set(['POST', 'PUT', 'PATCH', 'DELETE']);

export function parseAllowlist(value = '') {
  return String(value).split(',').map(item => item.trim()).filter(Boolean);
}

export function securityHeaders(env = process.env) {
  return {
    'x-content-type-options': 'nosniff',
    'x-frame-options': 'DENY',
    'referrer-policy': 'no-referrer',
    'permissions-policy': 'geolocation=(), microphone=(), camera=()',
    'content-security-policy': env.CONTENT_SECURITY_POLICY || "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'"
  };
}

export function corsHeaders(req, env = process.env) {
  const origin = req.headers?.origin;
  const allowlist = parseAllowlist(env.CORS_ORIGINS || 'http://localhost:3000,http://127.0.0.1:3000');
  if (!origin || !allowlist.includes(origin)) return {};
  return {
    'access-control-allow-origin': origin,
    'access-control-allow-methods': 'GET,POST,OPTIONS',
    'access-control-allow-headers': 'content-type,authorization,x-request-id,x-csrf-token',
    'access-control-max-age': '600',
    vary: 'origin'
  };
}

export function csrfStatus(req, env = process.env) {
  const method = req.method || 'GET';
  if (SAFE_METHODS.has(method)) return { ok: true };
  if (!MUTATING_METHODS.has(method)) return { ok: false, status: 405, error: 'method_not_allowed' };
  const required = env.CSRF_REQUIRED === 'true' || env.MODE === 'live';
  if (!required) return { ok: true };
  const expected = env.OPERATOR_CSRF_TOKEN;
  if (!expected) return { ok: false, status: 503, error: 'csrf_not_configured' };
  const actual = req.headers?.['x-csrf-token'] || req.headers?.['X-CSRF-Token'];
  if (actual !== expected) return { ok: false, status: 403, error: 'csrf_required' };
  return { ok: true };
}

export function preflightResponse(req, env = process.env) {
  return {
    status: 204,
    headers: { ...securityHeaders(env), ...corsHeaders(req, env) },
    body: ''
  };
}

export function securityResponse(status, error, requestId, req, env = process.env) {
  return {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', 'x-request-id': requestId, ...securityHeaders(env), ...corsHeaders(req, env) },
    body: JSON.stringify({ ok: false, error, requestId }, null, 2)
  };
}

export function withSecurityHeaders(out, req, env = process.env) {
  return {
    ...out,
    headers: { ...securityHeaders(env), ...corsHeaders(req, env), ...(out.headers || {}) }
  };
}
