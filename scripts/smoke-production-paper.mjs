const baseUrl = process.env.SMOKE_BASE_URL || process.env.BASE_URL || 'http://localhost:3000';
const adminToken = process.env.OPERATOR_ADMIN_TOKEN || process.env.OPERATOR_AUTH_TOKEN;
const csrfToken = process.env.OPERATOR_CSRF_TOKEN;

function headers(extra = {}) {
  return {
    'content-type': 'application/json',
    ...(adminToken ? { authorization: `Bearer ${adminToken}` } : {}),
    ...(csrfToken ? { 'x-csrf-token': csrfToken } : {}),
    ...extra
  };
}

async function request(path, options = {}) {
  const response = await fetch(`${baseUrl}${path}`, { ...options, headers: headers(options.headers || {}) });
  const text = await response.text();
  let body = text;
  try { body = JSON.parse(text); } catch {}
  return { path, status: response.status, ok: response.ok, body };
}

async function main() {
  const checks = [];
  checks.push(await request('/health'));
  checks.push(await request('/ready'));
  checks.push(await request('/ready/production-paper'));
  checks.push(await request('/api/operator/summary'));
  checks.push(await request('/metrics'));
  checks.push(await request('/metrics.prom'));
  checks.push(await request('/api/audit/verify'));

  const allowReady503 = process.env.SMOKE_ALLOW_READY_503 !== 'false';
  const failures = checks.filter(check => {
    if (allowReady503 && check.path === '/ready' && check.status === 503) return false;
    if (allowReady503 && check.path === '/ready/production-paper' && check.status === 503) return false;
    return check.status >= 400;
  });

  const result = { ok: failures.length === 0, baseUrl, checks, failures };
  console.log(JSON.stringify(result, null, 2));
  if (!result.ok) process.exit(1);
}

main().catch(error => {
  console.error(JSON.stringify({ ok: false, error: error.message }, null, 2));
  process.exit(1);
});
