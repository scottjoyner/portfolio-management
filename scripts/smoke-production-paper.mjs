#!/usr/bin/env node
const baseUrl = String(process.env.PORTFOLIO_BASE_URL || 'http://127.0.0.1:3000').replace(/\/+$/, '');
const token = process.env.OPERATOR_ADMIN_TOKEN || process.env.OPERATOR_AUTH_TOKEN || '';
const timeoutMs = Number(process.env.SMOKE_TIMEOUT_MS || 10000);
const requireReady = process.env.SMOKE_REQUIRE_PRODUCTION_PAPER_READY !== 'false';

const probes = [
  { path: '/health', expected: [200] },
  { path: '/ready', expected: [503] },
  { path: '/ready/production-paper', expected: requireReady ? [200] : [200, 503] },
  { path: '/api/operator/summary', expected: [200], auth: true },
  { path: '/api/economics/dashboard', expected: [200], auth: true },
  { path: '/api/economics/intelligence/nodes', expected: [200], auth: true },
  { path: '/metrics', expected: [200], auth: true },
  { path: '/metrics.prom', expected: [200] },
  { path: '/api/audit/verify', expected: [200], auth: true },
];

async function probe(row) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const headers = { accept: row.path === '/metrics.prom' ? 'text/plain' : 'application/json' };
    if (row.auth && token) headers.authorization = `Bearer ${token}`;
    const response = await fetch(`${baseUrl}${row.path}`, { headers, signal: controller.signal });
    const body = await response.text();
    return {
      path: row.path,
      ok: row.expected.includes(response.status),
      status: response.status,
      expected: row.expected,
      contentType: response.headers.get('content-type'),
      preview: body.slice(0, 240),
    };
  } catch (error) {
    return { path: row.path, ok: false, status: null, expected: row.expected, error: error?.name === 'AbortError' ? 'timeout' : String(error?.message || error) };
  } finally {
    clearTimeout(timer);
  }
}

if (!token) {
  process.stderr.write(`${JSON.stringify({ ok: false, error: 'OPERATOR_ADMIN_TOKEN_or_OPERATOR_AUTH_TOKEN_required_for_smoke' }, null, 2)}\n`);
  process.exit(1);
}

const results = [];
for (const row of probes) results.push(await probe(row));
const failures = results.filter(row => !row.ok);
const report = { ok: failures.length === 0, baseUrl, requireReady, results, failures };
process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
if (!report.ok) process.exitCode = 1;
