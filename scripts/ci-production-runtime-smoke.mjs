#!/usr/bin/env node
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { writeFileSync } from 'node:fs';

const databaseUrl = process.env.DATABASE_URL;
const reportPath = process.env.PRODUCTION_RUNTIME_SMOKE_REPORT || 'production-runtime-smoke.json';
const baseUrl = 'http://127.0.0.1:3000';
const fixtureUrl = 'http://127.0.0.1:4010';
const adminToken = randomUUID();
const csrfToken = randomUUID();
const children = new Set();
const logs = {};

if (!databaseUrl) {
  process.stderr.write(`${JSON.stringify({ ok: false, error: 'DATABASE_URL_required' }, null, 2)}\n`);
  process.exit(1);
}

function launch(name, script, env) {
  const child = spawn(process.execPath, [script], {
    env: { ...process.env, ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  children.add(child);
  logs[name] = { stdout: '', stderr: '' };
  child.stdout.on('data', chunk => { logs[name].stdout += String(chunk); });
  child.stderr.on('data', chunk => { logs[name].stderr += String(chunk); });
  child.once('exit', () => children.delete(child));
  return child;
}

async function stop(child) {
  if (!child || child.exitCode !== null) return;
  child.kill('SIGTERM');
  await Promise.race([
    new Promise(resolve => child.once('exit', resolve)),
    new Promise(resolve => setTimeout(resolve, 5000)),
  ]);
  if (child.exitCode === null) child.kill('SIGKILL');
}

async function waitFor(url, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url);
      if (response.ok) return response;
      lastError = new Error(`${url} returned ${response.status}`);
    } catch (error) {
      lastError = error;
    }
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  throw new Error(`runtime_wait_timeout:${url}:${lastError?.message || 'unavailable'}`);
}

async function request(path, options = {}) {
  const headers = { accept: 'application/json', ...(options.headers || {}) };
  if (options.auth !== false) headers.authorization = `Bearer ${adminToken}`;
  if (options.csrf) headers['x-csrf-token'] = csrfToken;
  if (options.body !== undefined) headers['content-type'] = 'application/json';
  const response = await fetch(`${baseUrl}${path}`, {
    method: options.method || 'GET',
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  const text = await response.text();
  let body = null;
  try { body = JSON.parse(text); } catch { body = text; }
  return { status: response.status, headers: Object.fromEntries(response.headers), body, text };
}

async function runSmokeCommand(runtimeEnv) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['scripts/smoke-production-paper.mjs'], {
      env: { ...process.env, ...runtimeEnv, PORTFOLIO_BASE_URL: baseUrl },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', chunk => { stdout += String(chunk); });
    child.stderr.on('data', chunk => { stderr += String(chunk); });
    child.once('exit', code => {
      if (code === 0) resolve({ code, report: JSON.parse(stdout), stderr });
      else reject(new Error(`production_smoke_failed:${stderr || stdout}`));
    });
  });
}

const runtimeEnv = {
  NODE_ENV: 'production',
  DEPLOYMENT_ENV: 'production',
  STRICT_RUNTIME_VALIDATION: 'true',
  OPERATOR_STORE: 'postgres',
  DATABASE_URL: databaseUrl,
  OPERATOR_AUTH_REQUIRED: 'true',
  OPERATOR_ADMIN_TOKEN: adminToken,
  CSRF_REQUIRED: 'true',
  OPERATOR_CSRF_TOKEN: csrfToken,
  CORS_ORIGINS: baseUrl,
  MODE: 'paper',
  PAPER_TRADING: 'true',
  LIVE_TRADING: 'false',
  LIVE_TRADING_ENABLED: 'false',
  ALLOW_POLYMARKET_ORDER_SUBMISSION: 'false',
  ALLOW_LIVE_SETTLEMENT_REDEMPTION: 'false',
  REQUIRE_RUNTIME_CONFIRMATION: 'true',
  REQUIRE_MANUAL_APPROVAL: 'true',
  REQUIRE_APPROVED_MARKET_PAIR: 'true',
  COINBASE_DRY_RUN: 'true',
  LOCAL_LLM_EXECUTION_REQUIRED: 'true',
  LOCAL_LLM_NODES_JSON: JSON.stringify([{
    id: 'ci-local-node',
    name: 'CI local node',
    kind: 'lmstudio',
    baseUrl: `${fixtureUrl}/v1`,
    models: ['ci-local-model'],
    priority: 10,
    maxConcurrent: 1,
    prefillTokensPerSecond: 100,
    decodeTokensPerSecond: 25,
    estimatedWatts: 25,
    electricityRatePerKwh: 0.14,
    hardwareDepreciationPerHour: 0.01,
    contextLength: 32768,
  }]),
  REMOTE_LLM_EXECUTION_ENABLED: 'false',
  OPENROUTER_API_KEY: '',
  ECONOMIC_RUNTIME_ENABLED: 'false',
  PORT: '3000',
};

const report = {
  ok: false,
  database: 'configured',
  runtimeMode: 'production-paper',
  checks: {},
  failures: [],
};

let fixture;
let api;
try {
  fixture = launch('fixture', 'tests/fixtures/openai-compatible-node.mjs', {
    FAKE_LLM_PORT: '4010',
    FAKE_LLM_MODEL: 'ci-local-model',
  });
  await waitFor(`${fixtureUrl}/health`);
  report.checks.fixture = true;

  api = launch('api-first', 'apps/api/src/server.p1.mjs', runtimeEnv);
  await waitFor(`${baseUrl}/health`);

  const page = await request('/', { auth: false });
  if (page.status !== 200 || !page.text.includes('/ui/operator-session.js') || !page.text.includes('/ui/intelligence-policy.js')) {
    throw new Error('served_console_missing_session_or_policy_assets');
  }
  report.checks.browserAssets = true;

  const unauthorized = await request('/api/economics/intelligence/policy', { auth: false });
  if (unauthorized.status !== 401) throw new Error(`unauthenticated_policy_status_${unauthorized.status}`);
  report.checks.authRequired = true;

  const initialPolicy = await request('/api/economics/intelligence/policy');
  if (initialPolicy.status !== 200 || initialPolicy.body.policy?.mode !== 'local_only') {
    throw new Error('initial_policy_not_fail_closed');
  }
  report.checks.initialPolicy = initialPolicy.body.policy;

  const savedPolicy = await request('/api/economics/intelligence/policy', {
    method: 'PUT',
    csrf: true,
    body: {
      mode: 'economic_auto',
      remoteSpendCapUsdPerDay: 2,
      remoteSpendCapUsdPerRequest: 0.25,
      minimumRemoteValueCoverage: 3,
      fallbackToLocalOnRemoteBlock: true,
    },
  });
  if (savedPolicy.status !== 200 || savedPolicy.body.policy?.mode !== 'economic_auto') {
    throw new Error(`policy_save_failed_${savedPolicy.status}`);
  }
  report.checks.csrfProtectedPolicySave = true;

  const smoke = await runSmokeCommand(runtimeEnv);
  if (!smoke.report.ok) throw new Error('production_paper_probe_report_failed');
  report.checks.productionPaperProbes = smoke.report.results;

  await stop(api);
  api = launch('api-restarted', 'apps/api/src/server.p1.mjs', runtimeEnv);
  await waitFor(`${baseUrl}/health`);

  const persistedPolicy = await request('/api/economics/intelligence/policy');
  if (persistedPolicy.status !== 200 || persistedPolicy.body.policy?.mode !== 'economic_auto') {
    throw new Error('routing_policy_not_persisted_across_api_restart');
  }
  report.checks.policyPersistedAcrossRestart = true;

  const nodes = await request('/api/economics/intelligence/nodes');
  if (nodes.status !== 200 || !nodes.body.nodes?.some(row => row.nodeId === 'ci-local-node' && row.ok)) {
    throw new Error('local_fleet_health_not_visible_after_restart');
  }
  report.checks.localFleetAfterRestart = true;

  const readiness = await request('/ready/production-paper');
  if (readiness.status !== 200 || readiness.body.productionPaperReady !== true) {
    throw new Error(`production_paper_readiness_failed_${readiness.status}`);
  }
  report.checks.productionPaperReady = true;
  report.ok = true;
} catch (error) {
  report.failures.push(String(error?.message || error));
  report.logs = Object.fromEntries(Object.entries(logs).map(([name, value]) => [name, {
    stdout: value.stdout.slice(-8000),
    stderr: value.stderr.slice(-8000),
  }]));
} finally {
  await stop(api);
  await stop(fixture);
  writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`);
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}

if (!report.ok) process.exitCode = 1;
