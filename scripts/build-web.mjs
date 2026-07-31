import { existsSync, readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

const required = [
  'apps/web/src/index.html',
  'apps/web/src/styles.css',
  'apps/web/src/app.js',
  'apps/web/src/economics.js',
  'apps/web/src/economics.css',
  'apps/web/src/intelligence-policy.js',
  'apps/web/src/operator-session.js',
];

const missing = required.filter(path => !existsSync(path));
if (missing.length) {
  console.error('web build failed: missing files', missing);
  process.exit(1);
}

for (const script of ['apps/web/src/app.js', 'apps/web/src/economics.js', 'apps/web/src/intelligence-policy.js', 'apps/web/src/operator-session.js']) {
  const checked = spawnSync(process.execPath, ['--check', script], { encoding: 'utf8' });
  if (checked.status !== 0) {
    console.error(`web build failed: JavaScript syntax error in ${script}`);
    console.error(checked.stderr || checked.stdout);
    process.exit(1);
  }
}

const html = readFileSync(required[0], 'utf8');
const css = readFileSync(required[1], 'utf8');
const app = readFileSync(required[2], 'utf8');
const economics = readFileSync(required[3], 'utf8');
const economicsCss = readFileSync(required[4], 'utf8');
const intelligencePolicy = readFileSync(required[5], 'utf8');
const operatorSession = readFileSync(required[6], 'utf8');
const server = readFileSync('apps/api/src/server.p1.mjs', 'utf8');
const combined = html + css + app + economics + economicsCss + intelligencePolicy + operatorSession + server;

for (const asset of ['/ui/app.js', '/ui/economics.js', '/ui/styles.css']) {
  if (!html.includes(asset)) {
    console.error(`web build failed: missing static asset ${asset}`);
    process.exit(1);
  }
}
if (!economics.includes('/ui/economics.css')) {
  console.error('web build failed: economics stylesheet is not loaded');
  process.exit(1);
}
if (!server.includes('/ui/operator-session.js')) {
  console.error('web build failed: operator session headers are not loaded before console requests');
  process.exit(1);
}
if (!server.includes('/ui/intelligence-policy.js')) {
  console.error('web build failed: intelligence routing control is not loaded by the served console');
  process.exit(1);
}
if (!operatorSession.includes("pathname.startsWith('/api/')")
  || !operatorSession.includes("headers.set('authorization'")
  || !operatorSession.includes("headers.set('x-csrf-token'")) {
  console.error('web build failed: operator session must attach auth and CSRF only to same-origin API requests');
  process.exit(1);
}

for (const section of ['overview', 'trades', 'positions', 'signals', 'race', 'agent', 'system']) {
  if (!html.includes(`id="${section}"`) || !html.includes(`#${section}`)) {
    console.error(`web build failed: missing daily-operations section ${section}`);
    process.exit(1);
  }
}

for (const token of [
  'app-frame',
  'sidebar',
  'command-bar',
  'safety-strip',
  'daily-brief-title',
  'command-queue',
  'execution-pipeline',
  'execution-list',
  'position-rows',
  'decision-list',
  'competition-grid',
  'economic-lifecycle',
  'economic-summary',
  'economic-forecast',
  'economic-intelligence',
  'economic-edge',
  'economic-maintenance',
  'economic-attribution',
  'economic-governance',
  'economic-decisions',
  'strip-economics',
  'intelligence-routing-policy',
  'intelligence-policy-mode',
  'remoteSpendCapUsdPerDay',
  'minimumRemoteValueCoverage',
  'operator-session-launcher',
  'portfolio.operatorBearer',
  'portfolio.operatorCsrf',
]) {
  if (!combined.includes(token)) {
    console.error(`web build failed: missing operator-workflow token ${token}`);
    process.exit(1);
  }
}

for (const endpoint of [
  '/api/competition',
  '/api/system-truth',
  '/api/agents/costs',
  '/api/executions',
  '/api/execution/events',
  '/api/opportunities',
  '/api/activity-feed',
  '/api/positions',
  '/api/market-data/live-quotes',
]) {
  if (!app.includes(endpoint)) {
    console.error(`web build failed: missing API endpoint ${endpoint}`);
    process.exit(1);
  }
}

for (const endpoint of [
  '/api/economics/dashboard',
  '/api/economics/maintenance/run',
  '/api/economics/model-pricing/refresh',
]) {
  if (!economics.includes(endpoint)) {
    console.error(`web build failed: missing economics API endpoint ${endpoint}`);
    process.exit(1);
  }
}

for (const endpoint of ['/api/economics/intelligence/policy']) {
  if (!intelligencePolicy.includes(endpoint) || !server.includes(endpoint)) {
    console.error(`web build failed: missing intelligence routing endpoint ${endpoint}`);
    process.exit(1);
  }
}

for (const token of [
  'net_equity_usd',
  'operating_cost_usd',
  'agent_cost_coverage_ratio',
  'valid_for_ranking',
  'budget-approval-rows',
  'request-budget-approval',
  'Local operator execution (non-canonical)',
  'maximumIntelligenceSpendUsd',
  'netExecutableEdgeUsd',
  'incrementalPnlUsd',
  'unreconciledQuotes',
  'modelUsageReconciled',
  'pendingAttribution',
  'Provider-reported actual cost becomes authoritative',
  'local_only',
  'economic_auto',
  'openrouter_allowed',
  'never bypasses forecast',
  "same-origin <code>/api/</code> requests",
]) {
  if (!combined.includes(token)) {
    console.error(`web build failed: missing accounting or safety token ${token}`);
    process.exit(1);
  }
}

for (const id of ['system-truth', 'truth-mode', 'truth-feed', 'truth-cache', 'truth-services', 'truth-paper-book', 'truth-execution-decision', 'truth-terminal']) {
  if (!html.includes(`id="${id}"`)) {
    console.error(`web build failed: missing system-truth element ${id}`);
    process.exit(1);
  }
}

console.log('web build ok: daily operations, authenticated browser session, economic lifecycle, and intelligence routing console validated');
