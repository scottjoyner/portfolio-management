import { existsSync, readFileSync } from 'node:fs';

const required = [
  'apps/web/src/index.html',
  'apps/web/src/styles.css',
  'apps/web/src/app.js',
  'apps/web/src/economics.js',
  'apps/web/src/economics.css',
];

const missing = required.filter(path => !existsSync(path));
if (missing.length) {
  console.error('web build failed: missing files', missing);
  process.exit(1);
}

const html = readFileSync(required[0], 'utf8');
const css = readFileSync(required[1], 'utf8');
const app = readFileSync(required[2], 'utf8');
const economics = readFileSync(required[3], 'utf8');
const economicsCss = readFileSync(required[4], 'utf8');
const combined = html + css + app + economics + economicsCss;

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

console.log('web build ok: daily operations and economic lifecycle console validated');
