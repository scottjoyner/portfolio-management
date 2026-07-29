import { existsSync, readFileSync } from 'node:fs';

const required = [
  'apps/web/src/index.html',
  'apps/web/src/styles.css',
  'apps/web/src/app.js',
];

const missing = required.filter(path => !existsSync(path));
if (missing.length) {
  console.error('web build failed: missing files', missing);
  process.exit(1);
}

const html = readFileSync(required[0], 'utf8');
const css = readFileSync(required[1], 'utf8');
const app = readFileSync(required[2], 'utf8');
const combined = html + css + app;

for (const asset of ['/ui/app.js', '/ui/styles.css']) {
  if (!html.includes(asset)) {
    console.error(`web build failed: missing static asset ${asset}`);
    process.exit(1);
  }
}

for (const section of ['overview', 'race', 'trades', 'signals', 'learning', 'costs', 'risk', 'system']) {
  if (!html.includes(`id="${section}"`) || !html.includes(`#${section}`)) {
    console.error(`web build failed: missing competition section ${section}`);
    process.exit(1);
  }
}

for (const token of ['app-frame', 'sidebar', 'command-bar', 'market-strip', 'command-queue', 'risk-stack', 'cockpit-hero']) {
  if (!combined.includes(token)) {
    console.error(`web build failed: missing cockpit token ${token}`);
    process.exit(1);
  }
}

for (const endpoint of ['/api/competition', '/api/system-truth', '/api/agents/costs', '/api/executions', '/api/opportunities', '/api/activity-feed']) {
  if (!app.includes(endpoint)) {
    console.error(`web build failed: missing API endpoint ${endpoint}`);
    process.exit(1);
  }
}

for (const token of ['net_equity_usd', 'operating_cost_usd', 'agent_cost_coverage_ratio', 'valid_for_ranking', 'budget-approval-rows', 'request-budget-approval']) {
  if (!combined.includes(token)) {
    console.error(`web build failed: missing competition token ${token}`);
    process.exit(1);
  }
}

for (const id of ['system-truth', 'truth-mode', 'truth-feed', 'truth-cache', 'truth-services', 'truth-paper-book', 'truth-execution-decision', 'truth-terminal']) {
  if (!html.includes(`id="${id}"`)) {
    console.error(`web build failed: missing system-truth element ${id}`);
    process.exit(1);
  }
}

console.log('web build ok: competition-first operator console validated');
