import { existsSync, readFileSync } from 'node:fs';

const required = [
  'apps/web/src/index.html',
  'apps/web/src/styles.css',
  'apps/web/src/app.js',
  'apps/web/src/p1.js',
  'apps/web/src/dashboard-data.js'
];

const missing = required.filter(path => !existsSync(path));
if (missing.length) {
  console.error('web build failed: missing files', missing);
  process.exit(1);
}

const html = readFileSync('apps/web/src/index.html', 'utf8');
const app = readFileSync('apps/web/src/app.js', 'utf8');
const p1 = readFileSync('apps/web/src/p1.js', 'utf8');
const data = readFileSync('apps/web/src/dashboard-data.js', 'utf8');

if (!html.includes('/ui/app.js') || !html.includes('/ui/styles.css')) {
  console.error('web build failed: expected static asset references are missing');
  process.exit(1);
}

for (const section of ['overview', 'portfolio', 'live-markets', 'opportunities', 'polymarket', 'agents', 'risk', 'audit']) {
  if (!html.includes(`id="${section}"`)) {
    console.error(`web build failed: missing dashboard section ${section}`);
    process.exit(1);
  }
}

if (!app.includes("from './p1.js'") || !app.includes("from './dashboard-data.js'")) {
  console.error('web build failed: app.js must import P1 and dashboard data modules');
  process.exit(1);
}

for (const endpoint of ['/api/accounts', '/api/instruments', '/api/strategy-templates', '/api/paper-executions']) {
  if (!p1.includes(endpoint)) {
    console.error(`web build failed: p1.js missing endpoint ${endpoint}`);
    process.exit(1);
  }
}

for (const token of ['opportunities', 'agentCostSummary', 'marketSnapshots']) {
  if (!data.includes(token)) {
    console.error(`web build failed: dashboard-data.js missing ${token}`);
    process.exit(1);
  }
}

for (const token of ['netExpectedValue', 'totalMoneyRisked', 'modelInferenceCost', 'agentResearchCost']) {
  if (!app.includes(token)) {
    console.error(`web build failed: app.js missing opportunity cost/risk token ${token}`);
    process.exit(1);
  }
}

console.log('web build ok: expanded trading dashboard assets validated');
