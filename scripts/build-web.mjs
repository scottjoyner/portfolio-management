import { existsSync, readFileSync } from 'node:fs';

const required = [
  'apps/web/src/index.html',
  'apps/web/src/styles.css',
  'apps/web/src/app.js',
  'apps/web/src/p1.js'
];

const missing = required.filter(path => !existsSync(path));
if (missing.length) {
  console.error('web build failed: missing files', missing);
  process.exit(1);
}

const html = readFileSync('apps/web/src/index.html', 'utf8');
const css = readFileSync('apps/web/src/styles.css', 'utf8');
const app = readFileSync('apps/web/src/app.js', 'utf8');
const p1 = readFileSync('apps/web/src/p1.js', 'utf8');

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

for (const token of ['app-frame', 'sidebar', 'command-bar', 'market-strip', 'command-queue', 'risk-stack', 'cockpit-hero']) {
  if (!(html + css + app).includes(token)) {
    console.error(`web build failed: missing cockpit layout token ${token}`);
    process.exit(1);
  }
}

if (!app.includes("from './p1.js'")) {
  console.error('web build failed: app.js must import P1 module');
  process.exit(1);
}

for (const endpoint of ['/api/accounts', '/api/instruments', '/api/strategy-templates', '/api/paper-executions']) {
  if (!p1.includes(endpoint)) {
    console.error(`web build failed: p1.js missing endpoint ${endpoint}`);
    process.exit(1);
  }
}

for (const endpoint of ['/api/opportunity-dashboard', '/api/opportunities', '/api/agents/jobs', '/api/agents/budget-approvals']) {
  if (!app.includes(endpoint)) {
    console.error(`web build failed: app.js missing API-backed dashboard endpoint ${endpoint}`);
    process.exit(1);
  }
}

for (const token of ['netExpectedValue', 'totalMoneyRisked', 'modelInferenceCost', 'agentResearchCost', 'budgetApprovals', 'budget-approval-cards', 'request-budget-approval']) {
  if (!(html + app).includes(token)) {
    console.error(`web build failed: missing opportunity/budget approval UI token ${token}`);
    process.exit(1);
  }
}

if (app.includes("from './dashboard-data.js'")) {
  console.error('web build failed: dashboard must not import static opportunity fixtures');
  process.exit(1);
}

console.log('web build ok: API-backed cockpit dashboard and budget approval controls validated');
