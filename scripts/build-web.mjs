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
const app = readFileSync('apps/web/src/app.js', 'utf8');
const p1 = readFileSync('apps/web/src/p1.js', 'utf8');

if (!html.includes('/ui/app.js') || !html.includes('/ui/styles.css')) {
  console.error('web build failed: expected static asset references are missing');
  process.exit(1);
}

if (!app.includes("from './p1.js'")) {
  console.error('web build failed: app.js must import the P1 UI module');
  process.exit(1);
}

for (const endpoint of ['/api/accounts', '/api/instruments', '/api/strategy-templates', '/api/paper-executions']) {
  if (!p1.includes(endpoint)) {
    console.error(`web build failed: p1.js missing endpoint ${endpoint}`);
    process.exit(1);
  }
}

console.log('web build ok: static operator console and P1 assets validated');
