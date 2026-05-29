import { existsSync, readFileSync } from 'node:fs';

const required = [
  'apps/web/src/index.html',
  'apps/web/src/styles.css',
  'apps/web/src/app.js'
];

const missing = required.filter(path => !existsSync(path));
if (missing.length) {
  console.error('web build failed: missing files', missing);
  process.exit(1);
}

const html = readFileSync('apps/web/src/index.html', 'utf8');
if (!html.includes('/ui/app.js') || !html.includes('/ui/styles.css')) {
  console.error('web build failed: expected static asset references are missing');
  process.exit(1);
}

console.log('web build ok: static operator console assets validated');
