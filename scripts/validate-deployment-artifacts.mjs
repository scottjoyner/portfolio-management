import { existsSync, readFileSync } from 'node:fs';

const requiredFiles = [
  'Dockerfile',
  'deploy/compose.production.yml',
  'deploy/kubernetes.yaml',
  'docs/PRODUCTION_DEPLOYMENT_CHECKLIST.md'
];

for (const path of requiredFiles) {
  if (!existsSync(path)) {
    console.error(`deployment validation failed: missing ${path}`);
    process.exit(1);
  }
}

const dockerfile = readFileSync('Dockerfile', 'utf8');
const compose = readFileSync('deploy/compose.production.yml', 'utf8');
const k8s = readFileSync('deploy/kubernetes.yaml', 'utf8');
const checklist = readFileSync('docs/PRODUCTION_DEPLOYMENT_CHECKLIST.md', 'utf8');

const expectations = [
  [dockerfile, 'HEALTHCHECK', 'Dockerfile must include a healthcheck'],
  [dockerfile, 'apps/api/src/server.p1.mjs', 'Dockerfile must start the operator API'],
  [compose, 'STRICT_RUNTIME_VALIDATION', 'compose production must enable strict runtime validation'],
  [compose, 'LIVE_TRADING: "false"', 'compose production must keep live trading disabled'],
  [compose, 'ALLOW_POLYMARKET_ORDER_SUBMISSION: "false"', 'compose production must keep polymarket order submission disabled'],
  [compose, 'ALLOW_LIVE_SETTLEMENT_REDEMPTION: "false"', 'compose production must keep live settlement redemption disabled'],
  [compose, 'condition: service_healthy', 'compose production must wait for database health'],
  [k8s, 'STRICT_RUNTIME_VALIDATION', 'kubernetes manifest must enable strict runtime validation'],
  [k8s, 'LIVE_TRADING', 'kubernetes manifest must pin live trading setting'],
  [k8s, 'value: "false"', 'kubernetes manifest must include false values for live flags'],
  [k8s, 'readinessProbe', 'kubernetes manifest must include readiness probe'],
  [k8s, 'livenessProbe', 'kubernetes manifest must include liveness probe'],
  [checklist, 'LIVE_TRADING=false', 'checklist must document live trading prohibition'],
  [checklist, 'pg_dump', 'checklist must document backup command shape'],
  [checklist, 'pg_restore', 'checklist must document restore command shape']
];

for (const [content, needle, message] of expectations) {
  if (!content.includes(needle)) {
    console.error(`deployment validation failed: ${message}`);
    process.exit(1);
  }
}

console.log('deployment artifact validation ok');
