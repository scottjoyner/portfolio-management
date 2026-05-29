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

const checks = [
  [dockerfile.includes('HEALTHCHECK'), 'Dockerfile must define HEALTHCHECK'],
  [dockerfile.includes('node') && dockerfile.includes('apps/api/src/server.p1.mjs'), 'Dockerfile must start the Node API server'],
  [compose.includes('STRICT_RUNTIME_VALIDATION: "true"'), 'production compose must enable strict runtime validation'],
  [compose.includes('LIVE_TRADING: "false"'), 'production compose must keep live trading disabled'],
  [compose.includes('ALLOW_POLYMARKET_ORDER_SUBMISSION: "false"'), 'production compose must block Polymarket order submission'],
  [compose.includes('ALLOW_LIVE_SETTLEMENT_REDEMPTION: "false"'), 'production compose must block live settlement redemption'],
  [compose.includes('OPERATOR_AUTH_REQUIRED: "true"'), 'production compose must require auth'],
  [compose.includes('CSRF_REQUIRED: "true"'), 'production compose must require CSRF'],
  [k8s.includes('STRICT_RUNTIME_VALIDATION') && k8s.includes('value: "true"'), 'kubernetes template must enable strict runtime validation'],
  [k8s.includes('LIVE_TRADING') && k8s.includes('value: "false"'), 'kubernetes template must keep live trading disabled'],
  [k8s.includes('readinessProbe') && k8s.includes('livenessProbe'), 'kubernetes template must include probes'],
  [checklist.includes('LIVE_TRADING=false'), 'deployment checklist must document live trading prohibition'],
  [checklist.includes('pg_dump') && checklist.includes('pg_restore'), 'deployment checklist must document backup and restore commands']
];

const failures = checks.filter(([ok]) => !ok).map(([, message]) => message);
if (failures.length) {
  console.error('deployment validation failed:');
  for (const failure of failures) console.error(`- ${failure}`);
  process.exit(1);
}

console.log('deployment validation ok');
