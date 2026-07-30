#!/usr/bin/env node
import { existsSync, readFileSync } from 'node:fs';

const requiredFiles = [
  'Dockerfile',
  'docker-compose.production.yml',
  '.env.production.example',
  'scripts/migrate-postgres.mjs',
  'scripts/run-economic-maintenance.mjs',
  'packages/storage/src/transactionalPostgresOperatorStore.mjs',
  'packages/storage/src/runtimeJobQueue.mjs',
  'packages/intelligence/src/providerRegistry.mjs',
];
const errors = [];
for (const path of requiredFiles) if (!existsSync(path)) errors.push(`missing_file:${path}`);
if (errors.length) {
  process.stderr.write(`${JSON.stringify({ ok: false, errors }, null, 2)}\n`);
  process.exit(1);
}

const dockerfile = readFileSync('Dockerfile', 'utf8');
const compose = readFileSync('docker-compose.production.yml', 'utf8');
const envExample = readFileSync('.env.production.example', 'utf8');
const migrations = readFileSync('scripts/migrate-postgres.mjs', 'utf8');
const worker = readFileSync('scripts/run-economic-maintenance.mjs', 'utf8');
const transactionalStore = readFileSync('packages/storage/src/transactionalPostgresOperatorStore.mjs', 'utf8');
const jobQueue = readFileSync('packages/storage/src/runtimeJobQueue.mjs', 'utf8');
const providerRegistry = readFileSync('packages/intelligence/src/providerRegistry.mjs', 'utf8');

function section(startToken, endToken) {
  const start = compose.indexOf(startToken);
  if (start < 0) return '';
  const end = compose.indexOf(endToken, start + startToken.length);
  return compose.slice(start, end < 0 ? compose.length : end);
}

function serviceBlock(name, nextName) {
  return section(`  ${name}:\n`, nextName ? `\n  ${nextName}:\n` : '\nvolumes:\n');
}

const appSecurity = section('x-app-security: &app-security\n', '\nservices:\n');
const postgresService = serviceBlock('postgres', 'migrate');
const apiService = serviceBlock('api', 'economic-worker');
const workerService = serviceBlock('economic-worker', 'postgres-backup');
const inheritedAppNetworks = appSecurity.includes('networks:')
  && appSecurity.includes('- backend')
  && appSecurity.includes('- inference-egress');
const apiHasRequiredNetworks = (apiService.includes('- backend') && apiService.includes('- inference-egress'))
  || (apiService.includes('<<: *app-security') && inheritedAppNetworks);
const workerHasRequiredNetworks = (workerService.includes('- backend') && workerService.includes('- inference-egress'))
  || (workerService.includes('<<: *app-security') && inheritedAppNetworks);

const checks = [
  [dockerfile.includes('HEALTHCHECK'), 'Dockerfile must define a health check'],
  [dockerfile.includes('apps/api/src/server.p1.mjs'), 'Dockerfile must start the Node operator API'],
  [compose.includes('postgres:17-bookworm'), 'production compose must pin PostgreSQL 17'],
  [compose.includes('command: ["pnpm", "migrations:up"]'), 'production compose must run migrations before services'],
  [compose.includes('command: ["pnpm", "api"]'), 'production compose must run the Node API'],
  [compose.includes('command: ["pnpm", "economics:worker"]'), 'production compose must run the leased economic worker'],
  [compose.includes('condition: service_completed_successfully'), 'API and worker must wait for migration completion'],
  [compose.includes('OPERATOR_AUTH_REQUIRED: "true"'), 'production compose must require operator authentication'],
  [compose.includes('CSRF_REQUIRED: "true"'), 'production compose must require CSRF'],
  [compose.includes('LIVE_TRADING: "false"'), 'production compose must keep live trading disabled'],
  [compose.includes('REMOTE_LLM_EXECUTION_ENABLED: "false"'), 'production compose must disable remote LLM execution by default'],
  [compose.includes('LOCAL_LLM_EXECUTION_REQUIRED: "true"'), 'production compose must require local inference'],
  [compose.includes('LOCAL_LLM_NODES_JSON'), 'production compose must expose fleet-aware node configuration'],
  [compose.includes('backend:\n    internal: true'), 'database backend network must be internal'],
  [compose.includes('inference-egress:'), 'API and worker need controlled inference egress'],
  [Boolean(postgresService), 'production compose must define a postgres service'],
  [!/^\s{4}ports:/m.test(postgresService), 'PostgreSQL must not publish a host port'],
  [/^\s{4}ports:/m.test(apiService), 'API service must publish only the configured operator port'],
  [apiHasRequiredNetworks, 'API must join database and inference networks'],
  [workerHasRequiredNetworks, 'worker must join database and inference networks'],
  [compose.includes('read_only: true'), 'application containers must use read-only root filesystems'],
  [compose.includes('cap_drop:\n    - ALL'), 'application containers must drop Linux capabilities'],
  [compose.includes('no-new-privileges:true'), 'containers must set no-new-privileges'],
  [compose.includes('COINBASE_DRY_RUN: "true"'), 'Coinbase must default to dry-run'],
  [envExample.includes('LOCAL_LLM_NODES_JSON='), 'production env example must document local fleet nodes'],
  [envExample.includes('x1-370') && envExample.includes('xwing') && envExample.includes('macbook-air'), 'production env example must include the intended fleet topology'],
  [migrations.includes('pg_advisory_lock') && migrations.includes('checksum'), 'migration runner must lock and checksum migrations'],
  [transactionalStore.includes('BEGIN ISOLATION LEVEL') && transactionalStore.includes('pg_advisory_xact_lock'), 'operator store must use pinned serializable transactions'],
  [transactionalStore.includes('005_runtime_job_queue') && transactionalStore.includes('006_normalized_execution_runtime'), 'operator store readiness must require runtime and execution migrations'],
  [jobQueue.includes('FOR UPDATE SKIP LOCKED') && jobQueue.includes('lease_expires_at'), 'runtime queue must claim jobs with leases'],
  [worker.includes('runtimeJobs') && worker.includes('heartbeat'), 'economic worker must use the durable lease queue'],
  [providerRegistry.includes('/chat/completions') && providerRegistry.includes('/models'), 'local provider must use the common OpenAI-compatible contract'],
  [providerRegistry.includes('REMOTE_LLM_EXECUTION_ENABLED'), 'remote provider must remain explicitly gated'],
];
for (const [ok, message] of checks) if (!ok) errors.push(message);

if (/POSTGRES_PASSWORD:\s*postgres\b/.test(compose) || /OPERATOR_ADMIN_TOKEN:\s*\w+/.test(compose)) {
  errors.push('production compose must not contain literal default secrets');
}

if (errors.length) {
  process.stderr.write(`${JSON.stringify({ ok: false, errors }, null, 2)}\n`);
  process.exit(1);
}
process.stdout.write(`${JSON.stringify({ ok: true, deployment: 'docker-compose.production.yml', liveTradingCertified: false, remoteInferenceDefault: false, localInferenceRequired: true }, null, 2)}\n`);
