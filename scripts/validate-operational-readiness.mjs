#!/usr/bin/env node
import { existsSync, readFileSync } from 'node:fs';

const requiredFiles = [
  'package.json',
  'package-lock.json',
  'Dockerfile',
  'docker-compose.production.yml',
  '.github/workflows/ci.yml',
  'apps/api/src/intelligenceExecution.mjs',
  'apps/api/src/openRouterUsageReconciliation.mjs',
  'scripts/ci-production-runtime-smoke.mjs',
  'scripts/smoke-production-paper.mjs',
  'scripts/check-economic-worker-health.mjs',
  'tests/integration/postgres-smoke.test.mjs',
  'tests/openrouter-at-most-once.test.mjs',
  'tests/fixtures/openai-compatible-node.mjs',
  'docs/FIRST_PROD_RELEASE_CHECKLIST.md',
  'docs/OPERATOR_RUNBOOK_P0_P1.md',
  'docs/DEPLOYMENT_ROLLBACK_RUNBOOK.md',
  'docs/RELEASE_READINESS_MATRIX.md',
];

const errors = [];
for (const path of requiredFiles) if (!existsSync(path)) errors.push(`missing_file:${path}`);
if (errors.length) {
  process.stderr.write(`${JSON.stringify({ ok: false, errors }, null, 2)}\n`);
  process.exit(1);
}

const read = path => readFileSync(path, 'utf8');
const packageJson = read('package.json');
const dockerfile = read('Dockerfile');
const compose = read('docker-compose.production.yml');
const workflow = read('.github/workflows/ci.yml');
const intelligenceExecution = read('apps/api/src/intelligenceExecution.mjs');
const openRouterReconciliation = read('apps/api/src/openRouterUsageReconciliation.mjs');
const runtimeSmoke = read('scripts/ci-production-runtime-smoke.mjs');
const productionSmoke = read('scripts/smoke-production-paper.mjs');
const workerHealth = read('scripts/check-economic-worker-health.mjs');
const postgresSmoke = read('tests/integration/postgres-smoke.test.mjs');
const openRouterTests = read('tests/openrouter-at-most-once.test.mjs');
const checklist = read('docs/FIRST_PROD_RELEASE_CHECKLIST.md');
const operatorRunbook = read('docs/OPERATOR_RUNBOOK_P0_P1.md');
const deploymentRunbook = read('docs/DEPLOYMENT_ROLLBACK_RUNBOOK.md');
const readinessMatrix = read('docs/RELEASE_READINESS_MATRIX.md');

const requiredBlockingGates = [
  'validation',
  'node-tests',
  'postgres-integration',
  'python-critical',
  'coverage-gate',
  'broad-python-suite',
];
const releaseReadinessBlock = workflow.split('\n  release-readiness:')[1] || '';
const releaseReadinessNeeds = releaseReadinessBlock.match(/\n\s+needs:\s*\[([^\]]+)\]/)?.[1] || '';
const declaredBlockingGates = new Set(
  releaseReadinessNeeds
    .split(',')
    .map(value => value.trim())
    .filter(Boolean),
);
const aggregatesAllBlockingGates = requiredBlockingGates.every(gate => declaredBlockingGates.has(gate));
const enforcesAllBlockingResults = [
  'VALIDATION_RESULT',
  'NODE_TESTS_RESULT',
  'POSTGRES_RESULT',
  'PYTHON_CRITICAL_RESULT',
  'COVERAGE_RESULT',
  'BROAD_PYTHON_RESULT',
].every(resultName => releaseReadinessBlock.includes(`test "$${resultName}" = success`));

const checks = [
  [packageJson.includes('"operational:validate"') && packageJson.includes('validate-operational-readiness.mjs'), 'package scripts must expose operational readiness validation'],
  [packageJson.includes('"smoke:production-runtime"') && packageJson.includes('ci-production-runtime-smoke.mjs'), 'package scripts must expose the authenticated production runtime smoke'],
  [packageJson.includes('"economics:health"') && packageJson.includes('check-economic-worker-health.mjs'), 'package scripts must expose the worker heartbeat check'],
  [dockerfile.includes('COPY package.json package-lock.json') && dockerfile.includes('npm ci --omit=dev --ignore-scripts'), 'production image must use the committed Node lockfile'],
  [compose.includes('scripts/check-economic-worker-health.mjs') && compose.includes('ECONOMIC_WORKER_HEALTH_MAX_AGE_MS'), 'Compose must enforce economic worker heartbeat health'],
  [compose.includes('profiles: ["backup"]') && compose.includes('pg_dump --format=custom'), 'Compose must retain the logical backup profile'],
  [workflow.includes('postgres-integration:') && workflow.includes('postgres:17-bookworm'), 'CI must start real PostgreSQL 17'],
  [workflow.includes('npm run test:integration:postgres') && workflow.includes('npm run smoke:production-runtime'), 'CI must run database and production runtime restart smoke tests'],
  [workflow.includes('pg_dump') && workflow.includes('pg_restore') && workflow.includes('portfolio_restore'), 'CI must prove logical backup restoration'],
  [workflow.includes('postgres-readiness-${{ github.sha }}') && workflow.includes('actions/upload-artifact@v4'), 'CI must retain exact-head readiness evidence'],
  [workflow.includes('release-readiness:') && aggregatesAllBlockingGates && enforcesAllBlockingResults, 'CI must aggregate and enforce all blocking release gates'],
  [postgresSmoke.includes('006_normalized_execution_runtime') && postgresSmoke.includes('fresh execution engine'), 'PostgreSQL smoke must prove migration 006 and process-boundary hydration'],
  [postgresSmoke.includes('execution_events') && postgresSmoke.includes('assert.rejects'), 'PostgreSQL smoke must verify append-only execution events'],
  [postgresSmoke.includes('runtimeJobs.enqueue') && postgresSmoke.includes('runtimeJobs.heartbeat') && postgresSmoke.includes('runtimeJobs.complete'), 'PostgreSQL smoke must verify durable leased jobs'],
  [runtimeSmoke.includes('OPERATOR_AUTH_REQUIRED') && runtimeSmoke.includes('OPERATOR_CSRF_TOKEN'), 'production runtime smoke must enforce authentication and CSRF'],
  [runtimeSmoke.includes('policyPersistedAcrossRestart') && runtimeSmoke.includes('localFleetAfterRestart'), 'production runtime smoke must prove policy and fleet behavior after restart'],
  [productionSmoke.includes('/ui/operator-session.js') && productionSmoke.includes('/api/economics/intelligence/policy'), 'deployed smoke must inspect browser session and routing policy surfaces'],
  [workerHealth.includes('economic_worker_heartbeat_stale') && workerHealth.includes('economic_worker_last_run_failed'), 'worker health must detect stale and failed scheduler state'],
  [intelligenceExecution.includes('providerAttemptId') && intelligenceExecution.includes('model_usage_pending_reconciliation'), 'remote execution must reserve provider attempts and reject pending reuse'],
  [intelligenceExecution.includes("providerOutcome === 'not_started'") && intelligenceExecution.includes("providerOutcome === 'uncertain'"), 'remote failures must distinguish safe retry from uncertain provider outcome'],
  [openRouterReconciliation.includes('/api/v1/generation') && openRouterReconciliation.includes('retry_scheduled') && openRouterReconciliation.includes('exhausted'), 'known OpenRouter generations must use bounded delayed metadata reconciliation'],
  [openRouterTests.includes('never posts twice') && openRouterTests.includes('transport-uncertain') && openRouterTests.includes('eventually require manual reconciliation'), 'remote provider tests must prove at-most-once and bounded reconciliation behavior'],
  [checklist.includes('postgres-integration') && checklist.includes('release-readiness'), 'release checklist must name the blocking CI evidence'],
  [checklist.includes('`broad-python-suite`') && readinessMatrix.includes('`broad-python-suite`'), 'release documentation must name the mandatory broad Python gate'],
  [!checklist.includes('legacy-python-diagnostic') && !readinessMatrix.includes('legacy-python-diagnostic'), 'release documentation must not classify the maintained broad Python suite as diagnostic'],
  [checklist.includes('at-most-once') && checklist.includes('generation metadata'), 'release checklist must document remote at-most-once and generation reconciliation gates'],
  [operatorRunbook.includes('sessionStorage') || deploymentRunbook.includes('sessionStorage'), 'operator documentation must explain same-tab browser credential handling'],
  [deploymentRunbook.includes('Stop conditions before deployment') && deploymentRunbook.includes('Rollback decision triggers'), 'deployment runbook must contain explicit stop and rollback conditions'],
  [deploymentRunbook.includes('Local fleet unavailable') && deploymentRunbook.includes('OpenRouter unavailable or spend cap exhausted'), 'deployment runbook must cover both inference outage modes'],
  [deploymentRunbook.includes('usage_pending') && deploymentRunbook.includes('Duplicate execution suspected'), 'deployment runbook must cover billing reconciliation and duplicate execution incidents'],
  [readinessMatrix.includes('Remote provider at-most-once') && readinessMatrix.includes('Delayed `usage_pending` reconciliation'), 'readiness matrix must record remote provider recovery as blocking automated evidence'],
  [readinessMatrix.includes('Remaining whole-state rewrites') && readinessMatrix.includes('Deterministic paid-agent counterfactual replay'), 'readiness matrix must preserve the remaining engineering blockers'],
  [readinessMatrix.includes('Blocking — manual') && readinessMatrix.includes('Rollback rehearsal'), 'readiness matrix must preserve host-only certification gates'],
];
for (const [ok, message] of checks) if (!ok) errors.push(message);

if (errors.length) {
  process.stderr.write(`${JSON.stringify({ ok: false, errors }, null, 2)}\n`);
  process.exit(1);
}

process.stdout.write(`${JSON.stringify({
  ok: true,
  operationalReadinessContract: 'production-paper-v1',
  blockingCiAggregate: 'release-readiness',
  requiredBlockingGates,
  releaseDocumentationAligned: true,
  postgresEvidenceArtifact: 'postgres-readiness-<sha>',
  workerHeartbeatHealthcheck: true,
  browserAuthRestartSmoke: true,
  remoteProviderAtMostOnce: true,
  delayedUsageReconciliation: true,
  backupRestoreEvidence: true,
  rollbackRunbook: true,
  liveTradingCertified: false,
}, null, 2)}\n`);
