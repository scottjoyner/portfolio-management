#!/usr/bin/env node
import { existsSync, readFileSync } from 'node:fs';

const requiredFiles = [
  'packages/storage/src/migrations/003_audit_and_certification.sql',
  'packages/storage/src/migrations/004_opportunity_agent_workflow.sql',
  'packages/storage/src/migrations/005_runtime_job_queue.sql',
  'packages/storage/src/migrations/006_normalized_execution_runtime.sql',
  'packages/storage/src/auditChain.mjs',
  'packages/storage/src/transactionalPostgresOperatorStore.mjs',
  'packages/storage/src/runtimeJobQueue.mjs',
  'packages/storage/src/executionRepository.mjs',
  'packages/storage/src/executionCompatibilitySync.mjs',
  'packages/execution/src/executionEngine.mjs',
  'packages/adapters/src/contracts.mjs',
  'packages/backtesting/src/replayEngine.mjs',
  'packages/intelligence/src/providerRegistry.mjs',
  'apps/api/src/intelligenceExecution.mjs',
  'apps/api/src/modelCallRecovery.mjs',
  'scripts/check-economic-worker-health.mjs',
  'scripts/ci-production-runtime-smoke.mjs',
  'scripts/validate-operational-readiness.mjs',
  'tests/integration/postgres-smoke.test.mjs',
  'docs/FIRST_PROD_RELEASE_CHECKLIST.md',
  'docs/API_CONTRACT_ECONOMICS.md',
  'docs/DEPLOYMENT_ROLLBACK_RUNBOOK.md',
  'docs/RELEASE_READINESS_MATRIX.md',
  '.github/workflows/ci.yml',
  'docker-compose.production.yml',
];
const errors = [];
for (const path of requiredFiles) if (!existsSync(path)) errors.push(`missing_file:${path}`);
if (errors.length) {
  process.stderr.write(`${JSON.stringify({ ok: false, errors }, null, 2)}\n`);
  process.exit(1);
}

const read = path => readFileSync(path, 'utf8');
const migration3 = read(requiredFiles[0]);
const migration4 = read(requiredFiles[1]);
const migration5 = read(requiredFiles[2]);
const migration6 = read(requiredFiles[3]);
const audit = read(requiredFiles[4]);
const transactional = read(requiredFiles[5]);
const jobs = read(requiredFiles[6]);
const executionRepository = read(requiredFiles[7]);
const compatibilitySync = read(requiredFiles[8]);
const executionEngine = read(requiredFiles[9]);
const adapters = read(requiredFiles[10]);
const replay = read(requiredFiles[11]);
const providers = read(requiredFiles[12]);
const intelligenceExecution = read(requiredFiles[13]);
const modelRecovery = read(requiredFiles[14]);
const workerHealth = read(requiredFiles[15]);
const runtimeSmoke = read(requiredFiles[16]);
const operationalValidator = read(requiredFiles[17]);
const postgresSmoke = read(requiredFiles[18]);
const checklist = read(requiredFiles[19]);
const economicsContract = read(requiredFiles[20]);
const deploymentRunbook = read(requiredFiles[21]);
const readinessMatrix = read(requiredFiles[22]);
const workflow = read(requiredFiles[23]);
const compose = read(requiredFiles[24]);
const checklistLower = checklist.toLowerCase();

const checks = [
  [migration3.includes('adapter_certifications'), 'migration 003 must include adapter certifications'],
  [migration3.includes('previous_hash') && migration3.includes('event_hash') && migration3.includes('sequence_number'), 'migration 003 must include audit hash-chain fields'],
  [migration4.includes('research_jobs') && migration4.includes('opportunities') && migration4.includes('agent_cost_ledger'), 'migration 004 must include opportunity and research workflow tables'],
  [migration5.includes('runtime_jobs') && migration5.includes('idempotency_key') && migration5.includes('lease_expires_at'), 'migration 005 must include the durable job queue'],
  [migration6.includes('execution_records') && migration6.includes('execution_events') && migration6.includes('execution_orders') && migration6.includes('execution_fills'), 'migration 006 must include normalized execution tables'],
  [migration6.includes('execution_events_are_append_only') && migration6.includes('BEFORE UPDATE OR DELETE'), 'migration 006 must enforce append-only execution events'],
  [audit.includes('verifyAuditChain') && audit.includes('hashAuditEvent'), 'audit helper must verify and hash events'],
  [transactional.includes('BEGIN ISOLATION LEVEL') && transactional.includes('pg_advisory_xact_lock'), 'operator store must pin serializable transactions'],
  [transactional.includes('005_runtime_job_queue') && transactional.includes('006_normalized_execution_runtime'), 'operator store must require migrations 005 and 006'],
  [transactional.includes('synchronizeCompatibilityExecutions') && transactional.includes('publishExecutionReadModel'), 'operator store must synchronize and publish durable execution state'],
  [jobs.includes('FOR UPDATE SKIP LOCKED') && jobs.includes('heartbeat') && jobs.includes('recoverExpired'), 'runtime queue must claim, heartbeat, and recover jobs'],
  [executionRepository.includes('FOR UPDATE') && executionRepository.includes('execution_version_conflict'), 'execution repository must use row locks and optimistic versions'],
  [executionRepository.includes('ON CONFLICT (idempotency_key) DO NOTHING') && executionRepository.includes('execution_transition_invalid'), 'execution repository must be idempotent and fail closed on invalid transitions'],
  [compatibilitySync.includes('execution_compatibility_divergence') && compatibilitySync.includes('orphan_order_reference_removed'), 'compatibility synchronization must preserve durable status and harden fill import'],
  [executionEngine.includes('__PORTFOLIO_EXECUTION_READ_MODEL__') && executionEngine.includes('retainedNewerLocal'), 'compatibility engine must hydrate durable state without overwriting newer local state'],
  [modelRecovery.includes('stale_model_call_recovered') && modelRecovery.includes("quote.status !== 'running'"), 'model recovery must fail stale running calls closed'],
  [adapters.includes('assertAdapterCertification') && adapters.includes('adapter_live_not_certified'), 'adapter contracts must enforce certification gates'],
  [replay.includes('replayMovingAverageCross') && replay.includes('validateHistoricalBars'), 'replay engine must expose deterministic replay and bar validation'],
  [providers.includes('/models') && providers.includes('/chat/completions'), 'local provider must implement the common OpenAI-compatible interface'],
  [providers.includes('estimatedQueueSeconds') && providers.includes('estimatedCostUsd'), 'local routing must estimate queue and economic cost'],
  [intelligenceExecution.includes('store.mutate(state => markRunning') && intelligenceExecution.includes('provider.execute(prepared)'), 'provider I/O must be separated from mutation transactions'],
  [intelligenceExecution.indexOf('provider.execute(prepared)') > intelligenceExecution.indexOf('store.mutate(state => markRunning'), 'provider execution must happen after the reservation mutation'],
  [economicsContract.includes('A model quote may authorize purchasing intelligence; it never authorizes a trade'), 'economic contract must separate intelligence and trade authorization'],
  [checklist.includes('Migration 006') && checklist.includes('append-only') && checklist.includes('expected version'), 'release checklist must include normalized execution gates'],
  [checklist.includes('MODEL_CALL_STALE_SECONDS') && checklist.includes('FOR UPDATE SKIP LOCKED'), 'release checklist must include model recovery and durable job queue gates'],
  [checklistLower.includes('local-first') && checklist.includes('REMOTE_LLM_EXECUTION_ENABLED=false'), 'release checklist must document local-first inference and remote-off defaults'],
  [checklist.includes('npm test') && checklist.includes('npm run build') && checklist.includes('npm run operational:validate'), 'release checklist must include locked test, build, and operational validation gates'],
  [checklist.includes('postgres-readiness-<sha>') && checklist.includes('release-readiness'), 'release checklist must require exact-head PostgreSQL and aggregate CI evidence'],
  [checklist.includes('pg_dump') && checklist.includes('pg_restore'), 'release checklist must include backup and restore'],
  [postgresSmoke.includes('RUN_POSTGRES_INTEGRATION') && postgresSmoke.includes('006_normalized_execution_runtime'), 'real PostgreSQL integration must be explicit and verify migration 006'],
  [postgresSmoke.includes('fresh execution engine') && postgresSmoke.includes('runtimeJobs.heartbeat'), 'PostgreSQL integration must verify restart hydration and durable job heartbeat'],
  [runtimeSmoke.includes('policyPersistedAcrossRestart') && runtimeSmoke.includes('productionPaperReady'), 'production runtime smoke must verify persistent policy and readiness after restart'],
  [workerHealth.includes('economic_worker_heartbeat_stale') && workerHealth.includes('economic_worker_last_run_failed'), 'worker health must detect stale and failed process state'],
  [operationalValidator.includes('backupRestoreEvidence') && operationalValidator.includes('rollbackRunbook'), 'operational validator must preserve backup/restore and rollback evidence'],
  [deploymentRunbook.includes('Stop conditions before deployment') && deploymentRunbook.includes('Rollback decision triggers'), 'deployment runbook must define stop and rollback decisions'],
  [readinessMatrix.includes('Provider-call idempotency') && readinessMatrix.includes('Blocking — manual'), 'readiness matrix must retain engineering and host-only blockers'],
  [workflow.includes('postgres-integration:') && workflow.includes('release-readiness:') && workflow.includes('pg_restore'), 'CI must include PostgreSQL, aggregate readiness, and restore proof'],
  [compose.includes('LIVE_TRADING: "false"') && compose.includes('COINBASE_DRY_RUN: "true"'), 'canonical deployment must remain paper-only'],
  [compose.includes('LOCAL_LLM_EXECUTION_REQUIRED: ${LOCAL_LLM_EXECUTION_REQUIRED:-true}'), 'canonical deployment must require local inference by default'],
  [compose.includes('REMOTE_LLM_EXECUTION_ENABLED: ${REMOTE_LLM_EXECUTION_ENABLED:-false}') && compose.includes('REMOTE_LLM_EXECUTION_ENABLED: "false"'), 'canonical deployment must keep remote inference off by default while allowing explicit opt-in'],
  [compose.includes('OPENROUTER_API_KEY: ${OPENROUTER_API_KEY:-}'), 'canonical deployment must pass an operator-supplied OpenRouter key without embedding it in source'],
  [compose.includes('scripts/check-economic-worker-health.mjs'), 'canonical deployment must enforce worker heartbeat health'],
  [compose.includes('MODEL_CALL_STALE_SECONDS') && compose.includes('ECONOMIC_JOB_MAX_ATTEMPTS'), 'canonical deployment must pass recovery and retry controls'],
];
for (const [ok, message] of checks) if (!ok) errors.push(message);

if (errors.length) {
  process.stderr.write(`${JSON.stringify({ ok: false, errors }, null, 2)}\n`);
  process.exit(1);
}
process.stdout.write(`${JSON.stringify({
  ok: true,
  certificationTarget: 'production-paper',
  liveTradingCertified: false,
  migrationsRequired: 6,
  localInferenceDefaultRequired: true,
  remoteInferenceDefault: false,
  remoteInferenceConfigurable: true,
  normalizedExecutionRequired: true,
  postgresRestartHydrationRequired: true,
  authenticatedBrowserRestartSmokeRequired: true,
  backupRestoreEvidenceRequired: true,
  workerHeartbeatRequired: true,
  rollbackRunbookRequired: true,
}, null, 2)}\n`);
