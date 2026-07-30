#!/usr/bin/env node
import { existsSync, readFileSync } from 'node:fs';

const requiredFiles = [
  'packages/storage/src/migrations/003_audit_and_certification.sql',
  'packages/storage/src/migrations/004_opportunity_agent_workflow.sql',
  'packages/storage/src/migrations/005_runtime_job_queue.sql',
  'packages/storage/src/auditChain.mjs',
  'packages/storage/src/transactionalPostgresOperatorStore.mjs',
  'packages/storage/src/runtimeJobQueue.mjs',
  'packages/adapters/src/contracts.mjs',
  'packages/backtesting/src/replayEngine.mjs',
  'packages/intelligence/src/providerRegistry.mjs',
  'apps/api/src/intelligenceExecution.mjs',
  'docs/FIRST_PROD_RELEASE_CHECKLIST.md',
  'docs/API_CONTRACT_ECONOMICS.md',
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
const audit = read(requiredFiles[3]);
const transactional = read(requiredFiles[4]);
const jobs = read(requiredFiles[5]);
const adapters = read(requiredFiles[6]);
const replay = read(requiredFiles[7]);
const providers = read(requiredFiles[8]);
const execution = read(requiredFiles[9]);
const checklist = read(requiredFiles[10]);
const economicsContract = read(requiredFiles[11]);
const compose = read(requiredFiles[12]);

const checks = [
  [migration3.includes('adapter_certifications'), 'migration 003 must include adapter certifications'],
  [migration3.includes('previous_hash') && migration3.includes('event_hash') && migration3.includes('sequence_number'), 'migration 003 must include audit hash-chain fields'],
  [migration4.includes('research_jobs') && migration4.includes('opportunities') && migration4.includes('agent_cost_ledger'), 'migration 004 must include opportunity and research workflow tables'],
  [migration5.includes('runtime_jobs') && migration5.includes('idempotency_key') && migration5.includes('lease_expires_at'), 'migration 005 must include the durable job queue'],
  [audit.includes('verifyAuditChain') && audit.includes('hashAuditEvent'), 'audit helper must verify and hash events'],
  [transactional.includes('BEGIN ISOLATION LEVEL') && transactional.includes('pg_advisory_xact_lock'), 'operator store must pin serializable transactions'],
  [transactional.includes('005_runtime_job_queue'), 'operator store must require migration 005'],
  [jobs.includes('FOR UPDATE SKIP LOCKED') && jobs.includes('heartbeat') && jobs.includes('recoverExpired'), 'runtime queue must claim, heartbeat, and recover jobs'],
  [adapters.includes('assertAdapterCertification') && adapters.includes('adapter_live_not_certified'), 'adapter contracts must enforce certification gates'],
  [replay.includes('replayMovingAverageCross') && replay.includes('validateHistoricalBars'), 'replay engine must expose deterministic replay and bar validation'],
  [providers.includes('/models') && providers.includes('/chat/completions'), 'local provider must implement the common OpenAI-compatible interface'],
  [providers.includes('estimatedQueueSeconds') && providers.includes('estimatedCostUsd'), 'local routing must estimate queue and economic cost'],
  [execution.includes('store.mutate(state => markRunning') && execution.includes('provider.execute(prepared)'), 'provider I/O must be separated from mutation transactions'],
  [execution.indexOf('provider.execute(prepared)') > execution.indexOf('store.mutate(state => markRunning'), 'provider execution must happen after the reservation mutation'],
  [economicsContract.includes('A model quote may authorize purchasing intelligence; it never authorizes a trade'), 'economic contract must separate intelligence and trade authorization'],
  [checklist.includes('Migration 005') && checklist.includes('FOR UPDATE SKIP LOCKED'), 'release checklist must include durable job queue gates'],
  [checklist.includes('LOCAL_LLM_EXECUTION_REQUIRED') && checklist.includes('REMOTE_LLM_EXECUTION_ENABLED=false'), 'release checklist must document local-first inference'],
  [checklist.includes('pnpm test') && checklist.includes('pnpm build'), 'release checklist must include test and build gates'],
  [checklist.includes('pg_dump') && checklist.includes('pg_restore'), 'release checklist must include backup and restore'],
  [compose.includes('LIVE_TRADING: "false"') && compose.includes('COINBASE_DRY_RUN: "true"'), 'canonical deployment must remain paper-only'],
  [compose.includes('REMOTE_LLM_EXECUTION_ENABLED: "false"') && compose.includes('LOCAL_LLM_EXECUTION_REQUIRED: "true"'), 'canonical deployment must remain local-first'],
];
for (const [ok, message] of checks) if (!ok) errors.push(message);

if (errors.length) {
  process.stderr.write(`${JSON.stringify({ ok: false, errors }, null, 2)}\n`);
  process.exit(1);
}
process.stdout.write(`${JSON.stringify({ ok: true, certificationTarget: 'production-paper', liveTradingCertified: false, migrationsRequired: 5, localInferenceRequired: true }, null, 2)}\n`);
