#!/usr/bin/env node
import { readdirSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const directory = resolve('packages/storage/src/migrations');
const files = readdirSync(directory)
  .filter(name => /^\d{3}_[a-z0-9_]+\.sql$/i.test(name))
  .sort((a, b) => a.localeCompare(b));

const errors = [];
const seenPrefixes = new Set();
const seenVersions = new Set();

for (let index = 0; index < files.length; index += 1) {
  const file = files[index];
  const expectedPrefix = String(index + 1).padStart(3, '0');
  const prefix = file.slice(0, 3);
  const version = file.replace(/\.sql$/i, '');
  const sql = readFileSync(resolve(directory, file), 'utf8');

  if (prefix !== expectedPrefix) errors.push(`migration_sequence_gap:${expectedPrefix}:${file}`);
  if (seenPrefixes.has(prefix)) errors.push(`duplicate_migration_prefix:${prefix}`);
  if (seenVersions.has(version)) errors.push(`duplicate_migration_version:${version}`);
  if (!sql.trim()) errors.push(`empty_migration:${file}`);
  if (!/^--\s*\d{3}_/m.test(sql)) errors.push(`migration_header_missing:${file}`);

  seenPrefixes.add(prefix);
  seenVersions.add(version);
}

const required = new Map([
  ['001_operator_state.sql', ['CREATE TABLE IF NOT EXISTS strategies', 'CREATE TABLE IF NOT EXISTS operator_flags']],
  ['002_operator_product_layer.sql', ['CREATE TABLE IF NOT EXISTS accounts', 'CREATE TABLE IF NOT EXISTS paper_executions']],
  ['004_opportunity_agent_workflow.sql', ['CREATE TABLE IF NOT EXISTS research_jobs', 'CREATE TABLE IF NOT EXISTS opportunities']],
  ['005_runtime_job_queue.sql', ['CREATE TABLE IF NOT EXISTS runtime_jobs', 'FOR UPDATE SKIP LOCKED']],
]);

for (const [file, tokens] of required) {
  if (!files.includes(file)) {
    errors.push(`required_migration_missing:${file}`);
    continue;
  }
  const sql = readFileSync(resolve(directory, file), 'utf8');
  for (const token of tokens) {
    // FOR UPDATE SKIP LOCKED belongs to the queue repository rather than DDL.
    if (token === 'FOR UPDATE SKIP LOCKED') continue;
    if (!sql.includes(token)) errors.push(`migration_contract_missing:${file}:${token}`);
  }
}

const queueRepository = readFileSync(resolve('packages/storage/src/runtimeJobQueue.mjs'), 'utf8');
for (const token of ['FOR UPDATE SKIP LOCKED', 'lease_expires_at', 'idempotency_key', "status = 'running'"]) {
  if (!queueRepository.includes(token)) errors.push(`runtime_job_queue_contract_missing:${token}`);
}

if (errors.length) {
  process.stderr.write(`${JSON.stringify({ ok: false, errors }, null, 2)}\n`);
  process.exit(1);
}

process.stdout.write(`${JSON.stringify({ ok: true, directory, migrations: files }, null, 2)}\n`);
