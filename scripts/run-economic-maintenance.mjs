#!/usr/bin/env node
import { hostname } from 'node:os';

import { createOperatorStore } from '../packages/storage/src/operatorStoreFactory.mjs';
import { runEconomicMaintenance } from '../apps/api/src/economicMaintenance.mjs';

function hasFlag(name) {
  return process.argv.slice(2).includes(name);
}

function print(value) {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const once = hasFlag('--once');
const intervalMs = Math.max(10000, Number(process.env.ECONOMIC_MAINTENANCE_INTERVAL_MS || 60000));
const leaseSeconds = Math.max(60, Number(process.env.ECONOMIC_JOB_LEASE_SECONDS || 300));
const retryDelaySeconds = Math.max(5, Number(process.env.ECONOMIC_JOB_RETRY_SECONDS || 60));
const workerId = process.env.ECONOMIC_WORKER_ID || `${hostname()}:${process.pid}`;
const store = createOperatorStore();
let stopping = false;
let activeRun = null;
let timer = null;

function maintenanceJobKey(now = new Date()) {
  const bucket = Math.floor(now.getTime() / intervalMs);
  return `economic-maintenance:${bucket}`;
}

async function executeMaintenance() {
  const result = await store.mutate(state => runEconomicMaintenance(state, { env: process.env }));
  return { worker: 'economic-maintenance', workerId, intervalMs, ...result };
}

async function runLeased() {
  const queue = store.runtimeJobs;
  const now = new Date();
  await queue.recoverExpiredLeases({ now, retryDelaySeconds: 5 });
  await queue.enqueue({
    jobType: 'economic-maintenance',
    scope: 'global',
    priority: 10,
    scheduledAt: now,
    maxAttempts: Math.max(1, Number(process.env.ECONOMIC_JOB_MAX_ATTEMPTS || 5)),
    idempotencyKey: maintenanceJobKey(now),
    payload: { intervalMs, requestedAt: now.toISOString() },
    now,
  });

  const job = await queue.claim({
    workerId,
    jobTypes: ['economic-maintenance'],
    leaseSeconds,
    now,
  });
  if (!job) return { ok: true, skipped: true, reason: 'no_economic_maintenance_job_available', workerId };

  let heartbeat = null;
  try {
    heartbeat = setInterval(() => {
      queue.heartbeat({ jobId: job.id, workerId, leaseSeconds }).catch(error => {
        print({ ok: false, worker: 'economic-maintenance', workerId, jobId: job.id, warning: 'job_heartbeat_failed', error: String(error?.message || error), at: new Date().toISOString() });
      });
    }, Math.max(10000, Math.floor(leaseSeconds * 1000 / 3)));
    heartbeat.unref?.();

    const result = await executeMaintenance();
    const completed = await queue.complete({ jobId: job.id, workerId, result });
    return { ...result, runtimeJob: completed };
  } catch (error) {
    const failed = await queue.fail({
      jobId: job.id,
      workerId,
      error,
      retryDelaySeconds,
    }).catch(() => null);
    error.runtimeJob = failed;
    throw error;
  } finally {
    if (heartbeat) clearInterval(heartbeat);
  }
}

async function runFallback() {
  return executeMaintenance();
}

async function run() {
  if (activeRun) return { ok: false, skipped: true, reason: 'economic_maintenance_already_running', workerId };
  activeRun = (async () => {
    try {
      const result = store.runtimeJobs ? await runLeased() : await runFallback();
      print(result);
      return result;
    } catch (error) {
      const failure = {
        ok: false,
        worker: 'economic-maintenance',
        workerId,
        error: String(error?.message || error),
        runtimeJob: error.runtimeJob || null,
        at: new Date().toISOString(),
      };
      print(failure);
      if (once) process.exitCode = 1;
      return failure;
    } finally {
      activeRun = null;
    }
  })();
  return activeRun;
}

async function shutdown(signal) {
  if (stopping) return;
  stopping = true;
  if (timer) clearInterval(timer);
  print({ worker: 'economic-maintenance', workerId, status: 'stopping', signal, at: new Date().toISOString() });
  if (activeRun) await Promise.race([activeRun, sleep(Math.max(1000, Number(process.env.ECONOMIC_SHUTDOWN_GRACE_MS || 30000)))]);
  await store.close?.().catch(() => {});
}

process.on('SIGINT', () => { shutdown('SIGINT').finally(() => process.exit(0)); });
process.on('SIGTERM', () => { shutdown('SIGTERM').finally(() => process.exit(0)); });

await run();
if (!once) {
  timer = setInterval(() => {
    if (!stopping) run().catch(() => {});
  }, intervalMs);
  timer.unref?.();
  // Keep the process alive even though the timer is unref'd so shutdown signals
  // can drain the active lease and close the PostgreSQL pool cleanly.
  await new Promise(resolve => {
    process.once('beforeExit', resolve);
  });
} else {
  await store.close?.().catch(() => {});
}
