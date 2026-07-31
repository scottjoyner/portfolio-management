#!/usr/bin/env node
import { hostname } from 'node:os';

import { createOperatorStore } from '../packages/storage/src/operatorStoreFactory.mjs';
import {
  fetchOpenRouterCatalog,
  runEconomicMaintenance,
} from '../apps/api/src/economicMaintenance.mjs';
import { recoverStaleModelCalls } from '../apps/api/src/modelCallRecovery.mjs';

function hasFlag(name) {
  return process.argv.slice(2).includes(name);
}

function print(value) {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function positive(value, fallback) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : fallback;
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

function pricingCatalogIsStale(state, now = new Date()) {
  const snapshots = Array.isArray(state?.modelPricingSnapshots) ? state.modelPricingSnapshots : [];
  const latest = [...snapshots].sort((a, b) => new Date(b.fetchedAt || 0) - new Date(a.fetchedAt || 0))[0];
  if (!latest) return true;
  const maximumAgeSeconds = positive(
    process.env.ECONOMIC_PRICING_REFRESH_SECONDS,
    positive(state?.config?.economicPricingRefreshSeconds, 21600),
  );
  return now.getTime() - new Date(latest.fetchedAt || 0).getTime() > maximumAgeSeconds * 1000;
}

async function fetchMarketQuotes() {
  const { fetchQuotes } = await import('../packages/execution/src/paperSweeper.mjs');
  return fetchQuotes();
}

async function prepareMaintenanceInputs(stateSnapshot, now = new Date()) {
  const warnings = [];
  let catalog = { data: [] };
  let quotes = {};

  if (pricingCatalogIsStale(stateSnapshot, now)) {
    try {
      catalog = await fetchOpenRouterCatalog(process.env, globalThis.fetch);
    } catch (error) {
      warnings.push(`pricing_refresh_failed:${String(error?.message || error)}`);
    }
  }

  try {
    quotes = await fetchMarketQuotes();
  } catch (error) {
    warnings.push(`market_quote_refresh_failed:${String(error?.message || error)}`);
  }

  return {
    catalog,
    quotes,
    warnings,
    preparedAt: new Date().toISOString(),
  };
}

function mergePreparationWarnings(state, maintenance, warnings = []) {
  if (!warnings.length) return maintenance;
  maintenance.warnings = [...new Set([...(maintenance.warnings || []), ...warnings])];
  maintenance.ok = false;
  state.economicMaintenance = {
    ...(state.economicMaintenance || {}),
    status: 'degraded',
    warnings: maintenance.warnings,
  };
  const audit = state.audit?.at?.(-1);
  if (audit?.action === 'economic_maintenance_completed') {
    audit.details = 'degraded';
    audit.payload = state.economicMaintenance.counters || audit.payload;
  }
  return maintenance;
}

async function executeMaintenance() {
  // External provider and market-data calls are deliberately completed before
  // entering the serializable mutation. The mutation holds a transaction-scoped
  // advisory lock and must never wait on network I/O.
  const stateSnapshot = await store.load();
  const prepared = await prepareMaintenanceInputs(stateSnapshot);

  const result = await store.mutate(async state => {
    const modelCallRecovery = recoverStaleModelCalls(state, {
      env: process.env,
      actor: workerId,
    });
    const maintenance = await runEconomicMaintenance(state, {
      env: process.env,
      catalog: prepared.catalog,
      quotes: prepared.quotes,
    });
    mergePreparationWarnings(state, maintenance, prepared.warnings);
    return {
      ...maintenance,
      modelCallRecovery,
      externalInputsPreparedAt: prepared.preparedAt,
    };
  });
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
        print({
          ok: false,
          worker: 'economic-maintenance',
          workerId,
          jobId: job.id,
          warning: 'job_heartbeat_failed',
          error: String(error?.message || error),
          at: new Date().toISOString(),
        });
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
  if (activeRun) {
    await Promise.race([
      activeRun,
      sleep(Math.max(1000, Number(process.env.ECONOMIC_SHUTDOWN_GRACE_MS || 30000))),
    ]);
  }
  await store.close?.().catch(() => {});
}

process.on('SIGINT', () => { shutdown('SIGINT').finally(() => process.exit(0)); });
process.on('SIGTERM', () => { shutdown('SIGTERM').finally(() => process.exit(0)); });

await run();
if (!once) {
  timer = setInterval(() => {
    if (!stopping) run().catch(() => {});
  }, intervalMs);
} else {
  await store.close?.().catch(() => {});
}
