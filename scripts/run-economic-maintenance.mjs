#!/usr/bin/env node
import { createOperatorStore } from '../packages/storage/src/operatorStoreFactory.mjs';
import { runEconomicMaintenance } from '../apps/api/src/economicMaintenance.mjs';

function hasFlag(name) {
  return process.argv.slice(2).includes(name);
}

function print(value) {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

const once = hasFlag('--once');
const intervalMs = Math.max(10000, Number(process.env.ECONOMIC_MAINTENANCE_INTERVAL_MS || 60000));
const store = createOperatorStore();
let stopping = false;
let running = false;

async function run() {
  if (running) return { ok: false, skipped: true, reason: 'economic_maintenance_already_running' };
  running = true;
  try {
    const result = await store.mutate(state => runEconomicMaintenance(state, { env: process.env }));
    print({ worker: 'economic-maintenance', intervalMs, ...result });
    return result;
  } catch (error) {
    const failure = { ok: false, worker: 'economic-maintenance', error: String(error?.message || error), at: new Date().toISOString() };
    print(failure);
    if (once) process.exitCode = 1;
    return failure;
  } finally {
    running = false;
  }
}

async function shutdown(signal) {
  if (stopping) return;
  stopping = true;
  print({ worker: 'economic-maintenance', status: 'stopping', signal, at: new Date().toISOString() });
  process.exit(0);
}

process.on('SIGINT', () => { shutdown('SIGINT'); });
process.on('SIGTERM', () => { shutdown('SIGTERM'); });

await run();
if (!once) {
  const timer = setInterval(() => { run().catch(() => {}); }, intervalMs);
  process.on('beforeExit', () => clearInterval(timer));
}
