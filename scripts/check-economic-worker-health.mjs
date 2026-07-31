#!/usr/bin/env node
import { readFileSync } from 'node:fs';

const healthFile = process.env.ECONOMIC_WORKER_HEALTH_FILE || '/tmp/economic-worker-health.json';
const intervalMs = Math.max(10000, Number(process.env.ECONOMIC_MAINTENANCE_INTERVAL_MS || 60000));
const maximumAgeMs = Math.max(
  intervalMs * 2,
  Number(process.env.ECONOMIC_WORKER_HEALTH_MAX_AGE_MS || intervalMs * 3),
);

function fail(error, details = {}) {
  process.stderr.write(`${JSON.stringify({ ok: false, error, healthFile, maximumAgeMs, ...details }, null, 2)}\n`);
  process.exit(1);
}

let health;
try {
  health = JSON.parse(readFileSync(healthFile, 'utf8'));
} catch (error) {
  fail('economic_worker_health_file_unavailable', { detail: String(error?.message || error) });
}

const heartbeatAt = new Date(health.lastCompletedAt || health.updatedAt || 0).getTime();
const ageMs = Date.now() - heartbeatAt;
if (!Number.isFinite(heartbeatAt) || heartbeatAt <= 0) fail('economic_worker_heartbeat_invalid', { health });
if (ageMs > maximumAgeMs) fail('economic_worker_heartbeat_stale', { ageMs, health });
if (health.processHealthy !== true) fail('economic_worker_last_run_failed', { ageMs, health });
if (health.status === 'stopping') fail('economic_worker_stopping', { ageMs, health });

process.stdout.write(`${JSON.stringify({
  ok: true,
  healthFile,
  workerId: health.workerId || null,
  status: health.status || 'unknown',
  lastRunOutcome: health.lastRunOutcome || null,
  ageMs,
  maximumAgeMs,
}, null, 2)}\n`);
