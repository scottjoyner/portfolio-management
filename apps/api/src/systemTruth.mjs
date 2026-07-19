import { readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';

const DATA_ROOT = '/app/data';
const SNAPSHOT_FILE = 'system-health.json';
const HEARTBEAT_FILE = '.daemon_heartbeat';
const STALE_AFTER_SECONDS = 180;

function round(value, decimals = 2) {
  return Number(value.toFixed(decimals));
}

function finiteNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function freshness(ageSeconds) {
  if (ageSeconds === null) return 'unknown';
  return ageSeconds <= STALE_AFTER_SECONDS ? 'fresh' : 'stale';
}

function readOptionalJson(path) {
  try {
    const parsed = JSON.parse(readFileSync(path, 'utf8'));
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function fileAgeSeconds(path, now) {
  try {
    return Math.max(0, now - statSync(path).mtimeMs / 1000);
  } catch {
    return null;
  }
}

function explicitMode(value) {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return normalized === 'paper' || normalized === 'live' ? normalized : null;
}

function snapshotMode(snapshot) {
  return explicitMode(snapshot?.trading_mode) || explicitMode(snapshot?.tradingMode) || explicitMode(snapshot?.mode);
}

function localMode(state) {
  return explicitMode(state?.tradingMode) || explicitMode(state?.mode) || explicitMode(state?.config?.tradingMode) || explicitMode(state?.config?.trading_mode);
}

function tradingMode(state, snapshot, snapshotFreshness) {
  const local = localMode(state);
  const health = snapshotFreshness === 'fresh' ? snapshotMode(snapshot) : null;
  if (local && health && local !== health) return { value: 'unknown', source: 'mode_conflict', status: 'warn', warning: 'trading mode evidence conflicts' };
  if (local && health) return { value: local, source: 'system_health_snapshot+local_state', status: 'warn' };
  if (health) return { value: health, source: 'system_health_snapshot', status: 'warn' };
  if (local) return { value: local, source: 'local_state', status: 'warn' };
  return { value: 'unknown', source: 'local_state', status: 'warn', warning: 'trading mode is unknown' };
}

function heartbeat(dataDir, now) {
  const path = join(dataDir, HEARTBEAT_FILE);
  let age = fileAgeSeconds(path, now);
  if (age !== null) {
    try {
      const timestamp = finiteNumber(readFileSync(path, 'utf8').trim());
      age = timestamp === null ? null : Math.max(0, now - timestamp);
    } catch {
      age = null;
    }
  }
  return { source: 'daemon_heartbeat', age_sec: age === null ? null : round(age, 1), freshness: freshness(age) };
}

function cacheTruth() {
  return {
    source: 'container_data_mount',
    status: 'unknown',
    readable: false,
    configured_root: null,
    resolved_root: null,
    fallback: null,
    message: 'cache/NAS health is not observable from this API container',
  };
}

function markedExposure(state) {
  const positions = Array.isArray(state?.positions) ? state.positions : null;
  if (!positions) return { gross_exposure_usd: null, open_positions: null, status: 'unknown', source: 'operator_state_marked_positions' };
  let gross = 0;
  let openPositions = 0;
  let unmarked = false;
  for (const position of positions) {
    if (!position || typeof position !== 'object' || String(position.status || 'open').toLowerCase() !== 'open') continue;
    openPositions += 1;
    const quantity = finiteNumber(position.quantity);
    const mark = finiteNumber(position.markPrice ?? position.mark_price);
    if (quantity === null || mark === null) {
      unmarked = true;
      continue;
    }
    gross += Math.abs(quantity * mark);
  }
  const capital = finiteNumber(state?.capitalInPlayUsd ?? state?.capital_in_play_usd);
  return {
    gross_exposure_usd: unmarked ? null : round(gross),
    open_positions: openPositions,
    status: unmarked ? 'unknown' : 'ok',
    source: 'operator_state_marked_positions',
    capital_in_play_usd: capital === null ? null : round(capital),
    capital_in_play_source: capital === null ? 'unknown' : 'operator_state',
  };
}

function terminal(env) {
  const configured = env.TRADING_TERMINAL_URL;
  if (configured === undefined) return { url: '/dashboard', source: 'dashboard_default', status: 'ok' };
  const value = typeof configured === 'string' ? configured.trim() : '';
  if (value.startsWith('/') && !value.startsWith('//') && !value.includes('\\')) return { url: value, source: 'TRADING_TERMINAL_URL', status: 'ok' };
  return { url: '/dashboard', source: 'dashboard_default', status: 'warn' };
}

/**
 * Build read-only operator diagnostics. Production always reads only /app/data;
 * dataDir is a test seam and is never sourced from environment configuration.
 */
export function buildSystemTruth({ state, env = {}, dataDir = DATA_ROOT, now = Date.now() / 1000 } = {}) {
  const warnings = [];
  const snapshotPath = join(dataDir, SNAPSHOT_FILE);
  const snapshotAge = fileAgeSeconds(snapshotPath, now);
  const snapshotData = readOptionalJson(snapshotPath);
  const snapshotFreshness = snapshotData ? freshness(snapshotAge) : 'unknown';
  const mode = tradingMode(state, snapshotData, snapshotFreshness);
  const feedHeartbeat = heartbeat(dataDir, now);
  const exposure = markedExposure(state);
  const terminalLink = terminal(env);

  if (mode.warning) warnings.push(mode.warning);
  if (feedHeartbeat.freshness !== 'fresh') warnings.push(`daemon heartbeat is ${feedHeartbeat.freshness}`);
  if (snapshotFreshness !== 'fresh') warnings.push(`system health snapshot is ${snapshotFreshness}`);
  if (exposure.status !== 'ok') warnings.push('gross exposure has unmarked positions');
  if (terminalLink.status !== 'ok') warnings.push('unsafe terminal URL; using dashboard default');
  warnings.push('cache/NAS health is unknown from this API container');

  return {
    generated_at: new Date(now * 1000).toISOString(),
    trading_mode: { value: mode.value, source: mode.source, status: mode.status },
    feed: { heartbeat: feedHeartbeat },
    cache: cacheTruth(),
    services: {
      trader: snapshotFreshness === 'fresh' && snapshotData?.trader
        ? { ...snapshotData.trader, source: 'system_health_snapshot' }
        : { available: false, source: 'not_probed', status: 'unknown' },
      snapshot: {
        source: 'system_health_snapshot',
        age_sec: snapshotAge === null ? null : round(snapshotAge, 1),
        freshness: snapshotFreshness,
        data: snapshotData,
      },
    },
    exposure,
    terminal: terminalLink,
    warnings,
  };
}
