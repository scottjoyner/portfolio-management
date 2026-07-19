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

function tradingMode(snapshot, snapshotFreshness) {
  const health = snapshotFreshness === 'fresh' ? snapshotMode(snapshot) : null;
  return health
    ? { value: health, source: 'system_health_snapshot', status: 'ok' }
    : { value: 'unknown', source: 'unknown', status: 'unknown' };
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

function unknownPaperBook() {
  return {
    gross_exposure_usd: null, open_positions: null, capital_in_play_usd: null, cash_usd: null,
    realized_pnl_usd: null, fees_paid_usd: null, state_age_sec: null, status: 'unknown', source: 'unknown',
  };
}

function unknownExecutionDecision() {
  return { value: 'unknown', status: 'unknown', source: 'unknown' };
}

function paperBook(snapshot, snapshotFreshness) {
  if (snapshotFreshness !== 'fresh') return unknownPaperBook();
  const book = snapshot?.trader?.paper_book;
  const gross = finiteNumber(book?.gross_exposure_usd);
  const positions = finiteNumber(book?.open_positions);
  const capital = finiteNumber(book?.capital_in_play_usd);
  const cash = finiteNumber(book?.cash_usd);
  const realized = finiteNumber(book?.realized_pnl_usd);
  const fees = finiteNumber(book?.fees_paid_usd);
  const stateAge = finiteNumber(book?.state_age_sec);
  if (!book || typeof book !== 'object' || Array.isArray(book) || gross === null || positions === null || capital === null || cash === null || realized === null || fees === null || stateAge === null || typeof book.status !== 'string' || book.status !== 'ok' || typeof book.source !== 'string' || !book.source) return unknownPaperBook();
  return {
    gross_exposure_usd: round(gross), open_positions: round(positions), capital_in_play_usd: round(capital),
    cash_usd: round(cash), realized_pnl_usd: round(realized), fees_paid_usd: round(fees),
    state_age_sec: round(stateAge, 1), status: 'ok', source: book.source,
  };
}

function executionDecision(snapshot, snapshotFreshness) {
  if (snapshotFreshness !== 'fresh') return unknownExecutionDecision();
  const decision = snapshot?.trader?.execution_decision;
  if (!decision || typeof decision !== 'object' || Array.isArray(decision) || typeof decision.value !== 'string' || !decision.value || typeof decision.status !== 'string' || !decision.status || typeof decision.source !== 'string' || !decision.source) return unknownExecutionDecision();
  return { value: decision.value, status: decision.status, source: decision.source };
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
export function buildSystemTruth({ env = {}, dataDir = DATA_ROOT, now = Date.now() / 1000 } = {}) {
  const warnings = [];
  const snapshotPath = join(dataDir, SNAPSHOT_FILE);
  const snapshotAge = fileAgeSeconds(snapshotPath, now);
  const snapshotData = readOptionalJson(snapshotPath);
  const snapshotFreshness = snapshotData ? freshness(snapshotAge) : 'unknown';
  const mode = tradingMode(snapshotData, snapshotFreshness);
  const feedHeartbeat = heartbeat(dataDir, now);
  const currentPaperBook = paperBook(snapshotData, snapshotFreshness);
  const currentExecutionDecision = executionDecision(snapshotData, snapshotFreshness);
  const terminalLink = terminal(env);

  if (feedHeartbeat.freshness !== 'fresh') warnings.push(`daemon heartbeat is ${feedHeartbeat.freshness}`);
  if (snapshotFreshness !== 'fresh') warnings.push(`system health snapshot is ${snapshotFreshness}`);
  if (currentPaperBook.status === 'unknown') warnings.push('paper book is unavailable from system health snapshot');
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
    paper_book: currentPaperBook,
    execution_decision: currentExecutionDecision,
    terminal: terminalLink,
    warnings,
  };
}
