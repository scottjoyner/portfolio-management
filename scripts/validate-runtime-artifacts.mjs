import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

export const ALLOWED_PREFIXES = [
  'tests/fixtures/',
  'trading_system/tests/fixtures/',
  'data/fixtures/',
];

export const FORBIDDEN_PATTERNS = [
  /^data\/state_backups\//,
  /^data\/legacy_agent_ledgers\//,
  /^data\/competition_epochs\//,
  /^portfolio-management\/data\//,
  /^data\/.*_state\.json(?:\.bak.*)?$/,
  /^data\/.*(?:cache|heartbeat).*$/,
  /^data\/(?:operator-state|operator-actions|pending_approvals|ranking_state|paper-trades|capital_buckets|equity_summary|experiment_proposals|hot_scores_v4|live_performance|strategy_analytics|hermes_agent_ledger|agent_cost_ledger|competition_epoch|competition_state|system-health|bot_killed_strategies)\.json$/,
  /^data\/.*\.(?:log|pid|sock|sqlite|sqlite3|db|db-shm|db-wal)$/,
  /(?:^|\/)(?:\.env|[^/]+\.pem|[^/]+\.key|[^/]+\.key\.json)$/,
];

export function isForbiddenTrackedPath(path) {
  if (ALLOWED_PREFIXES.some(prefix => path.startsWith(prefix))) return false;
  return FORBIDDEN_PATTERNS.some(pattern => pattern.test(path));
}

export function findForbiddenTrackedPaths(paths) {
  return [...new Set(paths.filter(Boolean).filter(isForbiddenTrackedPath))].sort();
}

export function trackedPaths() {
  const output = execFileSync('git', ['ls-files', '-z'], { encoding: 'utf8' });
  return output.split('\0').filter(Boolean);
}

export function validateRuntimeArtifacts(paths = trackedPaths()) {
  const forbidden = findForbiddenTrackedPaths(paths);
  return { ok: forbidden.length === 0, forbidden };
}

function main() {
  const result = validateRuntimeArtifacts();
  if (!result.ok) {
    console.error('runtime artifact policy failed; generated or sensitive files are tracked:');
    for (const path of result.forbidden) console.error(`  - ${path}`);
    console.error('Remove them from the index with git rm --cached and keep only sanitized fixtures.');
    process.exit(1);
  }
  console.log('runtime artifact policy ok: source tree contains no tracked runtime state');
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) main();
