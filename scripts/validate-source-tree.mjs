import { execFileSync } from 'node:child_process';

const tracked = execFileSync('git', ['ls-files'], { encoding: 'utf8' })
  .split('\n')
  .map(value => value.trim())
  .filter(Boolean);

const approvedPrefixes = ['tests/fixtures/', 'test-data/'];
const approvedExact = new Set(['data/README.md', 'data/.gitkeep']);
const forbiddenPrefixes = [
  'data/state_backups/',
  'data/competition_archive/',
  'portfolio-management/data/',
  'logs/',
  'runtime/',
];
const forbiddenExact = new Set([
  'data/equity_summary.json',
  'data/experiment_proposals.json',
  'data/hot_scores_v4.json',
  'data/live_performance.json',
  'data/strategy_analytics.json',
  'data/hermes_agent_ledger.json',
  'data/paper_trader_v4_state.json',
  'data/competition_state.json',
]);

function approved(path) {
  return approvedExact.has(path)
    || approvedPrefixes.some(prefix => path.startsWith(prefix))
    || path.endsWith('.example.json');
}

function forbidden(path) {
  return forbiddenExact.has(path)
    || forbiddenPrefixes.some(prefix => path.startsWith(prefix))
    || path.endsWith('.log')
    || path.endsWith('.pid')
    || path.endsWith('.sock');
}

const violations = tracked.filter(path => !approved(path) && forbidden(path));
if (violations.length) {
  console.error('source-tree validation failed: generated runtime files are tracked');
  for (const path of violations) console.error(` - ${path}`);
  process.exit(1);
}
console.log(`source-tree validation ok: ${tracked.length} tracked paths inspected`);
