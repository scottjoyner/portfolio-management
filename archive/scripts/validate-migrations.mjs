import { existsSync, readdirSync, readFileSync } from 'node:fs';

const migrationsDir = 'packages/storage/src/migrations';
const requiredTables = [
  'strategies',
  'backtest_runs',
  'approvals',
  'positions',
  'audit_events',
  'operator_flags',
  'accounts',
  'instruments',
  'strategy_templates',
  'paper_executions',
  'adapter_certifications',
  'market_data_snapshots',
  'agent_budgets',
  'budget_approvals',
  'research_jobs',
  'opportunities',
  'risk_breakdowns',
  'agent_cost_ledger'
];

const requiredFragments = [
  'previous_hash TEXT',
  'event_hash TEXT',
  'sequence_number BIGINT',
  'idx_audit_events_event_hash',
  'idx_adapter_certifications_status',
  'idx_opportunities_status',
  'idx_budget_approvals_agent_status',
  'idx_research_jobs_budget_approval',
  'idx_agent_cost_ledger_agent',
  'confidence_score NUMERIC NOT NULL CHECK',
  'local_or_remote TEXT NOT NULL CHECK',
  'budget_approval_id TEXT REFERENCES budget_approvals(id)',
  'net_expected_value NUMERIC NOT NULL DEFAULT 0'
];

if (!existsSync(migrationsDir)) {
  console.error(`migration validation failed: missing ${migrationsDir}`);
  process.exit(1);
}

const migrations = readdirSync(migrationsDir).filter(name => /^\d+_.*\.sql$/.test(name)).sort();
if (!migrations.length) {
  console.error('migration validation failed: no numbered SQL migrations found');
  process.exit(1);
}

const combined = migrations.map(name => readFileSync(`${migrationsDir}/${name}`, 'utf8')).join('\n');
const missingTables = requiredTables.filter(table => !combined.includes(`CREATE TABLE IF NOT EXISTS ${table}`));
if (missingTables.length) {
  console.error('migration validation failed: missing tables', missingTables);
  process.exit(1);
}

const missingFragments = requiredFragments.filter(fragment => !combined.includes(fragment));
if (missingFragments.length) {
  console.error('migration validation failed: missing required fragments', missingFragments);
  process.exit(1);
}

if (!combined.includes('CREATE INDEX IF NOT EXISTS')) {
  console.error('migration validation failed: expected at least one index');
  process.exit(1);
}

console.log(`migration validation ok: ${migrations.length} migration(s), ${requiredTables.length} required tables`);
