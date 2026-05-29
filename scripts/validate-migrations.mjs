import { existsSync, readdirSync, readFileSync } from 'node:fs';

const migrationsDir = 'packages/storage/src/migrations';
const requiredTables = ['strategies', 'backtest_runs', 'approvals', 'positions', 'audit_events', 'operator_flags'];

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

if (!combined.includes('CREATE INDEX IF NOT EXISTS')) {
  console.error('migration validation failed: expected at least one index');
  process.exit(1);
}

console.log(`migration validation ok: ${migrations.length} migration(s), ${requiredTables.length} required tables`);
