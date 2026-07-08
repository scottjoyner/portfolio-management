import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

const migrationsDir = 'packages/storage/src/migrations';
const databaseUrl = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/arb';
const dryRun = process.argv.includes('--dry-run');

function die(message) {
  console.error(message);
  process.exit(1);
}

if (!existsSync(migrationsDir)) die(`missing migrations directory: ${migrationsDir}`);
const migrations = readdirSync(migrationsDir).filter(name => /^\d+_.*\.sql$/.test(name)).sort();
if (!migrations.length) die('no migrations found');

const prelude = `
CREATE TABLE IF NOT EXISTS schema_migrations (
  version TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`;

const sql = [prelude, ...migrations.map(name => {
  const version = name.replace(/\.sql$/, '');
  const body = readFileSync(`${migrationsDir}/${name}`, 'utf8');
  return `
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = '${version}') THEN
    RAISE NOTICE 'applying migration ${version}';
  END IF;
END $$;

BEGIN;
${body}
INSERT INTO schema_migrations (version) VALUES ('${version}') ON CONFLICT (version) DO NOTHING;
COMMIT;
`;
})].join('\n');

if (dryRun) {
  console.log(sql);
  process.exit(0);
}

const result = spawnSync('psql', [databaseUrl, '--set', 'ON_ERROR_STOP=1'], {
  input: sql,
  encoding: 'utf8',
  stdio: ['pipe', 'inherit', 'inherit']
});

if (result.error) {
  die(`failed to run psql: ${result.error.message}\nInstall the PostgreSQL client or run with --dry-run to inspect SQL.`);
}

if (result.status !== 0) die(`migration failed with exit code ${result.status}`);
console.log(`migrations applied: ${migrations.length}`);
