#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { readdirSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const MIGRATION_DIR = resolve('packages/storage/src/migrations');
const MIGRATION_LOCK_KEY = Number(process.env.POSTGRES_MIGRATION_LOCK_KEY || 0x504d4752);
const dryRun = process.argv.includes('--dry-run');
const jsonOutput = process.argv.includes('--json');

function migrationFiles() {
  return readdirSync(MIGRATION_DIR)
    .filter(name => /^\d{3}_[a-z0-9_]+\.sql$/i.test(name))
    .sort((a, b) => a.localeCompare(b));
}

function checksum(content) {
  return createHash('sha256').update(content).digest('hex');
}

function plan() {
  return migrationFiles().map(file => {
    const sql = readFileSync(resolve(MIGRATION_DIR, file), 'utf8');
    if (!sql.trim()) throw new Error(`empty_migration:${file}`);
    return {
      file,
      version: file.replace(/\.sql$/i, ''),
      checksum: checksum(sql),
      bytes: Buffer.byteLength(sql),
      sql,
    };
  });
}

function print(value) {
  if (jsonOutput || typeof value !== 'string') process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
  else process.stdout.write(`${value}\n`);
}

async function migrate() {
  const migrations = plan();
  if (dryRun) {
    print({ ok: true, dryRun: true, migrationDirectory: MIGRATION_DIR, migrations: migrations.map(({ sql, ...row }) => row) });
    return;
  }

  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) throw new Error('DATABASE_URL_required_for_postgres_migrations');

  let pg;
  try {
    pg = await import('pg');
  } catch {
    throw new Error('pg_dependency_missing: run pnpm install before migrations');
  }

  const pool = new pg.Pool({ connectionString: databaseUrl, max: 2 });
  const client = await pool.connect();
  const applied = [];
  const skipped = [];
  try {
    await client.query('SELECT pg_advisory_lock($1)', [MIGRATION_LOCK_KEY]);
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version TEXT PRIMARY KEY,
        checksum TEXT,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    `);
    await client.query('ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum TEXT');

    for (const migration of migrations) {
      const existing = await client.query('SELECT version, checksum FROM schema_migrations WHERE version = $1', [migration.version]);
      if (existing.rows?.[0]) {
        const existingChecksum = existing.rows[0].checksum;
        if (existingChecksum && existingChecksum !== migration.checksum) {
          throw new Error(`migration_checksum_mismatch:${migration.version}`);
        }
        if (!existingChecksum) {
          await client.query('UPDATE schema_migrations SET checksum = $2 WHERE version = $1', [migration.version, migration.checksum]);
        }
        skipped.push(migration.version);
        continue;
      }

      await client.query('BEGIN');
      try {
        await client.query(migration.sql);
        await client.query(
          'INSERT INTO schema_migrations (version, checksum, applied_at) VALUES ($1,$2,now())',
          [migration.version, migration.checksum],
        );
        await client.query('COMMIT');
        applied.push(migration.version);
      } catch (error) {
        await client.query('ROLLBACK').catch(() => {});
        throw new Error(`migration_failed:${migration.version}:${error.message || error}`);
      }
    }

    print({ ok: true, database: 'configured', applied, skipped, total: migrations.length });
  } finally {
    await client.query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_KEY]).catch(() => {});
    client.release();
    await pool.end();
  }
}

migrate().catch(error => {
  process.stderr.write(`${JSON.stringify({ ok: false, error: error.message || String(error) }, null, 2)}\n`);
  process.exitCode = 1;
});
