#!/usr/bin/env node
import fs from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { migrateStoredAuthorizations } from '../packages/execution/src/overseerMigration.mjs';

/**
 * Re-mint stored overseer authorizations that were issued under a superseded
 * admission policy (G-012).
 *
 * Dry-run by default. This rewrites security-relevant state, so it refuses to
 * write anything without --apply, and it never writes a state that does not
 * verify under the current policy afterwards.
 *
 * The overseer options passed here MUST match the ones the deployment uses. A
 * migration run with different options would re-mint decisions production would
 * never have made, which is worse than not migrating at all.
 *
 * Usage:
 *   node scripts/migrate-overseer-authorizations.mjs --input <states.json> \
 *     [--output <migrated.json>] [--require-approval true|false] --apply
 */

const { values } = parseArgs({
  options: {
    input: { type: 'string' },
    output: { type: 'string' },
    'require-approval': { type: 'string' },
    'min-confidence': { type: 'string' },
    now: { type: 'string' },
    apply: { type: 'boolean', default: false }
  }
});

function fail(code, detail = {}) {
  process.stderr.write(`${JSON.stringify({ ok: false, error: code, ...detail }, null, 2)}\n`);
  process.exit(1);
}

if (!values.input) fail('overseer_migration_input_required');
if (values['require-approval'] === undefined) {
  fail('overseer_migration_overseer_options_required:pass --require-approval explicitly');
}

const overseerOptions = {
  requireApproval: values['require-approval'] !== 'false',
  ...(values['min-confidence'] ? { minConfidence: Number(values['min-confidence']) } : {})
};

let states;
try {
  states = JSON.parse(await fs.readFile(values.input, 'utf8'));
} catch (error) {
  fail('overseer_migration_input_unreadable', { detail: error.message });
}
if (!Array.isArray(states)) fail('overseer_migration_input_must_be_an_array');

// The evaluation moment is recorded so the migration is reproducible. An
// operator re-running it months later should be able to reproduce the same
// decision rather than get a different one from a different clock.
const evaluatedAt = values.now || new Date().toISOString();
if (Number.isNaN(Date.parse(evaluatedAt))) fail('overseer_migration_now_invalid', { now: evaluatedAt });
const report = migrateStoredAuthorizations(states, { now: evaluatedAt, overseerOptions });

if (values.apply) {
  // Only write when every item ended somewhere safe: migrated, already current,
  // or explicitly refused by policy. A quarantined item means the input
  // contains something that should not be written back at all.
  if (report.quarantined.length > 0) {
    fail('overseer_migration_quarantine_blocks_apply', {
      quarantined: report.quarantined,
      note: 'Refusing to write while items are quarantined; inspect and resolve them first.'
    });
  }
  if (values.output) {
    await fs.writeFile(values.output, `${JSON.stringify(report.migrated.map(row => row.state), null, 2)}\n`, { mode: 0o600 });
  }
}

process.stdout.write(`${JSON.stringify({
  ok: report.ok,
  applied: Boolean(values.apply),
  evaluatedAt,
  overseerOptions,
  summary: report.summary,
  migrated: report.migrated.map(row => ({ id: row.id, from: row.from, to: row.to })),
  quarantined: report.quarantined,
  expired: report.expired,
  refused: report.refused,
  output: values.output ?? null
}, null, 2)}\n`);

// A refusal is not a crash, but a quarantine is: it means the input contained
// records that do not verify under their own policy.
process.exit(report.quarantined.length > 0 ? 1 : 0);
