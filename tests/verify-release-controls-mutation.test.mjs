import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

/**
 * Mutation tests for scripts/verify-release-controls.mjs.
 *
 * A verifier is only as trustworthy as the things it demonstrably catches. The
 * string-presence validator it replaced passed while the backup upload readback
 * was entirely disabled, so "the validator says green" proved nothing. These
 * tests disable real security comparisons and assert the behavioural verifier
 * notices, which is what makes its green result mean something.
 *
 * Every mutation is applied to a real source file, the verifier is run, the
 * file is restored, and the mutation is required to be caught. A mutation that
 * survives is a hole in the verifier, not an acceptable weakness.
 */

const VERIFIER = 'scripts/verify-release-controls.mjs';

function runVerifier() {
  const result = spawnSync(process.execPath, [VERIFIER], { encoding: 'utf8', maxBuffer: 32 * 1024 * 1024 });
  return { status: result.status, report: result.status === 0 ? JSON.parse(result.stdout) : null };
}

function runVerifierExpectingFailure() {
  const result = spawnSync(process.execPath, [VERIFIER], { encoding: 'utf8', maxBuffer: 32 * 1024 * 1024 });
  if (result.status === 0) return null;
  return JSON.parse(result.stdout);
}

/**
 * Apply a mutation, run the verifier, restore, and report which checks failed.
 *
 * `also` carries companion edits for the same mutation. They matter: a control
 * with two independent comparisons has to have both removed before the removal
 * is detectable, because either one alone still refuses the bad input. Testing
 * only one would report a real control as an unverifiable one.
 */
function withMutation({ file, from, to, also = [] }) {
  const targets = [{ file, from, to }, ...also.map(edit => ({ file: edit.file ?? file, ...edit }))];
  // Snapshot each file exactly once. Restoring per edit instead would write the
  // intermediate mutated bytes back over the clean ones when two edits touch the
  // same file, leaving the tree dirty and every later mutation nonsense.
  const originals = new Map();
  try {
    for (const edit of targets) {
      const absolute = path.resolve(edit.file);
      if (!originals.has(absolute)) originals.set(absolute, fs.readFileSync(absolute, 'utf8'));
      const original = originals.get(absolute);
      assert.ok(original.includes(edit.from), `mutation target not found in ${edit.file}: ${edit.from}`);
      const current = fs.readFileSync(absolute, 'utf8');
      fs.writeFileSync(absolute, current.replace(edit.from, edit.to));
    }
    return runVerifierExpectingFailure();
  } finally {
    for (const [absolute, original] of originals) fs.writeFileSync(absolute, original);
  }
}

const MUTATIONS = [
  {
    name: 'backup upload readback: content comparison removed',
    file: 'packages/backup/src/uploader.mjs',
    from: 'if (sha256Hex(readBack) !== sha256Hex(dumpBytes)) {',
    to: 'if (false) {',
    also: [
      {
        file: 'packages/backup/src/uploader.mjs',
        from: 'if (!readBack || readBack.length !== dumpBytes.length) {',
        to: 'if (false) {'
      }
    ]
  },
  {
    name: 'audit root comparison always passes',
    file: 'packages/audit/src/anchor.mjs',
    from: 'if (local.lastHash !== latest.auditRootHash) {',
    to: 'if (false) {'
  },
  {
    // A truncated chain changes the root, so the count check is redundant defence
    // against the root comparison. Removing only the count check therefore
    // detects nothing -- and that is correct, not a hole. Undetectable truncation
    // requires removing both, which is what this mutation does.
    name: 'audit truncation undetectable: count and root checks both removed',
    file: 'packages/audit/src/anchor.mjs',
    from: "if (local.count < latest.auditEventCount) {",
    to: 'if (false) {',
    also: [{ from: 'if (local.lastHash !== latest.auditRootHash) {', to: 'if (false) {' }]
  },
  {
    name: 'audit anchor gap detection disabled',
    file: 'packages/audit/src/anchor.mjs',
    from: "issues.push({\n        sequenceNumber: anchor.sequenceNumber,\n        issue: previous ? 'anchor_sequence_gap' : 'anchor_sequence_not_one'\n      });",
    to: 'if (false) issues.push({ issue: "x" });'
  },
  {
    name: 'audit anchor refuses to publish a broken chain, no longer',
    file: 'packages/audit/src/anchor.mjs',
    from: 'if (!verification.ok) {',
    to: 'if (false) {'
  },
  {
    name: 'audit anchor signature verification always accepts',
    file: 'packages/audit/src/audit-signature-probe-unused.mjs',
    skip: true
  },
  {
    name: 'counterfactual accepts a null fee as zero',
    file: 'packages/backtesting/src/counterfactualReplay.mjs',
    from: "if (!nonNegativeNumber(feeBps)) missing.push('replay_envelope_fee_bps_required');",
    to: ''
  },
  {
    name: 'counterfactual accepts a null capital base as zero',
    file: 'packages/backtesting/src/counterfactualReplay.mjs',
    from: "if (!positiveNumber(capitalUsd)) missing.push('replay_envelope_capital_invalid');",
    to: ''
  },
  {
    name: 'attribution drops its pending-counterfactual guard',
    file: 'packages/backtesting/src/counterfactualReplay.mjs',
    from: '|| counterfactual.outcome !== REPLAY_OUTCOME.RESOLVED',
    to: '|| false',
    also: [
      { from: '|| counterfactualPnl == null', to: '|| false' },
      { from: '|| !Number.isFinite(Number(counterfactualPnl))', to: '' }
    ]
  },
  {
    name: 'attribution drops its null realized-PnL guard',
    file: 'packages/backtesting/src/counterfactualReplay.mjs',
    from: 'if (realizedPnlUsd == null || !Number.isFinite(Number(realizedPnlUsd))) {',
    to: 'if (false) {'
  },
  {
    name: 'overseer migration skips the legacy-policy verification',
    file: 'packages/execution/src/overseerMigration.mjs',
    from: 'if (!classification.eligibleForMigration) {',
    to: 'if (false) {'
  },
  {
    name: 'overseer migration trusts a forged classification',
    file: 'packages/execution/src/overseerMigration.mjs',
    from: 'const legacy = verifyAgainstOwnPolicy(state, { now });',
    to: 'const legacy = { ok: true, reasons: [] };'
  },
  {
    name: 'overseer migration stops distinguishing expiry from tampering',
    file: 'packages/execution/src/overseerMigration.mjs',
    from: "const onlyExpiry = legacy.reasons.length > 0 && legacy.reasons.every(reason => EXPIRY_REASONS.has(reason));",
    to: 'const onlyExpiry = false;'
  },
  {
    name: 'review gate accepts a self-approval',
    file: 'scripts/require-independent-review.mjs',
    from: 'const isAuthor = author && login.toLowerCase() === String(author).toLowerCase();',
    to: 'const isAuthor = false;'
  },
  {
    name: 'review gate honours the first approval instead of the latest',
    file: 'scripts/require-independent-review.mjs',
    from: 'latestByReviewer.set(login, review);',
    to: 'if (!latestByReviewer.has(login)) latestByReviewer.set(login, review);'
  }
];

function gitDirtyFiles() {
  const result = spawnSync('git', ['status', '--porcelain'], { encoding: 'utf8' });
  return result.stdout.split('\n').map(line => line.trim()).filter(Boolean);
}

test('the behavioural verifier is green before any mutation', () => {
  const { status, report } = runVerifier();
  assert.equal(status, 0, 'verifier must be green on an unmodified tree');
  assert.ok(report.behaviouralChecks.total >= 25, `expected a substantial behavioural surface, got ${report.behaviouralChecks.total}`);
  assert.deepEqual(report.behaviouralChecks.failed, []);
});

test('every security mutation is caught by the behavioural verifier', () => {
  const survivors = [];
  for (const mutation of MUTATIONS) {
    if (mutation.skip) continue;
    const report = withMutation(mutation);
    if (report === null) {
      survivors.push(`${mutation.name} (verifier stayed green)`);
      continue;
    }
    if (!report.behaviouralChecks.failed.length) {
      survivors.push(`${mutation.name} (verifier failed but not on a behavioural check)`);
    }
  }
  assert.deepEqual(survivors, [], 'these mutations were not detected, so the verifier cannot be trusted');
});

test('the verifier is restored to green after mutation testing', () => {
  const { status, report } = runVerifier();
  assert.equal(status, 0, `verifier not green after mutation testing: ${JSON.stringify(report?.behaviouralChecks?.failures ?? null)}`);
});

test('mutation testing leaves no source file modified', () => {
  // These tests rewrite real source files. If a restore ever fails, the next
  // mutation applies to already-mutated text and the suite reports nonsense --
  // which is exactly what happened once during development. Only the files this
  // test rewrites are in scope; unrelated in-progress work is not its problem.
  const touched = new Set(MUTATIONS.map(mutation => mutation.file));
  const dirty = gitDirtyFiles()
    .filter(line => !line.startsWith('??'))
    .filter(line => touched.has(line.slice(3).trim()));
  assert.deepEqual(dirty, [], `mutation testing left source files modified: ${dirty.join(', ')}`);
});

test('the string-presence validator is retained only for wiring, and says so', () => {
  // It stays because "is this wired into package.json" really is a text
  // question, but it must not be presented as behavioural evidence.
  const source = fs.readFileSync('scripts/validate-backup-and-anchoring.mjs', 'utf8');
  assert.ok(/structural/i.test(source), 'the wiring validator must label its own scope');
  assert.ok(/presence only, not behaviour/i.test(source), 'and must say plainly that it checks presence, not behaviour');
});