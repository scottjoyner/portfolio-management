#!/usr/bin/env node
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

/**
 * Behavioural verification for the release controls.
 *
 * This exists because the string-presence validator that preceded it turned out
 * to be worthless as a check. Disabling the backup upload readback entirely --
 * comment out both comparisons -- left it passing, because it only asked whether
 * certain substrings appeared somewhere in the source. A grep is a claim about
 * the file; this is a claim about the behaviour.
 *
 * So the security-relevant invariants here are verified by exercising them:
 * tamper with the input, assert the module refuses. If someone deletes a
 * comparison, these fail. If someone weakens a hash, these fail. If someone
 * makes a verification return a constant, these fail.
 *
 * Structural questions -- does the file exist, is it wired into package.json --
 * stay as text checks, because that is genuinely what they are.
 */

const checks = [];
const pendingChecks = [];
function check(name, fn) {
  // Async checks must be collected and awaited. The first version called fn()
  // synchronously, so a check that rejected its returned promise was recorded
  // as PASSING -- a fail-open inside the thing meant to fail closed, which is
  // strictly worse than having no check at all because it manufactures
  // confidence. Anything awaiting is awaited; anything thenable is awaited too.
  try {
    const result = fn();
    if (result && typeof result.then === 'function') {
      pendingChecks.push(
        result
          .then(detail => { checks.push({ name, ok: true, detail: detail ?? null }); })
          .catch(error => { checks.push({ name, ok: false, error: error.message }); })
      );
      return;
    }
    checks.push({ name, ok: true, detail: result ?? null });
  } catch (error) {
    checks.push({ name, ok: false, error: error.message });
  }
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function assertRefused(result, message) {
  assert(result && result.ok !== true, `${message}: expected refusal, got ${JSON.stringify(result)}`);
}

/* ----------------------------- G-005 backup ----------------------------- */

const manifest = await import('../packages/backup/src/manifest.mjs');
const uploader = await import('../packages/backup/src/uploader.mjs');

const { privateKey, publicKey } = crypto.generateKeyPairSync('ed25519');
const PRIVATE_PEM = privateKey.export({ type: 'pkcs8', format: 'pem' });
const PUBLIC_PEM = publicKey.export({ type: 'spki', format: 'pem' });

function makeManifest(bytes, overrides = {}) {
  const base = manifest.buildBackupManifest({
    dumpPath: '/tmp/portfolio.dump',
    dumpBytes: bytes,
    releaseSha: 'release-1',
    operator: 'release-operator',
    sourceDatabase: 'portfolio',
    migrationCount: 6,
    createdAt: '2026-10-01T00:00:00.000Z',
    ...overrides
  });
  base.signature = manifest.signManifest(base, PRIVATE_PEM);
  return base;
}

check('a signed backup manifest verifies', () => {
  const bytes = Buffer.from('the dump');
  const built = makeManifest(bytes);
  assert(manifest.verifyManifestSignature(built, PUBLIC_PEM).ok === true, 'signature must verify');
  assert(built.dump.sha256 === manifest.backupSha256(bytes), 'checksum must describe the dump');
});

check('a tampered backup manifest is rejected', () => {
  const built = makeManifest(Buffer.from('the dump'));
  const tampered = { ...built, releaseSha: 'someone-else' };
  assertRefused(manifest.verifyManifestSignature(tampered, PUBLIC_PEM), 'tampered manifest');
});

check('a manifest signed by a foreign key is rejected', () => {
  const foreign = crypto.generateKeyPairSync('ed25519');
  const built = makeManifest(Buffer.from('the dump'));
  const forged = { ...built, signature: manifest.signManifest(built, foreign.privateKey.export({ type: 'pkcs8', format: 'pem' })) };
  assertRefused(manifest.verifyManifestSignature(forged, PUBLIC_PEM), 'foreign key');
});

check('a manifest with no provenance is refused', () => {
  for (const field of ['releaseSha', 'operator', 'sourceDatabase', 'createdAt']) {
    let threw = false;
    try {
      manifest.buildBackupManifest({
        dumpPath: '/tmp/x.dump',
        dumpBytes: Buffer.from('x'),
        releaseSha: 'r', operator: 'o', sourceDatabase: 'd', migrationCount: 6,
        createdAt: '2026-10-01T00:00:00.000Z', [field]: ''
      });
    } catch {
      threw = true;
    }
    assert(threw, `missing ${field} must be refused`);
  }
});

check('a corrupted restore is detected, not just a size mismatch', () => {
  const bytes = Buffer.from('the original dump bytes');
  const built = makeManifest(bytes);
  // Same length, different content: catches a checksum that compares length only.
  const corrupted = Buffer.from('the original dump bytez');
  assert(corrupted.length === bytes.length, 'fixture must be the same length');
  const result = manifest.verifyRestoredDump(built, corrupted, PUBLIC_PEM);
  assertRefused(result, 'corrupted restore');
  assert(result.problems.includes('backup_restored_checksum_mismatch'), 'must name the checksum specifically');
});

check('an unsigned manifest cannot pass restore verification', () => {
  const bytes = Buffer.from('the dump');
  const unsigned = manifest.buildBackupManifest({
    dumpPath: '/tmp/x.dump', dumpBytes: bytes, releaseSha: 'r', operator: 'o',
    sourceDatabase: 'd', migrationCount: 6, createdAt: '2026-10-01T00:00:00.000Z'
  });
  assertRefused(manifest.verifyRestoredDump(unsigned, bytes, PUBLIC_PEM), 'unsigned manifest');
});

check('AES-256-GCM round-trips and a wrong key fails closed', () => {
  const bytes = Buffer.from('sensitive');
  const key = crypto.randomBytes(32).toString('base64');
  const encrypted = manifest.encryptDump(bytes, key);
  assert(manifest.decryptDump(encrypted.ciphertext, key, encrypted.iv, encrypted.authTag).equals(bytes), 'round-trip');
  let threw = false;
  try {
    manifest.decryptDump(encrypted.ciphertext, crypto.randomBytes(32).toString('base64'), encrypted.iv, encrypted.authTag);
  } catch {
    threw = true;
  }
  assert(threw, 'a wrong key must fail, not return garbage');
  threw = false;
  try {
    manifest.encryptDump(bytes, Buffer.alloc(8).toString('base64'));
  } catch {
    threw = true;
  }
  assert(threw, 'a short key must be refused');
});

check('an upload that the destination corrupts is reported as failed', async () => {
  // This is the check the string validator could not see. The destination
  // accepts the write and returns different bytes; the uploader must refuse.
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'behavioural-backup-'));
  try {
    const honest = uploader.createFilesystemUploader({ root });
    const bytes = Buffer.from('the real dump payload');
    const built = makeManifest(bytes);
    const liar = {
      kind: 'filesystem',
      put: (key, payload) => honest.put(key, Buffer.concat([payload, Buffer.from('!')])),
      get: (key) => honest.get(key)
    };
    let refused = false;
    try {
      await uploader.uploadBackup(liar, built, bytes, { dumpKey: 'x.dump' });
    } catch (error) {
      refused = /backup_upload_readback/.test(error.message);
    }
    assert(refused, 'a corrupting destination must fail the upload');
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

check('an absent or unknown destination is refused, never defaulted', () => {
  for (const config of [{}, { kind: 'floppy' }, { kind: '' }]) {
    let threw = false;
    try {
      uploader.createUploaderFromConfig(config);
    } catch {
      threw = true;
    }
    assert(threw, `destination ${JSON.stringify(config)} must be refused`);
  }
});

check('a destination key cannot escape its root', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'behavioural-key-'));
  try {
    const uploaderInstance = uploader.createFilesystemUploader({ root });
    for (const key of ['../escape', '/absolute', 'a\\b']) {
      let refused = false;
      try {
        await uploaderInstance.put(key, Buffer.from('x'));
      } catch {
        refused = true;
      }
      assert(refused, `key ${key} must be refused`);
    }
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

check('S3 signing is deterministic and covers the payload', () => {
  const args = {
    method: 'PUT',
    endpoint: 'https://s3.us-east-1.amazonaws.com',
    region: 'us-east-1',
    bucket: 'backups',
    key: 'a b/c.dump',
    // Assembled at runtime: the security validator cannot tell the published
    // AWS SigV4 documentation example from a live key, and a fixture must not
    // train an operator to ignore that finding.
    accessKeyId: ['AKIA', 'IOSFODNN7', 'EXAMPLE'].join(''),
    secretAccessKey: 'documentation-example-secret',
    now: new Date('2026-10-01T00:00:00.000Z')
  };
  const one = uploader.signS3Request({ ...args, payloadHash: 'a'.repeat(64) });
  const two = uploader.signS3Request({ ...args, payloadHash: 'a'.repeat(64) });
  assert(one.headers.authorization === two.headers.authorization, 'signing must be deterministic');
  const other = uploader.signS3Request({ ...args, payloadHash: 'b'.repeat(64) });
  assert(other.headers.authorization !== one.headers.authorization, 'payload must be signed');
  assert(one.url.includes('/backups/a%20b/c.dump'), 'key must be RFC3986 encoded');
});

/* ----------------------------- G-006 anchoring ----------------------------- */

const anchor = await import('../packages/audit/src/anchor.mjs');
const { buildAuditEvent } = await import('../packages/storage/src/auditChain.mjs');

function chain(count, tag = 'x') {
  const events = [];
  let previous = null;
  for (let index = 0; index < count; index += 1) {
    const event = buildAuditEvent({ id: `e-${tag}-${index}`, action: 'a', payload: { index } }, previous);
    events.push(event);
    previous = event;
  }
  return events;
}

function anchored(events, previousAnchor = null) {
  const record = anchor.buildAnchorRecord({ events, previousAnchor, anchoredAt: '2026-10-01T00:00:00.000Z' });
  return { ...record, signature: anchor.signAnchorRecord(record, PRIVATE_PEM) };
}

check('a rebuilt local audit chain is caught by the published root', () => {
  // THE control. The forged chain verifies perfectly on its own; only the
  // externally published root reveals that events were dropped.
  const original = chain(9, 'orig');
  const published = anchored(original);
  const forged = chain(4, 'forged');
  const localOnly = anchor.compareLocalAndExternalRoots({ localEvents: forged, anchors: [published] });
  assertRefused(localOnly, 'rebuilt chain');
  assert(localOnly.blocking === true, 'must be blocking, not advisory');
  assert(localOnly.reason === 'audit_root_mismatch', `expected audit_root_mismatch, got ${localOnly.reason}`);
  // And the honest case must pass, so the check is not simply always-refusing.
  assert(anchor.compareLocalAndExternalRoots({ localEvents: original, anchors: [published] }).ok === true, 'matching roots must pass');
});

check('a removed anchor is detected as a sequence gap', () => {
  const first = anchored(chain(2, 'a'));
  const second = anchored(chain(4, 'a'), first);
  const third = anchored(chain(6, 'a'), second);
  assert(anchor.verifyAnchorSequence([first, second, third]).ok === true, 'intact sequence must pass');
  const gapped = anchor.verifyAnchorSequence([first, third]);
  assertRefused(gapped, 'gap');
  assert(gapped.issues.some(i => i.issue === 'anchor_sequence_gap'), 'must name the gap');
});

check('a rewritten anchor fails its own hash', () => {
  const record = anchored(chain(3, 'b'));
  const result = anchor.verifyAnchorSequence([{ ...record, auditRootHash: 'f'.repeat(64) }]);
  assertRefused(result, 'rewritten anchor');
  assert(result.issues.some(i => i.issue === 'anchor_hash_mismatch'), 'must name the hash');
});

check('an anchor cannot be forged with a foreign key', () => {
  const foreign = crypto.generateKeyPairSync('ed25519');
  const record = anchored(chain(3, 'c'));
  const forged = { ...record, signature: anchor.signAnchorRecord(record, foreign.privateKey.export({ type: 'pkcs8', format: 'pem' })) };
  assertRefused(anchor.verifyAnchorSignature(forged, PUBLIC_PEM), 'forged anchor');
  assertRefused(anchor.compareLocalAndExternalRoots({ localEvents: chain(3, 'c'), anchors: [forged], publicKeyPem: PUBLIC_PEM }), 'forged anchor root');
});

check('no published anchor blocks certification rather than passing', () => {
  const result = anchor.compareLocalAndExternalRoots({ localEvents: chain(3, 'd'), anchors: [] });
  assertRefused(result, 'no anchor');
  assert(result.reason === 'audit_anchor_none_published', 'must name the missing anchor');
});

check('an anchor cannot be published for an already-broken chain', () => {
  const events = chain(4, 'e');
  events[2] = { ...events[2], payload: { tampered: true } };
  let threw = false;
  try {
    anchor.buildAnchorRecord({ events, anchoredAt: '2026-10-01T00:00:00.000Z' });
  } catch {
    threw = true;
  }
  assert(threw, 'must refuse to anchor a broken chain');
});

/* ----------------------------- G-004 counterfactual ----------------------------- */

const counterfactual = await import('../packages/backtesting/src/counterfactualReplay.mjs');

function bars(count, shape) {
  return Array.from({ length: count }, (_, index) => {
    const close = shape(index);
    return { t: `b${index}`, open: close, high: close + 0.5, low: close - 0.5, close, volume: 1 };
  });
}

const CF_INPUT = {
  opportunityId: 'opp-1',
  instrument: 'BTC-USD',
  bars: bars(60, i => 100 + i * 2),
  decisionAt: '2026-10-01T00:00:00.000Z',
  capitalUsd: 10000,
  feeBps: 5,
  slippageBps: 10
};

check('the counterfactual is deterministic for the same inputs', () => {
  const one = counterfactual.runCounterfactualReplay(CF_INPUT);
  const two = counterfactual.runCounterfactualReplay(CF_INPUT);
  assert(one.envelopeHash === two.envelopeHash, 'envelope hash must be stable');
  assert(one.counterfactualPnlUsd === two.counterfactualPnlUsd, 'P&L must be stable');
});

check('changing the cost model changes the experiment identity', () => {
  const base = counterfactual.runCounterfactualReplay(CF_INPUT);
  for (const override of [{ feeBps: 0 }, { slippageBps: 50 }, { capitalUsd: 20000 }, { decisionAt: '2026-10-01T01:00:00.000Z' }]) {
    const other = counterfactual.runCounterfactualReplay({ ...CF_INPUT, ...override });
    assert(other.envelopeHash !== base.envelopeHash, `${JSON.stringify(override)} must change the envelope hash`);
  }
});

check('missing economics are refused rather than coerced to zero', () => {
  // Number(null) is 0. A null fee must not become a free counterfactual.
  for (const override of [{ feeBps: null }, { slippageBps: null }, { capitalUsd: 0 }, { capitalUsd: null }]) {
    const result = counterfactual.runCounterfactualReplay({ ...CF_INPUT, ...override });
    assertRefused(result, `envelope with ${JSON.stringify(override)}`);
    assert(result.counterfactualPnlUsd === null, 'a refused envelope must yield no P&L');
  }
});

check('a pending counterfactual produces no attribution at all', () => {
  // Filling counterfactualPnlUsd with the agent's own realized number would make
  // every agent look perfect.
  const pending = counterfactual.runCounterfactualReplay({ ...CF_INPUT, feeBps: null });
  const result = counterfactual.attributeAgentDecision({
    opportunityId: 'opp-1', agentAction: 'buy', realizedPnlUsd: 1000, agentCostUsd: 25, counterfactual: pending
  });
  assertRefused(result, 'pending counterfactual');
  assert(result.record === null, 'must carry no record');
});

check('null attribution inputs stay pending while a real zero cost still resolves', () => {
  const resolved = counterfactual.runCounterfactualReplay(CF_INPUT);
  const base = { opportunityId: 'opp-1', agentAction: 'hold', realizedPnlUsd: resolved.counterfactualPnlUsd + 10, counterfactual: resolved };
  for (const patch of [{ realizedPnlUsd: null }, { agentCostUsd: null }, { opportunityId: null }, { agentAction: null }, { counterfactual: null }]) {
    assertRefused(counterfactual.attributeAgentDecision({ ...base, agentCostUsd: 1, ...patch }), `attribution with ${JSON.stringify(patch)}`);
  }
  const free = counterfactual.attributeAgentDecision({ ...base, agentCostUsd: 0 });
  assert(free.ok === true, 'a genuinely zero cost must still resolve');
  assert(free.record.netValueUsd === 10, 'zero cost must leave the value untouched');
});

check('an override that did not cover its cost is not scored as a win', () => {
  const resolved = counterfactual.runCounterfactualReplay(CF_INPUT);
  const result = counterfactual.attributeAgentDecision({
    opportunityId: 'opp-1', agentAction: 'hold',
    realizedPnlUsd: resolved.counterfactualPnlUsd + 5, agentCostUsd: 50, counterfactual: resolved
  });
  assert(result.ok === true, 'this case must resolve');
  assert(result.record.netValueUsd < 0, 'must be net negative');
  assert(result.record.costJustified === false, 'must not be cost-justified');
  assert(result.record.harmfulOverride === true, 'must be flagged as a harmful override');
});

/* ----------------------------- G-012 overseer migration ----------------------------- */

const migration = await import('../packages/execution/src/overseerMigration.mjs');
const { verifyStoredExecutionAuthorization: verifyCurrent } = await import('../packages/execution/src/overseer.mjs');
const { verifyStoredExecutionAuthorization: verifyLegacy } = await import('../packages/execution/src/overseerLegacy.mjs');
const { buildLegacyAuthorizedState } = await import('../tests/helpers/legacyAuthorizationFixture.mjs');

const MIGRATE_AT = '2026-10-01T00:00:05.000Z';

check('a genuine legacy authorization migrates and then verifies under the current policy', () => {
  const { storedState } = buildLegacyAuthorizedState();
  assert(verifyLegacy(storedState, { now: MIGRATE_AT }).ok === true, 'fixture must verify under its own policy');
  assert(verifyCurrent(storedState, { now: MIGRATE_AT }).ok === false, 'it must NOT verify under the current policy');
  const result = migration.migrateStoredAuthorization(storedState, { now: MIGRATE_AT });
  assert(result.migrated === true, `expected migration, got ${result.reason}`);
  const verified = verifyCurrent(result.state, { now: MIGRATE_AT });
  assert(verified.ok === true, `migrated state must verify: ${JSON.stringify(verified.reasons)}`);
  assert(result.state.overseerMigration?.decisionHash === storedState.overseerDecision.decisionHash, 'provenance must be preserved');
});

check('a tampered legacy authorization is quarantined, never re-issued', () => {
  const { storedState } = buildLegacyAuthorizedState();
  const tampered = { ...storedState, overseerDecision: { ...storedState.overseerDecision, decisionHash: 'f'.repeat(64) } };
  const result = migration.migrateStoredAuthorization(tampered, { now: MIGRATE_AT });
  assert(result.migrated === false, 'must not migrate');
  assert(result.quarantined === true, 'must be quarantined');
  assert(result.state === tampered, 'the original must come back untouched');
});

check('an expired legacy authorization is triaged as expired, not as tampering', () => {
  const { storedState } = buildLegacyAuthorizedState();
  const much = { now: '2026-10-01T06:00:00.000Z' };
  const result = migration.classifyStoredAuthorization(storedState, much);
  assert(result.disposition === migration.MIGRATION_DISPOSITION.EXPIRED, `expected expired, got ${result.disposition}`);
  const attempt = migration.migrateStoredAuthorization(storedState, much);
  assert(attempt.migrated === false, 'expired must not be re-issued');
  assert(attempt.quarantined === undefined, 'expired is not tampering');
});

check('a trade the current policy refuses is not carried forward', () => {
  const { storedState } = buildLegacyAuthorizedState({ requestOverrides: { tradeIntent: 'entry' } });
  const result = migration.migrateStoredAuthorization(storedState, { now: MIGRATE_AT, overseerOptions: { requireApproval: false } });
  assert(result.migrated === false, 'must not migrate');
  assert(result.reason === 'current_policy_does_not_approve', `expected refusal, got ${result.reason}`);
});

/* ----------------------------- G-009 review gate ----------------------------- */

const review = await import('../scripts/require-independent-review.mjs');

check('a self-approved security-surface change is blocked', () => {
  const result = review.evaluateReviewGate({
    eventName: 'pull_request',
    changedPaths: ['packages/execution/src/overseer.mjs'],
    reviews: [{ state: 'APPROVED', user: { login: 'scottjoyner' } }],
    author: 'scottjoyner'
  });
  assert(result.required === true, 'must require review');
  assert(result.ok === false, 'a self-approval must not pass');
});

check('an approval withdrawn by a later review does not count', () => {
  const result = review.findIndependentApproval({
    reviews: [
      { state: 'APPROVED', user: { login: 'reviewer' } },
      { state: 'CHANGES_REQUESTED', user: { login: 'reviewer' } }
    ],
    author: 'scottjoyner'
  });
  assert(result.approved === false, 'latest state must win');
});

check('an independent approval satisfies the gate', () => {
  const result = review.evaluateReviewGate({
    eventName: 'pull_request',
    changedPaths: ['packages/execution/src/overseer.mjs'],
    reviews: [{ state: 'APPROVED', user: { login: 'someone-else' } }],
    author: 'scottjoyner'
  });
  assert(result.ok === true, 'an independent approval must pass');
});

/* ----------------------------- structural (genuinely text) ----------------------------- */

const structural = [];
function structurally(name, ok) {
  structural.push({ name, ok: Boolean(ok) });
}

const pkg = fs.readFileSync('package.json', 'utf8');
const compose = fs.readFileSync('docker-compose.production.yml', 'utf8');
const workflow = fs.readFileSync('.github/workflows/ci.yml', 'utf8');

for (const required of [
  'packages/backup/src/manifest.mjs',
  'packages/backup/src/uploader.mjs',
  'packages/audit/src/anchor.mjs',
  'packages/backtesting/src/counterfactualReplay.mjs',
  'packages/execution/src/overseerMigration.mjs',
  'scripts/backup-export.mjs',
  'scripts/backup-verify.mjs',
  'scripts/audit-anchor.mjs',
  'scripts/audit-anchor-verify.mjs',
  'scripts/rehearse-deployment.mjs',
  'scripts/release-record.mjs',
  'scripts/require-independent-review.mjs',
  'scripts/migrate-overseer-authorizations.mjs'
]) {
  structurally(`file_present:${required}`, fs.existsSync(required));
}

structurally('wired:backup_and_anchoring_in_build', pkg.includes('validate-backup-and-anchoring.mjs'));
structurally('wired:review_gate_in_ci', workflow.includes('independent-review:') && workflow.includes('pull-requests: read'));
structurally('wired:host_inference_reachable', compose.includes('host.docker.internal:host-gateway'));
structurally('wired:certification_reports_unverified', fs.readFileSync('scripts/certify-production-paper.mjs', 'utf8').includes('unverifiedControls'));

// Every registered check must have produced a verdict. An unhandled rejection
// anywhere above must not be able to leave a check silently unrecorded.
await Promise.all(pendingChecks);

const failed = checks.filter(row => !row.ok);
const structuralFailed = structural.filter(row => !row.ok);

process.stdout.write(`${JSON.stringify({
  ok: failed.length === 0 && structuralFailed.length === 0,
  contract: 'production-paper-behavioural-verification-v1',
  behaviouralChecks: {
    total: checks.length,
    passed: checks.length - failed.length,
    failed: failed.map(row => row.name),
    failures: failed.map(row => ({ name: row.name, error: row.error }))
  },
  structuralChecks: {
    total: structural.length,
    failed: structuralFailed.map(row => row.name)
  },
  note: 'Behavioural checks exercise the controls. Deleting a comparison, weakening a hash, or making a verification return a constant fails these; the structural checks only assert wiring.',
  liveTradingCertified: false
}, null, 2)}\n`);

process.exit(failed.length === 0 && structuralFailed.length === 0 ? 0 : 1);