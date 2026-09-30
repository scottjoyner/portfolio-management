import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {
  ANCHOR_KIND,
  appendAnchor,
  buildAnchorRecord,
  compareLocalAndExternalRoots,
  hashAnchorRecord,
  signAnchorRecord,
  verifyAnchorSequence,
  verifyAnchorSignature
} from '../packages/audit/src/anchor.mjs';
import { buildAuditEvent } from '../packages/storage/src/auditChain.mjs';

const { privateKey, publicKey } = crypto.generateKeyPairSync('ed25519');
const PRIVATE_PEM = privateKey.export({ type: 'pkcs8', format: 'pem' });
const PUBLIC_PEM = publicKey.export({ type: 'spki', format: 'pem' });

function buildChain(count, { releaseSha = 'abc123' } = {}) {
  const events = [];
  let previous = null;
  for (let index = 0; index < count; index += 1) {
    const event = buildAuditEvent(
      { id: `evt-${index + 1}`, action: 'execution.submit', payload: { releaseSha, index } },
      previous
    );
    events.push(event);
    previous = event;
  }
  return events;
}

function anchorFor(events, overrides = {}) {
  const record = buildAnchorRecord({
    events,
    previousAnchor: overrides.previousAnchor ?? null,
    anchoredAt: overrides.anchoredAt ?? '2026-09-30T12:00:00.000Z',
    releaseSha: overrides.releaseSha ?? 'abc123'
  });
  return { ...record, signature: signAnchorRecord(record, PRIVATE_PEM) };
}

test('anchor record captures the chain root and is signed', () => {
  const events = buildChain(5);
  const anchor = anchorFor(events);
  assert.equal(anchor.kind, ANCHOR_KIND);
  assert.equal(anchor.sequenceNumber, 1);
  assert.equal(anchor.previousAnchorHash, null);
  assert.equal(anchor.auditEventCount, 5);
  assert.equal(anchor.auditRootHash, events.at(-1).eventHash);
  assert.equal(verifyAnchorSignature(anchor, PUBLIC_PEM).ok, true);
  assert.equal(anchor.anchorHash, hashAnchorRecord(anchor));
});

test('anchor refuses to publish a root for a broken chain', () => {
  const events = buildChain(4);
  const tampered = events.map(event => ({ ...event }));
  tampered[2] = { ...tampered[2], payload: { tampered: true } };
  assert.throws(() => buildAnchorRecord({ events: tampered, anchoredAt: '2026-09-30T12:00:00.000Z' }), /audit_anchor_refuses_invalid_chain/);
});

test('anchor refuses an empty chain', () => {
  assert.throws(() => buildAnchorRecord({ events: [], anchoredAt: '2026-09-30T12:00:00.000Z' }), /audit_anchor_refuses_empty_chain/);
});

test('anchor sequence chains to its predecessor', () => {
  const first = anchorFor(buildChain(3));
  const extended = buildChain(6);
  const second = anchorFor(extended, { previousAnchor: first, anchoredAt: '2026-09-30T13:00:00.000Z' });
  assert.equal(second.sequenceNumber, 2);
  assert.equal(second.previousAnchorHash, first.anchorHash);
  assert.equal(verifyAnchorSequence([first, second]).ok, true);
});

test('a removed anchor is detected as a sequence gap', () => {
  const first = anchorFor(buildChain(2));
  const second = anchorFor(buildChain(4), { previousAnchor: first, anchoredAt: '2026-09-30T13:00:00.000Z' });
  const third = anchorFor(buildChain(6), { previousAnchor: second, anchoredAt: '2026-09-30T14:00:00.000Z' });
  const withGap = verifyAnchorSequence([first, third]);
  assert.equal(withGap.ok, false);
  assert.ok(withGap.issues.some(issue => issue.issue === 'anchor_sequence_gap'));
  assert.equal(verifyAnchorSequence([first, second, third]).ok, true);
});

test('a rewritten anchor is detected by its own hash', () => {
  const anchor = anchorFor(buildChain(3));
  const rewritten = { ...anchor, auditRootHash: 'f'.repeat(64) };
  const result = verifyAnchorSequence([rewritten]);
  assert.equal(result.ok, false);
  assert.ok(result.issues.some(issue => issue.issue === 'anchor_hash_mismatch'));
});

test('a forged anchor signature is rejected', () => {
  const anchor = anchorFor(buildChain(3));
  const foreign = crypto.generateKeyPairSync('ed25519');
  const forged = { ...anchor, signature: signAnchorRecord(anchor, foreign.privateKey.export({ type: 'pkcs8', format: 'pem' })) };
  assert.equal(verifyAnchorSignature(forged, PUBLIC_PEM).ok, false);
  const comparison = compareLocalAndExternalRoots({ localEvents: buildChain(3), anchors: [forged], publicKeyPem: PUBLIC_PEM });
  assert.equal(comparison.ok, false);
  assert.equal(comparison.blocking, true);
  assert.equal(comparison.reason, 'audit_anchor_signature_invalid');
});

test('matching local and external roots pass', () => {
  const events = buildChain(7);
  const anchor = anchorFor(events);
  const result = compareLocalAndExternalRoots({ localEvents: events, anchors: [anchor], publicKeyPem: PUBLIC_PEM });
  assert.equal(result.ok, true);
  assert.equal(result.reason, 'audit_roots_match');
  assert.equal(result.localCount, 7);
});

test('THE POINT OF THE CONTROL: a rebuilt local chain is caught', () => {
  // An attacker with database access rebuilds a self-consistent chain that omits
  // the first three events. Locally it verifies perfectly; only the published
  // root reveals the truncation.
  const original = buildChain(9);
  const anchor = anchorFor(original);

  const forgedTail = [];
  let previous = null;
  for (let index = 0; index < 6; index += 1) {
    const event = buildAuditEvent(
      { id: `forged-${index + 1}`, action: 'execution.submit', payload: { index } },
      previous
    );
    forgedTail.push(event);
    previous = event;
  }

  const local = compareLocalAndExternalRoots({ localEvents: forgedTail, anchors: [anchor] });
  assert.equal(local.ok, false);
  assert.equal(local.blocking, true);
  assert.equal(local.reason, 'audit_root_mismatch');
  assert.equal(local.externalRoot, original.at(-1).eventHash);
  assert.equal(local.localRoot, forgedTail.at(-1).eventHash);
  assert.notEqual(local.localRoot, local.externalRoot);
});

test('truncation without a root change is still caught by count', () => {
  const original = buildChain(9);
  const anchor = anchorFor(original);
  const result = compareLocalAndExternalRoots({ localEvents: original.slice(0, 9), anchors: [anchor] });
  assert.equal(result.ok, true);
  const truncated = compareLocalAndExternalRoots({ localEvents: buildChain(9).slice(0, 3), anchors: [anchor] });
  assert.equal(truncated.ok, false);
  assert.ok(['audit_root_mismatch', 'audit_local_chain_truncated'].includes(truncated.reason));
});

test('no published anchor blocks certification rather than passing silently', () => {
  const result = compareLocalAndExternalRoots({ localEvents: buildChain(3), anchors: [] });
  assert.equal(result.ok, false);
  assert.equal(result.blocking, true);
  assert.equal(result.reason, 'audit_anchor_none_published');
});

test('append refuses to overwrite an existing sequence number', () => {
  const first = anchorFor(buildChain(2));
  const withFirst = appendAnchor([], first);
  assert.equal(withFirst.length, 1);
  assert.throws(() => appendAnchor(withFirst, { ...first, auditRootHash: 'e'.repeat(64) }), /audit_anchor_append_conflict:1/);
});
