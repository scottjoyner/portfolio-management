import crypto from 'node:crypto';
import { canonicalJson, verifyAuditChain } from '../../storage/src/auditChain.mjs';

/**
 * External audit-chain anchoring for G-006.
 *
 * A local hash chain proves the audit log has not been altered *locally*. It
 * does not prove the whole log has not been truncated or rewritten, because an
 * attacker who reaches the database can rebuild a self-consistent chain. The
 * fix is to publish the chain head somewhere they cannot retroactively edit,
 * so any rewrite becomes detectable by comparing the two.
 *
 * The destination is the deployment owner's choice. This module owns the
 * record format, the signature, the sequence rules, and the comparison, so the
 * destination only has to be append-only.
 */

export const ANCHOR_SCHEMA_VERSION = 1;
export const ANCHOR_KIND = 'portfolio-audit-chain-anchor';

function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

function anchorPayload(record) {
  return {
    schemaVersion: record.schemaVersion,
    kind: record.kind,
    sequenceNumber: record.sequenceNumber,
    previousAnchorHash: record.previousAnchorHash,
    anchoredAt: record.anchoredAt,
    auditRootHash: record.auditRootHash,
    auditEventCount: record.auditEventCount,
    releaseSha: record.releaseSha ?? null
  };
}

export function hashAnchorRecord(record) {
  return sha256(canonicalJson(anchorPayload(record)));
}

export function signAnchorRecord(record, privateKeyPem) {
  const unsigned = { ...record };
  delete unsigned.signature;
  const key = crypto.createPrivateKey(privateKeyPem);
  if (key.asymmetricKeyType !== 'ed25519') {
    throw new Error(`audit_anchor_key_unsupported:${key.asymmetricKeyType}`);
  }
  return crypto.sign(null, Buffer.from(canonicalJson(unsigned), 'utf8'), key).toString('base64');
}

export function verifyAnchorSignature(record, publicKeyPem) {
  if (!record?.signature) return { ok: false, reason: 'audit_anchor_signature_missing' };
  const unsigned = { ...record };
  delete unsigned.signature;
  let key;
  try {
    key = crypto.createPublicKey(publicKeyPem);
  } catch {
    return { ok: false, reason: 'audit_anchor_key_invalid' };
  }
  if (key.asymmetricKeyType !== 'ed25519') {
    return { ok: false, reason: `audit_anchor_key_unsupported:${key.asymmetricKeyType}` };
  }
  const valid = crypto.verify(
    null,
    Buffer.from(canonicalJson(unsigned), 'utf8'),
    key,
    Buffer.from(String(record.signature), 'base64')
  );
  return valid
    ? { ok: true, reason: 'audit_anchor_signature_valid' }
    : { ok: false, reason: 'audit_anchor_signature_invalid' };
}

/**
 * Build an anchor from the local chain. This deliberately runs the chain
 * verifier first: publishing a root for a chain that is already broken would
 * anchor the wrong thing and turn a detectable incident into an invisible one.
 */
export function buildAnchorRecord({ events, previousAnchor = null, anchoredAt, releaseSha = null }) {
  const verification = verifyAuditChain(events || []);
  if (!verification.ok) {
    const codes = [...new Set(verification.issues.map(issue => issue.issue))];
    throw new Error(`audit_anchor_refuses_invalid_chain:${codes.join(',')}`);
  }
  if (!verification.count) {
    throw new Error('audit_anchor_refuses_empty_chain');
  }
  if (!anchoredAt) throw new Error('audit_anchor_timestamp_required');

  const record = {
    schemaVersion: ANCHOR_SCHEMA_VERSION,
    kind: ANCHOR_KIND,
    sequenceNumber: previousAnchor ? Number(previousAnchor.sequenceNumber) + 1 : 1,
    previousAnchorHash: previousAnchor?.anchorHash ?? null,
    anchoredAt,
    auditRootHash: verification.lastHash,
    auditEventCount: verification.count,
    releaseSha
  };
  return { ...record, anchorHash: hashAnchorRecord(record) };
}

/**
 * Verify the anchor sequence itself. Anchors are append-only, so the sequence
 * must start at 1, increase by exactly one, and each record must chain to its
 * predecessor. A gap means anchors were removed; a fork means two different
 * histories were published.
 */
export function verifyAnchorSequence(anchors = []) {
  const ordered = [...anchors].sort((a, b) => Number(a.sequenceNumber || 0) - Number(b.sequenceNumber || 0));
  const issues = [];
  let previous = null;
  for (const anchor of ordered) {
    const expectedSequence = previous ? Number(previous.sequenceNumber) + 1 : 1;
    if (Number(anchor.sequenceNumber) !== expectedSequence) {
      issues.push({
        sequenceNumber: anchor.sequenceNumber,
        issue: previous ? 'anchor_sequence_gap' : 'anchor_sequence_not_one'
      });
    }
    const expectedPrevious = previous?.anchorHash ?? null;
    if ((anchor.previousAnchorHash ?? null) !== expectedPrevious) {
      issues.push({ sequenceNumber: anchor.sequenceNumber, issue: 'anchor_previous_hash_mismatch' });
    }
    const expectedHash = hashAnchorRecord(anchor);
    if (anchor.anchorHash && anchor.anchorHash !== expectedHash) {
      issues.push({ sequenceNumber: anchor.sequenceNumber, issue: 'anchor_hash_mismatch' });
    }
    if (anchor.schemaVersion !== ANCHOR_SCHEMA_VERSION) {
      issues.push({ sequenceNumber: anchor.sequenceNumber, issue: 'anchor_schema_unsupported' });
    }
    if (anchor.kind !== ANCHOR_KIND) {
      issues.push({ sequenceNumber: anchor.sequenceNumber, issue: 'anchor_kind_unexpected' });
    }
    previous = anchor;
  }
  return {
    ok: issues.length === 0,
    issues,
    count: ordered.length,
    latestHash: previous?.anchorHash ?? null,
    latestRoot: previous?.auditRootHash ?? null
  };
}

/**
 * Compare the locally computed chain head against the newest published anchor.
 * A mismatch is the signal that the local log was truncated or rewritten, and
 * it must block certification rather than be logged and ignored.
 */
export function compareLocalAndExternalRoots({ localEvents, anchors, publicKeyPem = null }) {
  const sequence = verifyAnchorSequence(anchors);
  if (!sequence.ok) {
    return {
      ok: false,
      blocking: true,
      reason: 'audit_anchor_sequence_invalid',
      issues: sequence.issues
    };
  }
  if (!sequence.count) {
    return { ok: false, blocking: true, reason: 'audit_anchor_none_published' };
  }
  if (publicKeyPem) {
    for (const anchor of anchors) {
      const signature = verifyAnchorSignature(anchor, publicKeyPem);
      if (!signature.ok) {
        return { ok: false, blocking: true, reason: signature.reason, sequenceNumber: anchor.sequenceNumber };
      }
    }
  }
  const local = verifyAuditChain(localEvents || []);
  if (!local.ok) {
    return {
      ok: false,
      blocking: true,
      reason: 'audit_local_chain_invalid',
      issues: local.issues
    };
  }
  const latest = [...anchors].sort((a, b) => Number(b.sequenceNumber) - Number(a.sequenceNumber))[0];
  if (local.lastHash !== latest.auditRootHash) {
    return {
      ok: false,
      blocking: true,
      reason: 'audit_root_mismatch',
      localRoot: local.lastHash,
      externalRoot: latest.auditRootHash,
      externalSequenceNumber: latest.sequenceNumber
    };
  }
  if (local.count < latest.auditEventCount) {
    return {
      ok: false,
      blocking: true,
      reason: 'audit_local_chain_truncated',
      localCount: local.count,
      externalCount: latest.auditEventCount
    };
  }
  return {
    ok: true,
    blocking: false,
    reason: 'audit_roots_match',
    localRoot: local.lastHash,
    externalRoot: latest.auditRootHash,
    externalSequenceNumber: latest.sequenceNumber,
    localCount: local.count
  };
}

/** Append an anchor, refusing to overwrite an existing sequence number. */
export function appendAnchor(anchors = [], record) {
  if (anchors.some(anchor => Number(anchor.sequenceNumber) === Number(record.sequenceNumber))) {
    throw new Error(`audit_anchor_append_conflict:${record.sequenceNumber}`);
  }
  return [...anchors, record].sort((a, b) => Number(a.sequenceNumber) - Number(b.sequenceNumber));
}
