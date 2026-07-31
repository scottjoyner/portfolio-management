import crypto from 'node:crypto';

export function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

export function auditPayload(event) {
  return {
    id: event.id,
    action: event.action,
    actor: event.actor || 'system',
    at: event.at,
    details: event.details || null,
    payload: event.payload || {},
    previousHash: event.previousHash || null,
    sequenceNumber: Number(event.sequenceNumber || 0)
  };
}

export function hashAuditEvent(event) {
  return crypto.createHash('sha256').update(canonicalJson(auditPayload(event))).digest('hex');
}

export function buildAuditEvent(event, previous = null) {
  const sequenceNumber = previous?.sequenceNumber ? Number(previous.sequenceNumber) + 1 : 1;
  const previousHash = previous?.eventHash || null;
  const next = { ...event, actor: event.actor || 'system', at: event.at || new Date().toISOString(), payload: event.payload || {}, previousHash, sequenceNumber };
  return { ...next, eventHash: hashAuditEvent(next) };
}

function hasChainMetadata(event = {}) {
  return event.previousHash != null || event.eventHash != null || event.sequenceNumber != null;
}

/**
 * Complete legacy/unhashed audit events without silently repairing tampered
 * chain metadata. Existing valid hashes are preserved; a partially populated
 * or conflicting chain fails closed.
 */
export function completeAuditChain(events = []) {
  const completed = [];
  let previous = null;
  for (const input of events || []) {
    const event = { ...input, payload: input?.payload || {} };
    if (!hasChainMetadata(event)) {
      const built = buildAuditEvent(event, previous);
      completed.push(built);
      previous = built;
      continue;
    }

    const expectedSequence = previous ? Number(previous.sequenceNumber) + 1 : 1;
    const expectedPreviousHash = previous?.eventHash || null;
    const sequenceNumber = Number(event.sequenceNumber);
    const previousHash = event.previousHash || null;
    if (!Number.isInteger(sequenceNumber) || sequenceNumber !== expectedSequence) {
      throw new Error(`audit_chain_sequence_conflict:${event.id || 'unknown'}`);
    }
    if (previousHash !== expectedPreviousHash) {
      throw new Error(`audit_chain_previous_hash_conflict:${event.id || 'unknown'}`);
    }
    if (!event.eventHash) {
      throw new Error(`audit_chain_event_hash_missing:${event.id || 'unknown'}`);
    }
    const expectedHash = hashAuditEvent(event);
    if (event.eventHash !== expectedHash) {
      throw new Error(`audit_chain_event_hash_conflict:${event.id || 'unknown'}`);
    }
    completed.push(event);
    previous = event;
  }
  return completed;
}

export function verifyAuditChain(events = []) {
  const ordered = [...events].sort((a, b) => Number(a.sequenceNumber || 0) - Number(b.sequenceNumber || 0));
  const issues = [];
  let previous = null;
  for (const event of ordered) {
    const expectedPreviousHash = previous?.eventHash || null;
    if ((event.previousHash || null) !== expectedPreviousHash) issues.push({ id: event.id, issue: 'previous_hash_mismatch' });
    const expectedHash = hashAuditEvent(event);
    if (event.eventHash && event.eventHash !== expectedHash) issues.push({ id: event.id, issue: 'event_hash_mismatch' });
    if (previous && Number(event.sequenceNumber) !== Number(previous.sequenceNumber) + 1) issues.push({ id: event.id, issue: 'sequence_gap' });
    previous = event;
  }
  return { ok: issues.length === 0, issues, count: ordered.length, lastHash: previous?.eventHash || null };
}
