import { verifyAuditChain } from '../../storage/src/auditChain.mjs';

export function verifyAuditIntegrity(events = []) {
  const missing = events.filter(event => !event.eventHash || !event.sequenceNumber).map(event => event.id);
  if (missing.length) {
    return {
      ok: false,
      reason: 'audit_hashes_missing',
      totalEvents: events.length,
      hashedEvents: events.length - missing.length,
      missing
    };
  }
  const verification = verifyAuditChain(events);
  return {
    ok: verification.ok,
    reason: verification.ok ? 'audit_chain_valid' : 'audit_chain_invalid',
    totalEvents: events.length,
    hashedEvents: events.length,
    terminalHash: verification.lastHash,
    failures: verification.issues
  };
}
