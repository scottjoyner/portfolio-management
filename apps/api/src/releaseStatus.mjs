import { verifyAuditChain } from '../../../packages/storage/src/auditChain.mjs';

export function mapAuditEvent(event) {
  return {
    id: event.id,
    action: event.action,
    actor: event.actor,
    at: event.at,
    details: event.details,
    payload: event.payload || event.payload_json || {},
    previousHash: event.previousHash || event.previous_hash || null,
    eventHash: event.eventHash || event.event_hash || null,
    sequenceNumber: Number(event.sequenceNumber || event.sequence_number || 0)
  };
}

export function auditVerificationReport(state = {}) {
  const events = (state.audit || []).map(mapAuditEvent).filter(event => event.sequenceNumber > 0 || event.eventHash);
  if (!events.length) return { ok: true, count: 0, lastHash: null, issues: [], mode: 'no_chained_events_yet' };
  return { ...verifyAuditChain(events), mode: 'hash_chain' };
}

export function certificationStatus(state = {}, runtime = {}) {
  const paperExecutions = state.paperExecutions || [];
  const runningPaperExecutions = paperExecutions.filter(execution => execution.status === 'running').length;
  const killSwitchEnabled = Boolean(state.killSwitch?.enabled);
  const audit = auditVerificationReport(state);
  const liveBlocked = runtime?.safeSummary?.LIVE_TRADING !== 'true';
  const strictReady = runtime?.strict === true ? runtime.ok === true : true;
  const blockers = [];
  if (!liveBlocked) blockers.push('live_trading_flag_enabled');
  if (!audit.ok) blockers.push('audit_chain_invalid');
  if (!strictReady) blockers.push('runtime_invalid');
  return {
    ok: blockers.length === 0,
    release: 'first-prod-paper-only',
    liveTradingCertified: false,
    liveBlocked,
    strictRuntime: Boolean(runtime?.strict),
    runtimeOk: runtime?.ok !== false,
    killSwitchEnabled,
    runningPaperExecutions,
    audit,
    blockers,
    capabilities: {
      paperTrading: true,
      replayBacktesting: true,
      adapterCertificationGates: true,
      auditHashChain: true,
      liveOrderSubmission: false
    }
  };
}
