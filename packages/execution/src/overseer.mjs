import * as legacy from './overseerLegacy.mjs';

// Keep the already-proven risk/allocation authorization machinery intact while
// adding an independent certified-runtime/economic-edge gate for automated
// strategy entries.
export const OVERSEER_SCHEMA_VERSION = 4;
export const OVERSEER_POLICY_VERSION = 'execution-admission-v4-certified-runtime';
export const DEFAULT_OVERSEER_TTL_MS = legacy.DEFAULT_OVERSEER_TTL_MS;
export const stableHash = legacy.stableHash;
export const normalizeRiskDecision = legacy.normalizeRiskDecision;
export const buildTradeIntentEnvelope = legacy.buildTradeIntentEnvelope;

const LEGACY_SCHEMA_VERSION = legacy.OVERSEER_SCHEMA_VERSION;
const LEGACY_POLICY_VERSION = legacy.OVERSEER_POLICY_VERSION;
const REQUIRED_CERTIFICATION_FIELDS = [
  'runtime_identity_hash',
  'candidate_id',
  'candidate_source_sha',
  'strategy_name',
  'strategy_config_hash',
  'alpha_validation_evidence_hash',
  'terminal_holdout_evidence_hash',
  'replay_attestation_hash',
  'runtime_binary_sha256',
  'runtime_evaluator',
];

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

function isoNow(now) {
  const date = now instanceof Date ? now : new Date(now ?? Date.now());
  if (!Number.isFinite(date.getTime())) throw new TypeError('invalid overseer evaluation time');
  return date;
}

function automatedStrategyEntry(envelope, options = {}) {
  if (options.requireApproval !== false) return false;
  if (!envelope?.strategyId) return false;
  const tradeIntent = String(envelope.tradeIntent || '').toLowerCase();
  const executionPurpose = String(envelope.executionPurpose || '').toLowerCase();
  return tradeIntent === 'entry' || executionPurpose.startsWith('open_');
}

function certifiedEntryReasons(envelope, options = {}) {
  if (!automatedStrategyEntry(envelope, options)) return [];
  const reasons = [];
  const certification = envelope?.tradePlan?.runtime_certification
    ?? envelope?.tradePlan?.runtimeCertification
    ?? null;

  if (!certification || certification.certified !== true) {
    reasons.push('certified_runtime_identity_required');
  } else {
    for (const field of REQUIRED_CERTIFICATION_FIELDS) {
      if (certification[field] === undefined || certification[field] === null || certification[field] === '') {
        reasons.push(`certified_runtime_identity_missing_field:${field}`);
      }
    }
    if (certification.strategy_name !== envelope.strategyId) {
      reasons.push('certified_runtime_strategy_mismatch');
    }
    if (certification.execution_lifecycle_certified !== true) {
      reasons.push('certified_execution_lifecycle_required');
    }
  }

  if (!envelope.economicDecisionId) reasons.push('economic_decision_required_for_automated_entry');
  if (!envelope.executionCostSnapshotId) reasons.push('execution_cost_snapshot_required_for_automated_entry');
  if (!Number.isFinite(envelope.netExecutableEdgeUsd)) {
    reasons.push('net_executable_edge_usd_required');
  } else if (envelope.netExecutableEdgeUsd <= 0) {
    reasons.push('net_executable_edge_usd_not_positive');
  }
  return reasons;
}

function v4Decision(baseDecision, extraReasons) {
  const reasons = unique([...(baseDecision.reasons || []), ...extraReasons]);
  const approved = reasons.length === 0;
  let decision = baseDecision.decision;
  if (!approved && baseDecision.mode !== 'live') decision = 'REJECT';
  if (approved && baseDecision.mode === 'paper') decision = 'PAPER';
  if (approved && baseDecision.mode === 'demo') decision = 'DEMO';

  const core = {
    schemaVersion: OVERSEER_SCHEMA_VERSION,
    policyVersion: OVERSEER_POLICY_VERSION,
    decision,
    approved,
    reasons,
    intentHash: baseDecision.intentHash,
    capitalRiskSnapshotHash: baseDecision.capitalRiskSnapshotHash,
    capitalRiskPolicyVersion: baseDecision.capitalRiskPolicyVersion,
    riskDecisionHash: baseDecision.riskDecisionHash,
    evaluatedAt: baseDecision.evaluatedAt,
    validUntil: baseDecision.validUntil,
    requiresHumanApproval: baseDecision.requiresHumanApproval,
    mode: baseDecision.mode,
  };
  return { ...core, decisionHash: stableHash(core) };
}

export function evaluateTradeIntent(input = {}, options = {}) {
  const base = legacy.evaluateTradeIntent(input, options);
  const extraReasons = certifiedEntryReasons(base.tradeIntentEnvelope, options);
  return {
    ...base,
    overseerDecision: v4Decision(base.overseerDecision, extraReasons),
  };
}

export function verifyOverseerDecision(decision, {
  intentHash,
  capitalRiskSnapshotHash,
  capitalRiskPolicyVersion,
  riskDecisionHash,
  now,
} = {}) {
  const reasons = [];
  if (!decision || typeof decision !== 'object') return { ok: false, reasons: ['overseer_decision_required'] };
  const required = [
    'schemaVersion', 'policyVersion', 'decision', 'approved', 'reasons', 'intentHash',
    'capitalRiskSnapshotHash', 'capitalRiskPolicyVersion', 'riskDecisionHash',
    'evaluatedAt', 'validUntil', 'requiresHumanApproval', 'mode', 'decisionHash',
  ];
  for (const field of required) {
    if (!(field in decision)) reasons.push(`overseer_decision_missing_field:${field}`);
  }
  if (reasons.length) return { ok: false, reasons };
  if (decision.schemaVersion !== OVERSEER_SCHEMA_VERSION || decision.policyVersion !== OVERSEER_POLICY_VERSION) {
    reasons.push('overseer_policy_version_mismatch');
  }
  if (intentHash && decision.intentHash !== intentHash) reasons.push('overseer_intent_hash_mismatch');
  if (capitalRiskSnapshotHash !== undefined && decision.capitalRiskSnapshotHash !== capitalRiskSnapshotHash) {
    reasons.push('overseer_capital_risk_hash_mismatch');
  }
  if (capitalRiskPolicyVersion !== undefined && decision.capitalRiskPolicyVersion !== capitalRiskPolicyVersion) {
    reasons.push('overseer_capital_risk_policy_mismatch');
  }
  if (riskDecisionHash !== undefined && decision.riskDecisionHash !== riskDecisionHash) {
    reasons.push('overseer_risk_decision_hash_mismatch');
  }
  if (decision.approved !== true) {
    reasons.push(...(Array.isArray(decision.reasons) && decision.reasons.length ? decision.reasons : ['overseer_rejected']));
  }
  if (!Array.isArray(decision.reasons) || decision.reasons.some(reason => typeof reason !== 'string')) {
    reasons.push('overseer_reasons_invalid');
  }

  const core = { ...decision };
  const suppliedHash = core.decisionHash;
  delete core.decisionHash;
  try {
    if (stableHash(core) !== suppliedHash) reasons.push('overseer_decision_hash_mismatch');
  } catch {
    reasons.push('overseer_decision_not_canonicalizable');
  }

  const expiresAt = new Date(decision.validUntil).getTime();
  const current = isoNow(now).getTime();
  if (!Number.isFinite(expiresAt)) reasons.push('overseer_expiry_invalid');
  else if (current > expiresAt) reasons.push('overseer_decision_expired');
  if (decision.mode === 'live') reasons.push('live_execution_not_certified');
  return { ok: reasons.length === 0, reasons: unique(reasons) };
}

function toLegacyDecision(decision) {
  if (!decision || typeof decision !== 'object') return decision;
  const core = {
    ...decision,
    schemaVersion: LEGACY_SCHEMA_VERSION,
    policyVersion: LEGACY_POLICY_VERSION,
  };
  delete core.decisionHash;
  return { ...core, decisionHash: stableHash(core) };
}

export function verifyStoredExecutionAuthorization(state, { now } = {}) {
  // Reuse the established v3 verification for intent immutability, portfolio
  // allocation, capital-risk and risk-decision bindings.  Only the overseer
  // schema marker is translated for compatibility, after which the real v4
  // decision/hash is independently verified below.
  const compatibilityState = {
    ...state,
    overseerDecision: toLegacyDecision(state?.overseerDecision),
  };
  const base = legacy.verifyStoredExecutionAuthorization(compatibilityState, { now });
  const v4 = verifyOverseerDecision(state?.overseerDecision, {
    intentHash: base.tradeIntentHash,
    capitalRiskSnapshotHash: base.capitalRiskSnapshotHash,
    capitalRiskPolicyVersion: state?.capitalRiskSnapshot?.policyVersion ?? null,
    riskDecisionHash: base.riskDecisionHash,
    now,
  });
  const reasons = unique([...(base.reasons || []), ...(v4.reasons || [])]);
  return {
    ...base,
    ok: reasons.length === 0,
    reasons,
  };
}
