import {
  OVERSEER_POLICY_VERSION,
  OVERSEER_SCHEMA_VERSION,
  evaluateTradeIntent,
  verifyStoredExecutionAuthorization
} from './overseer.mjs';
import { verifyStoredExecutionAuthorization as verifyStoredExecutionAuthorizationV3 } from './overseerLegacy.mjs';

/**
 * Stored-overseer migration for G-012.
 *
 * The policy moved from execution-admission-v2 to
 * execution-admission-v4-certified-runtime. Authorization minted under the old
 * policy fails closed, which is correct: an old decision is not evidence that
 * the new admission rules would have approved the same trade. But "fails
 * closed" without a path forward strands the state, and a stranded
 * authorization looks like an outage rather than a version difference.
 *
 * So the distinction this module draws is between two very different failures:
 *
 *   superseded  the authorization verifies under the policy it was minted
 *               against, and only needs re-minting under the current one.
 *   unsound     it does not verify even against its own policy -- tampered,
 *               truncated, or internally inconsistent. Re-minting that would
 *               launder a broken record into a fresh-looking valid one.
 *
 * Only the first is eligible for migration. The second is quarantined and
 * reported, because a migration path that will re-issue anything it is handed
 * is not a migration path.
 */

export const MIGRATION_DISPOSITION = {
  CURRENT: 'current',
  SUPERSEDED: 'superseded',
  // Stale, not tampered. Reported separately so an operator does not go hunting
  // for tampering when the only problem is that the authorization aged out.
  EXPIRED: 'expired',
  UNSOUND: 'unsound'
};

const EXPIRY_REASONS = new Set(['overseer_decision_expired', 'portfolio_allocation_expired', 'capital_risk_snapshot_expired']);

const LEGACY_POLICY_VERSIONS = new Set([
  'execution-admission-v2',
  'execution-admission-v3'
]);

/**
 * Verify a stored authorization against the policy it claims.
 *
 * For a legacy policy this runs the v3 verifier, which is the module that
 * actually defines that policy's rules. Checking a handful of field names
 * would be a weaker claim than "verified under its own policy", and a weaker
 * claim here is the one that lets a tampered record through.
 */
function verifyAgainstOwnPolicy(state, { now } = {}) {
  const policyVersion = state?.overseerDecision?.policyVersion;
  if (policyVersion === OVERSEER_POLICY_VERSION) {
    return verifyStoredExecutionAuthorization(state, { now });
  }
  if (LEGACY_POLICY_VERSIONS.has(policyVersion)) {
    return verifyStoredExecutionAuthorizationV3(state, { now });
  }
  return { ok: false, reasons: [`overseer_policy_version_unknown:${policyVersion ?? 'absent'}`] };
}

export function classifyStoredAuthorization(state, { now } = {}) {
  if (!state || typeof state !== 'object') {
    return {
      disposition: MIGRATION_DISPOSITION.UNSOUND,
      policyVersion: null,
      schemaVersion: null,
      reasons: ['stored_authorization_not_an_object'],
      eligibleForMigration: false
    };
  }

  const decision = state.overseerDecision || {};
  const policyVersion = decision.policyVersion ?? null;
  const schemaVersion = decision.schemaVersion ?? null;

  if (policyVersion === OVERSEER_POLICY_VERSION && schemaVersion === OVERSEER_SCHEMA_VERSION) {
    const current = verifyStoredExecutionAuthorization(state, { now });
    const onlyExpiry = !current.ok
      && current.reasons.length > 0
      && current.reasons.every(reason => EXPIRY_REASONS.has(reason));
    return {
      disposition: current.ok
        ? MIGRATION_DISPOSITION.CURRENT
        : onlyExpiry ? MIGRATION_DISPOSITION.EXPIRED : MIGRATION_DISPOSITION.UNSOUND,
      policyVersion,
      schemaVersion,
      reasons: current.reasons ?? [],
      eligibleForMigration: false
    };
  }

  const legacy = verifyAgainstOwnPolicy(state, { now });
  if (!legacy.ok) {
    // An expired record is stale, not forged. It still must not be re-minted --
    // re-issuing a lapsed decision would resurrect authority that was
    // deliberately allowed to lapse -- but it is triaged separately.
    const onlyExpiry = legacy.reasons.length > 0 && legacy.reasons.every(reason => EXPIRY_REASONS.has(reason));
    return {
      disposition: onlyExpiry ? MIGRATION_DISPOSITION.EXPIRED : MIGRATION_DISPOSITION.UNSOUND,
      policyVersion,
      schemaVersion,
      reasons: legacy.reasons,
      eligibleForMigration: false
    };
  }

  return {
    disposition: MIGRATION_DISPOSITION.SUPERSEDED,
    policyVersion,
    schemaVersion,
    reasons: [],
    eligibleForMigration: true
  };
}

/**
 * Rebuild the evaluation input from a stored state.
 *
 * The state persists the canonical trade-intent envelope, not the original raw
 * request. Feeding the envelope's own fields back through
 * buildTradeIntentEnvelope reproduces a byte-identical hash, so the current
 * policy can be re-run over stored state without the original request. The
 * stored risk decision and capital risk snapshot are carried across unchanged:
 * a migration re-applies the admission rules, it does not re-decide the risk.
 */
function evaluationInputFromState(state) {
  const envelope = state.tradeIntentEnvelope;
  if (!envelope || typeof envelope !== 'object') return null;
  return {
    ...envelope,
    orders: envelope.orders ?? [],
    riskDecision: state.riskDecision
  };
}

/**
 * Re-mint one superseded authorization under the current policy.
 *
 * This does not copy the old decision forward. It re-evaluates the trade intent
 * through the current admission rules, because that is the only thing that can
 * honestly produce a current-policy decision. If the current rules reject the
 * trade that the old policy approved, the migration refuses and says so; the
 * operator has to decide what to do, because silently dropping a trade the old
 * system would have taken is a policy change dressed as a migration.
 */
export function migrateStoredAuthorization(state, { now = new Date().toISOString(), overseerOptions = {} } = {}) {
  const classification = classifyStoredAuthorization(state, { now });

  if (classification.disposition === MIGRATION_DISPOSITION.CURRENT) {
    return { migrated: false, reason: 'authorization_already_current', state, classification };
  }
  if (classification.disposition === MIGRATION_DISPOSITION.EXPIRED) {
    return {
      migrated: false,
      reason: 'authorization_expired_under_its_own_policy',
      state,
      classification,
      expired: true
    };
  }
  if (!classification.eligibleForMigration) {
    return {
      migrated: false,
      reason: 'authorization_unsound_under_its_own_policy',
      state,
      classification,
      quarantined: true
    };
  }

  const input = evaluationInputFromState(state);
  if (!input) {
    return {
      migrated: false,
      reason: 'stored_trade_intent_envelope_absent',
      state,
      classification
    };
  }

  // Derive the hashes the *verifier* will recompute, rather than the ones this
  // module can guess at. The verification step below re-derives them from the
  // stored state; building the new decision from any other source produces a
  // decision that is internally consistent but fails its own hash check.
  let derived;
  try {
    const legacyVerification = verifyStoredExecutionAuthorizationV3(state, { now });
    derived = {
      tradeIntentHash: legacyVerification.tradeIntentHash ?? state.tradeIntentHash ?? null,
      capitalRiskSnapshotHash: legacyVerification.capitalRiskSnapshotHash ?? state.capitalRiskSnapshotHash ?? null,
      riskDecisionHash: legacyVerification.riskDecisionHash ?? state.riskDecisionHash ?? null
    };
  } catch {
    derived = {
      tradeIntentHash: state.tradeIntentHash ?? null,
      capitalRiskSnapshotHash: state.capitalRiskSnapshotHash ?? null,
      riskDecisionHash: state.riskDecisionHash ?? null
    };
  }

  let evaluation;
  try {
    // The caller's overseer options must be the ones production uses. Guessing
    // them would re-mint a decision that production would never have produced,
    // which is worse than refusing: it would look authoritative and be fiction.
    evaluation = evaluateTradeIntent(input, {
      ...overseerOptions,
      now,
      capitalRiskSnapshotHash: derived.capitalRiskSnapshotHash,
      capitalRiskPolicyVersion: state.riskDecision?.policyVersion ?? null,
      riskDecisionHash: derived.riskDecisionHash
    });
  } catch (error) {
    return {
      migrated: false,
      reason: `current_policy_evaluation_failed:${error.message}`,
      state,
      classification
    };
  }

  // evaluateTradeIntent nests the admission outcome under overseerDecision.
  // The terminal states are PAPER and DEMO; LIVE_REQUIRES_HUMAN is explicitly
  // not an approval, and REJECT is not either.
  const decision = evaluation?.overseerDecision ?? {};
  const approved = decision.approved === true || decision.decision === 'PAPER' || decision.decision === 'DEMO';
  if (!approved) {
    return {
      migrated: false,
      reason: 'current_policy_does_not_approve',
      state,
      classification,
      currentPolicyDecision: decision.decision ?? null,
      currentPolicyReasons: decision.reasons ?? evaluation?.riskDecision?.reasons ?? []
    };
  }

  // Only carry forward the fields the current gate derives for itself. The new
  // decision, its hash, and its policy version are the current module's output.
  //
  // Nothing may be added to the decision object itself: verifyOverseerDecision
  // hashes the entire decision minus decisionHash, so any extra field -- even
  // well-intentioned provenance -- invalidates the decision's own hash. The
  // migration provenance therefore lives on the state, beside the decision.
  const migrated = {
    ...state,
    overseerDecision: decision,
    overseerMigration: {
      policyVersion: classification.policyVersion,
      schemaVersion: classification.schemaVersion,
      decisionHash: state.overseerDecision?.decisionHash ?? null,
      migratedAt: now,
      toPolicyVersion: OVERSEER_POLICY_VERSION,
      toSchemaVersion: OVERSEER_SCHEMA_VERSION
    }
  };

  const verification = verifyStoredExecutionAuthorization(migrated, { now });
  return {
    migrated: verification.ok,
    reason: verification.ok ? 'authorization_migrated' : 'migrated_authorization_failed_current_verification',
    state: verification.ok ? migrated : state,
    classification,
    // When the re-minted decision does not verify, the operator needs to see
    // what was actually produced; otherwise they are debugging blind.
    attemptedOverseerDecision: verification.ok ? null : migrated.overseerDecision,
    verification: { ok: verification.ok, reasons: verification.reasons ?? [] },
    fromPolicyVersion: classification.policyVersion,
    toPolicyVersion: OVERSEER_POLICY_VERSION
  };
}

/**
 * Migrate a set of authorizations and report the outcome per item. Quarantined
 * items are listed separately so a migration run can never quietly shrink the
 * set of live authorizations.
 */
export function migrateStoredAuthorizations(states, options = {}) {
  const migrated = [];
  const quarantined = [];
  const expired = [];
  const refused = [];
  let alreadyCurrent = 0;

  (states || []).forEach((state, index) => {
    const result = migrateStoredAuthorization(state, options);
    const id = state?.executionId ?? state?.opportunityId ?? `index:${index}`;
    if (result.reason === 'authorization_already_current') {
      alreadyCurrent += 1;
      return;
    }
    if (result.migrated) {
      migrated.push({ id, from: result.fromPolicyVersion, to: result.toPolicyVersion, state: result.state });
      return;
    }
    if (result.quarantined) {
      quarantined.push({ id, reasons: result.classification.reasons });
      return;
    }
    if (result.expired) {
      expired.push({ id, reasons: result.classification.reasons });
      return;
    }
    refused.push({ id, reason: result.reason, currentPolicyDecision: result.currentPolicyDecision ?? null });
  });

  return {
    ok: quarantined.length === 0 && refused.length === 0,
    total: (states || []).length,
    alreadyCurrent,
    migrated,
    quarantined,
    expired,
    refused,
    summary: {
      migratedCount: migrated.length,
      quarantinedCount: quarantined.length,
      expiredCount: expired.length,
      refusedCount: refused.length,
      alreadyCurrentCount: alreadyCurrent
    }
  };
}
