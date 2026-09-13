import { stableHash } from './overseerLegacy.mjs';

export const CERTIFIED_SPOT_LIFECYCLE_POLICY_VERSION = 'certified-spot-long-only-v1';
export const CERTIFIED_SPOT_LIFECYCLE_METHOD = 'spot_long_only_opposite_signal_or_max_hold_v1';

function finite(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function canonicalLifecycle(certification = {}) {
  return certification.spot_execution_lifecycle
    || certification.spotExecutionLifecycle
    || null;
}

function attestationHash(certification = {}) {
  return certification.spot_execution_attestation_hash
    || certification.spotExecutionAttestationHash
    || null;
}

function lifecycleHash(certification = {}) {
  return certification.spot_execution_lifecycle_hash
    || certification.spotExecutionLifecycleHash
    || null;
}

export function validateCertifiedSpotLifecycle(certification, { symbol } = {}) {
  const reasons = [];
  if (!certification || typeof certification !== 'object' || certification.certified !== true) {
    return { ok: false, reasons: ['certified_runtime_identity_required'] };
  }
  if (certification.execution_lifecycle_certified !== true) {
    reasons.push('certified_execution_lifecycle_required');
  }
  const lifecycle = canonicalLifecycle(certification);
  if (!lifecycle || typeof lifecycle !== 'object') {
    reasons.push('spot_execution_lifecycle_required');
  } else {
    if (lifecycle.method !== CERTIFIED_SPOT_LIFECYCLE_METHOD) {
      reasons.push('spot_execution_lifecycle_method_mismatch');
    }
    if (lifecycle.position_mode !== 'spot_long_only') {
      reasons.push('spot_execution_position_mode_mismatch');
    }
    if (lifecycle.buy_while_flat !== 'open_long') reasons.push('spot_execution_buy_flat_mismatch');
    if (lifecycle.buy_while_long !== 'noop') reasons.push('spot_execution_buy_long_mismatch');
    if (lifecycle.sell_while_flat !== 'noop') reasons.push('spot_execution_sell_flat_mismatch');
    if (lifecycle.sell_while_long !== 'close_long') reasons.push('spot_execution_sell_long_mismatch');
    if (lifecycle.max_hold_on_quiet_bar !== true) reasons.push('spot_execution_max_hold_semantics_mismatch');
    if (lifecycle.same_bar_reentry_after_forced_exit !== false) reasons.push('spot_execution_same_bar_reentry_mismatch');
    if (!(finite(lifecycle.granularity_seconds, 0) > 0)) reasons.push('spot_execution_granularity_invalid');
    if (finite(lifecycle.max_hold_bars, -1) < 0) reasons.push('spot_execution_max_hold_invalid');
    if (symbol && lifecycle.dataset_symbol && lifecycle.dataset_symbol !== symbol) {
      reasons.push('spot_execution_symbol_mismatch');
    }
    try {
      if (!lifecycleHash(certification) || stableHash(lifecycle) !== lifecycleHash(certification)) {
        reasons.push('spot_execution_lifecycle_hash_mismatch');
      }
    } catch {
      reasons.push('spot_execution_lifecycle_not_canonicalizable');
    }
  }
  if (!attestationHash(certification)) reasons.push('spot_execution_attestation_hash_required');
  return { ok: reasons.length === 0, reasons: [...new Set(reasons)], lifecycle };
}

function normalizedSignalAction(value) {
  const action = String(value || '').toUpperCase();
  return ['BUY', 'SELL', 'HOLD'].includes(action) ? action : null;
}

function maxHoldReached(lifecycle, { positionOpenedAt, observedAt } = {}) {
  const bars = Number(lifecycle?.max_hold_bars || 0);
  const granularitySeconds = Number(lifecycle?.granularity_seconds || 0);
  if (!(bars > 0) || !(granularitySeconds > 0) || !positionOpenedAt || !observedAt) return false;
  const opened = new Date(positionOpenedAt).getTime();
  const observed = new Date(observedAt).getTime();
  if (!Number.isFinite(opened) || !Number.isFinite(observed)) return false;
  return observed >= opened + bars * granularitySeconds * 1000;
}

export function deriveCertifiedSpotLifecycleAction({
  certification,
  symbol,
  signalAction,
  openQuantity = 0,
  positionOpenedAt = null,
  observedAt = new Date().toISOString(),
} = {}) {
  const validation = validateCertifiedSpotLifecycle(certification, { symbol });
  if (!validation.ok) return { ok: false, reasons: validation.reasons };

  const action = normalizedSignalAction(signalAction);
  if (!action) return { ok: false, reasons: ['certified_spot_signal_action_invalid'] };
  const quantity = finite(openQuantity, null);
  if (quantity == null || quantity < 0) return { ok: false, reasons: ['certified_spot_position_quantity_invalid'] };
  const isLong = quantity > 0;

  let decision = 'NOOP';
  let side = null;
  let reason = 'hold_signal';

  // Max hold is evaluated before the current signal and wins ties. This exactly
  // matches the new canonical spot replay and prevents a forced exit bar from
  // becoming an immediate re-entry/reversal bar.
  if (isLong && maxHoldReached(validation.lifecycle, { positionOpenedAt, observedAt })) {
    decision = 'CLOSE_LONG';
    side = 'sell';
    reason = 'max_hold';
  } else if (!isLong && action === 'BUY') {
    decision = 'OPEN_LONG';
    side = 'buy';
    reason = 'buy_while_flat';
  } else if (isLong && action === 'SELL') {
    decision = 'CLOSE_LONG';
    side = 'sell';
    reason = 'sell_while_long';
  } else if (isLong && action === 'BUY') {
    reason = 'buy_while_long';
  } else if (!isLong && action === 'SELL') {
    reason = 'sell_while_flat';
  }

  const core = {
    schemaVersion: 1,
    policyVersion: CERTIFIED_SPOT_LIFECYCLE_POLICY_VERSION,
    runtimeIdentityHash: certification.runtime_identity_hash,
    spotExecutionAttestationHash: attestationHash(certification),
    spotExecutionLifecycleHash: lifecycleHash(certification),
    symbol,
    signalAction: action,
    openQuantity: quantity,
    positionOpenedAt,
    observedAt,
    decision,
    side,
    reason,
  };
  return {
    ok: true,
    authorization: { ...core, authorizationHash: stableHash(core) },
  };
}

export function verifyCertifiedSpotLifecycleAuthorization(
  authorization,
  { certification, symbol, requiredDecision = null } = {},
) {
  const reasons = [];
  const validation = validateCertifiedSpotLifecycle(certification, { symbol });
  reasons.push(...validation.reasons);
  if (!authorization || typeof authorization !== 'object') {
    return { ok: false, reasons: [...new Set([...reasons, 'certified_spot_lifecycle_authorization_required'])] };
  }
  if (authorization.policyVersion !== CERTIFIED_SPOT_LIFECYCLE_POLICY_VERSION) {
    reasons.push('certified_spot_lifecycle_policy_mismatch');
  }
  if (authorization.runtimeIdentityHash !== certification?.runtime_identity_hash) {
    reasons.push('certified_spot_lifecycle_runtime_identity_mismatch');
  }
  if (authorization.spotExecutionAttestationHash !== attestationHash(certification)) {
    reasons.push('certified_spot_lifecycle_attestation_mismatch');
  }
  if (authorization.spotExecutionLifecycleHash !== lifecycleHash(certification)) {
    reasons.push('certified_spot_lifecycle_hash_binding_mismatch');
  }
  if (authorization.symbol !== symbol) reasons.push('certified_spot_lifecycle_symbol_mismatch');
  if (requiredDecision && authorization.decision !== requiredDecision) {
    reasons.push('certified_spot_lifecycle_decision_mismatch');
  }
  const core = { ...authorization };
  const supplied = core.authorizationHash;
  delete core.authorizationHash;
  try {
    if (!supplied || stableHash(core) !== supplied) reasons.push('certified_spot_lifecycle_authorization_hash_mismatch');
  } catch {
    reasons.push('certified_spot_lifecycle_authorization_not_canonicalizable');
  }
  return { ok: reasons.length === 0, reasons: [...new Set(reasons)] };
}
