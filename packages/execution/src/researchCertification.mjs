import { createHash } from 'node:crypto';

export const RESEARCH_CERTIFICATION_SCHEMA_VERSION = 1;
export const RESEARCH_CERTIFICATION_TYPE = 'research_tournament_terminal_promotion_v1';
export const RESEARCH_RUNTIME_BINDING_METHOD = 'configured_rust_signal_v1';

const IDENTITY_FIELDS = [
  'schema_version',
  'certification_type',
  'candidate_id',
  'strategy_name',
  'symbol',
  'granularity',
  'candidate_source_sha',
  'candidate_config_hash',
  'alpha_validation_evidence_hash',
  'terminal_holdout_evidence_hash',
  'experiment_hash',
  'selection_hash',
  'terminal_result_hash',
  'terminal_metrics_hash',
];

function canonicalJson(value) {
  if (value === null) return 'null';
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value && typeof value === 'object') {
    const keys = Object.keys(value).filter(key => value[key] !== undefined).sort();
    return `{${keys.map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
  }
  if (typeof value === 'number' && !Number.isFinite(value)) {
    throw new TypeError('research certification contains non-finite number');
  }
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new TypeError('research certification contains unsupported value');
  return encoded;
}

export function researchStableHash(value) {
  return createHash('sha256').update(canonicalJson(value)).digest('hex');
}

export function researchCertificationIdentity(certification = {}) {
  return Object.fromEntries(IDENTITY_FIELDS.map(name => [name, certification?.[name] ?? null]));
}

export function runtimeBindingIdentity(binding = {}) {
  return {
    method: binding?.method ?? null,
    certification_hash: binding?.certification_hash ?? null,
    strategy_name: binding?.strategy_name ?? null,
    strategy_config_hash: binding?.strategy_config_hash ?? null,
    symbol: binding?.symbol ?? null,
    granularity: binding?.granularity ?? null,
  };
}

function text(value) {
  return typeof value === 'string' && value.trim() ? value : null;
}

export function verifyResearchCertification(certification, { strategyId = null, symbol = null } = {}) {
  const reasons = [];
  if (!certification || typeof certification !== 'object' || Array.isArray(certification)) {
    return { ok: false, reasons: ['research_certification_required'] };
  }
  if (certification.schema_version !== RESEARCH_CERTIFICATION_SCHEMA_VERSION) {
    reasons.push('research_certification_schema_mismatch');
  }
  if (certification.certification_type !== RESEARCH_CERTIFICATION_TYPE) {
    reasons.push('research_certification_type_mismatch');
  }
  for (const field of IDENTITY_FIELDS) {
    if (field === 'schema_version') continue;
    if (!text(certification[field])) reasons.push(`research_certification_field_invalid:${field}`);
  }
  try {
    if (researchStableHash(researchCertificationIdentity(certification)) !== certification.certification_hash) {
      reasons.push('research_certification_hash_mismatch');
    }
  } catch {
    reasons.push('research_certification_not_canonicalizable');
  }

  const binding = certification.runtime_binding;
  if (!binding || typeof binding !== 'object' || Array.isArray(binding)) {
    reasons.push('research_runtime_binding_required');
  } else {
    if (binding.method !== RESEARCH_RUNTIME_BINDING_METHOD) reasons.push('research_runtime_binding_method_mismatch');
    if (binding.certification_hash !== certification.certification_hash) reasons.push('research_runtime_certification_hash_mismatch');
    if (binding.strategy_name !== certification.strategy_name) reasons.push('research_runtime_strategy_mismatch');
    if (binding.strategy_config_hash !== certification.candidate_config_hash) reasons.push('research_runtime_config_hash_mismatch');
    if (binding.symbol !== certification.symbol) reasons.push('research_runtime_symbol_mismatch');
    if (String(binding.granularity ?? '') !== String(certification.granularity ?? '')) reasons.push('research_runtime_granularity_mismatch');
    try {
      if (researchStableHash(runtimeBindingIdentity(binding)) !== binding.runtime_binding_hash) {
        reasons.push('research_runtime_binding_hash_mismatch');
      }
    } catch {
      reasons.push('research_runtime_binding_not_canonicalizable');
    }
  }

  if (strategyId && certification.strategy_name !== strategyId) reasons.push('research_certification_strategy_mismatch');
  if (symbol && certification.symbol !== symbol) reasons.push('research_certification_symbol_mismatch');
  return { ok: reasons.length === 0, reasons: [...new Set(reasons)] };
}

export function isExposureIncreasingStrategyIntent(input = {}) {
  const side = String(input.side || input.orders?.[0]?.side || '').toLowerCase();
  const tradeIntent = String(input.tradeIntent || '').toLowerCase();
  const executionPurpose = String(input.executionPurpose || '').toLowerCase();
  if (tradeIntent === 'exit' || executionPurpose.includes('exit') || side === 'sell') return false;
  return tradeIntent === 'entry' || ['buy', 'yes', 'no'].includes(side);
}

export function strategyScannerResearchRequired(input = {}) {
  return input.sourceAgentId === 'strategy-comparison-scanner'
    && isExposureIncreasingStrategyIntent(input);
}
