export function redact(value) {
  if (!value) return value;
  const text = String(value);
  if (text.length <= 8) return '***';
  return `${text.slice(0, 3)}***${text.slice(-3)}`;
}

function bool(value) {
  return String(value || '').toLowerCase() === 'true';
}

function hasValue(value) {
  return value !== undefined && value !== null && String(value).trim() !== '';
}

function validHttpUrl(value) {
  try {
    const url = new URL(String(value));
    return ['http:', 'https:'].includes(url.protocol) && Boolean(url.hostname);
  } catch {
    return false;
  }
}

function parseLocalNodes(env, errors, warnings) {
  const endpointRows = String(env.LOCAL_LLM_ENDPOINTS || '')
    .split(',')
    .map(value => value.trim())
    .filter(Boolean);
  let jsonRows = [];
  if (hasValue(env.LOCAL_LLM_NODES_JSON)) {
    try {
      const parsed = JSON.parse(env.LOCAL_LLM_NODES_JSON);
      if (!Array.isArray(parsed)) errors.push('LOCAL_LLM_NODES_JSON must be a JSON array');
      else jsonRows = parsed;
    } catch {
      errors.push('LOCAL_LLM_NODES_JSON must contain valid JSON');
    }
  }

  const ids = new Set();
  for (const [index, row] of jsonRows.entries()) {
    if (!row || typeof row !== 'object' || Array.isArray(row)) {
      errors.push(`LOCAL_LLM_NODES_JSON[${index}] must be an object`);
      continue;
    }
    const id = String(row.id || row.name || '').trim();
    if (!id) errors.push(`LOCAL_LLM_NODES_JSON[${index}] requires id or name`);
    else if (ids.has(id)) errors.push(`LOCAL_LLM_NODES_JSON contains duplicate node id ${id}`);
    else ids.add(id);
    const baseUrl = row.baseUrl || row.url || row.endpoint;
    if (!validHttpUrl(baseUrl)) errors.push(`LOCAL_LLM_NODES_JSON[${index}] requires a valid http(s) baseUrl`);
    if (row.models !== undefined && (!Array.isArray(row.models) || row.models.some(model => !String(model || '').trim()))) {
      errors.push(`LOCAL_LLM_NODES_JSON[${index}].models must be a non-empty string array when provided`);
    }
    if (row.maxConcurrent !== undefined && (!Number.isInteger(Number(row.maxConcurrent)) || Number(row.maxConcurrent) < 1)) {
      errors.push(`LOCAL_LLM_NODES_JSON[${index}].maxConcurrent must be an integer >= 1`);
    }
    if (row.contextLength !== undefined && (!Number.isFinite(Number(row.contextLength)) || Number(row.contextLength) <= 0)) {
      errors.push(`LOCAL_LLM_NODES_JSON[${index}].contextLength must be positive`);
    }
  }
  for (const [index, endpoint] of endpointRows.entries()) {
    if (!validHttpUrl(endpoint)) errors.push(`LOCAL_LLM_ENDPOINTS[${index}] must be a valid http(s) URL`);
  }
  if (jsonRows.length && endpointRows.length) warnings.push('LOCAL_LLM_NODES_JSON takes precedence over LOCAL_LLM_ENDPOINTS');
  return { jsonRows, endpointRows, nodeCount: jsonRows.length || endpointRows.length };
}

export function validateRuntimeEnv(env = process.env) {
  const deploymentEnv = env.DEPLOYMENT_ENV || env.NODE_ENV || 'development';
  const strictOverride = env.STRICT_RUNTIME_VALIDATION !== undefined && env.STRICT_RUNTIME_VALIDATION !== null && env.STRICT_RUNTIME_VALIDATION !== '';
  const strict = strictOverride ? bool(env.STRICT_RUNTIME_VALIDATION) : deploymentEnv === 'production';
  const errors = [];
  const warnings = [];
  const localRequired = bool(env.LOCAL_LLM_EXECUTION_REQUIRED);
  const remoteEnabled = bool(env.REMOTE_LLM_EXECUTION_ENABLED);
  const local = parseLocalNodes(env, errors, warnings);

  if (strict) {
    if (env.OPERATOR_STORE !== 'postgres') errors.push('OPERATOR_STORE must be postgres in strict/production deployment');
    if (!hasValue(env.DATABASE_URL)) errors.push('DATABASE_URL is required in strict/production deployment');
    if (env.DATABASE_URL?.includes('postgres:postgres@localhost')) errors.push('DATABASE_URL must not use the default local postgres password/host in production');
    if (env.OPERATOR_AUTH_REQUIRED !== 'true') errors.push('OPERATOR_AUTH_REQUIRED=true is required in strict/production deployment');
    if (!hasValue(env.OPERATOR_ADMIN_TOKEN) && !hasValue(env.OPERATOR_AUTH_TOKEN)) errors.push('OPERATOR_ADMIN_TOKEN or OPERATOR_AUTH_TOKEN is required');
    if (env.CSRF_REQUIRED !== 'true') errors.push('CSRF_REQUIRED=true is required in strict/production deployment');
    if (!hasValue(env.OPERATOR_CSRF_TOKEN)) errors.push('OPERATOR_CSRF_TOKEN is required when CSRF is required');
    if (!hasValue(env.CORS_ORIGINS)) errors.push('CORS_ORIGINS allowlist is required in strict/production deployment');
    if (env.LIVE_TRADING === 'true') errors.push('LIVE_TRADING must remain false until live certification is complete');
    if (env.ALLOW_POLYMARKET_ORDER_SUBMISSION === 'true') errors.push('ALLOW_POLYMARKET_ORDER_SUBMISSION must remain false until certified');
    if (env.ALLOW_LIVE_SETTLEMENT_REDEMPTION === 'true') errors.push('ALLOW_LIVE_SETTLEMENT_REDEMPTION must remain false until certified');
    if (localRequired && local.nodeCount === 0) errors.push('LOCAL_LLM_NODES_JSON or LOCAL_LLM_ENDPOINTS is required when local inference is required');
    if (remoteEnabled && !hasValue(env.OPENROUTER_API_KEY)) errors.push('OPENROUTER_API_KEY is required when remote LLM execution is enabled');
    if (localRequired && remoteEnabled) warnings.push('both local-required and remote-enabled are set; remote calls still require explicit remote quotes');
  } else {
    if (!hasValue(env.DATABASE_URL)) warnings.push('DATABASE_URL not set; non-production runtime may fall back to local defaults');
    if (env.OPERATOR_AUTH_REQUIRED !== 'true') warnings.push('operator auth is not required in this non-production runtime');
    if (localRequired && local.nodeCount === 0) warnings.push('local inference is required but no local nodes are configured');
  }

  return {
    ok: errors.length === 0,
    strict,
    deploymentEnv,
    errors,
    warnings,
    safeSummary: {
      OPERATOR_STORE: env.OPERATOR_STORE || null,
      DATABASE_URL: env.DATABASE_URL ? redact(env.DATABASE_URL) : null,
      OPERATOR_AUTH_REQUIRED: env.OPERATOR_AUTH_REQUIRED || 'false',
      CSRF_REQUIRED: env.CSRF_REQUIRED || 'false',
      CORS_ORIGINS: env.CORS_ORIGINS || null,
      LIVE_TRADING: env.LIVE_TRADING || 'false',
      LOCAL_LLM_EXECUTION_REQUIRED: env.LOCAL_LLM_EXECUTION_REQUIRED || 'false',
      REMOTE_LLM_EXECUTION_ENABLED: env.REMOTE_LLM_EXECUTION_ENABLED || 'false',
      LOCAL_LLM_NODE_COUNT: local.nodeCount,
      OPENROUTER_API_KEY: env.OPENROUTER_API_KEY ? 'configured' : null,
    }
  };
}

export function assertRuntimeEnv(env = process.env) {
  const result = validateRuntimeEnv(env);
  if (!result.ok) {
    const error = new Error(`runtime_env_invalid: ${result.errors.join('; ')}`);
    error.validation = result;
    throw error;
  }
  return result;
}
