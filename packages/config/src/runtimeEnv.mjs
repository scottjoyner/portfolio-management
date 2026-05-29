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

export function validateRuntimeEnv(env = process.env) {
  const deploymentEnv = env.DEPLOYMENT_ENV || env.NODE_ENV || 'development';
  const strict = bool(env.STRICT_RUNTIME_VALIDATION) || deploymentEnv === 'production';
  const errors = [];
  const warnings = [];

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
  } else {
    if (!hasValue(env.DATABASE_URL)) warnings.push('DATABASE_URL not set; non-production runtime may fall back to local defaults');
    if (env.OPERATOR_AUTH_REQUIRED !== 'true') warnings.push('operator auth is not required in this non-production runtime');
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
      LIVE_TRADING: env.LIVE_TRADING || 'false'
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
