import test from 'node:test';
import assert from 'node:assert/strict';
import { assertRuntimeEnv, validateRuntimeEnv } from '../packages/config/src/runtimeEnv.mjs';

function strictEnv(overrides = {}) {
  return {
    DEPLOYMENT_ENV: 'production',
    OPERATOR_STORE: 'postgres',
    DATABASE_URL: 'postgresql://portfolio:strong-hosted-password@db.example.internal:5432/portfolio',
    OPERATOR_AUTH_REQUIRED: 'true',
    OPERATOR_ADMIN_TOKEN: 'admin-token',
    CSRF_REQUIRED: 'true',
    OPERATOR_CSRF_TOKEN: 'csrf-token',
    CORS_ORIGINS: 'https://operator.example.com',
    LIVE_TRADING: 'false',
    ALLOW_POLYMARKET_ORDER_SUBMISSION: 'false',
    ALLOW_LIVE_SETTLEMENT_REDEMPTION: 'false',
    ...overrides,
  };
}

test('runtime env allows development with warnings', () => {
  const result = validateRuntimeEnv({ NODE_ENV: 'development' });
  assert.equal(result.ok, true);
  assert.equal(result.strict, false);
  assert.ok(result.warnings.length >= 1);
});

test('runtime env rejects production without postgres auth csrf and cors', () => {
  const result = validateRuntimeEnv({ DEPLOYMENT_ENV: 'production' });
  assert.equal(result.ok, false);
  assert.ok(result.errors.includes('OPERATOR_STORE must be postgres in strict/production deployment'));
  assert.ok(result.errors.includes('DATABASE_URL is required in strict/production deployment'));
  assert.ok(result.errors.includes('OPERATOR_AUTH_REQUIRED=true is required in strict/production deployment'));
});

test('runtime env rejects live flags in strict mode', () => {
  const result = validateRuntimeEnv(strictEnv({ LIVE_TRADING: 'true' }));
  assert.equal(result.ok, false);
  assert.ok(result.errors.includes('LIVE_TRADING must remain false until live certification is complete'));
});

test('runtime env accepts strict paper-only production config', () => {
  const result = validateRuntimeEnv(strictEnv());
  assert.equal(result.ok, true);
  assert.equal(result.strict, true);
  assert.equal(result.safeSummary.DATABASE_URL.includes('strong-hosted-password'), false);
});

test('strict local-first runtime requires at least one valid inference node', () => {
  const missing = validateRuntimeEnv(strictEnv({ LOCAL_LLM_EXECUTION_REQUIRED: 'true' }));
  assert.equal(missing.ok, false);
  assert.ok(missing.errors.includes('LOCAL_LLM_NODES_JSON or LOCAL_LLM_ENDPOINTS is required when local inference is required'));

  const configured = validateRuntimeEnv(strictEnv({
    LOCAL_LLM_EXECUTION_REQUIRED: 'true',
    LOCAL_LLM_NODES_JSON: JSON.stringify([{ id: 'x1-370', baseUrl: 'http://x1-370.lan:1234/v1', models: ['qwen-local'], maxConcurrent: 1, contextLength: 65536 }]),
  }));
  assert.equal(configured.ok, true);
  assert.equal(configured.safeSummary.LOCAL_LLM_NODE_COUNT, 1);
});

test('runtime env rejects malformed or duplicate local fleet nodes', () => {
  const malformed = validateRuntimeEnv(strictEnv({
    LOCAL_LLM_EXECUTION_REQUIRED: 'true',
    LOCAL_LLM_NODES_JSON: '[not-json',
  }));
  assert.equal(malformed.ok, false);
  assert.ok(malformed.errors.includes('LOCAL_LLM_NODES_JSON must contain valid JSON'));

  const duplicate = validateRuntimeEnv(strictEnv({
    LOCAL_LLM_EXECUTION_REQUIRED: 'true',
    LOCAL_LLM_NODES_JSON: JSON.stringify([
      { id: 'x1-370', baseUrl: 'http://x1-370.lan:1234/v1' },
      { id: 'x1-370', baseUrl: 'http://xwing.lan:8080/v1' },
    ]),
  }));
  assert.equal(duplicate.ok, false);
  assert.ok(duplicate.errors.some(error => error.includes('duplicate node id x1-370')));
});

test('remote inference requires an explicit provider key', () => {
  const blocked = validateRuntimeEnv(strictEnv({ REMOTE_LLM_EXECUTION_ENABLED: 'true' }));
  assert.equal(blocked.ok, false);
  assert.ok(blocked.errors.includes('OPENROUTER_API_KEY is required when remote LLM execution is enabled'));

  const allowed = validateRuntimeEnv(strictEnv({ REMOTE_LLM_EXECUTION_ENABLED: 'true', OPENROUTER_API_KEY: 'test-key' }));
  assert.equal(allowed.ok, true);
  assert.equal(allowed.safeSummary.OPENROUTER_API_KEY, 'configured');
});

test('assert runtime env throws in invalid production mode', () => {
  assert.throws(() => assertRuntimeEnv({ DEPLOYMENT_ENV: 'production' }), /runtime_env_invalid/);
});
