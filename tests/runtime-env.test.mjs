import test from 'node:test';
import assert from 'node:assert/strict';
import { validateRuntimeEnv } from '../packages/config/src/runtimeEnv.mjs';

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
  const result = validateRuntimeEnv({
    DEPLOYMENT_ENV: 'production',
    OPERATOR_STORE: 'postgres',
    DATABASE_URL: 'postgresql://portfolio:strong-hosted-password@db.example.internal:5432/portfolio',
    OPERATOR_AUTH_REQUIRED: 'true',
    OPERATOR_ADMIN_TOKEN: 'admin-token',
    CSRF_REQUIRED: 'true',
    OPERATOR_CSRF_TOKEN: 'csrf-token',
    CORS_ORIGINS: 'https://operator.example.com',
    LIVE_TRADING: 'true'
  });
  assert.equal(result.ok, false);
  assert.ok(result.errors.includes('LIVE_TRADING must remain false until live certification is complete'));
});

test('runtime env accepts strict paper-only production config', () => {
  const result = validateRuntimeEnv({
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
    ALLOW_LIVE_SETTLEMENT_REDEMPTION: 'false'
  });
  assert.equal(result.ok, true);
  assert.equal(result.strict, true);
  assert.equal(result.safeSummary.DATABASE_URL.includes('strong-hosted-password'), false);
});
