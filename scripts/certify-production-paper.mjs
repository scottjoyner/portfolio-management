#!/usr/bin/env node
import { spawnSync } from 'node:child_process';

const commands = [
  ['migrations', ['scripts/validate-migrations.mjs']],
  ['apiContract', ['scripts/validate-api-contract.mjs']],
  ['security', ['scripts/validate-security.mjs']],
  ['deployment', ['scripts/validate-deployment.mjs']],
  ['firstProductionRelease', ['scripts/validate-first-prod-release.mjs']],
  ['runtime', ['scripts/validate-runtime-env.mjs']],
  ['migrationPlan', ['scripts/migrate-postgres.mjs', '--dry-run', '--json']],
];
if (process.env.CERTIFY_RUN_SMOKE === 'true') commands.push(['smoke', ['scripts/smoke-production-paper.mjs']]);

const checks = [];
for (const [name, args] of commands) {
  const result = spawnSync(process.execPath, args, { encoding: 'utf8', env: process.env });
  checks.push({
    name,
    ok: result.status === 0,
    status: result.status,
    stdout: String(result.stdout || '').trim().slice(0, 4000),
    stderr: String(result.stderr || '').trim().slice(0, 4000),
  });
}

const liveFlags = {
  LIVE_TRADING: process.env.LIVE_TRADING || 'false',
  LIVE_TRADING_ENABLED: process.env.LIVE_TRADING_ENABLED || 'false',
  ALLOW_POLYMARKET_ORDER_SUBMISSION: process.env.ALLOW_POLYMARKET_ORDER_SUBMISSION || 'false',
  ALLOW_LIVE_SETTLEMENT_REDEMPTION: process.env.ALLOW_LIVE_SETTLEMENT_REDEMPTION || 'false',
};
const liveBlocked = Object.values(liveFlags).every(value => value !== 'true');
const localRequired = process.env.LOCAL_LLM_EXECUTION_REQUIRED === 'true';
const remoteDisabled = process.env.REMOTE_LLM_EXECUTION_ENABLED !== 'true';
const failures = checks.filter(row => !row.ok).map(row => row.name);
if (!liveBlocked) failures.push('live_flags');
if (!localRequired) failures.push('local_inference_required');
if (!remoteDisabled) failures.push('remote_inference_disabled');

const report = {
  ok: failures.length === 0,
  certification: 'production-paper',
  liveTradingCertified: false,
  localInferenceRequired: localRequired,
  remoteInferenceEnabled: !remoteDisabled,
  liveFlags,
  checks,
  failures,
};
process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
if (!report.ok) process.exitCode = 1;
