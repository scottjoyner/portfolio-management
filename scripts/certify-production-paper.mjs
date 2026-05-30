import { spawnSync } from 'node:child_process';
import { validateRuntimeEnv } from '../packages/config/src/runtimeEnv.mjs';

const checks = [
  ['migrations', ['node', 'scripts/validate-migrations.mjs']],
  ['api_contract', ['node', 'scripts/validate-api-contract.mjs']],
  ['security', ['node', 'scripts/validate-security.mjs']],
  ['deployment', ['node', 'scripts/validate-deployment.mjs']],
  ['first_prod_release', ['node', 'scripts/validate-first-prod-release.mjs']]
];

const results = [];
for (const [name, command] of checks) {
  const result = spawnSync(command[0], command.slice(1), { encoding: 'utf8' });
  results.push({ name, ok: result.status === 0, stdout: result.stdout?.trim(), stderr: result.stderr?.trim() });
}

const runtime = validateRuntimeEnv({
  ...process.env,
  DEPLOYMENT_ENV: process.env.DEPLOYMENT_ENV || 'production',
  STRICT_RUNTIME_VALIDATION: process.env.STRICT_RUNTIME_VALIDATION || 'true'
});
results.push({ name: 'runtime_env', ok: runtime.ok, errors: runtime.errors, warnings: runtime.warnings, safeSummary: runtime.safeSummary });

const liveFlags = {
  LIVE_TRADING: process.env.LIVE_TRADING || 'false',
  ALLOW_POLYMARKET_ORDER_SUBMISSION: process.env.ALLOW_POLYMARKET_ORDER_SUBMISSION || 'false',
  ALLOW_LIVE_SETTLEMENT_REDEMPTION: process.env.ALLOW_LIVE_SETTLEMENT_REDEMPTION || 'false'
};
const liveBlocked = Object.values(liveFlags).every(value => value === 'false');
results.push({ name: 'live_trading_blocked', ok: liveBlocked, liveFlags });

const ok = results.every(result => result.ok);
console.log(JSON.stringify({ ok, certification: 'production-paper', liveTradingCertified: false, results }, null, 2));
if (!ok) process.exit(1);
