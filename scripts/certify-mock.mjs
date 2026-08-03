#!/usr/bin/env node
import { spawnSync } from 'node:child_process';

const commands = [
  ['runtimeArtifacts', ['scripts/validate-runtime-artifacts.mjs']],
  ['web', ['scripts/build-web.mjs']],
  ['migrations', ['scripts/validate-migrations.mjs']],
  ['apiContract', ['scripts/validate-api-contract.mjs']],
  ['security', ['scripts/validate-security.mjs']],
  ['deployment', ['scripts/validate-deployment.mjs']],
];

const checks = commands.map(([name, args]) => {
  const result = spawnSync(process.execPath, args, { encoding: 'utf8', env: { ...process.env, DEPLOYMENT_ENV: 'development', STRICT_RUNTIME_VALIDATION: 'false' } });
  return {
    name,
    ok: result.status === 0,
    status: result.status,
    stdout: String(result.stdout || '').trim().slice(0, 3000),
    stderr: String(result.stderr || '').trim().slice(0, 3000),
  };
});
const failures = checks.filter(row => !row.ok).map(row => row.name);
const report = { ok: failures.length === 0, certification: 'mock-paper', liveTradingCertified: false, checks, failures };
process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
if (!report.ok) process.exitCode = 1;
