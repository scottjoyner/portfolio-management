#!/usr/bin/env node
import { spawnSync } from 'node:child_process';

const commands = [
  ['migrations', ['scripts/validate-migrations.mjs']],
  ['apiContract', ['scripts/validate-api-contract.mjs']],
  ['security', ['scripts/validate-security.mjs']],
  ['deployment', ['scripts/validate-deployment.mjs']],
  ['firstProductionRelease', ['scripts/validate-first-prod-release.mjs']],
  ['runtime', ['scripts/validate-runtime-env.mjs']],
  ['backupAndAnchoring', ['scripts/validate-backup-and-anchoring.mjs']],
  ['migrationPlan', ['scripts/migrate-postgres.mjs', '--dry-run', '--json']],
];
if (process.env.CERTIFY_RUN_SMOKE === 'true') commands.push(['smoke', ['scripts/smoke-production-paper.mjs']]);

// G-005 and G-006 are destination-neutral, so the destination is chosen by the
// deployment owner. When they have configured one, the real verification runs
// and a mismatch is blocking. When they have not, certification reports the gap
// as unresolved rather than treating an unconfigured control as a pass.
if (process.env.CERTIFY_VERIFY_BACKUP === 'true') {
  commands.push(['backupRestoreVerification', ['scripts/backup-verify.mjs']]);
}
if (process.env.CERTIFY_VERIFY_ANCHORS === 'true') {
  commands.push(['auditAnchorVerification', ['scripts/audit-anchor-verify.mjs']]);
}
// G-008/G-009: the manual gates. The record cannot be auto-satisfied, so it is
// reported as an unresolved control unless a named, reviewed record exists.
if (process.env.CERTIFY_VERIFY_RELEASE_RECORD === 'true') {
  commands.push(['releaseRecord', ['scripts/release-record.mjs', 'validate']]);
}

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
if (!remoteDisabled) failures.push('remote_llm_execution_disabled');

// An unconfigured off-host destination is an unresolved gap, not a pass. The
// destination and its credentials are the deployment owner's to supply, so this
// reports the state instead of guessing at it.
const unverified = [];
if (process.env.CERTIFY_VERIFY_BACKUP !== 'true') unverified.push('offsite_backup_destination_unverified');
if (process.env.CERTIFY_VERIFY_ANCHORS !== 'true') unverified.push('external_audit_anchor_unverified');
if (process.env.CERTIFY_VERIFY_RELEASE_RECORD !== 'true') unverified.push('named_ownership_and_human_review_unverified');

const report = {
  ok: failures.length === 0,
  certification: 'production-paper',
  liveTradingCertified: false,
  localInferenceRequired: localRequired,
  remoteInferenceEnabled: !remoteDisabled,
  liveFlags,
  checks,
  failures,
  unverifiedControls: unverified,
};
process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
if (!report.ok) process.exitCode = 1;
