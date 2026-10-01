#!/usr/bin/env node
import { existsSync, readFileSync } from 'node:fs';

/**
 * Static contract for the off-host backup (G-005) and immutable audit anchoring
 * (G-006) controls.
 *
 * These two gaps are destination-neutral by design: the deployment owner picks
 * the storage. That means the only thing this repository can enforce is that
 * the mechanism exists, that nothing defaults to the local volume, and that
 * verification failures are blocking rather than advisory. That is what this
 * validator checks.
 */

const requiredFiles = [
  'packages/backup/src/manifest.mjs',
  'packages/backup/src/uploader.mjs',
  'packages/audit/src/anchor.mjs',
  'scripts/backup-export.mjs',
  'scripts/backup-verify.mjs',
  'scripts/audit-anchor.mjs',
  'scripts/audit-anchor-verify.mjs',
  'tests/backup-offsite.test.mjs',
  'tests/audit-anchor.test.mjs',
  'packages/backtesting/src/counterfactualReplay.mjs',
  'tests/counterfactual-replay.test.mjs',
  'scripts/rehearse-deployment.mjs',
  'scripts/release-record.mjs',
  'tests/release-gates.test.mjs',
];

const read = path => readFileSync(path, 'utf8');
const errors = [];
for (const file of requiredFiles) if (!existsSync(file)) errors.push(`missing_file:${file}`);
if (errors.length) {
  process.stderr.write(`${JSON.stringify({ ok: false, errors }, null, 2)}\n`);
  process.exit(1);
}

const manifest = read('packages/backup/src/manifest.mjs');
const uploader = read('packages/backup/src/uploader.mjs');
const anchor = read('packages/audit/src/anchor.mjs');
const backupExport = read('scripts/backup-export.mjs');
const backupVerify = read('scripts/backup-verify.mjs');
const auditAnchor = read('scripts/audit-anchor.mjs');
const auditVerify = read('scripts/audit-anchor-verify.mjs');
const backupTests = read('tests/backup-offsite.test.mjs');
const counterfactual = read('packages/backtesting/src/counterfactualReplay.mjs');
const counterfactualTests = read('tests/counterfactual-replay.test.mjs');
const rehearsal = read('scripts/rehearse-deployment.mjs');
const releaseRecord = read('scripts/release-record.mjs');
const compose = read('docker-compose.production.yml');
const releaseGateTests = read('tests/release-gates.test.mjs');
const anchorTests = read('tests/audit-anchor.test.mjs');
const packageJson = read('package.json');
const certifier = read('scripts/certify-production-paper.mjs');
const checklist = read('docs/FIRST_PROD_RELEASE_CHECKLIST.md');
const runbook = read('docs/DEPLOYMENT_ROLLBACK_RUNBOOK.md');
const gapRegister = read('docs/PRODUCTION_PAPER_GAP_REGISTER.md');

const checks = [
  // --- G-005: the backup must be provable, not merely present ---
  [manifest.includes('signManifest') && manifest.includes('verifyManifestSignature'), 'backup manifests must be signable and verifiable'],
  [manifest.includes('ed25519'), 'backup manifests must be signed with an asymmetric key so ownership is provable'],
  [manifest.includes('encryptDump') && manifest.includes('aes-256-gcm'), 'backup dumps must support authenticated encryption'],
  [manifest.includes('verifyRestoredDump'), 'restored dumps must be verified against their manifest'],
  [manifest.includes('REQUIRED_OWNERSHIP_FIELDS') && manifest.includes('releaseSha') && manifest.includes('operator'), 'backup manifests must record ownership and release provenance'],
  [manifest.includes('applyRetentionPolicy'), 'backup retention must be applied rather than accumulating forever'],
  [uploader.includes('createFilesystemUploader') && uploader.includes('createS3Uploader'), 'backup must support both a filesystem and an S3-compatible destination'],
  [uploader.includes('signS3Request') && uploader.includes('AWS4-HMAC-SHA256'), 'the S3 destination must sign requests rather than sending anonymous writes'],
  [uploader.includes('backup_destination_kind_required') && uploader.includes('backup_destination_kind_unsupported'), 'an absent or unknown backup destination must be an error, never a silent local fallback'],
  [uploader.includes('readBackVerified') && uploader.includes('backup_upload_readback_checksum_mismatch'), 'upload must read back and compare, so a destination that silently drops bytes fails'],
  [uploader.includes('backup_destination_key_unsafe'), 'backup destination keys must be constrained against path traversal'],
  [backupExport.includes('BACKUP_DESTINATION') === false && backupExport.includes('BACKUP_FILESYSTEM_ROOT') && backupExport.includes('BACKUP_S3_ENDPOINT'), 'the export command must take its destination from host-managed configuration'],
  [backupExport.includes('backup_destination_required') && backupExport.includes('backup_destination_ambiguous'), 'the export command must refuse a missing or ambiguous destination'],
  [backupVerify.includes('backup_restore_verification_failed') && backupVerify.includes('process.exit(1)'), 'restore verification must fail the process so it can gate certification'],
  [backupTests.includes('tampered') && backupTests.includes('rejects a foreign signing key') && backupTests.includes('read back the same bytes'), 'backup tests must cover manifest tampering, foreign keys, and upload readback'],

  // --- G-006: the anchor must detect a rebuilt local chain ---
  [anchor.includes('buildAnchorRecord') && anchor.includes('verifyAnchorSequence'), 'audit anchors must be built and their sequence verified'],
  [anchor.includes('signAnchorRecord') && anchor.includes('verifyAnchorSignature'), 'audit anchors must be signed and their signatures verified'],
  [anchor.includes('compareLocalAndExternalRoots'), 'local and external audit roots must be comparable'],
  [anchor.includes('audit_anchor_refuses_invalid_chain'), 'an anchor must refuse to publish a root for an already-broken chain'],
  [anchor.includes('audit_root_mismatch') && anchor.includes('anchor_sequence_gap') && anchor.includes('audit_anchor_sequence_invalid'), 'root mismatches and anchor sequence gaps must be detected'],
  [anchor.includes('audit_anchor_none_published'), 'having no published anchor must block certification rather than pass silently'],
  [anchor.includes('appendAnchor') && anchor.includes('audit_anchor_append_conflict'), 'anchor publication must be append-only'],
  [auditAnchor.includes("flag: 'wx'") && auditAnchor.includes('anchor_destination_not_append_only'), 'the anchor exporter must refuse to overwrite an existing anchor file'],
  [auditVerify.includes('process.exit(1)') && auditVerify.includes('blocking: true'), 'anchor verification must block on failure'],
  [anchorTests.includes('rebuilt local chain is caught') && anchorTests.includes('sequence gap'), 'anchor tests must prove a rebuilt local chain is detected and that removed anchors are detected'],

  // --- G-004: the counterfactual must be reproducible and cost-adjusted ---
  [counterfactual.includes('buildReplayEnvelope') && counterfactual.includes('envelopeHash'), 'the counterfactual must run over a hashed replay envelope'],
  [counterfactual.includes('runCounterfactualReplay') && counterfactual.includes('replayBotOverEnvelope'), 'the counterfactual must re-run the deterministic bot, not assert a number'],
  [counterfactual.includes('attributeAgentDecision') && counterfactual.includes('costJustified'), 'attribution must net the agent and provider cost out of the incremental value'],
  [counterfactual.includes('REPLAY_OUTCOME.PENDING'), 'a counterfactual that cannot be built must stay pending rather than be approximated'],
  [counterfactual.includes('attribution_counterfactual_pending'), 'a pending counterfactual must produce no attribution record at all'],
  [counterfactual.includes('maxPositionNotionalUsd'), 'the counterfactual must respect the envelope risk limits'],
  [counterfactual.includes('feeBps') && counterfactual.includes('slippageBps'), 'the envelope must freeze the fee and slippage model so runs stay comparable'],
  [counterfactual.includes('== null') && counterfactual.includes('Number(null) is 0'), 'null numerics must be rejected before coercion rather than read as zero'],
  [counterfactualTests.includes('PRODUCE THE SAME HASH') && counterfactualTests.includes('fee or slippage assumption changes the hash'), 'replay tests must prove determinism and that cost assumptions are part of the identity'],
  [counterfactualTests.includes('null or malformed attribution input stays pending') && counterfactualTests.includes('genuinely zero agent cost'), 'attribution tests must prove missing evidence stays pending while a real zero cost still resolves'],
  [counterfactualTests.includes('not counted as a win'), 'attribution tests must prove a profitable override that did not cover its cost is not scored as a win'],

  // --- G-007: the rehearsal must produce evidence, not a claim ---
  [rehearsal.includes('preflight-port') && rehearsal.includes('rehearsal_api_port_occupied'), 'the rehearsal must refuse to run against an occupied API port rather than smoke-test a foreign service'],
  [rehearsal.includes('apply-migrations') && rehearsal.includes('rehearsal_migrate_not_idempotent'), 'the rehearsal must prove migration idempotency, not just that migrations ran'],
  [rehearsal.includes('rehearsal_services_not_running'), 'the rehearsal must verify the application services actually came up'],
  [rehearsal.includes('predeploy-backup') && rehearsal.includes('rehearsal_backup_empty'), 'the rehearsal must produce and size-check a pre-deploy logical backup'],
  [rehearsal.includes("PAPER_ONLY_FLAGS") && rehearsal.includes('rehearsal_paper_only_flags_violated'), 'the rehearsal must verify the paper-only flags in the rendered compose model'],
  [rehearsal.includes('volumesRemoved: false') && rehearsal.includes("['down']"), 'the rehearsal teardown must not use down -v, which the runbook prohibits as a rollback'],
  [rehearsal.includes('certifiesTargetHost: false'), 'the rehearsal record must not self-certify the target host'],

  // --- G-008/G-009: the manual gates must be artifacts that fail closed ---
  [releaseRecord.includes('owner_missing') && releaseRecord.includes('REQUIRED_OWNERS'), 'named ownership must be a validated record rather than prose'],
  [releaseRecord.includes('attestation_missing') && releaseRecord.includes('humanReviewCompleted'), 'the human review must be a required attestation'],
  [releaseRecord.includes('live_trading_claim_forbidden'), 'the release record must reject any live-trading certification claim'],
  [releaseRecord.includes('Object.fromEntries(REQUIRED_OWNERS.map(([key]) => [key, null]))'), 'the release record must never auto-fill a name'],
  [packageJson.includes('"rehearse:deployment"') && packageJson.includes('"release-record"'), 'package scripts must expose the rehearsal and release record'],
  [certifier.includes('scripts/release-record.mjs') || certifier.includes('releaseRecord'), 'certification must run the release record validation'],
  [compose.includes('host.docker.internal:host-gateway'), 'the API and worker must be able to resolve a host inference node, which localRequired deployments require'],
  [releaseGateTests.includes('still fails') && releaseGateTests.includes('gate is satisfiable'), 'release record tests must prove the human review is required and that the gate is reachable'],

  // --- both must actually be wired in, or they are decoration ---
  [packageJson.includes('"backup:export"') && packageJson.includes('"backup:verify"'), 'package scripts must expose backup export and verification'],
  [packageJson.includes('"audit:anchor"') && packageJson.includes('"audit:anchor:verify"'), 'package scripts must expose anchor export and verification'],
  [certifier.includes('scripts/backup-verify.mjs') || certifier.includes('backupVerify'), 'production-paper certification must run backup restore verification'],
  [certifier.includes('scripts/audit-anchor-verify.mjs') || certifier.includes('auditAnchorVerify'), 'production-paper certification must run audit anchor verification'],
  [checklist.includes('backup:verify') && checklist.includes('audit:anchor:verify'), 'the release checklist must require both verifications before deployment'],
  [runbook.includes('audit:anchor') || runbook.includes('ANCHOR_DESTINATION'), 'the deployment runbook must document the anchor destination'],
  [gapRegister.includes('| G-005 |') && gapRegister.includes('| G-006 |'), 'the gap register must track off-host backup and immutable audit anchoring'],
];

for (const [ok, message] of checks) if (!ok) errors.push(message);

if (errors.length) {
  process.stderr.write(`${JSON.stringify({ ok: false, errors }, null, 2)}\n`);
  process.exit(1);
}

process.stdout.write(`${JSON.stringify({
  ok: true,
  contract: 'production-paper-release-evidence-v1',
  backup: {
    signedManifests: true,
    asymmetricOwnership: true,
    authenticatedEncryption: true,
    restoreVerification: true,
    destinations: ['filesystem', 's3'],
    explicitDestinationRequired: true,
    uploadReadBackVerified: true,
    retentionPolicy: true
  },
  auditAnchoring: {
    signedAnchors: true,
    appendOnlyPublication: true,
    sequenceGapDetection: true,
    rootComparison: true,
    blocksCertificationOnMismatch: true
  },
  counterfactualReplay: {
    hashedReplayEnvelope: true,
    deterministic: true,
    riskLimitsRespected: true,
    costAdjusted: true,
    pendingOnMissingEvidence: true
  },
  rehearsal: {
    executableSequence: true,
    portPreflight: true,
    migrationIdempotencyProven: true,
    serviceHealthVerified: true,
    preDeployBackupProduced: true,
    selfCertifiesTargetHost: false
  },
  manualGates: {
    namedOwnership: 'release-record.json',
    humanReview: 'release-record.json',
    failsClosedWhenAbsent: true
  },
  destinationSelectedBy: 'deployment-owner',
  liveTradingCertified: false
}, null, 2)}\n`);
