#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

/**
 * Release record for G-008 and review packet for G-009.
 *
 * The manual gates in the release checklist -- named owners, a target host, a
 * rehearsal, a human review -- were a paragraph of prose. That is a claim
 * rather than an artifact, and it is exactly the kind of thing that quietly
 * does not happen. This generates the record, and validates it.
 *
 * Two hard rules:
 *
 *  1. It never fills in a name. A missing owner stays visibly missing, because a
 *     plausible-looking placeholder is worse than a blank.
 *  2. It cannot mark the human review complete. That field is a human
 *     assertion; this script can only refuse to certify while it is absent.
 *
 * Usage:
 *   node scripts/release-record.mjs init
 *   node scripts/release-record.mjs validate [--record <path>]
 *   node scripts/release-record.mjs packet
 */

const RECORD_NAME = 'release-record.json';

const REQUIRED_OWNERS = [
  ['releaseOperator', 'Release operator', 'Runs the deployment and signs off on go/no-go.'],
  ['codeReviewer', 'Code reviewer', 'Reviewed the change and the release evidence.'],
  ['securityReviewer', 'Security reviewer', 'Reviewed the authorization boundary and secret handling.'],
  ['rollbackOwner', 'Rollback owner', 'Can execute and verify the rollback procedure.'],
  ['incidentOwner', 'Incident owner', 'On call for this release window.'],
  ['monitoringDestination', 'Monitoring destination', 'Where alerts and health signals are observed.'],
  ['backupDestination', 'Backup destination', 'Off-host destination for logical backups and manifests.'],
  ['auditAnchorDestination', 'Audit anchor destination', 'Append-only or object-locked destination for audit roots.']
];

const REQUIRED_EVIDENCE = [
  ['releaseSha', 'Reviewed source SHA'],
  ['currentImageDigest', 'Current image digest'],
  ['candidateImageDigest', 'Candidate image digest'],
  ['restoreTarget', 'Tested restore target'],
  ['backupManifestSha256', 'Pre-deploy backup manifest sha256'],
  ['rehearsalRecordPath', 'Rehearsal evidence path'],
  ['rehearsalHostConfirmed', 'Rehearsal ran on the intended target host']
];

const REQUIRED_ATTESTATIONS = [
  ['humanReviewCompleted', 'Human code, architecture, security, and operational review completed'],
  ['acceptedResidualRisks', 'Accepted residual risks recorded']
];

function defaultRecord() {
  return {
    schemaVersion: 1,
    kind: 'portfolio-release-record',
    certification: 'production-paper',
    liveTradingCertified: false,
    releaseSha: null,
    createdAt: new Date().toISOString(),
    owners: Object.fromEntries(REQUIRED_OWNERS.map(([key]) => [key, null])),
    evidence: Object.fromEntries(REQUIRED_EVIDENCE.map(([key]) => [key, null])),
    attestations: Object.fromEntries(REQUIRED_ATTESTATIONS.map(([key]) => [key, null])),
    notes: null
  };
}

function currentSha() {
  return spawnSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).stdout.trim() || null;
}

function loadRecord(explicit) {
  const target = explicit ? path.resolve(explicit) : path.join(process.cwd(), 'data', 'rehearsal', RECORD_NAME);
  if (!fs.existsSync(target)) return { record: null, path: target };
  return { record: JSON.parse(fs.readFileSync(target, 'utf8')), path: target };
}

function validate(record) {
  const missing = [];
  const isSet = value => value != null && String(value).trim() !== '';

  for (const [key, label] of REQUIRED_OWNERS) {
    if (!isSet(record?.owners?.[key])) missing.push(`owner_missing:${key}`);
    void label;
  }
  for (const [key] of REQUIRED_EVIDENCE) {
    if (!isSet(record?.evidence?.[key])) missing.push(`evidence_missing:${key}`);
  }
  for (const [key] of REQUIRED_ATTESTATIONS) {
    if (!isSet(record?.attestations?.[key])) missing.push(`attestation_missing:${key}`);
  }
  if (record?.liveTradingCertified === true) missing.push('live_trading_claim_forbidden');
  if (record?.certification !== 'production-paper') missing.push('certification_scope_must_be_production_paper');

  return { ok: missing.length === 0, missing };
}

function init(explicit) {
  const target = explicit ? path.resolve(explicit) : path.join(process.cwd(), 'data', 'rehearsal', RECORD_NAME);
  fs.mkdirSync(path.dirname(target), { recursive: true });
  const record = defaultRecord();
  record.releaseSha = currentSha();
  fs.writeFileSync(target, `${JSON.stringify(record, null, 2)}\n`, { mode: 0o600 });
  process.stdout.write(`${JSON.stringify({ ok: true, recordPath: target, record }, null, 2)}\n`);
}

/**
 * The review packet: what a reviewer actually needs to read, narrowed to the
 * security-relevant surface, so "review the diff" is not the instruction.
 */
function packet() {
  const lastMerge = spawnSync('git', ['log', '--merges', '-1', '--format=%H'], { encoding: 'utf8' }).stdout.trim();
  const base = lastMerge ? `${lastMerge}^1` : null;
  const args = base ? ['diff', '--stat', `${base}..${lastMerge}`] : ['diff', '--stat', 'HEAD~50..HEAD'];
  const diff = spawnSync('git', args, { encoding: 'utf8' }).stdout;
  const securityTouched = spawnSync('git', ['log', '-1', '--format=%H'], { encoding: 'utf8' }).stdout.trim();

  const surface = [
    'packages/execution/src/overseer.mjs — the fail-closed authorization boundary; note the policy version moved to execution-admission-v4-certified-runtime',
    'packages/execution/src/overseerLegacy.mjs — the v3 machinery the v4 wrapper layers on',
    'packages/audit/src/anchor.mjs — external anchoring; a root comparison that blocks certification',
    'packages/backup/src/manifest.mjs — signed manifests and restore verification',
    'scripts/validate-security.mjs — rescoped to git-visible files; confirm this did not weaken the commit-path check',
    'scripts/validate-runtime-artifacts.mjs and scripts/check_no_runtime_state.sh — the runtime-state guards',
    'docker-compose.production.yml — the host.docker.internal change that makes local inference reachable',
    'scripts/rehearse-deployment.mjs — the rehearsal runner itself'
  ];

  process.stdout.write(`${JSON.stringify({
    ok: true,
    kind: 'portfolio-review-packet',
    releaseSha: currentSha(),
    lastMerge,
    securityRelevantSurface: surface,
    lastCommit: securityTouched,
    diffStat: diff.split('\n').filter(line => line.includes('|')).join('\n'),
    requiredOfReviewer: [
      'Confirm the v2 to v4 overseer policy change cannot be used to bypass an authorization check.',
      'Confirm validate-security.mjs still rejects a secret that is staged or force-added.',
      'Confirm a pending or missing counterfactual can never become a non-pending attribution.',
      'Confirm no live-trading route or flag is enabled by this change.',
      'Confirm the accepted residual risks are written down, not implied.'
    ],
    note: 'This packet is generated evidence for a human review. It is not a review, and it cannot substitute for one.'
  }, null, 2)}\n`);
}

const [command, ...rest] = process.argv.slice(2);
const explicitIndex = rest.indexOf('--record');
const explicit = explicitIndex >= 0 ? rest[explicitIndex + 1] : null;

if (command === 'init') {
  init(explicit);
} else if (command === 'validate') {
  const { record, path: target } = loadRecord(explicit);
  if (!record) {
    process.stderr.write(`${JSON.stringify({ ok: false, reason: 'release_record_absent', expectedPath: target }, null, 2)}\n`);
    process.exit(1);
  }
  const result = validate(record);
  process.stdout.write(`${JSON.stringify({
    ...result,
    recordPath: target,
    requiredOwners: REQUIRED_OWNERS.map(([key, label, why]) => ({ key, label, why })),
    requiredEvidence: REQUIRED_EVIDENCE.map(([key, label]) => ({ key, label })),
    requiredAttestations: REQUIRED_ATTESTATIONS.map(([key, label]) => ({ key, label }))
  }, null, 2)}\n`);
  process.exit(result.ok ? 0 : 1);
} else if (command === 'packet') {
  packet();
} else {
  process.stderr.write(`${JSON.stringify({ ok: false, error: 'release_record_command_required', commands: ['init', 'validate', 'packet'] }, null, 2)}\n`);
  process.exit(1);
}
