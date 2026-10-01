#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';

/**
 * One honest picture of release readiness.
 *
 * Everything here already exists: the gap register, the rehearsal record, the
 * release record, the CI runs, the behavioural verifier. What was missing is a
 * single command that states plainly what is proven and what is not, because
 * during this work the most dangerous thing that happened was a *wiring*
 * validator's green result being read as behavioural proof. That confusion is
 * what this command exists to prevent.
 *
 * It reports; it does not decide. It never marks a human gate satisfied.
 */

function git(args, cwd) {
  const result = spawnSync('git', args, { encoding: 'utf8', cwd });
  return result.status === 0 ? result.stdout.trim() : null;
}

function nodeCommand(args, env = {}) {
  const result = spawnSync(process.execPath, args, {
    encoding: 'utf8',
    env: { ...process.env, ...env },
    maxBuffer: 32 * 1024 * 1024
  });
  return { status: result.status, stdout: result.stdout, stderr: result.stderr };
}

const head = git(['rev-parse', 'HEAD']);
const treeDirty = (git(['status', '--porcelain']) || '').length > 0;
const branch = git(['rev-parse', '--abbrev-ref', 'HEAD']);

const sections = [];

/* ------------------------------ source state ------------------------------ */

sections.push({
  area: 'source',
  lines: [
    { label: 'head', value: head ? head.slice(0, 12) : null },
    { label: 'branch', value: branch },
    { label: 'workingTreeClean', value: !treeDirty }
  ]
});

/* ------------------------------ test gates ------------------------------ */

// Running the suite from here recurses: the suite contains the test that calls
// this command. The guard makes the nesting explicit rather than incidental.
const skipTestRun = process.env.RELEASE_STATUS_SKIP_TESTS === 'true' || process.env.NODE_TEST_CONTEXT !== undefined;
const nodeSummary = {};
if (skipTestRun) {
  sections.push({
    area: 'tests',
    note: 'skipped: invoked from within a test run, which would otherwise recurse',
    lines: [{ label: 'skipped', value: true }]
  });
} else {
  const nodeTests = nodeCommand(['scripts/run-node-test-shard.mjs']);
  if (nodeTests.status !== 0) {
    // A failed test run must be a blocker, not an empty section. Reporting nulls
    // here is what let a broken nested spawn look like a pass.
    process.stderr.write(`${JSON.stringify({ ok: false, error: 'release_status_test_run_failed', stderr: String(nodeTests.stderr).slice(0, 2000) }, null, 2)}\n`);
    process.exit(1);
  }
  for (const line of nodeTests.stdout.split('\n')) {
    const match = line.match(/^# (tests|pass|fail|skipped) (\d+)$/);
    if (match) nodeSummary[match[1]] = Number(match[2]);
  }
  sections.push({
    area: 'tests',
    note: 'run with the same recursive discovery CI uses, via npm test',
    lines: [
      { label: 'nodeTests', value: nodeSummary.tests ?? null },
      { label: 'nodePassing', value: nodeSummary.pass ?? null },
      { label: 'nodeFailing', value: nodeSummary.fail ?? null },
      { label: 'nodeSkipped', value: nodeSummary.skipped ?? null }
    ]
  });
}

/* ------------------------------ controls ------------------------------ */

const controls = nodeCommand(['scripts/verify-release-controls.mjs']);
let controlReport = null;
try {
  controlReport = JSON.parse(controls.stdout);
} catch {
  controlReport = null;
}
sections.push({
  area: 'releaseControls',
  note: 'behavioural: each control is exercised by breaking its input',
  lines: [
    { label: 'behaviouralChecks', value: controlReport?.behaviouralChecks?.total ?? null },
    { label: 'behaviouralFailed', value: controlReport?.behaviouralChecks?.failed ?? null },
    { label: 'structuralChecks', value: controlReport?.structuralChecks?.total ?? null },
    { label: 'ok', value: controlReport?.ok ?? false }
  ]
});

/* ------------------------------ rehearsal evidence ------------------------------ */

let rehearsal = null;
const rehearsalPath = 'data/rehearsal/rehearsal-record.json';
if (fs.existsSync(rehearsalPath)) {
  try {
    rehearsal = JSON.parse(fs.readFileSync(rehearsalPath, 'utf8'));
  } catch {
    rehearsal = null;
  }
}
const rehearsalSha = rehearsal?.releaseSha ?? null;

// The SHA match is necessary but not sufficient. A `--only` subset run records
// the same releaseSha with a fraction of the steps, so a SHA comparison alone
// accepted a one-step record as a rehearsal of the head. A record that did not
// execute the whole sequence does not certify anything about this revision.
const REHEARSAL_REQUIRED_STEPS = [
  'source-revision', 'declared-services', 'trading-services-startup-contract',
  'preflight-port', 'render-compose-model', 'build-images', 'start-postgres',
  'apply-migrations', 'start-application', 'predeploy-backup',
  'production-paper-smoke', 'teardown'
];
const rehearsalStepNames = new Set((rehearsal?.steps ?? []).map(s => s?.name));
const rehearsalMissingSteps = rehearsal ? REHEARSAL_REQUIRED_STEPS.filter(n => !rehearsalStepNames.has(n)) : null;
const rehearsalIsPartial = Boolean(rehearsal && (rehearsal.partial === true || rehearsalMissingSteps.length > 0));
// Pure SHA comparison, so a reader can tell "wrong revision" apart from
// "incomplete run". They are independent: a partial record for this head matches
// the SHA and still certifies nothing.
const rehearsalMatchesHead = rehearsalSha != null && rehearsalSha === head;
sections.push({
  area: 'rehearsal',
  note: 'a passing record only counts if it was produced by the head being deployed',
  lines: [
    { label: 'rehearsedSha', value: rehearsalSha ? rehearsalSha.slice(0, 12) : null },
    { label: 'matchesCurrentHead', value: rehearsalMatchesHead },
    { label: 'stepsPassed', value: rehearsal ? rehearsal.steps.filter(s => s.status === 'passed').length : null },
    { label: 'stepsTotal', value: rehearsal?.steps?.length ?? null },
    { label: 'workingTreeCleanAtRehearsal', value: rehearsal?.steps?.find(s => s.name === 'source-revision')?.detail?.workingTreeClean ?? null },
    { label: 'certifiesTargetHost', value: rehearsal?.certifiesTargetHost ?? null },
    { label: 'recordIsComplete', value: rehearsal ? !rehearsalIsPartial : null },
    { label: 'missingSteps', value: rehearsalMissingSteps }
  ]
});

/* ------------------------------ gap register ------------------------------ */

const gaps = [];
if (fs.existsSync('docs/PRODUCTION_PAPER_GAP_REGISTER.md')) {
  for (const line of fs.readFileSync('docs/PRODUCTION_PAPER_GAP_REGISTER.md', 'utf8').split('\n')) {
    if (!line.startsWith('| G-')) continue;
    const cells = line.split('|').map(cell => cell.trim());
    if (cells.length > 4) gaps.push({ id: cells[1], gap: cells[2], status: cells[3] });
  }
}
const closedGaps = gaps.filter(g => g.status === 'Closed');
sections.push({
  area: 'gapRegister',
  lines: [
    { label: 'closed', value: closedGaps.length },
    { label: 'total', value: gaps.length },
    { label: 'outstanding', value: gaps.filter(g => g.status !== 'Closed').map(g => `${g.id} ${g.status}`) }
  ]
});

/* ------------------------------ human gates ------------------------------ */

const record = nodeCommand(['scripts/release-record.mjs', 'validate']);
let recordReport = null;
try {
  recordReport = JSON.parse(record.stdout);
} catch {
  recordReport = null;
}
const outstanding = recordReport?.missing ?? [];
const ownerGaps = outstanding.filter(item => item.startsWith('owner_missing:'));
const attestationGaps = outstanding.filter(item => item.startsWith('attestation_missing:'));
sections.push({
  area: 'humanGates',
  note: 'these cannot be satisfied by any automated check; they require people',
  lines: [
    { label: 'namedOwnersOutstanding', value: ownerGaps.length },
    { label: 'attestationsOutstanding', value: attestationGaps.length },
    { label: 'evidenceFieldsOutstanding', value: outstanding.filter(item => item.startsWith('evidence_missing:')).length },
    { label: 'items', value: outstanding }
  ]
});

/* ------------------------------ certification ------------------------------ */

const certification = nodeCommand(['scripts/certify-production-paper.mjs'], {
  DEPLOYMENT_ENV: 'development',
  STRICT_RUNTIME_VALIDATION: 'false',
  LOCAL_LLM_EXECUTION_REQUIRED: 'true',
  REMOTE_LLM_EXECUTION_ENABLED: 'false'
});
let certReport = null;
try {
  certReport = JSON.parse(certification.stdout);
} catch {
  certReport = null;
}
sections.push({
  area: 'certification',
  lines: [
    { label: 'ok', value: certReport?.ok ?? null },
    { label: 'failures', value: certReport?.failures ?? null },
    { label: 'unverifiedControls', value: certReport?.unverifiedControls ?? null },
    { label: 'liveTradingCertified', value: certReport?.liveTradingCertified ?? null }
  ]
});

/* ------------------------------ verdict ------------------------------ */

// The verdict is deliberately narrow. Automation can prove the machine behaves;
// it cannot prove a person reviewed a diff or owns an incident.
const blockers = [];
if (treeDirty) blockers.push('working tree is dirty: the head does not identify the tree you would deploy');
if (rehearsalIsPartial) blockers.push('rehearsal record is partial; re-run the full sequence without --only');
if (nodeSummary.fail > 0) blockers.push(`${nodeSummary.fail} node test(s) failing`);
if (controlReport && controlReport.ok === false) blockers.push('release control verification is failing');
if (!rehearsal) blockers.push('no rehearsal evidence on disk');
else {
  if (!rehearsalMatchesHead) blockers.push(`rehearsal evidence is for ${rehearsalSha ? rehearsalSha.slice(0, 12) : 'unknown'}, not the current head`);
  if (rehearsalIsPartial) {
    blockers.push(
      `rehearsal evidence for ${rehearsalSha ? rehearsalSha.slice(0, 12) : 'unknown'} is partial ` +
      `(missing: ${rehearsalMissingSteps.join(', ')}); a subset run cannot certify a revision`
    );
  }
}
if (recordReport && recordReport.ok === false) blockers.push(`${outstanding.length} release-record item(s) outstanding, including human sign-off`);

const automatedGreen = !treeDirty
  && (nodeSummary.fail ?? 1) === 0
  && controlReport?.ok === true
  && Boolean(rehearsal)
  && rehearsalMatchesHead;

process.stdout.write(`${JSON.stringify({
  ok: automatedGreen,
  automatedGatesGreen: automatedGreen,
  humanGatesOutstanding: (recordReport?.ok === false) || (certReport?.unverifiedControls?.length > 0),
  readyToDeploy: false,
  readyToDeployReason: 'Automation cannot attest to human sign-off, named ownership, a host-chosen backup destination, or an independent review. Those are recorded as outstanding on purpose.',
  sections,
  blockers,
  liveTradingCertified: false
}, null, 2)}\n`);

process.exit(automatedGreen ? 0 : 1);