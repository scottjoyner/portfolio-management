import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const SCRIPT = 'scripts/release-record.mjs';

function runScript(args, cwd = process.cwd()) {
  return spawnSync(process.execPath, [SCRIPT, ...args], { encoding: 'utf8', cwd });
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

test('a fresh release record leaves every owner blank', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'release-record-'));
  const target = path.join(dir, 'record.json');
  const result = runScript(['init', '--record', target]);
  assert.equal(result.status, 0, result.stderr);
  const record = readJson(target);

  // The SHA may be filled from git; nothing else may be invented.
  assert.ok(record.releaseSha);
  for (const value of Object.values(record.owners)) assert.equal(value, null);
  for (const value of Object.values(record.attestations)) assert.equal(value, null);
  assert.equal(record.liveTradingCertified, false);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('an empty record fails closed and names every missing item', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'release-record-'));
  const target = path.join(dir, 'record.json');
  runScript(['init', '--record', target]);
  const result = runScript(['validate', '--record', target]);
  assert.equal(result.status, 1, 'an unfilled record must not validate');
  const report = JSON.parse(result.stdout);
  assert.equal(report.ok, false);
  for (const key of ['releaseOperator', 'codeReviewer', 'securityReviewer', 'rollbackOwner', 'incidentOwner']) {
    assert.ok(report.missing.includes(`owner_missing:${key}`), `expected ${key} to be reported missing`);
  }
  assert.ok(report.missing.includes('attestation_missing:humanReviewCompleted'));
  fs.rmSync(dir, { recursive: true, force: true });
});

test('a record claiming everything except the human review still fails', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'release-record-'));
  const target = path.join(dir, 'record.json');
  runScript(['init', '--record', target]);
  const record = readJson(target);
  record.owners = Object.fromEntries(Object.keys(record.owners).map(key => [key, 'a person']));
  record.evidence = Object.fromEntries(Object.keys(record.evidence).map(key => [key, 'recorded']));
  record.attestations = { acceptedResidualRisks: 'none material' };
  fs.writeFileSync(target, JSON.stringify(record, null, 2));

  const result = runScript(['validate', '--record', target]);
  assert.equal(result.status, 1, 'the human review is not optional');
  const report = JSON.parse(result.stdout);
  assert.deepEqual(report.missing, ['attestation_missing:humanReviewCompleted']);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('any live-trading certification claim is rejected outright', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'release-record-'));
  const target = path.join(dir, 'record.json');
  runScript(['init', '--record', target]);
  const record = readJson(target);
  record.owners = Object.fromEntries(Object.keys(record.owners).map(key => [key, 'a person']));
  record.evidence = Object.fromEntries(Object.keys(record.evidence).map(key => [key, 'recorded']));
  record.attestations = { humanReviewCompleted: 'done', acceptedResidualRisks: 'none' };
  record.liveTradingCertified = true;
  fs.writeFileSync(target, JSON.stringify(record, null, 2));
  assert.equal(runScript(['validate', '--record', target]).status, 1);
  assert.ok(JSON.parse(runScript(['validate', '--record', target]).stdout).missing.includes('live_trading_claim_forbidden'));
  fs.rmSync(dir, { recursive: true, force: true });
});

test('a fully completed record validates, so the gate is satisfiable', () => {
  // A gate that can never pass is not a gate; it is a wall. This proves the
  // record describes a reachable end state.
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'release-record-'));
  const target = path.join(dir, 'record.json');
  runScript(['init', '--record', target]);
  const record = readJson(target);
  record.owners = Object.fromEntries(Object.keys(record.owners).map(key => [key, 'a named person']));
  record.evidence = Object.fromEntries(Object.keys(record.evidence).map(key => [key, 'recorded']));
  record.attestations = { humanReviewCompleted: 'reviewed', acceptedResidualRisks: 'none material' };
  fs.writeFileSync(target, JSON.stringify(record, null, 2));
  const result = runScript(['validate', '--record', target]);
  assert.equal(result.status, 0, result.stdout);
  assert.equal(JSON.parse(result.stdout).ok, true);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('a missing record fails rather than defaulting to approved', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'release-record-'));
  const result = runScript(['validate', '--record', path.join(dir, 'absent.json')]);
  assert.equal(result.status, 1);
  assert.equal(JSON.parse(result.stderr).reason, 'release_record_absent');
  fs.rmSync(dir, { recursive: true, force: true });
});

test('the review packet names the security surface and does not claim a review', () => {
  const result = runScript(['packet']);
  assert.equal(result.status, 0);
  const packet = JSON.parse(result.stdout);
  assert.ok(packet.securityRelevantSurface.length >= 5);
  assert.ok(packet.requiredOfReviewer.some(item => /overseer/i.test(item)));
  assert.ok(packet.requiredOfReviewer.some(item => /counterfactual/i.test(item)));
  assert.match(packet.note, /not a review/i);
});

test('the rehearsal refuses a dirty tree, because a SHA then identifies nothing', () => {
  // Regression guard for a real evidence-integrity defect: the runner recorded
  // the HEAD SHA of a dirty tree, so the record cited a commit that could not
  // have produced the result. Loading that commit later shows a broken compose
  // file and makes a passing rehearsal look fabricated.
  const rehearsal = fs.readFileSync('scripts/rehearse-deployment.mjs', 'utf8');
  assert.ok(rehearsal.includes('rehearsal_working_tree_dirty'));
  assert.ok(rehearsal.includes('status.stdout.trim() !=='));
  // The dirty-tree refusal must throw rather than merely record a flag.
  assert.ok(/if \(status\.stdout\.trim\(\) !== ''\) \{\s*throw/.test(rehearsal), 'a dirty tree must abort the rehearsal, not just be noted');
});

test('the rehearsal record cannot certify its own target host', () => {
  const rehearsal = fs.readFileSync('scripts/rehearse-deployment.mjs', 'utf8');
  assert.ok(rehearsal.includes('certifiesTargetHost: false'));
  assert.ok(rehearsal.includes('liveTradingCertified: false'));
  // The teardown must not use the prohibited `down -v`.
  assert.ok(!rehearsal.includes("'down', '-v'"));
});

test('release status separates automated gates from human gates, and refuses to certify', () => {
  // The point of this command is the distinction, so assert the distinction.
  // The test run is skipped: release-status invokes the suite, and the suite
  // contains this test, so leaving it on recurses.
  const result = spawnSync(process.execPath, ['scripts/release-status.mjs'], {
    encoding: 'utf8',
    env: { ...process.env, RELEASE_STATUS_SKIP_TESTS: 'true' },
    maxBuffer: 32 * 1024 * 1024
  });
  const report = JSON.parse(result.stdout);
  // The exit code tracks the automated gates only. Outstanding human gates are
  // the expected state and must not be reported as an automated failure.
  assert.equal(result.status, report.automatedGatesGreen ? 0 : 1, 'exit code must track automatedGatesGreen, not the human gates');
  assert.equal(report.humanGatesOutstanding, true, 'human gates are genuinely outstanding and must be reported');

  for (const area of ['source', 'tests', 'releaseControls', 'rehearsal', 'gapRegister', 'humanGates', 'certification']) {
    assert.ok(report.sections.some(section => section.area === area), `missing section: ${area}`);
  }

  // No tool can attest to human sign-off, so this must never be computed true.
  assert.equal(report.readyToDeploy, false);
  assert.match(report.readyToDeployReason, /cannot attest/i);

  // The rehearsal must be cross-checked against the head being deployed. CI has
  // no rehearsal record -- the evidence is host-local and gitignored -- so both
  // "no record" and "record for another head" must surface as blockers rather
  // than as a quiet pass.
  const rehearsal = report.sections.find(section => section.area === 'rehearsal');
  const line = label => rehearsal.lines.find(entry => entry.label === label)?.value ?? null;
  assert.ok(rehearsal.lines.some(entry => entry.label === 'matchesCurrentHead'), 'the rehearsal must be compared against the current head');

  const host = line('certifiesTargetHost');
  if (line('rehearsedSha') !== null) {
    assert.equal(host, false, 'a rehearsal must never claim to certify its target host');
  }
  // Rehearsal evidence must be rejected for the right reason. A record is
  // acceptable only when it is complete AND was produced by the head being
  // deployed; those are independent facts and either alone is disqualifying.
  const blockers = report.blockers.join(' | ');
  const complete = line('recordIsComplete');
  if (line('rehearsedSha') === null) {
    assert.match(blockers, /no rehearsal evidence/i, 'no rehearsal on disk must be an explicit blocker');
  } else if (line('matchesCurrentHead') === false) {
    assert.match(blockers, /rehearsal evidence is for/i, 'a rehearsal for another head must be an explicit blocker');
  } else if (complete === false) {
    // The branch that was missing. `--only` writes a record carrying the current
    // head's SHA with a fraction of the steps, so a SHA comparison alone reported
    // a one-step run as a rehearsal of the head, and every assertion below then
    // passed against evidence that certified nothing.
    assert.match(blockers, /partial/i, 'a partial rehearsal must be an explicit blocker');
  } else {
    assert.doesNotMatch(blockers, /rehearsal/i, 'a complete rehearsal of the current head must not be reported as a blocker');
  }

  // Human gates must be enumerated by name rather than summarised away. The
  // release record is host-local and gitignored, so on a clean CI checkout it
  // is absent entirely; that must still read as outstanding rather than green.
  const human = report.sections.find(section => section.area === 'humanGates');
  const owners = human.lines.find(line => line.label === 'namedOwnersOutstanding');
  const items = human.lines.find(line => line.label === 'items').value ?? [];
  if (owners.value > 0) {
    assert.equal(items.length > 0, true, 'outstanding gates must be listed by name, not just counted');
    assert.ok(items.some(item => item.startsWith('owner_missing:')), 'named owners must appear by name');
  } else {
    assert.equal(report.humanGatesOutstanding, true, 'with no release record at all, human gates must still read as outstanding');
  }
});
