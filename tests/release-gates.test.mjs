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

test('the rehearsal record cannot certify its own target host', () => {
  const rehearsal = fs.readFileSync('scripts/rehearse-deployment.mjs', 'utf8');
  assert.ok(rehearsal.includes('certifiesTargetHost: false'));
  assert.ok(rehearsal.includes('liveTradingCertified: false'));
  // The teardown must not use the prohibited `down -v`.
  assert.ok(!rehearsal.includes("'down', '-v'"));
});
