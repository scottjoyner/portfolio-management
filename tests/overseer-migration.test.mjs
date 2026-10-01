import test from 'node:test';
import assert from 'node:assert/strict';
import {
  MIGRATION_DISPOSITION,
  classifyStoredAuthorization,
  migrateStoredAuthorization,
  migrateStoredAuthorizations
} from '../packages/execution/src/overseerMigration.mjs';
import {
  OVERSEER_POLICY_VERSION,
  OVERSEER_SCHEMA_VERSION,
  verifyStoredExecutionAuthorization as verifyCurrent
} from '../packages/execution/src/overseer.mjs';
import { verifyStoredExecutionAuthorization as verifyLegacy } from '../packages/execution/src/overseerLegacy.mjs';
import { FIXTURE_VERIFY_AT, buildLegacyAuthorizedState } from './helpers/legacyAuthorizationFixture.mjs';

const AT = FIXTURE_VERIFY_AT;

test('the fixture is a real legacy authorization, not a hand-written one', () => {
  // Guards the guard: if this fixture stops being genuinely valid, every test
  // below would be asserting against fiction.
  const { storedState, evaluation } = buildLegacyAuthorizedState();
  assert.equal(evaluation.overseerDecision.policyVersion, 'execution-admission-v3');
  assert.equal(evaluation.overseerDecision.decision, 'PAPER');
  const own = verifyLegacy(storedState, { now: AT });
  assert.deepEqual(own.reasons, [], 'the fixture must verify under its own policy');
});

test('a valid legacy authorization does NOT verify under the current policy', () => {
  // This is the problem G-012 exists to solve, stated as a test.
  const { storedState } = buildLegacyAuthorizedState();
  const current = verifyCurrent(storedState, { now: AT });
  assert.equal(current.ok, false);
  assert.ok(current.reasons.includes('overseer_policy_version_mismatch'));
});

test('a valid legacy authorization is classified superseded and is eligible', () => {
  const { storedState } = buildLegacyAuthorizedState();
  const result = classifyStoredAuthorization(storedState, { now: AT });
  assert.equal(result.disposition, MIGRATION_DISPOSITION.SUPERSEDED);
  assert.equal(result.eligibleForMigration, true);
  assert.equal(result.policyVersion, 'execution-admission-v3');
});

test('MIGRATION WORKS: a superseded authorization is re-minted and verifies under v4', () => {
  const { storedState } = buildLegacyAuthorizedState();
  const result = migrateStoredAuthorization(storedState, { now: AT });
  assert.equal(result.migrated, true, `expected migration to succeed, got ${result.reason} ${JSON.stringify(result.verification?.reasons ?? [])}`);
  assert.equal(result.fromPolicyVersion, 'execution-admission-v3');
  assert.equal(result.toPolicyVersion, OVERSEER_POLICY_VERSION);

  const decision = result.state.overseerDecision;
  assert.equal(decision.policyVersion, OVERSEER_POLICY_VERSION);
  assert.equal(decision.schemaVersion, OVERSEER_SCHEMA_VERSION);
  assert.equal(decision.decision, 'PAPER');

  // The migrated authorization must verify under the current policy, and the
  // current verifier is the only thing that gets a say.
  const verified = verifyCurrent(result.state, { now: AT });
  assert.deepEqual(verified.reasons, []);
  assert.equal(classifyStoredAuthorization(result.state, { now: AT }).disposition, MIGRATION_DISPOSITION.CURRENT);
});

test('migration provenance lives on the state, never on the decision', () => {
  // verifyOverseerDecision hashes the whole decision minus decisionHash, so any
  // field added to the decision invalidates its own hash. The provenance block
  // therefore has to sit beside the decision.
  const { storedState } = buildLegacyAuthorizedState();
  const before = storedState.overseerDecision.decisionHash;
  const result = migrateStoredAuthorization(storedState, { now: AT });
  assert.equal(result.migrated, true);
  assert.ok(result.state.overseerMigration, 'provenance must be recorded on the state');
  assert.equal(result.state.overseerMigration.decisionHash, before, 'the superseded decision hash must be preserved');
  assert.equal(result.state.overseerMigration.policyVersion, 'execution-admission-v3');
  assert.equal(result.state.overseerMigration.toPolicyVersion, OVERSEER_POLICY_VERSION);
  assert.equal(result.state.overseerDecision.migratedFrom, undefined, 'nothing may be attached to the decision');
});

test('a trade the legacy policy approved but the current policy refuses is not carried forward', () => {
  // The real reason this migration path is not a formality: v4 adds a
  // certified-runtime gate for automated entries that v3 never had. A legacy
  // authorization for such a trade verifies under v3 and must be refused here
  // rather than silently re-issued.
  const { storedState } = buildLegacyAuthorizedState({ requestOverrides: { tradeIntent: 'entry' } });
  const legacyCheck = verifyLegacy(storedState, { now: AT });
  assert.deepEqual(legacyCheck.reasons, [], 'v3 must still consider this trade approved');

  const result = migrateStoredAuthorization(storedState, {
    now: AT,
    overseerOptions: { requireApproval: false }
  });
  assert.equal(result.migrated, false);
  assert.equal(result.reason, 'current_policy_does_not_approve');
  assert.equal(result.currentPolicyDecision, 'REJECT');
  assert.ok(result.currentPolicyReasons.includes('certified_runtime_identity_required'));
  // A policy refusal is not tampering, so it must not be quarantined.
  assert.equal(result.quarantined, undefined);
  // And the original authorization must come back untouched.
  assert.equal(result.state.overseerDecision.policyVersion, 'execution-admission-v3');
});

test('a tampered legacy authorization is unsound and never re-minted', () => {
  const { storedState } = buildLegacyAuthorizedState();
  const tampered = { ...storedState, overseerDecision: { ...storedState.overseerDecision, decisionHash: 'f'.repeat(64) } };
  const classification = classifyStoredAuthorization(tampered, { now: AT });
  assert.equal(classification.disposition, MIGRATION_DISPOSITION.UNSOUND);
  assert.equal(classification.eligibleForMigration, false);

  const result = migrateStoredAuthorization(tampered, { now: AT });
  assert.equal(result.migrated, false);
  assert.equal(result.quarantined, true);
  assert.equal(result.reason, 'authorization_unsound_under_its_own_policy');
  // A tampered record must never be laundered into a fresh-looking valid one.
  assert.equal(result.state, tampered);
});

test('a mutated trade intent is unsound', () => {
  const { storedState } = buildLegacyAuthorizedState();
  const mutated = { ...storedState, tradeIntentHash: 'a'.repeat(64) };
  assert.equal(classifyStoredAuthorization(mutated, { now: AT }).disposition, MIGRATION_DISPOSITION.UNSOUND);
  assert.equal(migrateStoredAuthorization(mutated, { now: AT }).quarantined, true);
});

test('an unrecognised policy version is unsound', () => {
  const { storedState } = buildLegacyAuthorizedState();
  const future = { ...storedState, overseerDecision: { ...storedState.overseerDecision, policyVersion: 'execution-admission-v99' } };
  const result = classifyStoredAuthorization(future, { now: AT });
  assert.equal(result.disposition, MIGRATION_DISPOSITION.UNSOUND);
  assert.ok(result.reasons.some(reason => reason.includes('policy_version_unknown')));
});

test('non-objects and empty states are unsound', () => {
  for (const value of [null, undefined, 42, 'x', {}]) {
    assert.equal(classifyStoredAuthorization(value, { now: AT }).disposition, MIGRATION_DISPOSITION.UNSOUND);
  }
});

test('an already-current authorization is left alone', () => {
  const { storedState } = buildLegacyAuthorizedState();
  const current = migrateStoredAuthorization(storedState, { now: AT }).state;
  const again = migrateStoredAuthorization(current, { now: AT });
  assert.equal(again.migrated, false);
  assert.equal(again.reason, 'authorization_already_current');
});

test('a batch migrates the good and quarantines the bad, reporting both', () => {
  const { storedState } = buildLegacyAuthorizedState();
  const tampered = { ...storedState, executionId: 'exec-bad', overseerDecision: { ...storedState.overseerDecision, decisionHash: 'e'.repeat(64) } };
  const report = migrateStoredAuthorizations(
    [storedState, tampered, { ...storedState, executionId: 'exec-3' }],
    { now: AT }
  );
  assert.equal(report.total, 3);
  assert.equal(report.quarantined.length, 1);
  assert.equal(report.quarantined[0].id, 'exec-bad');
  assert.equal(report.migrated.length, 2, 'both sound legacy authorizations must migrate');
  assert.equal(report.ok, false, 'a quarantined item must make the whole run non-ok');
  assert.equal(report.summary.migratedCount, 2);
  assert.equal(report.summary.quarantinedCount, 1);
});

test('an empty batch is trivially ok', () => {
  const report = migrateStoredAuthorizations([], { now: AT });
  assert.equal(report.ok, true);
  assert.equal(report.total, 0);
});

test('an expired legacy authorization is triaged as expired, not as tampering', () => {
  // The allocation TTL is 30s; verifying well past it must not read as forged.
  const { storedState } = buildLegacyAuthorizedState();
  const much = { now: '2026-10-01T02:00:00.000Z' };
  const legacy = verifyLegacy(storedState, much);
  assert.equal(legacy.ok, false);
  assert.ok(legacy.reasons.includes('overseer_decision_expired'));

  const classification = classifyStoredAuthorization(storedState, much);
  assert.equal(classification.disposition, MIGRATION_DISPOSITION.EXPIRED);
  assert.equal(classification.eligibleForMigration, false);

  const result = migrateStoredAuthorization(storedState, much);
  assert.equal(result.migrated, false);
  assert.equal(result.reason, 'authorization_expired_under_its_own_policy');
  assert.equal(result.expired, true);
  // Expired is not forged: it must not be quarantined, and it must not be
  // re-issued, because that would resurrect authority allowed to lapse.
  assert.equal(result.quarantined, undefined);
  assert.equal(result.state.overseerDecision.policyVersion, 'execution-admission-v3');
});

test('a batch reports expired separately from quarantined', () => {
  const { storedState } = buildLegacyAuthorizedState();
  const tampered = { ...storedState, executionId: 'exec-bad', overseerDecision: { ...storedState.overseerDecision, decisionHash: 'e'.repeat(64) } };
  const report = migrateStoredAuthorizations([storedState, tampered], { now: '2026-10-01T02:00:00.000Z' });
  assert.equal(report.expired.length, 1);
  assert.equal(report.expired[0].id, 'exec-legacy-1');
  assert.equal(report.quarantined.length, 1);
  assert.equal(report.quarantined[0].id, 'exec-bad');
  assert.equal(report.summary.expiredCount, 1);
  assert.equal(report.summary.quarantinedCount, 1);
});
