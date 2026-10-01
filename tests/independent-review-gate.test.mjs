import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {
  SECURITY_SURFACE,
  evaluateReviewGate,
  findIndependentApproval,
  requiresIndependentReview,
  securitySurfaceTouched
} from '../scripts/require-independent-review.mjs';

const AUTHOR = 'scottjoyner';
const OVERSEER = 'packages/execution/src/overseer.mjs';
const UNRELATED = 'docs/some-note.md';

test('the gate is inert for pull requests that do not touch the security surface', () => {
  // A gate on every change trains everyone to click through it.
  const result = evaluateReviewGate({
    eventName: 'pull_request',
    changedPaths: [UNRELATED, 'apps/api/src/operatorRouter.mjs'],
    reviews: [],
    author: AUTHOR
  });
  assert.equal(result.ok, true);
  assert.equal(result.required, false);
  assert.equal(result.reason, 'security_surface_untouched');
});

test('the gate is inert for pushes, which have no pull request to review', () => {
  const result = evaluateReviewGate({
    eventName: 'push',
    changedPaths: [OVERSEER],
    reviews: [],
    author: AUTHOR
  });
  assert.equal(result.ok, true);
  assert.equal(result.required, false);
  assert.equal(result.reason, 'not_a_pull_request');
});

test('touching the security surface requires independent review', () => {
  const result = requiresIndependentReview({
    eventName: 'pull_request',
    changedPaths: [UNRELATED, OVERSEER]
  });
  assert.equal(result.required, true);
  assert.deepEqual(result.surface, [OVERSEER]);
});

test('THE FAILURE THIS EXISTS FOR: a self-approved security change fails', () => {
  // The exact shape of the observed failure: author approves their own change.
  const result = evaluateReviewGate({
    eventName: 'pull_request',
    changedPaths: [OVERSEER],
    reviews: [{ state: 'APPROVED', user: { login: AUTHOR } }],
    author: AUTHOR
  });
  assert.equal(result.required, true);
  assert.equal(result.ok, false);
  assert.equal(result.reason, 'independent_review_missing');
});

test('a self-approval does not count even when spelled differently', () => {
  const approval = findIndependentApproval({
    reviews: [{ state: 'APPROVED', user: { login: 'ScottJoyner' } }],
    author: 'scottjoyner'
  });
  assert.equal(approval.approved, false, 'login comparison must be case-insensitive');
});

test('an approval from somebody else satisfies the gate', () => {
  const result = evaluateReviewGate({
    eventName: 'pull_request',
    changedPaths: [OVERSEER],
    reviews: [
      { state: 'COMMENTED', user: { login: AUTHOR } },
      { state: 'APPROVED', user: { login: 'independent-reviewer' } }
    ],
    author: AUTHOR
  });
  assert.equal(result.ok, true);
  assert.equal(result.approvedBy, 'independent-reviewer');
});

test('a later request-for-changes withdraws the approval', () => {
  // An approval followed by CHANGES_REQUESTED is not an approval.
  const approval = findIndependentApproval({
    reviews: [
      { state: 'APPROVED', user: { login: 'reviewer' } },
      { state: 'CHANGES_REQUESTED', user: { login: 'reviewer' } }
    ],
    author: AUTHOR
  });
  assert.equal(approval.approved, false);
});

test('no reviews at all fails closed', () => {
  const result = evaluateReviewGate({
    eventName: 'pull_request',
    changedPaths: [OVERSEER],
    reviews: [],
    author: AUTHOR
  });
  assert.equal(result.ok, false);
  assert.deepEqual(result.reviewStates, []);
});

test('a dismissed review does not count', () => {
  const approval = findIndependentApproval({
    reviews: [{ state: 'DISMISSED', user: { login: 'reviewer' } }],
    author: AUTHOR
  });
  assert.equal(approval.approved, false);
});

test('the failure message lists the surface so the reviewer knows what is at stake', () => {
  const result = evaluateReviewGate({
    eventName: 'pull_request',
    changedPaths: [OVERSEER, 'scripts/validate-security.mjs'],
    reviews: [],
    author: AUTHOR
  });
  assert.equal(result.ok, false);
  assert.ok(result.securitySurface.includes(OVERSEER));
  assert.ok(result.securitySurface.includes('scripts/validate-security.mjs'));
  assert.deepEqual(result.reviewStates, []);
});

test('the security surface exists and covers the authorization boundary', () => {
  for (const required of [
    'packages/execution/src/overseer.mjs',
    'packages/execution/src/overseerLegacy.mjs',
    'packages/execution/src/overseerMigration.mjs',
    'packages/audit/src/anchor.mjs',
    'scripts/validate-security.mjs'
  ]) {
    assert.ok(SECURITY_SURFACE.includes(required), `${required} must be on the review surface`);
  }
  // Narrow on purpose.
  assert.ok(SECURITY_SURFACE.length < 30, 'the surface must stay small enough to be taken seriously');
});

test('every surface path is a file that actually exists', () => {
  // A stale entry would silently stop gating.
  for (const path of SECURITY_SURFACE) {
    assert.ok(fs.existsSync(path), `${path} is on the security surface but does not exist`);
  }
});

test('securitySurfaceTouched matches exactly, not by substring', () => {
  assert.deepEqual(securitySurfaceTouched([`${OVERSEER}.bak`]), []);
  assert.deepEqual(securitySurfaceTouched(['packages/execution/src/overseer.mjs']), [OVERSEER]);
});