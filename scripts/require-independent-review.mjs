#!/usr/bin/env node
import { spawnSync } from 'node:child_process';

/**
 * Independent-review gate for the security-critical surface.
 *
 * This exists because of a specific, observed failure: a 141-commit stack that
 * rewrote the fail-closed execution authorization boundary was merged with every
 * pull request authored and approved by the same account. The CI suite was
 * green throughout, which is exactly why it was not sufficient. A test suite
 * cannot tell you that an authorization check got weaker; a second reader can.
 *
 * So this is not a general "please review" reminder. It is a hard gate that
 * fails closed, on a defined surface, when that surface changes without an
 * approving review from somebody other than the author.
 *
 * The surface is deliberately narrow. Making it broad would train everyone to
 * ignore it, which is how gates decay.
 */

export const SECURITY_SURFACE = [
  'packages/execution/src/overseer.mjs',
  'packages/execution/src/overseerLegacy.mjs',
  'packages/execution/src/overseerMigration.mjs',
  'packages/execution/src/certifiedSpotLifecycle.mjs',
  'packages/execution/src/executionEngine.mjs',
  'packages/execution/src/capitalRiskSnapshot.mjs',
  'packages/execution/src/portfolioAllocator.mjs',
  'packages/audit/src/anchor.mjs',
  'packages/storage/src/auditChain.mjs',
  'packages/storage/src/transactionalPostgresOperatorStore.mjs',
  'apps/api/src/executionRoutePersistence.mjs',
  'scripts/validate-security.mjs',
  'scripts/validate-runtime-artifacts.mjs',
  'scripts/check_no_runtime_state.sh',
  'scripts/migrate-overseer-authorizations.mjs',
  'scripts/rehearse-deployment.mjs',
  'scripts/release-record.mjs',
  'docker-compose.production.yml',
  'config/release-performance-thresholds.json'
];

export const REVIEW_DECISIONS = new Set(['APPROVED']);

/** Which security-critical paths, if any, this change touches. */
export function securitySurfaceTouched(changedPaths = []) {
  return SECURITY_SURFACE.filter(surface => changedPaths.includes(surface));
}

export function requiresIndependentReview({ eventName, baseRef, changedPaths }) {
  // Pushes to a protected branch have no pull request to review, and the
  // post-merge audit is the wrong place to demand a review that already
  // happened. This gate applies to pull requests only.
  if (eventName !== 'pull_request') return { required: false, reason: 'not_a_pull_request', surface: [] };
  const surface = securitySurfaceTouched(changedPaths);
  if (!surface.length) return { required: false, reason: 'security_surface_untouched', surface: [] };
  void baseRef;
  return { required: true, reason: 'security_surface_touched', surface };
}

/**
 * An approving review only counts if it came from someone other than the author.
 * A self-approval is not a review, however approving it looks in the UI.
 *
 * Reviews are reduced to each reviewer's *latest* state before anything is
 * counted. An approval followed by CHANGES_REQUESTED is not an approval, and
 * taking the first match would let a reviewer who found a problem still be
 * counted as having signed off.
 */
export function findIndependentApproval({ reviews = [], author, authorIsBot = false }) {
  const latestByReviewer = new Map();
  for (const review of reviews) {
    const login = review.user?.login ?? review.user?.name ?? null;
    if (!login) continue;
    latestByReviewer.set(login, review);
  }

  const considered = [];
  for (const [login, review] of latestByReviewer) {
    const state = String(review.state ?? '').toUpperCase();
    const isAuthor = author && login.toLowerCase() === String(author).toLowerCase();
    const counts = REVIEW_DECISIONS.has(state) && !isAuthor && !authorIsBot;
    considered.push({ login, state, isAuthor, counts });
    if (counts) return { approved: true, by: login, considered };
  }
  return { approved: false, by: null, considered };
}

export function evaluateReviewGate({ eventName, baseRef, changedPaths = [], reviews = [], author }) {
  const requirement = requiresIndependentReview({ eventName, baseRef, changedPaths });
  if (!requirement.required) {
    return { ok: true, required: false, reason: requirement.reason, securitySurface: requirement.surface };
  }
  const approval = findIndependentApproval({ reviews, author });
  return {
    ok: approval.approved,
    required: true,
    reason: approval.approved ? 'independent_review_present' : 'independent_review_missing',
    securitySurface: requirement.surface,
    approvedBy: approval.by,
    reviewStates: approval.considered.map(row => `${row.login || 'unknown'}:${row.state}${row.isAuthor ? '(author)' : ''}`)
  };
}

/* ------------------------------- CLI -------------------------------- */

function main() {
  const eventName = process.env.GITHUB_EVENT_NAME || '';
  const baseRef = process.env.GITHUB_BASE_REF || '';
  const repository = process.env.GITHUB_REPOSITORY || '';
  const pullNumber = (process.env.GITHUB_REF || '').split('/').pop();
  const author = process.env.GITHUB_ACTOR || '';

  let changedPaths = [];
  if (process.env.CHANGED_PATHS_JSON) {
    changedPaths = JSON.parse(process.env.CHANGED_PATHS_JSON);
  } else if (eventName === 'pull_request') {
    const base = process.env.BASE_SHA || '';
    const head = process.env.HEAD_SHA || 'HEAD';
    if (!base) {
      // Failing open here would mean a gate that silently passes whenever it
      // cannot see the diff, which is precisely when it is needed most.
      process.stderr.write(`${JSON.stringify({
        ok: false,
        error: 'independent_review_changed_paths_unavailable',
        reason: 'BASE_SHA is required on a pull_request; refusing to conclude the security surface is untouched',
      }, null, 2)}\n`);
      process.exit(1);
    }
    const diff = spawnSync('git', ['diff', '--name-only', `${base}...${head}`], { encoding: 'utf8' });
    if (diff.status !== 0) {
      process.stderr.write(`${JSON.stringify({
        ok: false,
        error: 'independent_review_changed_paths_uncomputable',
        detail: String(diff.stderr || '').slice(0, 500),
      }, null, 2)}\n`);
      process.exit(1);
    }
    changedPaths = diff.stdout.split('\n').map(line => line.trim()).filter(Boolean);
  }

  let reviews = [];
  if (process.env.REVIEWS_JSON) {
    reviews = JSON.parse(process.env.REVIEWS_JSON);
  } else if (repository && pullNumber && process.env.GITHUB_TOKEN) {
    const api = spawnSync('gh', [
      'api', `repos/${repository}/pulls/${pullNumber}/reviews`, '--paginate'
    ], { encoding: 'utf8' });
    if (api.status === 0) {
      try {
        reviews = JSON.parse(api.stdout);
      } catch {
        reviews = [];
      }
    }
  }

  const result = evaluateReviewGate({ eventName, baseRef, changedPaths, reviews, author });
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  process.exit(result.ok ? 0 : 1);
}

if (process.argv[1] && process.argv[1].endsWith('require-independent-review.mjs')) main();