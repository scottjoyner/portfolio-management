import test from 'node:test';
import assert from 'node:assert/strict';

import {
  evaluateThresholds,
  summarizeSamples,
} from '../scripts/benchmark-release-critical-paths.mjs';

const profile = {
  runner: { nodeMajor: 22, platform: 'linux', architecture: 'x64' },
  measurements: {
    replay: {
      maxMedianElapsedMs: 100,
      maxP95ElapsedMs: 150,
      minMedianOperationsPerSecond: 10,
    },
  },
};

test('sample summaries use medians and upper-tail elapsed time', () => {
  const summary = summarizeSamples('replay', [
    { elapsedMs: 40, operationsPerSecond: 25, result: { ok: true } },
    { elapsedMs: 50, operationsPerSecond: 20, result: { ok: true } },
    { elapsedMs: 200, operationsPerSecond: 5, result: { ok: true } },
    { elapsedMs: 60, operationsPerSecond: 16.67, result: { ok: true } },
    { elapsedMs: 55, operationsPerSecond: 18.18, result: { ok: true } },
  ], 1);

  assert.equal(summary.medianElapsedMs, 55);
  assert.equal(summary.p95ElapsedMs, 200);
  assert.equal(summary.medianOperationsPerSecond, 18.18);
  assert.equal(summary.sampleCount, 5);
});

test('threshold evaluation passes a matching runner and healthy measurements', () => {
  const result = evaluateThresholds({
    runtime: { nodeMajor: 22, platform: 'linux', architecture: 'x64' },
    measurements: [{
      name: 'replay',
      medianElapsedMs: 80,
      p95ElapsedMs: 120,
      medianOperationsPerSecond: 15,
    }],
  }, profile, { strictRunner: true });

  assert.equal(result.ok, true);
  assert.deepEqual(result.failures, []);
});

test('threshold evaluation fails performance regressions and runner drift', () => {
  const result = evaluateThresholds({
    runtime: { nodeMajor: 23, platform: 'linux', architecture: 'arm64' },
    measurements: [{
      name: 'replay',
      medianElapsedMs: 125,
      p95ElapsedMs: 225,
      medianOperationsPerSecond: 7,
    }],
  }, profile, { strictRunner: true });

  assert.equal(result.ok, false);
  assert.ok(result.failures.some(value => value.startsWith('runner_profile_mismatch:node_major')));
  assert.ok(result.failures.some(value => value.startsWith('runner_profile_mismatch:architecture')));
  assert.ok(result.failures.some(value => value.includes('median_elapsed_ms')));
  assert.ok(result.failures.some(value => value.includes('p95_elapsed_ms')));
  assert.ok(result.failures.some(value => value.includes('median_ops_per_sec')));
});

test('non-strict local runs report runner drift without failing it', () => {
  const result = evaluateThresholds({
    runtime: { nodeMajor: 22, platform: 'darwin', architecture: 'arm64' },
    measurements: [{
      name: 'replay',
      medianElapsedMs: 80,
      p95ElapsedMs: 120,
      medianOperationsPerSecond: 15,
    }],
  }, profile, { strictRunner: false });

  assert.equal(result.ok, true);
  assert.equal(result.runnerMismatches.length, 2);
});
