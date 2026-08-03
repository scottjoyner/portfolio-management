#!/usr/bin/env node
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { performance } from 'node:perf_hooks';

import {
  buildAuditEvent,
  verifyAuditChain,
} from '../packages/storage/src/auditChain.mjs';
import { replayMovingAverageCross } from '../packages/backtesting/src/replayEngine.mjs';
import {
  createInitialOperatorState,
  normalizeOperatorState,
} from '../packages/storage/src/operatorStore.mjs';

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const DEFAULT_THRESHOLD_PATH = resolve(SCRIPT_DIR, '../config/release-performance-thresholds.json');

function finite(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function quantile(values, percentile) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(percentile * sorted.length) - 1));
  return sorted[index];
}

function oneMeasurement(operation, iterations) {
  const started = performance.now();
  let result;
  for (let index = 0; index < iterations; index += 1) result = operation(index);
  const elapsedMs = performance.now() - started;
  return {
    elapsedMs,
    operationsPerSecond: (iterations / Math.max(elapsedMs, 0.001)) * 1000,
    result,
  };
}

export function summarizeSamples(name, samples, iterations) {
  const elapsed = samples.map(sample => sample.elapsedMs);
  const throughput = samples.map(sample => sample.operationsPerSecond);
  return {
    name,
    iterationsPerSample: iterations,
    sampleCount: samples.length,
    medianElapsedMs: Number(quantile(elapsed, 0.5).toFixed(3)),
    p95ElapsedMs: Number(quantile(elapsed, 0.95).toFixed(3)),
    medianOperationsPerSecond: Number(quantile(throughput, 0.5).toFixed(2)),
    minimumOperationsPerSecond: Number(Math.min(...throughput).toFixed(2)),
    samples: samples.map(sample => ({
      elapsedMs: Number(sample.elapsedMs.toFixed(3)),
      operationsPerSecond: Number(sample.operationsPerSecond.toFixed(2)),
    })),
    result: samples.at(-1)?.result,
  };
}

function measure(name, operation, { iterations, warmups, samples }) {
  for (let index = 0; index < warmups; index += 1) oneMeasurement(operation, iterations);
  const measured = [];
  for (let index = 0; index < samples; index += 1) measured.push(oneMeasurement(operation, iterations));
  return summarizeSamples(name, measured, iterations);
}

function nodeMajor(version = process.version) {
  return Number(String(version).replace(/^v/, '').split('.')[0]);
}

export function evaluateThresholds({ runtime, measurements }, profile, { strictRunner = false } = {}) {
  const failures = [];
  const runnerMismatches = [];
  if (profile.runner) {
    if (finite(runtime.nodeMajor, -1) !== finite(profile.runner.nodeMajor, -2)) {
      runnerMismatches.push(`node_major_expected_${profile.runner.nodeMajor}_actual_${runtime.nodeMajor}`);
    }
    if (runtime.platform !== profile.runner.platform) {
      runnerMismatches.push(`platform_expected_${profile.runner.platform}_actual_${runtime.platform}`);
    }
    if (runtime.architecture !== profile.runner.architecture) {
      runnerMismatches.push(`architecture_expected_${profile.runner.architecture}_actual_${runtime.architecture}`);
    }
  }
  if (strictRunner) failures.push(...runnerMismatches.map(reason => `runner_profile_mismatch:${reason}`));

  const byName = new Map(measurements.map(measurement => [measurement.name, measurement]));
  for (const [name, threshold] of Object.entries(profile.measurements || {})) {
    const measurement = byName.get(name);
    if (!measurement) {
      failures.push(`measurement_missing:${name}`);
      continue;
    }
    if (measurement.medianElapsedMs > finite(threshold.maxMedianElapsedMs, Number.POSITIVE_INFINITY)) {
      failures.push(`${name}:median_elapsed_ms:${measurement.medianElapsedMs}>${threshold.maxMedianElapsedMs}`);
    }
    if (measurement.p95ElapsedMs > finite(threshold.maxP95ElapsedMs, Number.POSITIVE_INFINITY)) {
      failures.push(`${name}:p95_elapsed_ms:${measurement.p95ElapsedMs}>${threshold.maxP95ElapsedMs}`);
    }
    if (measurement.medianOperationsPerSecond < finite(threshold.minMedianOperationsPerSecond, 0)) {
      failures.push(`${name}:median_ops_per_sec:${measurement.medianOperationsPerSecond}<${threshold.minMedianOperationsPerSecond}`);
    }
  }

  return {
    ok: failures.length === 0,
    failures,
    runnerMismatches,
  };
}

function historicalBars(count = 10000) {
  const start = Date.parse('2026-01-01T00:00:00.000Z');
  return Array.from({ length: count }, (_, index) => {
    const trend = index * 0.025;
    const cycle = Math.sin(index / 19) * 3.5;
    const close = 100 + trend + cycle;
    return {
      timestamp: new Date(start + index * 60000).toISOString(),
      open: close - 0.1,
      high: close + 0.5,
      low: close - 0.5,
      close,
      volume: 1000 + index,
    };
  });
}

export function runReleaseBenchmarks(options = {}) {
  const thresholdPath = resolve(options.thresholdPath || process.env.PERFORMANCE_THRESHOLD_CONFIG || DEFAULT_THRESHOLD_PATH);
  if (!existsSync(thresholdPath)) throw new Error(`performance_threshold_config_missing:${thresholdPath}`);
  const profile = JSON.parse(readFileSync(thresholdPath, 'utf8'));
  const warmups = Math.max(0, Math.floor(finite(profile.sampling?.warmups, 1)));
  const samples = Math.max(3, Math.floor(finite(profile.sampling?.samples, 5)));

  const bars = historicalBars();
  const replayConfig = profile.measurements['moving-average-replay-10k-bars'];
  const replay = measure('moving-average-replay-10k-bars', () => replayMovingAverageCross({
    strategy: { parameters: { fastPeriod: 10, slowPeriod: 30 } },
    bars,
    initialCapitalUsd: 100000,
    feeBps: 5,
    slippageBps: 10,
  }), {
    iterations: replayConfig.iterationsPerSample,
    warmups,
    samples,
  });
  if (!replay.result?.ok) throw new Error(`replay_benchmark_failed:${JSON.stringify(replay.result?.errors || [])}`);

  const auditConfig = profile.measurements['audit-chain-build-and-verify-5k-events'];
  const audit = measure('audit-chain-build-and-verify-5k-events', () => {
    const events = [];
    let previous = null;
    for (let index = 0; index < 5000; index += 1) {
      const event = buildAuditEvent({
        id: `benchmark-audit-${index}`,
        action: 'benchmark_event',
        actor: 'performance-gate',
        at: new Date(Date.parse('2026-01-01T00:00:00.000Z') + index * 1000).toISOString(),
        payload: { index, symbol: index % 2 ? 'BTC-USD' : 'ETH-USD' },
      }, previous);
      events.push(event);
      previous = event;
    }
    const verification = verifyAuditChain(events);
    if (!verification.ok) throw new Error(`audit_benchmark_failed:${JSON.stringify(verification.issues)}`);
    return { events: verification.count, lastHash: verification.lastHash };
  }, {
    iterations: auditConfig.iterationsPerSample,
    warmups,
    samples,
  });

  const seed = createInitialOperatorState('2026-01-01T00:00:00.000Z');
  seed.modelUsageLedger = Array.from({ length: 250 }, (_, index) => ({
    id: `benchmark-model-${index}`,
    status: 'reconciled',
    localOrRemote: 'local',
    actualCostUsd: index / 100000,
  }));
  seed.executions = Array.from({ length: 250 }, (_, index) => ({
    id: `benchmark-execution-${index}`,
    status: 'draft',
    symbol: index % 2 ? 'BTC-USD' : 'ETH-USD',
    version: 1,
  }));
  const normalizationConfig = profile.measurements['operator-state-normalization-500-records'];
  const normalization = measure('operator-state-normalization-500-records', () => normalizeOperatorState(seed), {
    iterations: normalizationConfig.iterationsPerSample,
    warmups,
    samples,
  });
  if (normalization.result.modelUsageLedger.length !== 250 || normalization.result.executions.length !== 250) {
    throw new Error('state_normalization_benchmark_failed');
  }

  const runtime = {
    node: process.version,
    nodeMajor: nodeMajor(),
    platform: process.platform,
    architecture: process.arch,
  };
  const measurements = [
    {
      ...replay,
      result: undefined,
      barsPerReplay: bars.length,
      totalTrades: replay.result.metrics.totalTrades,
    },
    {
      ...audit,
      result: undefined,
      events: audit.result.events,
      lastHash: audit.result.lastHash,
    },
    {
      ...normalization,
      result: undefined,
      recordsPerIteration: 500,
    },
  ];
  const strictRunner = options.strictRunner ?? String(process.env.PERFORMANCE_STRICT_RUNNER || '').toLowerCase() === 'true';
  const evaluation = evaluateThresholds({ runtime, measurements }, profile, { strictRunner });

  return {
    ok: evaluation.ok,
    benchmark: 'release-critical-paths',
    measuredAt: new Date().toISOString(),
    thresholdProfile: profile.profile,
    thresholdConfig: thresholdPath,
    baseline: profile.baseline,
    runtime,
    sampling: { warmups, samples },
    measurements,
    failures: evaluation.failures,
    runnerMismatches: evaluation.runnerMismatches,
    note: 'Blocking runner-normalized release gate. Update thresholds only with exact-runner evidence and an explicit rationale.',
  };
}

function isMainModule() {
  return process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url);
}

if (isMainModule()) {
  try {
    const report = runReleaseBenchmarks();
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    if (!report.ok) process.exitCode = 1;
  } catch (error) {
    process.stderr.write(`${JSON.stringify({
      ok: false,
      benchmark: 'release-critical-paths',
      error: error.message || String(error),
    }, null, 2)}\n`);
    process.exitCode = 1;
  }
}
