#!/usr/bin/env node
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

function measure(name, operation, iterations = 1) {
  const started = performance.now();
  let result;
  for (let index = 0; index < iterations; index += 1) result = operation(index);
  const elapsedMs = performance.now() - started;
  return {
    name,
    iterations,
    elapsedMs: Number(elapsedMs.toFixed(3)),
    operationsPerSecond: Number(((iterations / Math.max(elapsedMs, 0.001)) * 1000).toFixed(2)),
    result,
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

const bars = historicalBars();
const replay = measure('moving-average-replay-10k-bars', () => replayMovingAverageCross({
  strategy: { parameters: { fastPeriod: 10, slowPeriod: 30 } },
  bars,
  initialCapitalUsd: 100000,
  feeBps: 5,
  slippageBps: 10,
}), 5);
if (!replay.result?.ok) throw new Error(`replay_benchmark_failed:${JSON.stringify(replay.result?.errors || [])}`);

const audit = measure('audit-chain-build-and-verify-5k-events', () => {
  const events = [];
  let previous = null;
  for (let index = 0; index < 5000; index += 1) {
    const event = buildAuditEvent({
      id: `benchmark-audit-${index}`,
      action: 'benchmark_event',
      actor: 'performance-smoke',
      at: new Date(Date.parse('2026-01-01T00:00:00.000Z') + index * 1000).toISOString(),
      payload: { index, symbol: index % 2 ? 'BTC-USD' : 'ETH-USD' },
    }, previous);
    events.push(event);
    previous = event;
  }
  const verification = verifyAuditChain(events);
  if (!verification.ok) throw new Error(`audit_benchmark_failed:${JSON.stringify(verification.issues)}`);
  return { events: verification.count, lastHash: verification.lastHash };
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
const normalization = measure('operator-state-normalization-500-records', () => normalizeOperatorState(seed), 250);
if (normalization.result.modelUsageLedger.length !== 250 || normalization.result.executions.length !== 250) {
  throw new Error('state_normalization_benchmark_failed');
}

const report = {
  ok: true,
  benchmark: 'release-critical-paths',
  measuredAt: new Date().toISOString(),
  runtime: {
    node: process.version,
    platform: process.platform,
    architecture: process.arch,
  },
  measurements: [
    {
      name: replay.name,
      iterations: replay.iterations,
      elapsedMs: replay.elapsedMs,
      operationsPerSecond: replay.operationsPerSecond,
      barsPerReplay: bars.length,
      totalTrades: replay.result.metrics.totalTrades,
    },
    {
      name: audit.name,
      iterations: audit.iterations,
      elapsedMs: audit.elapsedMs,
      operationsPerSecond: audit.operationsPerSecond,
      events: audit.result.events,
      lastHash: audit.result.lastHash,
    },
    {
      name: normalization.name,
      iterations: normalization.iterations,
      elapsedMs: normalization.elapsedMs,
      operationsPerSecond: normalization.operationsPerSecond,
      recordsPerIteration: 500,
    },
  ],
  note: 'Diagnostic baseline only; compare exact runner and workload before treating changes as regressions.',
};

process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
