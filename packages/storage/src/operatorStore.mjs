import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';

const CURRENT_SCHEMA_VERSION = 1;

export function createInitialOperatorState(now = '2026-05-29T00:00:00.000Z') {
  return {
    schemaVersion: CURRENT_SCHEMA_VERSION,
    strategies: [
      {
        id: 'strategy-ema-cross-v1',
        name: 'EMA Crossover',
        version: 1,
        status: 'draft',
        riskLevel: 'medium',
        parameters: { fastPeriod: 9, slowPeriod: 21, symbol: 'BTC-USD', timeframe: '1h' },
        createdAt: now,
        updatedAt: now
      },
      {
        id: 'strategy-zscore-v1',
        name: 'Z-Score Mean Reversion',
        version: 1,
        status: 'draft',
        riskLevel: 'low',
        parameters: { lookback: 20, entryZ: -2, exitZ: 0, symbol: 'ETH-USD', timeframe: '1h' },
        createdAt: now,
        updatedAt: now
      }
    ],
    backtests: [
      {
        id: 'bt-demo-001',
        strategyId: 'strategy-ema-cross-v1',
        status: 'completed',
        startedAt: now,
        completedAt: '2026-05-29T00:00:01.000Z',
        assumptions: { initialCapitalUsd: 100000, feeBps: 5, slippageBps: 10, dataSource: 'demo-fixture' },
        metrics: { totalReturnPct: 3.42, maxDrawdownPct: 1.18, sharpe: 1.12, totalTrades: 14, winRatePct: 57.14 },
        equityCurve: [100000, 100800, 100150, 101250, 103420],
        trades: [
          { timestamp: '2026-01-02T10:00:00.000Z', symbol: 'BTC-USD', side: 'buy', quantity: 0.1, price: 45000 },
          { timestamp: '2026-01-03T15:00:00.000Z', symbol: 'BTC-USD', side: 'sell', quantity: 0.1, price: 46539 }
        ]
      }
    ],
    approvals: [
      {
        id: 'approval-demo-001',
        strategyId: 'strategy-ema-cross-v1',
        status: 'pending_review',
        tier: 'canary',
        reason: 'Backtest evidence required before paper incubation.',
        createdAt: now
      }
    ],
    positions: [],
    audit: [
      { id: 'audit-001', action: 'system_bootstrap', actor: 'system', at: now, details: 'Mock/paper operator surface initialized.' }
    ],
    killSwitch: { enabled: false, reason: null, updatedAt: null }
  };
}

export function normalizeOperatorState(input = {}) {
  const seeded = createInitialOperatorState();
  return {
    schemaVersion: Number(input.schemaVersion || CURRENT_SCHEMA_VERSION),
    strategies: Array.isArray(input.strategies) ? input.strategies : seeded.strategies,
    backtests: Array.isArray(input.backtests) ? input.backtests : seeded.backtests,
    approvals: Array.isArray(input.approvals) ? input.approvals : seeded.approvals,
    positions: Array.isArray(input.positions) ? input.positions : seeded.positions,
    audit: Array.isArray(input.audit) ? input.audit : seeded.audit,
    killSwitch: input.killSwitch && typeof input.killSwitch === 'object' ? input.killSwitch : seeded.killSwitch
  };
}

export function nextId(prefix, collection) {
  return `${prefix}-${String(collection.length + 1).padStart(3, '0')}`;
}

export class MemoryOperatorStore {
  constructor(initialState = createInitialOperatorState()) {
    this.state = normalizeOperatorState(initialState);
  }

  async load() {
    return this.state;
  }

  async save(nextState) {
    this.state = normalizeOperatorState(nextState);
    return this.state;
  }

  async mutate(mutator) {
    const state = await this.load();
    const result = await mutator(state);
    await this.save(state);
    return result;
  }
}

export class FileOperatorStore extends MemoryOperatorStore {
  constructor(filePath = process.env.OPERATOR_STATE_PATH || 'data/operator-state.json', options = {}) {
    super(options.seedState || createInitialOperatorState());
    this.filePath = resolve(filePath);
    this.bootstrap = options.bootstrap !== false;
  }

  ensureFile() {
    mkdirSync(dirname(this.filePath), { recursive: true });
    if (!existsSync(this.filePath) && this.bootstrap) {
      this.writeState(createInitialOperatorState());
    }
  }

  readState() {
    this.ensureFile();
    if (!existsSync(this.filePath)) return createInitialOperatorState();
    const raw = readFileSync(this.filePath, 'utf8');
    if (!raw.trim()) return createInitialOperatorState();
    return normalizeOperatorState(JSON.parse(raw));
  }

  writeState(state) {
    mkdirSync(dirname(this.filePath), { recursive: true });
    const normalized = normalizeOperatorState(state);
    const tmpPath = `${this.filePath}.tmp`;
    writeFileSync(tmpPath, `${JSON.stringify(normalized, null, 2)}\n`, 'utf8');
    renameSync(tmpPath, this.filePath);
    return normalized;
  }

  async load() {
    this.state = this.readState();
    return this.state;
  }

  async save(nextState) {
    this.state = this.writeState(nextState);
    return this.state;
  }
}

export function createOperatorStore(options = {}) {
  if (options.store) return options.store;
  if (options.state) return new MemoryOperatorStore(options.state);
  if (options.persist === false || process.env.OPERATOR_STATE_DISABLED === 'true') return new MemoryOperatorStore(options.seedState);
  return new FileOperatorStore(options.filePath, { seedState: options.seedState });
}
