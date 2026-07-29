import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { DEFAULT_ACCOUNTS } from './defaultOperatorState.mjs';

export const CURRENT_SCHEMA_VERSION = 4;

export function createInitialOperatorState(now = '2026-05-29T00:00:00.000Z') {
  return {
    schemaVersion: CURRENT_SCHEMA_VERSION,
    accounts: DEFAULT_ACCOUNTS.map(account => ({ ...account, updatedAt: account.updatedAt || now })),
    instruments: [],
    strategyTemplates: [],
    strategies: [],
    backtests: [],
    approvals: [],
    opportunities: [],
    riskBreakdowns: [],
    researchJobs: [],
    agentBudgets: [],
    budgetApprovals: [],
    agentCostLedger: [],
    marketDataSnapshots: [],
    modelPricingSnapshots: [],
    modelUsageLedger: [],
    priceForecasts: [],
    forecastOutcomes: [],
    executionCostSnapshots: [],
    economicDecisions: [],
    agentAttributionRecords: [],
    positions: [],
    capitalInPlayUsd: null,
    paperExecutions: [],
    executions: [],
    audit: [],
    killSwitch: { enabled: false, reason: null, updatedAt: null },
    config: {
      confidenceThreshold: 0.60,
      approvalThreshold: 0.80,
      maxPositionSizeUsd: 50000,
      defaultHoldingPeriodDays: 7,
      maxConcurrentTrades: 5,
      requiredIntelligenceCostCoverageMultiple: 3,
      maximumModelPricingAgeSeconds: 86400,
      maximumForecastDataAgeSeconds: 180,
      requireEconomicDecisionForRemoteAgent: true,
      capitalPolicy: {
        presetName: 'balanced',
        targets: { reserve: 0.50, core: 0.20, opportunity: 0.30 },
        coreAllowlist: ['BTC', 'ETH'],
        coreMinAllocationPct: 10,
        coreBatchFraction: 0.05,
        opportunityBatchFraction: 0.03,
      },
      coinbaseApiKey: '',
      coinbaseApiSecret: '',
      kalshiEmail: '',
      kalshiPassword: '',
      polymarketApiKey: '',
      polymarketWalletAddress: '',
      polymarketPrivateKey: '',
      updatedAt: null
    }
  };
}

export function normalizeOperatorState(input = {}) {
  const seeded = createInitialOperatorState();
  return {
    schemaVersion: Number(input.schemaVersion || CURRENT_SCHEMA_VERSION),
    accounts: Array.isArray(input.accounts) ? input.accounts : seeded.accounts,
    instruments: Array.isArray(input.instruments) ? input.instruments : [],
    strategyTemplates: Array.isArray(input.strategyTemplates) ? input.strategyTemplates : [],
    strategies: Array.isArray(input.strategies) ? input.strategies : [],
    backtests: Array.isArray(input.backtests) ? input.backtests : [],
    approvals: Array.isArray(input.approvals) ? input.approvals : [],
    opportunities: Array.isArray(input.opportunities) ? input.opportunities : [],
    riskBreakdowns: Array.isArray(input.riskBreakdowns) ? input.riskBreakdowns : [],
    researchJobs: Array.isArray(input.researchJobs) ? input.researchJobs : [],
    agentBudgets: Array.isArray(input.agentBudgets) ? input.agentBudgets : [],
    budgetApprovals: Array.isArray(input.budgetApprovals) ? input.budgetApprovals : [],
    agentCostLedger: Array.isArray(input.agentCostLedger) ? input.agentCostLedger : [],
    marketDataSnapshots: Array.isArray(input.marketDataSnapshots) ? input.marketDataSnapshots : [],
    modelPricingSnapshots: Array.isArray(input.modelPricingSnapshots) ? input.modelPricingSnapshots : [],
    modelUsageLedger: Array.isArray(input.modelUsageLedger) ? input.modelUsageLedger : [],
    priceForecasts: Array.isArray(input.priceForecasts) ? input.priceForecasts : [],
    forecastOutcomes: Array.isArray(input.forecastOutcomes) ? input.forecastOutcomes : [],
    executionCostSnapshots: Array.isArray(input.executionCostSnapshots) ? input.executionCostSnapshots : [],
    economicDecisions: Array.isArray(input.economicDecisions) ? input.economicDecisions : [],
    agentAttributionRecords: Array.isArray(input.agentAttributionRecords) ? input.agentAttributionRecords : [],
    positions: Array.isArray(input.positions) ? input.positions : [],
    capitalInPlayUsd: input.capitalInPlayUsd ?? input.capital_in_play_usd ?? null,
    paperExecutions: Array.isArray(input.paperExecutions) ? input.paperExecutions : [],
    executions: Array.isArray(input.executions) ? input.executions : [],
    audit: Array.isArray(input.audit) ? input.audit : [],
    killSwitch: input.killSwitch && typeof input.killSwitch === 'object' ? input.killSwitch : seeded.killSwitch,
    config: input.config && typeof input.config === 'object' ? { ...seeded.config, ...input.config } : seeded.config
  };
}

export function nextId(prefix, collection) {
  return `${prefix}-${String(collection.length + 1).padStart(3, '0')}`;
}

export class MemoryOperatorStore {
  constructor(initialState = createInitialOperatorState()) {
    this.kind = 'memory';
    this.durable = false;
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

  getStatus() {
    return { kind: this.kind, durable: this.durable, schemaVersion: this.state.schemaVersion };
  }
}

export class FileOperatorStore extends MemoryOperatorStore {
  constructor(filePath = process.env.OPERATOR_STATE_PATH || 'data/operator-state.json', options = {}) {
    super(options.seedState || createInitialOperatorState());
    this.kind = 'file';
    this.durable = true;
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

  getStatus() {
    return { kind: this.kind, durable: this.durable, schemaVersion: this.state.schemaVersion, path: this.filePath };
  }
}

export function createOperatorStore(options = {}) {
  if (options.store) return options.store;
  if (options.state) return new MemoryOperatorStore(options.state);
  if (options.persist === false || process.env.OPERATOR_STATE_DISABLED === 'true') return new MemoryOperatorStore(options.seedState);
  return new FileOperatorStore(options.filePath, { seedState: options.seedState });
}
