import test from 'node:test';
import assert from 'node:assert/strict';

import {
  allocatePortfolioRequest,
  verifyPortfolioAllocationSnapshot,
} from '../packages/execution/src/portfolioAllocator.mjs';
import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';

const NOW = '2026-09-11T04:45:00.000Z';
const ACCOUNT_ID = 'acct-paper-primary';

function stateFixture() {
  const state = createInitialOperatorState(NOW);
  state.accounts[0].cash = 100000;
  state.accounts[0].nav = 100000;
  state.accounts[0].updatedAt = NOW;
  state.portfolioRiskState = {
    accounts: [{
      accountId: ACCOUNT_ID,
      highWaterNavUsd: 100000,
      highWaterAt: NOW,
      initializedAt: NOW,
      updatedAt: NOW,
      source: 'test',
    }],
    correlationMatrix: {},
    volatilityBySymbol: {},
    updatedAt: NOW,
  };
  state.marketDataSnapshots = [
    {
      id: 'md-btc',
      symbol: 'BTC-USD',
      venue: 'coinbase-paper',
      bid: 99990,
      ask: 100010,
      volume24h: 1000,
      status: 'connected',
      source: 'test',
      timestamp: NOW,
    },
    {
      id: 'md-eth',
      symbol: 'ETH-USD',
      venue: 'coinbase-paper',
      bid: 2999,
      ask: 3001,
      volume24h: 10000,
      status: 'connected',
      source: 'test',
      timestamp: NOW,
    },
  ];
  return state;
}

function request(overrides = {}) {
  const base = {
    strategyId: 'strategy-target',
    accountId: ACCOUNT_ID,
    mode: 'paper',
    venue: 'coinbase-paper',
    symbol: 'BTC-USD',
    side: 'buy',
    confidenceScore: 0.9,
    entryPrice: 100000,
    orders: [{
      id: 'order-target',
      symbol: 'BTC-USD',
      venue: 'coinbase-paper',
      side: 'buy',
      quantity: 0.05,
      price: 100000,
      orderType: 'market',
      timeInForce: 'GTC',
      confidenceScore: 0.9,
    }],
  };
  return { ...base, ...overrides };
}

function allocate(state, input = request(), policy = {}) {
  return allocatePortfolioRequest({
    state,
    source: { source: 'test-store', revision: 'r1', observedAt: NOW },
    request: input,
    now: NOW,
    policy,
  });
}

test('portfolio allocator approves a request that is inside every budget', () => {
  const state = stateFixture();
  const { allocation, executionRequest } = allocate(state);

  assert.equal(allocation.approved, true);
  assert.equal(allocation.decision, 'APPROVE');
  assert.equal(allocation.requestedNotionalUsd, 5000);
  assert.equal(allocation.approvedNotionalUsd, 5000);
  assert.equal(allocation.scale, 1);
  assert.equal(executionRequest.orders[0].quantity, 0.05);
  assert.deepEqual(verifyPortfolioAllocationSnapshot(allocation, {
    tradeIntentHash: allocation.approvedIntentHash,
    now: NOW,
  }), { ok: true, reasons: [] });
});

test('manual request is deterministically reduced to the tightest portfolio budget', () => {
  const state = stateFixture();
  const input = request({
    orders: [{ ...request().orders[0], quantity: 0.2 }],
  });
  const { allocation, executionRequest } = allocate(state, input);

  assert.equal(allocation.approved, true);
  assert.equal(allocation.decision, 'REDUCE');
  assert.equal(allocation.requestedNotionalUsd, 20000);
  assert.equal(allocation.approvedNotionalUsd, 10000);
  assert.equal(allocation.scale, 0.5);
  assert.equal(executionRequest.orders[0].quantity, 0.1);
  assert.ok(allocation.scalingConstraints.includes('singleTradeUsd'));
});

test('economic request that requires downsizing fails closed and requests repricing', () => {
  const state = stateFixture();
  const input = request({
    sourceAgentId: 'agent-1',
    economicDecisionId: 'decision-1',
    executionCostSnapshotId: 'cost-1',
    netExecutableEdgeUsd: 25,
    orders: [{ ...request().orders[0], quantity: 0.2 }],
  });
  const { allocation, executionRequest } = allocate(state, input);

  assert.equal(allocation.approved, false);
  assert.equal(allocation.decision, 'REJECT');
  assert.equal(allocation.approvedNotionalUsd, 0);
  assert.equal(allocation.suggestedNotionalUsd, 10000);
  assert.ok(allocation.reasons.includes('economic_reprice_required_after_allocation'));
  assert.equal(executionRequest.orders[0].quantity, 0.2);
});

test('maximum drawdown puts increasing trades in EXIT_ONLY posture', () => {
  const state = stateFixture();
  state.accounts[0].nav = 85000;
  state.accounts[0].cash = 85000;
  const { allocation } = allocate(state);

  assert.equal(allocation.approved, false);
  assert.equal(allocation.exitOnly, true);
  assert.ok(allocation.exitOnlyReasons.includes('max_drawdown_reached'));
  assert.ok(allocation.reasons.includes('exit_only:max_drawdown_reached'));
});

test('risk-reducing sell remains allowed while portfolio is EXIT_ONLY', () => {
  const state = stateFixture();
  state.accounts[0].nav = 85000;
  state.accounts[0].cash = 85000;
  state.positions = [{
    id: 'position-btc',
    accountId: ACCOUNT_ID,
    strategyId: 'strategy-target',
    symbol: 'BTC-USD',
    quantity: 0.1,
    markPrice: 100000,
    status: 'open',
  }];
  const input = request({
    side: 'sell',
    orders: [{ ...request().orders[0], side: 'sell', quantity: 0.05 }],
  });
  const { allocation } = allocate(state, input);

  assert.equal(allocation.approved, true);
  assert.equal(allocation.decision, 'EXIT_ONLY');
  assert.equal(allocation.approvedNotionalUsd, 5000);
  assert.ok(allocation.exitOnlyReasons.includes('max_drawdown_reached'));
});

test('drawdown throttle scales requests continuously before the hard stop', () => {
  const state = stateFixture();
  state.accounts[0].nav = 88000;
  state.accounts[0].cash = 88000;
  const { allocation, executionRequest } = allocate(state);

  assert.equal(allocation.accountState.drawdownPct, 12);
  assert.equal(allocation.budgets.drawdownThrottleUsd, 3000);
  assert.equal(allocation.decision, 'REDUCE');
  assert.equal(allocation.approvedNotionalUsd, 3000);
  assert.equal(executionRequest.orders[0].quantity, 0.03);
});

test('existing gross leverage breach blocks increases but still permits exits', () => {
  const state = stateFixture();
  state.positions = [{
    id: 'position-btc',
    accountId: ACCOUNT_ID,
    strategyId: 'strategy-target',
    symbol: 'BTC-USD',
    quantity: 2,
    markPrice: 100000,
    status: 'open',
  }];
  const increase = allocate(state).allocation;
  assert.equal(increase.approved, false);
  assert.ok(increase.exitOnlyReasons.includes('gross_leverage_already_exceeded'));

  const exit = allocate(state, request({
    side: 'sell',
    orders: [{ ...request().orders[0], side: 'sell', quantity: 0.05 }],
  })).allocation;
  assert.equal(exit.approved, true);
  assert.equal(exit.decision, 'EXIT_ONLY');
});

test('symbol, strategy and correlation-cluster budgets are portfolio-wide', () => {
  const symbolState = stateFixture();
  symbolState.positions = [{
    id: 'position-btc', accountId: ACCOUNT_ID, strategyId: 'other', symbol: 'BTC-USD',
    quantity: 0.19, markPrice: 100000, status: 'open',
  }];
  const symbolAllocation = allocate(symbolState).allocation;
  assert.equal(symbolAllocation.approvedNotionalUsd, 1000);
  assert.ok(symbolAllocation.scalingConstraints.includes('symbolExposureUsd'));

  const strategyState = stateFixture();
  strategyState.positions = [{
    id: 'position-eth', accountId: ACCOUNT_ID, strategyId: 'strategy-target', symbol: 'ETH-USD',
    quantity: 29 / 3, markPrice: 3000, status: 'open',
  }];
  const strategyAllocation = allocate(strategyState).allocation;
  assert.equal(strategyAllocation.approvedNotionalUsd, 1000);
  assert.ok(strategyAllocation.scalingConstraints.includes('strategyExposureUsd'));

  const clusterState = stateFixture();
  clusterState.positions = [{
    id: 'position-eth', accountId: ACCOUNT_ID, strategyId: 'other', symbol: 'ETH-USD',
    quantity: 38 / 3, markPrice: 3000, status: 'open',
  }];
  const clusterAllocation = allocate(clusterState).allocation;
  assert.equal(clusterAllocation.approvedNotionalUsd, 2000);
  assert.ok(clusterAllocation.scalingConstraints.includes('clusterExposureUsd'));
});

test('pending orders reserve account-wide cash before a new allocation', () => {
  const state = stateFixture();
  state.accounts[0].cash = 30000;
  state.executions = [{
    id: 'pending-eth',
    accountId: ACCOUNT_ID,
    strategyId: 'other',
    symbol: 'ETH-USD',
    side: 'buy',
    status: 'draft',
    orders: [{ symbol: 'ETH-USD', venue: 'coinbase-paper', side: 'buy', quantity: 5, price: 3000 }],
    fills: [],
  }];
  const input = request({ orders: [{ ...request().orders[0], quantity: 0.08 }] });
  const { allocation } = allocate(state, input);

  assert.equal(allocation.accountState.pendingBuyCashUsd, 15000);
  assert.equal(allocation.budgets.cashReserveUsd, 5000);
  assert.equal(allocation.approvedNotionalUsd, 5000);
  assert.ok(allocation.scalingConstraints.includes('cashReserveUsd'));
});

test('explicit covariance state tightens marginal risk budget', () => {
  const state = stateFixture();
  state.positions = [{
    id: 'position-eth', accountId: ACCOUNT_ID, strategyId: 'other', symbol: 'ETH-USD',
    quantity: 40 / 3, markPrice: 3000, status: 'open',
  }];
  state.portfolioRiskState.volatilityBySymbol = { 'BTC-USD': 0.8, 'ETH-USD': 0.8 };
  state.portfolioRiskState.correlationMatrix = {
    'BTC-USD': { 'ETH-USD': 1 },
    'ETH-USD': { 'BTC-USD': 1 },
  };
  const input = request({ orders: [{ ...request().orders[0], quantity: 0.2 }] });
  const { allocation } = allocate(state, input, {
    maxSingleTradePct: 1,
    maxSymbolExposurePct: 1,
    maxStrategyExposurePct: 1,
    maxCorrelationClusterExposurePct: 1,
    maxGrossLeverage: 3,
    maxCovarianceRiskPct: 0.35,
  });

  assert.ok(allocation.covarianceState.currentRiskUsd > 31999);
  assert.ok(allocation.approvedNotionalUsd > 3000 && allocation.approvedNotionalUsd < 4000);
  assert.ok(allocation.scalingConstraints.includes('covarianceRiskUsd'));
  assert.ok(allocation.covarianceState.explicitCorrelationPairs.includes('BTC-USD|ETH-USD'));
});

test('liquidity participation and spread are first-class allocation gates', () => {
  const state = stateFixture();
  state.marketDataSnapshots[0].volume24h = 1;
  const volumeLimited = allocate(state).allocation;
  assert.equal(volumeLimited.budgets.liquidityUsd, 1000);
  assert.equal(volumeLimited.approvedNotionalUsd, 1000);
  assert.ok(volumeLimited.scalingConstraints.includes('liquidityUsd'));

  const spreadState = stateFixture();
  spreadState.marketDataSnapshots[0].spreadBps = 150;
  const spreadLimited = allocate(spreadState).allocation;
  assert.equal(spreadLimited.approved, false);
  assert.ok(spreadLimited.exitOnlyReasons.includes('liquidity_spread_limit'));
});

test('missing high-water with existing risk blocks increases but not exits', () => {
  const state = stateFixture();
  state.portfolioRiskState.accounts = [];
  state.positions = [{
    id: 'position-btc', accountId: ACCOUNT_ID, strategyId: 'strategy-target', symbol: 'BTC-USD',
    quantity: 0.1, markPrice: 100000, status: 'open',
  }];
  const increase = allocate(state).allocation;
  assert.equal(increase.approved, false);
  assert.ok(increase.exitOnlyReasons.includes('high_water_mark_unavailable'));

  const exit = allocate(state, request({
    side: 'sell',
    orders: [{ ...request().orders[0], side: 'sell', quantity: 0.05 }],
  })).allocation;
  assert.equal(exit.approved, true);
  assert.equal(exit.decision, 'EXIT_ONLY');
});

test('maximum concurrent trade count blocks new risk', () => {
  const state = stateFixture();
  state.config.maxConcurrentTrades = 1;
  state.positions = [{
    id: 'position-eth', accountId: ACCOUNT_ID, strategyId: 'other', symbol: 'ETH-USD',
    quantity: 1, markPrice: 3000, status: 'open',
  }];
  const { allocation } = allocate(state);
  assert.equal(allocation.approved, false);
  assert.ok(allocation.exitOnlyReasons.includes('max_concurrent_trades_reached'));
});

test('allocator selects the minimum across simultaneous constraints instead of early returning', () => {
  const state = stateFixture();
  state.accounts[0].cash = 23000;
  state.positions = [{
    id: 'position-btc', accountId: ACCOUNT_ID, strategyId: 'other', symbol: 'BTC-USD',
    quantity: 0.15, markPrice: 100000, status: 'open',
  }];
  const input = request({ orders: [{ ...request().orders[0], quantity: 0.2 }] });
  const { allocation } = allocate(state, input);

  assert.equal(allocation.budgets.singleTradeUsd, 10000);
  assert.equal(allocation.budgets.cashReserveUsd, 13000);
  assert.equal(allocation.budgets.symbolExposureUsd, 5000);
  assert.equal(allocation.approvedNotionalUsd, 5000);
  assert.ok(allocation.scalingConstraints.includes('singleTradeUsd'));
  assert.ok(allocation.scalingConstraints.includes('symbolExposureUsd'));
});

test('allocation hash and decision hash detect tampering', () => {
  const state = stateFixture();
  const { allocation } = allocate(state);
  allocation.budgets.singleTradeUsd = 999999;
  const verified = verifyPortfolioAllocationSnapshot(allocation, {
    tradeIntentHash: allocation.approvedIntentHash,
    now: NOW,
  });
  assert.equal(verified.ok, false);
  assert.ok(verified.reasons.includes('portfolio_allocation_hash_mismatch'));
});
