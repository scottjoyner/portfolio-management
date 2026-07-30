import test from 'node:test';
import assert from 'node:assert/strict';

import { synchronizeCompatibilityExecutions } from '../packages/storage/src/executionCompatibilitySync.mjs';

class FakeCompatibilityRepository {
  constructor() {
    this.current = null;
    this.events = new Set();
    this.orders = [];
    this.fills = [];
    this.transitions = [];
    this.appendedEvents = [];
    this.store = {
      query: async (sql, params = []) => {
        const normalized = String(sql).replace(/\s+/g, ' ').trim();
        if (normalized.startsWith('SELECT event_id FROM execution_events')) {
          return { rows: this.events.has(params[0]) ? [{ event_id: params[0] }] : [] };
        }
        if (normalized.startsWith('UPDATE execution_records')) {
          this.current = { ...this.current, version: params[13], updatedAt: params[14] };
          return { rows: [{ version: params[13] }] };
        }
        throw new Error(`unexpected_sql:${normalized}`);
      },
    };
  }

  async transaction(operation) { return operation(); }
  async get() { return this.current; }
  async create(input) {
    this.current = { ...input, version: 1 };
    return { execution: this.current, idempotent: false };
  }
  async transition(input) {
    this.transitions.push(input);
    this.current = { ...this.current, status: input.toStatus, version: this.current.version + 1 };
    return { execution: this.current, event: { id: input.idempotencyKey } };
  }
  async appendEvent(input) {
    this.appendedEvents.push(input);
    this.events.add(input.idempotencyKey);
    return { execution: this.current, event: { id: input.idempotencyKey } };
  }
  async putOrder(input) {
    this.orders.push(input);
    return { order: { ...input }, idempotent: false };
  }
  async recordFill(input) {
    this.fills.push(input);
    return { fill: { ...input }, idempotent: false };
  }
}

test('compatibility sync imports execution lineage, orders, and safe fills', async () => {
  const repository = new FakeCompatibilityRepository();
  const result = await synchronizeCompatibilityExecutions(repository, [{
    id: 'exec-001',
    idempotencyKey: 'intent-001',
    opportunityId: 'opp-001',
    economicDecisionId: 'decision-001',
    modelQuoteId: 'quote-001',
    forecastId: 'forecast-001',
    executionCostSnapshotId: 'cost-001',
    symbol: 'BTC-USD',
    venue: 'coinbase',
    mode: 'paper',
    side: 'buy',
    status: 'filled',
    quantity: 0.01,
    notional: 1000,
    entryPrice: 100000,
    orders: [{ id: 'order-001', side: 'buy', quantity: 0.01, price: 100000 }],
    fills: [
      { id: 'fill-001', orderId: 'order-001', quantity: 0.01, price: 100000, fee: 1 },
      { id: 'fill-invalid', quantity: 0, price: 100000 },
    ],
    updatedAt: '2026-07-30T20:00:00.000Z',
  }], { now: '2026-07-30T20:01:00.000Z' });

  assert.equal(result.ok, true);
  assert.equal(result.createdCount, 1);
  assert.equal(repository.current.status, 'filled');
  assert.equal(repository.current.economicDecisionId, 'decision-001');
  assert.equal(repository.orders.length, 1);
  assert.equal(repository.fills.length, 1);
  assert.equal(repository.fills[0].orderId, 'order-001');
  assert.equal(result.reports[0].skippedFills[0].reason, 'fill_quantity_or_price_invalid');
  assert.ok(repository.appendedEvents.some(event => event.eventType === 'execution_snapshot_synchronized'));
});

test('compatibility status aliases are normalized and orphan fill references are removed', async () => {
  const repository = new FakeCompatibilityRepository();
  const result = await synchronizeCompatibilityExecutions(repository, [{
    id: 'exec-expired',
    symbol: 'ETH-USD',
    venue: 'coinbase',
    side: 'sell',
    status: 'expired',
    orders: [],
    fills: [{ id: 'fill-orphan', orderId: 'missing-order', quantity: 2, price: 3000 }],
    updatedAt: '2026-07-30T20:00:00.000Z',
  }]);

  assert.equal(repository.current.status, 'cancelled');
  assert.equal(repository.fills[0].orderId, null);
  assert.equal(repository.fills[0].metadata.originalOrderId, 'missing-order');
  assert.ok(result.reports[0].skippedFills.some(row => row.reason === 'orphan_order_reference_removed'));
});

test('unsupported legacy status skips safely instead of rolling back unrelated state', async () => {
  const repository = new FakeCompatibilityRepository();
  const result = await synchronizeCompatibilityExecutions(repository, [{
    id: 'exec-unknown',
    symbol: 'SOL-USD',
    venue: 'coinbase',
    side: 'buy',
    status: 'mystery-state',
  }]);

  assert.equal(result.ok, true);
  assert.equal(result.skippedCount, 1);
  assert.match(result.reports[0].reason, /execution_status_unsupported/);
  assert.equal(repository.current, null);
});
