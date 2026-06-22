import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';

describe('ExecutionReconciler', () => {
  let ExecutionReconciler;

  before(async () => {
    const mod = await import('../packages/execution/src/reconciliation.mjs');
    ExecutionReconciler = mod.ExecutionReconciler;
  });

  it('reports clean for matched fills', () => {
    const reconciler = new ExecutionReconciler();
    const execution = {
      id: 'exec-test-1',
      orders: [{ venue: 'paper', marketId: 'BTC-USD', quantity: 1 }],
      fills: [{ venue: 'paper', marketId: 'BTC-USD', quantity: 1, settlementStatus: 'settled' }],
    };
    const report = reconciler.reconcile(execution);
    assert.equal(report.status, 'clean');
    assert.equal(report.issues.length, 0);
  });

  it('detects fill quantity mismatch', () => {
    const reconciler = new ExecutionReconciler();
    const execution = {
      id: 'exec-test-2',
      orders: [{ venue: 'paper', marketId: 'BTC-USD', quantity: 2 }],
      fills: [{ venue: 'paper', marketId: 'BTC-USD', quantity: 1, settlementStatus: 'settled' }],
    };
    const report = reconciler.reconcile(execution);
    assert.equal(report.status, 'issues_found');
    assert.ok(report.issues.some(i => i.type === 'fill_quantity_mismatch'));
  });

  it('detects pending settlements', () => {
    const reconciler = new ExecutionReconciler();
    const execution = {
      id: 'exec-test-3',
      orders: [{ venue: 'paper', marketId: 'BTC-USD', quantity: 1 }],
      fills: [{ venue: 'paper', marketId: 'BTC-USD', quantity: 1, price: 50000, settlementStatus: 'pending' }],
    };
    const report = reconciler.reconcile(execution);
    assert.ok(report.issues.some(i => i.type === 'pending_settlements'));
  });

  it('settleFill marks fill as settled', () => {
    const reconciler = new ExecutionReconciler();
    const execution = {
      id: 'exec-test-4',
      orders: [],
      fills: [{ id: 'fill-1', venue: 'paper', marketId: 'BTC-USD', quantity: 1, price: 50000, settlementStatus: 'pending' }],
    };
    const result = reconciler.settleFill(execution, 'fill-1');
    assert.equal(result.ok, true);
    assert.equal(result.fill.settlementStatus, 'settled');
  });

  it('settleFill returns error for unknown fill', () => {
    const reconciler = new ExecutionReconciler();
    const result = reconciler.settleFill({ fills: [] }, 'nonexistent');
    assert.equal(result.ok, false);
  });

  it('generates audit event from report', () => {
    const reconciler = new ExecutionReconciler();
    const report = {
      status: 'clean',
      checkedAt: new Date().toISOString(),
      executionId: 'exec-test',
      fillCount: 2,
      orderCount: 2,
      issues: [],
    };
    const audit = reconciler.toAuditEvent(report);
    assert.equal(audit.action, 'execution_reconciled');
    assert.equal(audit.details, 'exec-test');
  });
});

describe('SettlementTracker', () => {
  let SettlementTracker;

  before(async () => {
    const mod = await import('../packages/execution/src/settlement.mjs');
    SettlementTracker = mod.SettlementTracker;
  });

  it('tracks a fill', () => {
    const tracker = new SettlementTracker();
    tracker.trackFill({ id: 'fill-1', orderId: 'exec-1', settlementStatus: 'pending' });
    const status = tracker.getStatus('fill-1');
    assert.ok(status);
    assert.equal(status.status, 'pending');
  });

  it('getAllStatuses returns all tracked fills', () => {
    const tracker = new SettlementTracker();
    tracker.trackFill({ id: 'f1', orderId: 'e1' });
    tracker.trackFill({ id: 'f2', orderId: 'e2' });
    assert.equal(tracker.getAllStatuses().length, 2);
  });

  it('getSettlementSummary computes summary', () => {
    const tracker = new SettlementTracker();
    tracker.trackFill({ id: 'f1', orderId: 'exec-1', settlementStatus: 'settled' });
    tracker.trackFill({ id: 'f2', orderId: 'exec-1', settlementStatus: 'pending' });

    const summary = tracker.getSettlementSummary({
      id: 'exec-1',
      fills: [
        { id: 'f1', orderId: 'exec-1', settlementStatus: 'settled' },
        { id: 'f2', orderId: 'exec-1', settlementStatus: 'pending' },
      ],
    });
    assert.equal(summary.totalFills, 2);
    assert.equal(summary.settled, 1); // f1 was tracked as settled
  });
});
