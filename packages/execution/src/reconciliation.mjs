// Execution reconciliation engine: detects discrepancies between expected and actual fills,
// tracks settlement status, and produces reconciliation reports.

export class ExecutionReconciler {
  constructor() {
    this.checks = [];
  }

  // Compare execution fills to expected order quantities
  reconcile(execution) {
    const issues = [];
    const expectedByVenue = {};
    const actualByVenue = {};

    for (const order of execution.orders || []) {
      const key = `${order.venue}:${order.marketId}`;
      expectedByVenue[key] = (expectedByVenue[key] || 0) + order.quantity;
    }

    for (const fill of execution.fills || []) {
      const key = `${fill.venue}:${fill.marketId}`;
      actualByVenue[key] = (actualByVenue[key] || 0) + fill.quantity;
    }

    const allKeys = new Set([...Object.keys(expectedByVenue), ...Object.keys(actualByVenue)]);
    for (const key of allKeys) {
      const expected = expectedByVenue[key] || 0;
      const actual = actualByVenue[key] || 0;
      const diff = Math.abs(expected - actual);
      if (diff > 0.0001) {
        issues.push({
          type: 'fill_quantity_mismatch',
          venueMarket: key,
          expected,
          actual,
          diff,
          severity: diff / expected > 0.05 ? 'high' : diff / expected > 0.01 ? 'medium' : 'low',
        });
      }
    }

    // Check for unsettled fills
    const unsettled = (execution.fills || []).filter(f => f.settlementStatus === 'pending');
    if (unsettled.length > 0) {
      issues.push({
        type: 'pending_settlements',
        count: unsettled.length,
        totalValue: unsettled.reduce((s, f) => s + f.quantity * f.price, 0),
        severity: 'medium',
      });
    }

    return {
      executionId: execution.id,
      status: issues.length === 0 ? 'clean' : 'issues_found',
      checkedAt: new Date().toISOString(),
      issues,
      fillCount: (execution.fills || []).length,
      orderCount: (execution.orders || []).length,
      totalFilledQuantity: (execution.fills || []).reduce((s, f) => s + f.quantity, 0),
      totalExpectedQuantity: (execution.orders || []).reduce((s, o) => s + o.quantity, 0),
    };
  }

  // Mark fills as settled
  settleFill(execution, fillId) {
    const fill = (execution.fills || []).find(f => f.id === fillId);
    if (!fill) return { ok: false, error: 'fill_not_found' };
    fill.settlementStatus = 'settled';
    return { ok: true, fill };
  }

  // Generate audit trail entry for a reconciliation check
  toAuditEvent(report) {
    return {
      id: `recon-${Date.now()}`,
      action: report.status === 'clean' ? 'execution_reconciled' : 'execution_recon_issues',
      actor: 'reconciliation-engine',
      at: report.checkedAt,
      details: report.executionId,
      payload: {
        fillCount: report.fillCount,
        orderCount: report.orderCount,
        issueCount: report.issues.length,
        issues: report.issues.slice(0, 5),
      },
    };
  }
}

export function createReconciler() {
  return new ExecutionReconciler();
}
