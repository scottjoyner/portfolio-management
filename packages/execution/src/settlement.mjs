// Settlement tracker: manages the lifecycle of fill settlement.
// Tracks pending settlements, retries failed settlements, and produces settlement reports.

const SETTLEMENT_TIMEOUT_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

export class SettlementTracker {
  constructor() {
    this.settlements = new Map(); // fillId -> { status, attempts, lastAttempt }
  }

  // Register a fill for settlement tracking
  trackFill(fill) {
    this.settlements.set(fill.id, {
      fillId: fill.id,
      executionId: fill.orderId,
      status: fill.settlementStatus || 'pending',
      attempts: 0,
      lastAttempt: null,
      error: null,
      createdAt: new Date().toISOString(),
    });
  }

  // Attempt to settle a fill
  async attemptSettle(fill, venueAdapter) {
    const record = this.settlements.get(fill.id);
    if (!record) return { ok: false, error: 'fill_not_tracked' };
    if (record.status === 'settled') return { ok: true, fill };

    record.attempts++;
    record.lastAttempt = new Date().toISOString();

    try {
      const status = await venueAdapter.getOrderStatus(fill.orderId);
      const isSettled = status.status === 'filled' || status.status === 'settled';
      if (isSettled) {
        record.status = 'settled';
        fill.settlementStatus = 'settled';
        return { ok: true, fill };
      }
      record.error = `unsettled_status: ${status.status}`;
      return { ok: false, error: record.error };
    } catch (error) {
      record.error = String(error);
      return { ok: false, error: record.error };
    }
  }

  // Get settlement summary for an execution
  getSettlementSummary(execution) {
    const fills = execution.fills || [];
    const tracked = fills.filter(f => this.settlements.has(f.id));
    return {
      executionId: execution.id,
      totalFills: fills.length,
      trackedFills: tracked.length,
      settled: tracked.filter(f => this.settlements.get(f.id)?.status === 'settled').length,
      pending: tracked.filter(f => this.settlements.get(f.id)?.status === 'pending').length,
      failed: tracked.filter(f => this.settlements.get(f.id)?.status === 'failed').length,
      stale: tracked.filter(f => {
        const r = this.settlements.get(f.id);
        return r && Date.now() - new Date(r.createdAt).getTime() > SETTLEMENT_TIMEOUT_MS;
      }).length,
    };
  }

  // Retry all pending settlements for an execution
  async retryPending(execution, getAdapterForVenue) {
    const results = [];
    for (const fill of execution.fills || []) {
      if (fill.settlementStatus !== 'pending' && fill.settlementStatus !== 'failed') continue;
      const adapter = getAdapterForVenue(fill.venue, execution.mode);
      if (!adapter) continue;
      const result = await this.attemptSettle(fill, adapter);
      results.push({ fillId: fill.id, ...result });
    }
    return results;
  }

  getStatus(fillId) {
    return this.settlements.get(fillId) || null;
  }

  getAllStatuses() {
    return Array.from(this.settlements.values());
  }
}

export function createSettlementTracker() {
  return new SettlementTracker();
}
