export class AdapterError extends Error {
  constructor(code, message = code, details = {}) {
    super(message);
    this.name = 'AdapterError';
    this.code = code;
    this.details = details;
  }
}

export class ReadOnlyMarketDataAdapter {
  constructor({ name }) {
    this.name = name;
    this.capabilities = ['market_discovery', 'quotes', 'historical_bars'];
  }

  async discoverMarkets() { throw new AdapterError('not_implemented'); }
  async getQuote() { throw new AdapterError('not_implemented'); }
  async getHistoricalBars() { throw new AdapterError('not_implemented'); }
}

export class BrokerExecutionAdapter {
  constructor({ name, liveEnabled = false }) {
    this.name = name;
    this.liveEnabled = liveEnabled;
    this.capabilities = ['preview_order', 'paper_order'];
  }

  assertLiveDisabled() {
    if (!this.liveEnabled) throw new AdapterError('live_execution_disabled', 'Live execution is disabled for this adapter.');
  }

  async previewOrder() { throw new AdapterError('not_implemented'); }
  async submitPaperOrder() { throw new AdapterError('not_implemented'); }
  async submitLiveOrder() { this.assertLiveDisabled(); throw new AdapterError('not_implemented'); }
}

export class FailClosedExecutionAdapter extends BrokerExecutionAdapter {
  constructor(options = {}) { super({ name: options.name || 'fail-closed', liveEnabled: false }); }
  async previewOrder(order) { return { ok: true, adapter: this.name, order, liveEnabled: false }; }
  async submitPaperOrder(order) { return { ok: true, adapter: this.name, order, mode: 'paper' }; }
  async submitLiveOrder() { throw new AdapterError('live_execution_disabled', 'Live order submission remains disabled.'); }
}

export function createFailClosedAdapter(name = 'fail-closed') {
  return new FailClosedExecutionAdapter({ name });
}
