export class AdapterError extends Error {
  constructor(code, message = code, details = {}) {
    super(message);
    this.name = 'AdapterError';
    this.code = code;
    this.details = details;
  }
}

export function assertAdapterCertification(adapter, certification = null, { requireLive = false } = {}) {
  if (!certification) throw new AdapterError('adapter_not_certified', 'Adapter certification is required.');
  if (certification.adapterName && certification.adapterName !== adapter.name) throw new AdapterError('adapter_certification_mismatch');
  if (certification.status === 'revoked' || certification.status === 'blocked') throw new AdapterError('adapter_certification_blocked');
  if (requireLive) {
    if (certification.status !== 'certified_live' || certification.liveEnabled !== true) throw new AdapterError('adapter_live_not_certified');
  } else if (!['certified_paper', 'certified_live'].includes(certification.status)) {
    throw new AdapterError('adapter_paper_not_certified');
  }
  if (certification.expiresAt && new Date(certification.expiresAt).getTime() <= Date.now()) throw new AdapterError('adapter_certification_expired');
  return true;
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
  constructor({ name, liveEnabled = false, certification = null }) {
    this.name = name;
    this.liveEnabled = liveEnabled;
    this.certification = certification;
    this.capabilities = ['preview_order', 'paper_order'];
  }

  assertPaperCertified() {
    return assertAdapterCertification(this, this.certification, { requireLive: false });
  }

  assertLiveCertified() {
    if (!this.liveEnabled) throw new AdapterError('live_execution_disabled', 'Live execution is disabled for this adapter.');
    return assertAdapterCertification(this, this.certification, { requireLive: true });
  }

  async previewOrder() { throw new AdapterError('not_implemented'); }
  async submitPaperOrder() { this.assertPaperCertified(); throw new AdapterError('not_implemented'); }
  async submitLiveOrder() { this.assertLiveCertified(); throw new AdapterError('not_implemented'); }
}

export class FailClosedExecutionAdapter extends BrokerExecutionAdapter {
  constructor(options = {}) { super({ name: options.name || 'fail-closed', liveEnabled: false, certification: options.certification || { adapterName: options.name || 'fail-closed', status: 'certified_paper', liveEnabled: false } }); }
  async previewOrder(order) { return { ok: true, adapter: this.name, order, liveEnabled: false }; }
  async submitPaperOrder(order) { this.assertPaperCertified(); return { ok: true, adapter: this.name, order, mode: 'paper' }; }
  async submitLiveOrder() { throw new AdapterError('live_execution_disabled', 'Live order submission remains disabled.'); }
}

export function createFailClosedAdapter(name = 'fail-closed') {
  return new FailClosedExecutionAdapter({ name });
}
