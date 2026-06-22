// Self-contained adapter registry for runtime use by the API server

class PaperBrokerAdapter {
  name = 'paper';
  venue = 'paper';
  mode = 'paper';
  connected = false;

  constructor(initialCash = 100000) {
    this.cash = initialCash;
    this.nav = initialCash;
    this.positions = new Map();
    this.orderMap = new Map();
  }

  async connect() { this.connected = true; return true; }
  async disconnect() { this.connected = false; }
  async health() { return { ok: this.connected, venue: 'paper', latencyMs: 0, mode: 'paper', authenticated: this.connected }; }

  async getAccounts() {
    return [{ id: 'acct-paper-primary', name: 'Paper Account', provider: 'paper', status: 'connected', currency: 'USD', cash: this.cash, nav: this.nav, availableBalance: this.cash, buyingPower: this.cash, marginUsed: 0, updatedAt: new Date().toISOString(), capabilities: ['spot'], mode: 'paper' }];
  }
  async getBalances() { return [{ currency: 'USD', total: this.cash, available: this.cash, held: 0 }]; }
  async getPositions() {
    return Array.from(this.positions.entries()).map(([symbol, pos]) => ({ symbol, venue: 'paper', quantity: pos.quantity, averagePrice: pos.averagePrice, markPrice: pos.averagePrice, unrealizedPnl: 0, realizedPnl: 0, currency: 'USD', status: pos.quantity !== 0 ? 'open' : 'closed', openedAt: pos.openedAt }));
  }
  async discoverMarkets() {
    return [
      { id: 'BTC-USD', venue: 'paper', title: 'Bitcoin / USD', symbol: 'BTC-USD', assetClass: 'crypto', status: 'active', closeTime: '2099-12-31T23:59:59Z', tickSize: 0.01, minSize: 0.0001, pricePrecision: 2 },
      { id: 'ETH-USD', venue: 'paper', title: 'Ethereum / USD', symbol: 'ETH-USD', assetClass: 'crypto', status: 'active', closeTime: '2099-12-31T23:59:59Z', tickSize: 0.01, minSize: 0.001, pricePrecision: 2 },
    ];
  }
  async getOrderBook() { return { marketId: '', venue: 'paper', yesAsks: [], noAsks: [], bids: [], asks: [], ts: Date.now() }; }
  async getQuote(marketId) {
    const mid = marketId.includes('BTC') ? 68250 : marketId.includes('ETH') ? 3712 : 100;
    return { bid: mid * 0.999, ask: mid * 1.001, mid, spreadBps: 10 };
  }
  async previewOrder(order) {
    const price = order.price || 100;
    const total = order.quantity * price;
    if (order.side === 'buy' && total > this.cash) return { ok: false, errors: ['insufficient_paper_cash'] };
    return { ok: true, preview: { estimatedPrice: price, estimatedFee: total * 0.0005, estimatedSlippage: total * 0.001, estimatedTotal: total, liquidity: 'taker', slippageBps: 10, validUntil: new Date(Date.now() + 30000).toISOString() } };
  }
  async submitOrder(order) {
    const brokerOrderId = `paper-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const fill = { id: `fill-${brokerOrderId}`, orderId: brokerOrderId, marketId: order.marketId, venue: 'paper', side: order.side, quantity: order.quantity, price: order.price || 100, fee: 0, feeCurrency: 'USD', liquidity: 'taker', filledAt: new Date().toISOString(), settlementStatus: 'settled' };
    const signedQty = order.side === 'buy' ? order.quantity : -order.quantity;
    this.cash -= fill.price * fill.quantity * (order.side === 'buy' ? 1 : -1);
    this.nav = this.cash;
    const key = order.symbol || order.marketId;
    const existing = this.positions.get(key);
    if (existing) {
      existing.quantity += signedQty;
      existing.averagePrice = existing.quantity === 0 ? fill.price : ((Math.abs(existing.quantity - signedQty) * existing.averagePrice + Math.abs(signedQty) * fill.price) / Math.abs(existing.quantity));
    } else {
      this.positions.set(key, { quantity: signedQty, averagePrice: fill.price, openedAt: new Date().toISOString() });
    }
    this.orderMap.set(brokerOrderId, { fills: [fill], status: 'filled' });
    return { ok: true, brokerOrderId, status: 'filled', fills: [fill] };
  }
  async cancelOrder() { return true; }
  async getOrderStatus(brokerOrderId) {
    const o = this.orderMap.get(brokerOrderId);
    return { brokerOrderId, status: o?.status || 'cancelled', fills: o?.fills || [], remainingQuantity: 0 };
  }
}

class CoinbaseBrokerAdapter {
  name = 'coinbase';
  venue = 'coinbase';
  mode = 'paper';
  connected = false;

  constructor(mode = 'paper') { this.mode = mode; }

  async _cli(action, payload = {}) {
    const { execSync } = await import('node:child_process');
    const { resolve, dirname } = await import('node:path');
    const { existsSync } = await import('node:fs');
    const { fileURLToPath } = await import('node:url');
    const _dirname = typeof __dirname !== 'undefined' ? __dirname : dirname(fileURLToPath(import.meta.url));
    const bridge = resolve(process.env.COINBASE_CLI_BRIDGE || _dirname + '/../../../coinbase/src/bridge_cli.mjs');
    if (existsSync(bridge)) {
      try {
        const out = execSync(`node ${bridge} '${JSON.stringify({ action, payload }).replace(/'/g, "'\\''")}'`, { encoding: 'utf-8', timeout: 30000 });
        return JSON.parse(out.trim());
      } catch { /* fall through */ }
    }
    return { ok: false, error: 'bridge_unavailable' };
  }

  async connect() {
    try {
      const res = await this._cli('health');
      this.connected = res.ok;
    } catch { this.connected = false; }
    return this.connected;
  }
  async disconnect() { this.connected = false; }
  async health() { return { ok: this.connected, venue: 'coinbase', latencyMs: 0, mode: this.mode, authenticated: this.connected }; }

  async getAccounts() {
    const res = await this._cli('list_accounts');
    if (!res.ok || !res.data) return [{ id: 'acct-coinbase-primary', name: 'Coinbase Account', provider: 'coinbase', status: 'mock', currency: 'USD', cash: 10000, nav: 10000, availableBalance: 10000, buyingPower: 10000, marginUsed: 0, updatedAt: new Date().toISOString(), capabilities: ['spot'], mode: this.mode }];
    const accounts = res.data;
    const usdBalance = accounts.find(a => a.currency === 'USDC')?.balance || 0;
    return [{
      id: 'acct-coinbase-primary',
      name: 'Coinbase Account',
      provider: 'coinbase',
      status: 'connected',
      currency: 'USD',
      cash: usdBalance,
      nav: usdBalance,
      availableBalance: usdBalance,
      buyingPower: usdBalance,
      marginUsed: 0,
      updatedAt: new Date().toISOString(),
      capabilities: ['spot'],
      mode: this.mode,
    }];
  }

  async getBalances() {
    const res = await this._cli('list_accounts');
    if (!res.ok || !res.data) return [{ currency: 'USD', total: 10000, available: 10000, held: 0 }];
    return res.data.map(a => ({ currency: a.currency, total: a.balance, available: a.available, held: a.hold }));
  }

  async getPositions() {
    const res = await this._cli('list_accounts');
    if (!res.ok || !res.data) return [];
    return res.data.filter(a => a.currency !== 'USD' && a.currency !== 'USDC' && a.balance > 0).map(a => ({
      symbol: `${a.currency}-USD`,
      venue: 'coinbase',
      quantity: a.balance,
      averagePrice: 0,
      markPrice: 0,
      unrealizedPnl: 0,
      realizedPnl: 0,
      currency: a.currency,
      status: 'open',
      openedAt: new Date().toISOString(),
    }));
  }

  async discoverMarkets() {
    return [
      { id: 'BTC-USD', venue: 'coinbase', title: 'Bitcoin / USD', symbol: 'BTC-USD', assetClass: 'crypto', status: 'active', closeTime: '2099-12-31T23:59:59Z', tickSize: 0.01, minSize: 0.0001, pricePrecision: 2 },
      { id: 'ETH-USD', venue: 'coinbase', title: 'Ethereum / USD', symbol: 'ETH-USD', assetClass: 'crypto', status: 'active', closeTime: '2099-12-31T23:59:59Z', tickSize: 0.01, minSize: 0.001, pricePrecision: 2 },
    ];
  }

  async getOrderBook() { return { marketId: '', venue: 'coinbase', yesAsks: [], noAsks: [], bids: [], asks: [], ts: Date.now() }; }

  async getQuote(marketId) {
    const res = await this._cli('best_bid_ask', { product_ids: [marketId] });
    if (res.ok && res.data?.pricebooks?.[0]) {
      const book = res.data.pricebooks[0];
      const bid = Number(book.bids?.[0]?.price || 0);
      const ask = Number(book.asks?.[0]?.price || 0);
      if (bid > 0 && ask > 0) {
        const mid = (bid + ask) / 2;
        const spreadBps = ask > 0 ? ((ask - bid) / ask) * 10000 : 0;
        return { bid, ask, mid, spreadBps };
      }
    }
    const mid = marketId.includes('BTC') ? 68250 : 3712;
    return { bid: mid * 0.999, ask: mid * 1.001, mid, spreadBps: 10 };
  }

  async previewOrder(order) {
    const productId = order.symbol || order.marketId;
    if (!productId) return { ok: false, errors: ['product_id_required'] };
    const res = await this._cli('preview_order', {
      product_id: productId,
      side: (order.side || 'buy').toLowerCase(),
      base_size: order.quantity ? String(order.quantity) : undefined,
      quote_size: order.notional ? String(order.notional) : undefined,
    });
    if (res.ok && res.data) {
      const d = res.data;
      const quoteSize = Number(d.quote_size || d.estimated_total || 0);
      const baseSize = Number(d.base_size || d.estimated_quantity || order.quantity || 0);
      const price = quoteSize > 0 && baseSize > 0 ? quoteSize / baseSize : (order.price || 0);
      return {
        ok: true,
        preview: {
          estimatedPrice: price,
          estimatedFee: Number(d.commission_total || d.estimated_fees || d.commission || 0),
          estimatedSlippage: quoteSize * 0.001,
          estimatedTotal: quoteSize,
          liquidity: d.liquidity || 'taker',
          slippageBps: 10,
          validUntil: new Date(Date.now() + 15000).toISOString(),
        },
      };
    }
    return { ok: true, preview: { estimatedPrice: order.price || 0, estimatedFee: 0, estimatedSlippage: 0, estimatedTotal: (order.price || 0) * (order.quantity || 0), liquidity: 'taker', slippageBps: 0, validUntil: new Date(Date.now() + 15000).toISOString() } };
  }

  async submitOrder(order) {
    const productId = order.symbol || order.marketId;
    if (!productId) return { ok: false, errors: ['product_id_required'] };
    const res = await this._cli('submit_order', {
      product_id: productId,
      side: (order.side || 'buy').toLowerCase(),
      base_size: order.quantity ? String(order.quantity) : undefined,
      quote_size: order.notional ? String(order.notional) : undefined,
    });
    if (res.ok && res.data) {
      const d = res.data;
      const brokerOrderId = d.order_id || d.success?.order_id || `cb-${Date.now()}`;
      const status = d.status === 'FILLED' ? 'filled' : d.status === 'CANCELLED' ? 'cancelled' : 'submitted';
      const fillPrice = Number(d.filled_price || d.average_filled_price || d.price || order.price || 0);
      const fillQty = Number(d.filled_size || d.base_size || order.quantity || 0);
      return {
        ok: true,
        brokerOrderId,
        status,
        fills: d.fills?.length ? d.fills.map(f => ({
          id: f.fill_id || `fill-${brokerOrderId}`,
          orderId: brokerOrderId,
          marketId: productId,
          venue: 'coinbase',
          side: f.side?.toLowerCase() || order.side,
          quantity: Number(f.size || f.quantity || 0),
          price: Number(f.price || f.fill_price || fillPrice),
          fee: Number(f.fee || f.commission || f. fees || 0),
          feeCurrency: 'USD',
          liquidity: f.liquidity === 'MAKER' ? 'maker' : 'taker',
          filledAt: f.fill_time || f.created_at || new Date().toISOString(),
          settlementStatus: 'settled',
        })) : [{
          id: `fill-${brokerOrderId}`,
          orderId: brokerOrderId,
          marketId: productId,
          venue: 'coinbase',
          side: order.side,
          quantity: fillQty,
          price: fillPrice,
          fee: 0,
          feeCurrency: 'USD',
          liquidity: 'taker',
          filledAt: new Date().toISOString(),
          settlementStatus: 'settled',
        }],
      };
    }
    return { ok: false, errors: ['coinbase_order_failed'] };
  }

  async cancelOrder() { return true; }
  async getOrderStatus(brokerOrderId) { return { brokerOrderId, status: 'filled', fills: [], remainingQuantity: 0 }; }

  async syncOperatorState() {
    const [accounts, positions, markets] = await Promise.all([
      this.getAccounts(),
      this.getPositions(),
      this.discoverMarkets(),
    ]);
    const quotes = [];
    for (const m of markets.slice(0, 10)) {
      try {
        const q = await this.getQuote(m.symbol);
        if (q.bid > 0) quotes.push({ symbol: m.symbol, ...q });
      } catch { /* skip */ }
    }
    return { accounts, positions, markets, quotes };
  }
}

export class AdapterRegistry {
  constructor() {
    this.adapters = new Map();
    this.registered = new Map();
  }

  registerAdapter(name, adapter) { this.adapters.set(name, adapter); }
  getAdapter(name) { return this.adapters.get(name) || this.registered.get(name); }

  getOrCreate(name, mode) {
    let a = this.adapters.get(name);
    if (a) return a;
    if (name === 'coinbase' || name === 'coinbase-paper') a = new CoinbaseBrokerAdapter(mode || 'paper');
    else if (name === 'coinbase-live') a = new CoinbaseBrokerAdapter('live');
    else a = new PaperBrokerAdapter();
    this.adapters.set(name, a);
    return a;
  }

  getAdapterForVenue(venue, mode) {
    const cmode = mode || 'paper';
    const key = venue === 'coinbase' ? `coinbase-${cmode}` : venue === 'paper' ? 'paper' : venue;
    return this.getOrCreate(key, cmode);
  }

  listAdapters() {
    return Array.from(this.adapters.entries()).map(([name, a]) => ({ name, venue: a.venue || 'paper', mode: a.mode || 'paper', connected: a.connected }));
  }

  async connectAll() {
    const results = [];
    for (const [name, adapter] of this.adapters) {
      try { await adapter.connect(); results.push({ name, ok: true }); }
      catch (e) { results.push({ name, ok: false, error: String(e) }); }
    }
    return results;
  }
}

let defaultRegistry = null;
export function getDefaultRegistry() {
  if (!defaultRegistry) { defaultRegistry = new AdapterRegistry(); }
  return defaultRegistry;
}
export function resetDefaultRegistry() { defaultRegistry = null; }
