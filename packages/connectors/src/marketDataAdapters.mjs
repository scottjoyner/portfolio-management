export class ConnectorError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = 'ConnectorError';
    this.code = code;
    this.details = details;
  }
}

export function normalizeNumber(value, fallback = null) {
  if (value === null || value === undefined || value === '') return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export function nowIso(clock = () => new Date()) {
  return clock().toISOString();
}

export function normalizeMarketSnapshot(raw = {}, options = {}) {
  const symbol = String(raw.symbol || raw.marketSlug || raw.id || '').trim();
  const venue = String(raw.venue || options.venue || '').trim();
  if (!symbol) throw new ConnectorError('symbol_required', 'Market snapshot requires a symbol or market slug', { raw });
  if (!venue) throw new ConnectorError('venue_required', 'Market snapshot requires a venue', { raw });
  const bid = normalizeNumber(raw.bid, null);
  const ask = normalizeNumber(raw.ask, null);
  const spreadBps = normalizeNumber(raw.spreadBps, bid !== null && ask !== null && ask > 0 ? ((ask - bid) / ask) * 10000 : null);
  return {
    id: raw.id || `md-${venue}-${symbol}`.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, ''),
    symbol,
    venue,
    assetClass: raw.assetClass || options.assetClass || 'unknown',
    bid,
    ask,
    spreadBps,
    volume24h: normalizeNumber(raw.volume24h, 0),
    liquidityScore: normalizeNumber(raw.liquidityScore, 50),
    volatilityScore: normalizeNumber(raw.volatilityScore, 50),
    status: raw.status || 'watching',
    source: raw.source || options.source || 'connector',
    timestamp: raw.timestamp || nowIso(options.clock)
  };
}

export class StaticMarketDataAdapter {
  constructor({ venue = 'static', assetClass = 'unknown', source = 'static-market-data', snapshots = [] } = {}) {
    this.venue = venue;
    this.assetClass = assetClass;
    this.source = source;
    this.snapshots = snapshots;
  }

  async listSnapshots() {
    return this.snapshots.map(snapshot => normalizeMarketSnapshot(snapshot, { venue: this.venue, assetClass: this.assetClass, source: this.source }));
  }
}

export class PolymarketWatchAdapter extends StaticMarketDataAdapter {
  constructor(options = {}) {
    super({
      venue: 'polymarket-watch',
      assetClass: 'prediction_market',
      source: 'polymarket-watch-adapter',
      snapshots: options.snapshots || [
        {
          id: 'md-polymarket-demo-turnout',
          symbol: 'POLY:TURNOUT-BASELINE',
          bid: 0.42,
          ask: 0.45,
          volume24h: 241000,
          liquidityScore: 71,
          volatilityScore: 52,
          status: 'research_candidate'
        },
        {
          id: 'md-polymarket-demo-rates',
          symbol: 'POLY:RATES-CUT-Q3',
          bid: 0.31,
          ask: 0.35,
          volume24h: 98000,
          liquidityScore: 64,
          volatilityScore: 48,
          status: 'watching'
        }
      ]
    });
  }
}

export class PaperCryptoMarketAdapter extends StaticMarketDataAdapter {
  constructor(options = {}) {
    super({
      venue: 'coinbase-paper',
      assetClass: 'crypto',
      source: 'paper-crypto-market-adapter',
      snapshots: options.snapshots || [
        { id: 'md-btc-usd', symbol: 'BTC-USD', bid: 68250, ask: 68268, volume24h: 18420000000, liquidityScore: 82, volatilityScore: 61, status: 'watching' },
        { id: 'md-eth-usd', symbol: 'ETH-USD', bid: 3712, ask: 3715, volume24h: 9120000000, liquidityScore: 79, volatilityScore: 58, status: 'eligible' }
      ]
    });
  }
}

export async function collectMarketSnapshots(adapters = []) {
  const results = [];
  const errors = [];
  for (const adapter of adapters) {
    try {
      const snapshots = await adapter.listSnapshots();
      results.push(...snapshots);
    } catch (error) {
      errors.push({ adapter: adapter?.constructor?.name || 'unknown', code: error.code || 'adapter_error', message: error.message });
    }
  }
  return { snapshots: results, errors };
}
