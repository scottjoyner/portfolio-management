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
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
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
    timestamp: raw.timestamp || nowIso(options.clock),
  };
}

export class StaticMarketDataAdapter {
  constructor({ venue = 'static', assetClass = 'unknown', source = 'static-market-data', snapshots = [], clock } = {}) {
    this.venue = venue;
    this.assetClass = assetClass;
    this.source = source;
    this.snapshots = snapshots;
    this.clock = clock;
  }

  async listSnapshots() {
    return this.snapshots.map(snapshot => normalizeMarketSnapshot(snapshot, {
      venue: this.venue,
      assetClass: this.assetClass,
      source: this.source,
      clock: this.clock,
    }));
  }
}

const DEFAULT_POLYMARKET_WATCH_SNAPSHOTS = [
  {
    id: 'polymarket-watch-demo-rate-cut',
    symbol: 'DEMO-FED-RATE-CUT',
    bid: 0.47,
    ask: 0.50,
    volume24h: 125000,
    liquidityScore: 72,
    volatilityScore: 55,
    status: 'watching',
  },
];

const DEFAULT_PAPER_CRYPTO_SNAPSHOTS = [
  {
    id: 'paper-crypto-btc-usd',
    symbol: 'BTC-USD',
    bid: 99990,
    ask: 100010,
    volume24h: 1200000000,
    liquidityScore: 92,
    volatilityScore: 48,
    status: 'paper_only',
  },
  {
    id: 'paper-crypto-eth-usd',
    symbol: 'ETH-USD',
    bid: 2999,
    ask: 3001,
    volume24h: 650000000,
    liquidityScore: 88,
    volatilityScore: 52,
    status: 'paper_only',
  },
  {
    id: 'paper-crypto-sol-usd',
    symbol: 'SOL-USD',
    bid: 149.8,
    ask: 150.2,
    volume24h: 180000000,
    liquidityScore: 79,
    volatilityScore: 64,
    status: 'paper_only',
  },
];

export class PolymarketWatchAdapter extends StaticMarketDataAdapter {
  constructor(options = {}) {
    super({
      venue: 'polymarket-watch',
      assetClass: 'prediction_market',
      source: 'polymarket-watch-adapter-demo',
      snapshots: options.snapshots || DEFAULT_POLYMARKET_WATCH_SNAPSHOTS,
      clock: options.clock,
    });
  }
}

export class PaperCryptoMarketAdapter extends StaticMarketDataAdapter {
  constructor(options = {}) {
    super({
      venue: 'coinbase-paper',
      assetClass: 'crypto',
      source: 'paper-crypto-market-adapter-demo',
      snapshots: options.snapshots || DEFAULT_PAPER_CRYPTO_SNAPSHOTS,
      clock: options.clock,
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
