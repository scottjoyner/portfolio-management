export const DEFAULT_ACCOUNTS = [
  { id: 'acct-paper-primary', name: 'Primary Paper Portfolio', provider: 'paper', status: 'connected', currency: 'USD', cash: 100000, nav: 100000 },
  { id: 'acct-plaid-sandbox', name: 'Plaid Sandbox Placeholder', provider: 'plaid_sandbox', status: 'mock', currency: 'USD', cash: 25000, nav: 25000 }
];

export const DEFAULT_INSTRUMENTS = [
  { symbol: 'BTC-USD', name: 'Bitcoin / USD', assetClass: 'crypto', venue: 'paper', status: 'active', minOrderSize: 0.0001, pricePrecision: 2 },
  { symbol: 'ETH-USD', name: 'Ether / USD', assetClass: 'crypto', venue: 'paper', status: 'active', minOrderSize: 0.001, pricePrecision: 2 },
  { symbol: 'SOL-USD', name: 'Solana / USD', assetClass: 'crypto', venue: 'paper', status: 'active', minOrderSize: 0.01, pricePrecision: 2 },
  { symbol: 'AAPL', name: 'Apple Inc.', assetClass: 'equity', venue: 'paper', status: 'active', minOrderSize: 1, pricePrecision: 2 }
];

export const DEFAULT_STRATEGY_TEMPLATES = [
  {
    id: 'template-ema-crossover',
    name: 'EMA Crossover',
    description: 'Trend-following template using fast and slow EMA crossover signals.',
    riskLevel: 'medium',
    parameterSchema: {
      symbol: { type: 'instrument', required: true, default: 'BTC-USD' },
      timeframe: { type: 'enum', values: ['15m', '1h', '4h', '1d'], default: '1h' },
      fastPeriod: { type: 'number', min: 2, max: 100, default: 9 },
      slowPeriod: { type: 'number', min: 3, max: 250, default: 21 }
    }
  },
  {
    id: 'template-zscore-mean-reversion',
    name: 'Z-Score Mean Reversion',
    description: 'Mean-reversion template using rolling z-score entry and exit thresholds.',
    riskLevel: 'low',
    parameterSchema: {
      symbol: { type: 'instrument', required: true, default: 'ETH-USD' },
      timeframe: { type: 'enum', values: ['15m', '1h', '4h', '1d'], default: '1h' },
      lookback: { type: 'number', min: 5, max: 250, default: 20 },
      entryZ: { type: 'number', min: -5, max: 5, default: -2 },
      exitZ: { type: 'number', min: -2, max: 2, default: 0 }
    }
  }
];
