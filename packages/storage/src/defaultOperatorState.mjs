export const DEFAULT_ACCOUNTS = [
  {
    id: 'acct-paper-primary',
    name: 'Paper Trading',
    provider: 'paper',
    status: 'connected',
    currency: 'USD',
    cash: 100000,
    nav: 100000,
  },
];

export const DEFAULT_INSTRUMENTS = [
  {
    id: 'instrument-btc-usd',
    symbol: 'BTC-USD',
    name: 'Bitcoin / US Dollar',
    assetClass: 'crypto_spot',
    venue: 'coinbase-paper',
    quoteCurrency: 'USD',
    status: 'active',
  },
  {
    id: 'instrument-eth-usd',
    symbol: 'ETH-USD',
    name: 'Ether / US Dollar',
    assetClass: 'crypto_spot',
    venue: 'coinbase-paper',
    quoteCurrency: 'USD',
    status: 'active',
  },
  {
    id: 'instrument-sol-usd',
    symbol: 'SOL-USD',
    name: 'Solana / US Dollar',
    assetClass: 'crypto_spot',
    venue: 'coinbase-paper',
    quoteCurrency: 'USD',
    status: 'active',
  },
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
      slowPeriod: { type: 'number', min: 3, max: 250, default: 21 },
    },
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
      exitZ: { type: 'number', min: -2, max: 2, default: 0 },
    },
  },
];

export const DEFAULT_STRATEGIES = [
  {
    id: 'strategy-paper-seed',
    templateId: 'template-ema-crossover',
    name: 'Paper BTC EMA Baseline',
    version: 1,
    status: 'draft',
    riskLevel: 'medium',
    parameters: {
      symbol: 'BTC-USD',
      timeframe: '1h',
      fastPeriod: 9,
      slowPeriod: 21,
    },
  },
  {
    id: 'strategy-paper-mean-reversion',
    templateId: 'template-zscore-mean-reversion',
    name: 'Paper ETH Mean Reversion Baseline',
    version: 1,
    status: 'draft',
    riskLevel: 'low',
    parameters: {
      symbol: 'ETH-USD',
      timeframe: '1h',
      lookback: 20,
      entryZ: -2,
      exitZ: 0,
    },
  },
];

export const DEFAULT_APPROVALS = [
  {
    id: 'approval-paper-seed',
    strategyId: 'strategy-paper-seed',
    backtestId: null,
    status: 'pending_review',
    tier: 'canary',
    reason: 'Seeded paper strategy requires explicit operator approval.',
  },
];
