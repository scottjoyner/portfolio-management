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
    id: 'strategy-ema-cross-v1',
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
    id: 'strategy-mean-reversion-v1',
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

export const DEFAULT_BACKTESTS = [
  {
    id: 'backtest-ema-cross-v1',
    strategyId: 'strategy-ema-cross-v1',
    status: 'completed',
    assumptions: {
      initialCapitalUsd: 100000,
      feeBps: 5,
      slippageBps: 10,
      dataSource: 'deterministic-paper-seed',
    },
    metrics: {
      totalReturnPct: 3.05,
      maxDrawdownPct: 1.02,
      sharpe: 1.11,
      totalTrades: 14,
      winRatePct: 57.14,
    },
    equityCurve: [100000, 100500, 99800, 101700, 103050],
    trades: [
      { timestamp: '2026-01-02T10:00:00.000Z', symbol: 'BTC-USD', side: 'buy', quantity: 1, price: 100 },
      { timestamp: '2026-01-03T15:00:00.000Z', symbol: 'BTC-USD', side: 'sell', quantity: 1, price: 103.05 },
    ],
  },
];

export const DEFAULT_APPROVALS = [
  {
    id: 'approval-paper-seed',
    strategyId: 'strategy-ema-cross-v1',
    backtestId: 'backtest-ema-cross-v1',
    status: 'pending_review',
    tier: 'canary',
    reason: 'Seeded paper strategy requires explicit operator approval.',
  },
];
