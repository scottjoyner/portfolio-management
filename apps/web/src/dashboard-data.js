export const marketSnapshots = [
  { symbol: 'BTC-USD', venue: 'coinbase', assetClass: 'crypto', bid: 68250, ask: 68268, spreadBps: 2.64, volume24h: 18420000000, liquidityScore: 82, volatilityScore: 61, status: 'watching' },
  { symbol: 'ETH-USD', venue: 'coinbase', assetClass: 'crypto', bid: 3712, ask: 3715, spreadBps: 8.08, volume24h: 9120000000, liquidityScore: 79, volatilityScore: 58, status: 'eligible' },
  { symbol: 'SPY', venue: 'equities-demo', assetClass: 'equity', bid: 531.12, ask: 531.18, spreadBps: 1.13, volume24h: 61200000, liquidityScore: 94, volatilityScore: 27, status: 'data-only' }
];

export const opportunities = [
  {
    id: 'opp-poly-001',
    sourceAgent: 'polymarket-election-researcher',
    marketType: 'prediction_market',
    venue: 'polymarket',
    market: 'US Election turnout above modeled baseline',
    recommendation: 'buy_yes',
    confidenceScore: 0.68,
    winProbability: 0.57,
    lossProbability: 0.43,
    totalMoneyRisked: 500,
    maxLoss: 500,
    potentialUpside: 420,
    grossExpectedValue: 68.4,
    netExpectedValue: 41.15,
    liquidityScore: 71,
    riskScore: 63,
    backtestStatus: 'historical_analog_complete',
    approvalStatus: 'needs_review',
    agentResearchCost: 9.35,
    modelInferenceCost: 2.9,
    estimatedFees: 5,
    estimatedSlippage: 10,
    notes: 'Agent found edge versus baseline turnout model, but resolution wording and correlated exposure require review.',
    riskBreakdown: {
      liquidity: 0.31,
      resolutionAmbiguity: 0.42,
      slippage: 0.2,
      dataFreshness: 0.14,
      agentConfidence: 0.32
    }
  },
  {
    id: 'opp-crypto-002',
    sourceAgent: 'cross-venue-liquidity-scanner',
    marketType: 'crypto_spot',
    venue: 'coinbase-paper',
    market: 'ETH-USD mean reversion paper setup',
    recommendation: 'paper_buy',
    confidenceScore: 0.61,
    winProbability: 0.54,
    lossProbability: 0.46,
    totalMoneyRisked: 1200,
    maxLoss: 180,
    potentialUpside: 310,
    grossExpectedValue: 37.4,
    netExpectedValue: 20.1,
    liquidityScore: 79,
    riskScore: 55,
    backtestStatus: 'deterministic_scaffold_only',
    approvalStatus: 'paper_only',
    agentResearchCost: 5.2,
    modelInferenceCost: 1.1,
    estimatedFees: 6,
    estimatedSlippage: 5,
    notes: 'Candidate is suitable for paper workflow only until historical replay and real market data adapters exist.',
    riskBreakdown: {
      liquidity: 0.21,
      slippage: 0.19,
      drawdown: 0.38,
      dataFreshness: 0.22,
      agentConfidence: 0.39
    }
  }
];

export const agentCostSummary = {
  dailyBudgetUsd: 35,
  spentTodayUsd: 16.55,
  spentThisMonthUsd: 214.7,
  remoteModelCostUsd: 11.25,
  localModelCostUsd: 5.3,
  openResearchJobs: 3,
  costPerOpportunityUsd: 8.28,
  rejectedOpportunityCostUsd: 4.1,
  profitableAttributionPendingUsd: 16.55,
  localCostFormula: 'runtime_hours * estimated_watts / 1000 * electricity_rate_per_kwh + hardware_depreciation_per_hour * runtime_hours'
};

export const agentJobs = [
  { id: 'job-001', agent: 'polymarket-election-researcher', model: 'gpt-remote', status: 'completed', totalTokens: 18400, estimatedCostUsd: 8.7, opportunities: ['opp-poly-001'] },
  { id: 'job-002', agent: 'cross-venue-liquidity-scanner', model: 'local-qwen', status: 'completed', totalTokens: 11200, estimatedCostUsd: 2.4, opportunities: ['opp-crypto-002'] },
  { id: 'job-003', agent: 'macro-news-screener', model: 'local-llama', status: 'queued', totalTokens: 0, estimatedCostUsd: 0, opportunities: [] }
];
