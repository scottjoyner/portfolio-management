from pathlib import Path

path = Path('apps/api/src/opportunityGenerator.mjs')
text = path.read_text()
marker = "tp_sl_payoff_expected_pnl_v1"
if marker in text:
    print('strategy opportunity economics patch already present')
    raise SystemExit(0)

start = text.index('function strategySignalToOpportunityInput(signal) {')
end = text.index('\n}\n\nexport function runStrategySignalScanner', start) + 2
replacement = r'''export function strategySignalToOpportunityInput(signal) {
  const weightedConfidence = Number(signal.weighted_confidence || signal.weightedConfidence || signal.confidence || 0.5);
  const rawWinRate = Number(signal.win_rate || signal.winRate || 0.5);
  const winProbability = Number.isFinite(rawWinRate) ? Math.min(0.99, Math.max(0.01, rawWinRate)) : 0.5;
  const lossProbability = 1 - winProbability;
  const tradeSize = Number(signal.notional_usd || signal.totalMoneyRisked || signal.size_usd || 1000);
  const isSell = String(signal.action || 'BUY').toUpperCase() === 'SELL';
  const direction = isSell ? 'sell' : 'buy';
  const tradePlan = signal.trade_plan && typeof signal.trade_plan === 'object' ? signal.trade_plan : {};
  const takeProfitPrice = Number(signal.take_profit_price || tradePlan.take_profit_price || signal.price || 0);
  const stopLossPrice = Number(signal.stop_loss_price || tradePlan.stop_loss_price || signal.price || 0);
  const entryPrice = Number(signal.entry_price || tradePlan.entry_price || signal.price || 0);
  const positionSide = tradePlan.position_side || 'long';
  const executionPurpose = signal.execution_purpose || tradePlan.execution_purpose || (isSell ? 'take_profit_exit' : 'open_long');
  const tradeIntent = signal.trade_intent || tradePlan.plan_type || (isSell ? 'exit' : 'entry');
  const recommendation = executionPurpose === 'take_profit_exit' ? 'take_profit' : (isSell ? 'review_short' : 'paper_review');

  const validTradeSize = Number.isFinite(tradeSize) && tradeSize > 0 ? tradeSize : 0;
  const validEntryPrice = Number.isFinite(entryPrice) && entryPrice > 0 ? entryPrice : 0;
  const quantity = validEntryPrice > 0 ? validTradeSize / validEntryPrice : 0;
  const potentialUpside = validEntryPrice > 0 && Number.isFinite(takeProfitPrice) && takeProfitPrice > 0
    ? Math.max(0, Math.abs(takeProfitPrice - validEntryPrice) * quantity)
    : 0;
  const maxLoss = validEntryPrice > 0 && Number.isFinite(stopLossPrice) && stopLossPrice > 0
    ? Math.max(0, Math.abs(validEntryPrice - stopLossPrice) * quantity)
    : 0;
  const isEntry = tradeIntent === 'entry';
  const grossExpectedValueUsd = isEntry
    ? (winProbability * potentialUpside) - (lossProbability * maxLoss)
    : 0;
  const expectedValueMethod = isEntry
    ? 'tp_sl_payoff_expected_pnl_v1'
    : 'risk_reduction_not_edge_scored_v1';

  return {
    sourceAgentId: 'strategy-comparison-scanner',
    strategyId: signal.strategy,
    marketType: 'crypto_spot',
    venue: 'coinbase-paper',
    symbol: signal.symbol || signal.product_id,
    side: direction,
    tradeIntent,
    executionPurpose,
    positionSide,
    takeProfitPrice,
    stopLossPrice,
    entryPrice,
    tradePlan,
    marketSlug: String(signal.symbol || signal.product_id || '').toLowerCase().replace(/[^a-z0-9]+/g, '-'),
    title: `${signal.symbol || signal.product_id} ${direction} — ${signal.strategy}`,
    recommendation,
    confidenceScore: weightedConfidence,
    winProbability,
    lossProbability,
    expectedValue: Number(grossExpectedValueUsd.toFixed(2)),
    grossExpectedValue: Number(grossExpectedValueUsd.toFixed(2)),
    totalMoneyRisked: validTradeSize,
    maxLoss: Number(maxLoss.toFixed(2)),
    potentialUpside: Number(potentialUpside.toFixed(2)),
    rewardRiskRatio: maxLoss > 0 ? Number((potentialUpside / maxLoss).toFixed(4)) : 0,
    liquidityScore: Number(signal.liquidity_score || signal.liquidityScore || 50),
    dataFreshnessScore: 95,
    backtestStatus: 'same_window_30d_screen_uncertified',
    estimatedFees: Number(signal.estimated_fees || 5),
    estimatedSlippage: Number(signal.estimated_slippage || 3),
    estimatedGas: 0,
    agentResearchCost: 0,
    modelInferenceCost: 0,
    notes: `${signal.reason || signal.backtest_reason || 'strategy scan'} | same-window screen only; not tournament-certified | win_rate=${(winProbability * 100).toFixed(1)}% | sentiment=${Number(signal.sentiment_score || 0).toFixed(2)} | weighted_conf=${weightedConfidence.toFixed(2)} | tp=${takeProfitPrice || 'n/a'} | sl=${stopLossPrice || 'n/a'} | source=${signal.source || 'live_cli'}`,
    evidence: [{
      type: 'strategy_live_30d_test',
      strategy: signal.strategy,
      source: signal.source || 'live_cli',
      productId: signal.product_id || signal.symbol,
      winRate: winProbability,
      sentimentScore: Number(signal.sentiment_score || 0),
      consensus: Number(signal.consensus || 0),
      regime: signal.regime || 'neutral',
      score: Number(signal.score || weightedConfidence * winProbability),
      backtestTotalReturnPct: Number(signal.backtest_total_return_pct || 0),
      backtestSharpe: Number(signal.backtest_sharpe || 0),
      backtestProfitFactor: Number(signal.backtest_profit_factor || 0),
      backtestMaxDrawdownPct: Number(signal.backtest_max_drawdown_pct || 0),
      candles: Number(signal.candles || 0),
      marketDirection: signal.market_direction || (Number(signal.sentiment_score || 0) >= 0 ? 'bullish' : 'bearish'),
      tradePlan,
      validationScope: 'same_window_in_sample_screen',
      tournamentCertified: false,
      expectedValueUnit: 'USD',
      expectedValueMethod,
      grossExpectedValueUsd: Number(grossExpectedValueUsd.toFixed(2)),
      potentialUpsideUsd: Number(potentialUpside.toFixed(2)),
      maxLossUsd: Number(maxLoss.toFixed(2)),
    }],
  };
}'''
text = text[:start] + replacement + text[end:]
path.write_text(text)
