import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createOpportunity, createResearchJob, decideOpportunity, ensureOpportunityState } from './opportunityFlows.mjs';
import { collectMarketSnapshots, PaperCryptoMarketAdapter, PolymarketWatchAdapter } from '../../../packages/connectors/src/marketDataAdapters.mjs';
import { GraphAlphaBotAdapter } from '../../../packages/adapters/src/graphAlphaBotAdapter.mjs';

const STRATEGY_SIGNAL_SCANNER = fileURLToPath(new URL('../../../scripts/strategy_signal_scanner.py', import.meta.url));

function upsertSnapshot(state, snapshot) {
  state.marketDataSnapshots ||= [];
  const idx = state.marketDataSnapshots.findIndex(row => row.id === snapshot.id || (row.symbol === snapshot.symbol && row.venue === snapshot.venue));
  if (idx >= 0) state.marketDataSnapshots[idx] = { ...state.marketDataSnapshots[idx], ...snapshot };
  else state.marketDataSnapshots.push(snapshot);
}

function snapshotToOpportunityInput(snapshot, job, options = {}) {
  const isPrediction = snapshot.assetClass === 'prediction_market' || snapshot.venue.includes('polymarket');
  const spreadPenalty = Math.min(50, Number(snapshot.spreadBps || 0) / 100);
  const liquidityScore = Number(snapshot.liquidityScore || 50);
  const grossExpectedValue = isPrediction ? Math.max(12, liquidityScore - spreadPenalty - 20) : Math.max(8, liquidityScore - Number(snapshot.volatilityScore || 50) / 2);
  const totalMoneyRisked = isPrediction ? 500 : 1000;
  const maxLoss = isPrediction ? totalMoneyRisked : Math.round(totalMoneyRisked * 0.18);
  const potentialUpside = Math.round(grossExpectedValue * (isPrediction ? 7 : 5));
  const winProbability = Math.min(0.7, Math.max(0.45, 0.5 + (liquidityScore - 50) / 300 - spreadPenalty / 300));
  return {
    researchJobId: job.id,
    sourceAgentId: job.agentId,
    marketType: isPrediction ? 'prediction_market' : `${snapshot.assetClass || 'market'}_review`,
    venue: snapshot.venue,
    symbol: snapshot.symbol,
    marketSlug: isPrediction ? snapshot.symbol.toLowerCase().replace(/[^a-z0-9]+/g, '-') : null,
    title: isPrediction ? `${snapshot.symbol} prediction-market research candidate` : `${snapshot.symbol} market review candidate`,
    recommendation: isPrediction ? 'review_yes' : 'paper_review',
    confidenceScore: Math.min(0.75, Math.max(0.45, 0.5 + liquidityScore / 500)),
    winProbability: Number(winProbability.toFixed(2)),
    lossProbability: Number((1 - winProbability).toFixed(2)),
    grossExpectedValue: Number(grossExpectedValue.toFixed(2)),
    totalMoneyRisked,
    maxLoss,
    potentialUpside,
    liquidityScore,
    dataFreshnessScore: 85,
    backtestStatus: options.backtestStatus || 'connector_generated_requires_backtest',
    estimatedFees: isPrediction ? 2 : 5,
    estimatedSlippage: Number(Math.max(1, spreadPenalty).toFixed(2)),
    estimatedGas: 0,
    agentResearchCost: Number((Number(job.estimatedRemoteCost || 0) + Number(job.estimatedLocalCost || 0)).toFixed(2)),
    modelInferenceCost: 0,
    notes: `Generated from ${snapshot.source} snapshot. Requires operator review and backtest/replay before any paper allocation.`,
    evidence: [{ type: 'market_snapshot', snapshotId: snapshot.id, source: snapshot.source, timestamp: snapshot.timestamp }]
  };
}

function normalizePredictionMarketSnapshot(market, venue, source) {
  const title = String(market.title || market.question || market.slug || market.id || 'prediction market');
  const bid = Number(market.yes_bid ?? market.yesBid ?? market.bid ?? market.outcomePrices?.[0] ?? 0);
  const ask = Number(market.yes_ask ?? market.yesAsk ?? market.ask ?? market.outcomePrices?.[1] ?? 0);
  const spreadBps = ask > 0 && bid > 0 ? ((ask - bid) / ask) * 10000 : 0;
  const volume24h = Number(market.volume ?? market.volume24h ?? market.volume_24h ?? 0);
  const liquidityBase = Number(market.liquidity ?? market.liquidityScore ?? volume24h / 1000 ?? 0);
  return {
    id: `${venue}:${market.id || market.conditionId || market.ticker || market.slug}`,
    symbol: String(market.id || market.conditionId || market.ticker || market.slug || title).toUpperCase(),
    venue,
    assetClass: 'prediction_market',
    title,
    bid,
    ask,
    spreadBps: Number.isFinite(spreadBps) ? Number(spreadBps.toFixed(2)) : 0,
    volume24h,
    liquidityScore: Math.min(95, Math.max(10, Math.round(liquidityBase > 0 ? Math.log10(liquidityBase + 1) * 20 : 50))),
    volatilityScore: 50,
    status: market.status || 'active',
    timestamp: new Date().toISOString(),
    source,
    marketSlug: market.slug || market.conditionId || market.ticker || null,
    outcomes: Array.isArray(market.outcomes) ? market.outcomes : undefined,
    outcomePrices: Array.isArray(market.outcomePrices) ? market.outcomePrices : undefined,
  };
}

function strategySignalToOpportunityInput(signal) {
  const weightedConfidence = Number(signal.weighted_confidence || signal.weightedConfidence || signal.confidence || 0.5);
  const winRate = Number(signal.win_rate || signal.winRate || 0.5);
  const tradeSize = Number(signal.notional_usd || signal.totalMoneyRisked || signal.size_usd || 1000);
  const expectedValue = Number((weightedConfidence * winRate * 100).toFixed(2));
  const isSell = String(signal.action || 'BUY').toUpperCase() === 'SELL';
  const direction = isSell ? 'sell' : 'buy';
  const tradePlan = signal.trade_plan || {};
  const takeProfitPrice = Number(signal.take_profit_price || tradePlan.take_profit_price || signal.price || 0);
  const stopLossPrice = Number(signal.stop_loss_price || tradePlan.stop_loss_price || signal.price || 0);
  const entryPrice = Number(signal.entry_price || tradePlan.entry_price || signal.price || 0);
  const positionSide = tradePlan.position_side || (isSell ? 'long' : 'long');
  const executionPurpose = signal.execution_purpose || tradePlan.execution_purpose || (isSell ? 'take_profit_exit' : 'open_long');
  const recommendation = executionPurpose === 'take_profit_exit' ? 'take_profit' : (isSell ? 'review_short' : 'paper_review');
  return {
    sourceAgentId: 'strategy-comparison-scanner',
    strategyId: signal.strategy,
    marketType: 'crypto_spot',
    venue: 'coinbase-paper',
    symbol: signal.symbol || signal.product_id,
    side: direction,
    tradeIntent: signal.trade_intent || tradePlan.plan_type || (isSell ? 'exit' : 'entry'),
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
    winProbability: Math.max(0.01, Math.min(0.99, winRate)),
    lossProbability: Math.max(0.01, Math.min(0.99, 1 - winRate)),
    expectedValue,
    grossExpectedValue: Number((expectedValue + 5).toFixed(2)),
    totalMoneyRisked: tradeSize,
    maxLoss: Math.max(0, Number(tradePlan.stop_loss_pct || 0) * tradeSize),
    potentialUpside: Math.max(0, Math.abs(takeProfitPrice - entryPrice) * (tradeSize > 0 && entryPrice > 0 ? tradeSize / entryPrice : 1)),
    rewardRiskRatio: Number(tradePlan.risk_reward_ratio || 0),
    liquidityScore: Number(signal.liquidity_score || signal.liquidityScore || 50),
    dataFreshnessScore: 95,
    backtestStatus: 'live_data_30d_mock_tested',
    estimatedFees: Number(signal.estimated_fees || 5),
    estimatedSlippage: Number(signal.estimated_slippage || 3),
    estimatedGas: 0,
    agentResearchCost: 0,
    modelInferenceCost: 0,
    notes: `${signal.reason || signal.backtest_reason || 'strategy scan'} | win_rate=${(winRate * 100).toFixed(1)}% | sentiment=${Number(signal.sentiment_score || 0).toFixed(2)} | weighted_conf=${weightedConfidence.toFixed(2)} | tp=${takeProfitPrice || 'n/a'} | sl=${stopLossPrice || 'n/a'} | source=${signal.source || 'live_cli'}`,
    evidence: [{
      type: 'strategy_live_30d_test',
      strategy: signal.strategy,
      source: signal.source || 'live_cli',
      productId: signal.product_id || signal.symbol,
      winRate,
      sentimentScore: Number(signal.sentiment_score || 0),
      consensus: Number(signal.consensus || 0),
      regime: signal.regime || 'neutral',
      score: Number(signal.score || weightedConfidence * winRate),
      backtestTotalReturnPct: Number(signal.backtest_total_return_pct || 0),
      backtestSharpe: Number(signal.backtest_sharpe || 0),
      backtestProfitFactor: Number(signal.backtest_profit_factor || 0),
      backtestMaxDrawdownPct: Number(signal.backtest_max_drawdown_pct || 0),
      candles: Number(signal.candles || 0),
      marketDirection: signal.market_direction || (Number(signal.sentiment_score || 0) >= 0 ? 'bullish' : 'bearish'),
      tradePlan,
    }],
  };
}

export function runStrategySignalScanner(options = {}) {
  const scannerPath = options.scannerPath || STRATEGY_SIGNAL_SCANNER;
  const products = Array.isArray(options.products) && options.products.length
    ? options.products
    : String(options.products || 'BTC-USD,ETH-USD,SOL-USD,ADA-USD,DOT-USD,MATIC-USD,AVAX-USD,LINK-USD');
  const args = [
    scannerPath,
    '--products',
    Array.isArray(products) ? products.join(',') : products,
    '--granularity',
    String(options.granularity || 'ONE_HOUR'),
    '--days-back',
    String(options.daysBack || options.days_back || 30),
    '--min-win-rate',
    String(options.minWinRate || options.min_win_rate || 0.55),
    '--min-weighted-confidence',
    String(options.minWeightedConfidence || options.min_weighted_confidence || 0.55),
    '--limit',
    String(options.limit || 10),
  ];

  if (options.discover !== false) args.push('--discover');
  args.push('--quote-currencies', String(options.quoteCurrencies || options.quote_currencies || 'USD,BTC'));
  if (options.refresh) args.push('--refresh');
  if (options.cacheTtl || options.cache_ttl) {
    args.push('--cache-ttl', String(options.cacheTtl || options.cache_ttl));
  }
  if (options.productsCacheTtl || options.products_cache_ttl) {
    args.push('--products-cache-ttl', String(options.productsCacheTtl || options.products_cache_ttl));
  }

  const stdout = execFileSync('python3', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
  return JSON.parse(stdout);
}

export async function ingestConnectorSnapshots(state, options = {}) {
  ensureOpportunityState(state);
  const adapters = options.adapters || [new PaperCryptoMarketAdapter(), new PolymarketWatchAdapter()];
  const collected = await collectMarketSnapshots(adapters);
  for (const snapshot of collected.snapshots) upsertSnapshot(state, snapshot);
  return { snapshots: collected.snapshots, errors: collected.errors };
}

export async function generateOpportunitiesFromConnectors(state, options = {}) {
  ensureOpportunityState(state);
  const { snapshots, errors } = await ingestConnectorSnapshots(state, options);
  const created = [];
  for (const snapshot of snapshots) {
    if (state.opportunities.some(opp => opp.symbol === snapshot.symbol && opp.venue === snapshot.venue && ['needs_review', 'approved', 'research_requested', 'deferred'].includes(opp.status))) continue;
    const jobResult = createResearchJob(state, {
      agentId: options.agentId || (snapshot.venue.includes('polymarket') ? 'market-research-agent' : 'liquidity-scanner'),
      triggerType: 'connector_ingest',
      marketScope: snapshot.symbol,
      symbolScope: snapshot.symbol,
      provider: 'local',
      model: options.model || 'connector-review-model',
      localOrRemote: 'local',
      promptTokens: 1200,
      completionTokens: 600,
      totalTokens: 1800,
      runtimeSeconds: 30,
      approvedBudgetOverride: true
    });
    if (jobResult.errors) {
      errors.push({ snapshotId: snapshot.id, code: 'research_job_failed', errors: jobResult.errors });
      continue;
    }
    const opportunityResult = createOpportunity(state, snapshotToOpportunityInput(snapshot, jobResult.job, options));
    if (opportunityResult.errors) errors.push({ snapshotId: snapshot.id, code: 'opportunity_generation_failed', errors: opportunityResult.errors });
    else created.push(opportunityResult.opportunity);
  }
  return { snapshots, opportunities: created, errors };
}

export async function fetchPredictionMarketSnapshots(options = {}) {
  const limit = Number(options.limit || 25);
  const snapshots = [];
  const errors = [];
  const clients = [];

  try {
    const { KalshiClient } = await import('../../../packages/kalshi/src/client.ts');
    clients.push({ venue: 'kalshi', source: 'kalshi_api', client: new KalshiClient() });
  } catch (error) {
    errors.push({ venue: 'kalshi', error: String(error) });
  }

  try {
    const { PolymarketClient } = await import('../../../packages/polymarket/src/client.ts');
    clients.push({ venue: 'polymarket', source: 'polymarket_api', client: new PolymarketClient() });
  } catch (error) {
    errors.push({ venue: 'polymarket', error: String(error) });
  }

  for (const { venue, source, client } of clients) {
    try {
      const markets = venue === 'kalshi'
        ? await client.listMarkets({ limit, status: options.status || 'open' })
        : await client.listMarkets({ limit, closed: false });
      for (const market of markets.slice(0, limit)) {
        snapshots.push(normalizePredictionMarketSnapshot(market, venue, source));
      }
    } catch (error) {
      errors.push({ venue, error: String(error) });
    }
  }

  return { snapshots, errors };
}

export async function generateOpportunitiesFromPredictionMarkets(state, options = {}) {
  ensureOpportunityState(state);
  const { snapshots, errors } = await fetchPredictionMarketSnapshots(options);
  const created = [];

  for (const snapshot of snapshots) {
    if (state.opportunities.some(opp => opp.symbol === snapshot.symbol && opp.venue === snapshot.venue && ['needs_review', 'approved', 'research_requested', 'deferred'].includes(opp.status))) {
      continue;
    }

    const jobResult = createResearchJob(state, {
      agentId: options.agentId || 'market-research-agent',
      triggerType: 'prediction_market_scan',
      marketScope: snapshot.symbol,
      symbolScope: snapshot.symbol,
      provider: 'local',
      model: options.model || 'prediction-market-review-model',
      localOrRemote: 'local',
      promptTokens: 1000,
      completionTokens: 500,
      totalTokens: 1500,
      runtimeSeconds: 20,
      approvedBudgetOverride: true,
    });

    if (jobResult.errors) {
      errors.push({ snapshotId: snapshot.id, code: 'research_job_failed', errors: jobResult.errors });
      continue;
    }

    const opportunityResult = createOpportunity(state, snapshotToOpportunityInput(snapshot, jobResult.job, { backtestStatus: 'prediction_market_scan' }));
    if (opportunityResult.errors) {
      errors.push({ snapshotId: snapshot.id, code: 'opportunity_generation_failed', errors: opportunityResult.errors });
      continue;
    }

    created.push(opportunityResult.opportunity);
  }

  return { snapshots, opportunities: created, errors };
}

function arbitrageOpportunityInput(arb) {
  return {
    sourceAgentId: 'arbitrage-scanner',
    marketType: 'prediction_market',
    venue: `${arb.kalshiMarket.venue}+${arb.polymarketMarket.venue}`,
    title: arb.title,
    symbol: arb.kalshiMarket.id,
    marketSlug: arb.polymarketMarket.conditionId || arb.polymarketMarket.id,
    tradeIntent: 'arbitrage',
    executionPurpose: 'two_leg_cross_venue',
    positionSide: 'flat',
    tradePlan: {
      plan_type: 'arbitrage',
      execution_purpose: 'two_leg_cross_venue',
      expectedProfitUsd: arb.expectedProfitUsd,
      edgeBps: arb.edgeBps,
      payoutPerShare: arb.bestStrategy.payout,
      costPerShare: arb.bestStrategy.totalCost,
      legs: arb.bestStrategy.legs,
      venuePair: [arb.kalshiMarket.venue, arb.polymarketMarket.venue],
    },
    recommendation: 'review',
    confidenceScore: arb.confidenceScore,
    expectedValue: arb.profitPerShare * arb.size,
    grossExpectedValue: arb.returnPct / 100 * arb.size,
    totalMoneyRisked: arb.totalCostPerShare * arb.size,
    maxLoss: arb.totalCostPerShare * arb.size,
    potentialUpside: arb.profitPerShare * arb.size,
    liquidityScore: arb.liquidityScore,
    estimatedFees: arb.size * 0.002,
    estimatedSlippage: arb.size * 0.001,
    notes: `Arbitrage: ${arb.bestStrategy.label} (${arb.edgeBps} bps edge)`,
    evidence: [{
      type: 'arbitrage_scan',
      kalshiMarket: { id: arb.kalshiMarket.id, title: arb.kalshiMarket.title },
      polymarketMarket: { id: arb.polymarketMarket.conditionId || arb.polymarketMarket.id, title: arb.polymarketMarket.question || arb.polymarketMarket.title },
      bestStrategy: { ...arb.bestStrategy },
      similarity: arb.similarity,
      expectedProfitUsd: arb.expectedProfitUsd,
    }],
    status: 'needs_review',
  };
}

export async function generateOpportunitiesFromArbitrage(state, options = {}) {
  ensureOpportunityState(state);
  const { scanForArbitrage } = await import('../../../packages/arbitrage/src/arbitrageScanner.mjs');
  const opportunities = await scanForArbitrage(options);
  const created = [];
  const errors = [];

  for (const arb of opportunities) {
    if (state.opportunities.some(opp => opp.symbol === arb.kalshiMarket.id && opp.venue === `${arb.kalshiMarket.venue}+${arb.polymarketMarket.venue}` && ['needs_review', 'approved', 'research_requested', 'deferred'].includes(opp.status))) continue;
    const result = createOpportunity(state, arbitrageOpportunityInput(arb));
    if (result.errors) errors.push({ pairId: arb.pairId, code: 'opportunity_generation_failed', errors: result.errors });
    else created.push(result.opportunity);
  }

  return { opportunities: created, scan: opportunities, errors };
}

function signalToOpportunityInput(signal) {
  const isHighConviction = signal.conviction === 'high';
  const score = Math.min(0.95, Math.max(0.1, signal.score));
  const totalMoneyRisked = isHighConviction ? 2000 : 1000;
  const maxLoss = Math.round(totalMoneyRisked * (isHighConviction ? 0.15 : 0.25));
  const potentialUpside = Math.round(totalMoneyRisked * score * (isHighConviction ? 4 : 3));
  const grossExpectedValue = potentialUpside * 0.4 - maxLoss * 0.6;
  const direction = signal.direction === 'long' ? 'buy' : 'sell';

  return {
    sourceAgentId: `graph-alpha-${signal.source}`,
    marketType: 'crypto_spot',
    venue: 'coinbase-paper',
    symbol: signal.symbol,
    marketSlug: signal.symbol.toLowerCase().replace(/[^a-z0-9]+/g, '-'),
    title: `${signal.symbol} ${direction} — ${signal.strategyName}`,
    recommendation: isHighConviction ? 'review_yes' : 'paper_review',
    confidenceScore: score,
    winProbability: Math.min(0.75, Math.max(0.4, 0.5 + (score - 0.5))),
    lossProbability: Math.min(0.6, Math.max(0.25, 0.5 - (score - 0.5))),
    grossExpectedValue: Number(grossExpectedValue.toFixed(2)),
    totalMoneyRisked,
    maxLoss,
    potentialUpside,
    liquidityScore: Math.min(90, Math.round(50 + score * 40)),
    dataFreshnessScore: 88,
    backtestStatus: 'graph_signal_requires_backtest',
    estimatedFees: 5,
    estimatedSlippage: 8,
    estimatedGas: 0,
    agentResearchCost: 0,
    modelInferenceCost: 0,
    notes: `Graph-alpha-bot signal from ${signal.source}. Conviction: ${signal.conviction}. Score: ${(score * 100).toFixed(0)}%. Requires operator review and backtest.`,
    evidence: [{ type: 'graph_alpha_signal', source: signal.source, strategyName: signal.strategyName, conviction: signal.conviction, score: signal.score, direction: signal.direction, symbol: signal.symbol }]
  };
}

export async function fetchGraphSignals(options = {}) {
  const adapter = new GraphAlphaBotAdapter();
  const signals = await adapter.fetchSignals(options.limit || 20);
  return signals;
}

export async function generateOpportunitiesFromGraphSignals(state, options = {}) {
  ensureOpportunityState(state);
  const signals = await fetchGraphSignals(options);
  const errors = [];
  const created = [];

  for (const signal of signals) {
    if (state.opportunities.some(opp =>
      opp.symbol === signal.symbol &&
      opp.evidence?.some(e => e.type === 'graph_alpha_signal' && e.source === signal.source) &&
      ['needs_review', 'approved', 'research_requested', 'deferred'].includes(opp.status)
    )) continue;

    const opportunityInput = signalToOpportunityInput(signal);
    const opportunityResult = createOpportunity(state, opportunityInput);
    if (opportunityResult.errors) {
      errors.push({ symbol: signal.symbol, code: 'opportunity_generation_failed', errors: opportunityResult.errors });
    } else {
      created.push(opportunityResult.opportunity);
    }
  }

  return { signals, opportunities: created, errors };
}

export async function generateOpportunitiesFromStrategySignals(state, options = {}) {
  ensureOpportunityState(state);
  const scan = runStrategySignalScanner(options);
  const signals = Array.isArray(scan.signals) ? scan.signals : [];
  const errors = Array.isArray(scan.errors) ? [...scan.errors] : [];
  const created = [];
  const executions = [];

  for (const signal of signals) {
    const symbol = signal.symbol || signal.product_id;
    if (!symbol || !signal.strategy) continue;

    const duplicate = state.opportunities.some(opp =>
      opp.symbol === symbol &&
      opp.evidence?.some(e => e.type === 'strategy_live_30d_test' && e.strategy === signal.strategy) &&
      ['needs_review', 'approved', 'research_requested', 'deferred'].includes(opp.status)
    );
    if (duplicate) continue;

    const strategyId = state.strategies.some(strategy => strategy.id === signal.strategy) ? signal.strategy : null;

    const opportunityResult = createOpportunity(state, {
      ...strategySignalToOpportunityInput(signal),
      strategyId,
      status: 'needs_review',
      approvalStatus: 'needs_review',
    });

    if (opportunityResult.errors) {
      errors.push({ symbol, strategy: signal.strategy, code: 'opportunity_generation_failed', errors: opportunityResult.errors });
      continue;
    }

    const { opportunity } = opportunityResult;
    const approvalResult = decideOpportunity(state, opportunity.id, {
      status: 'approved',
      reviewer: 'system:auto-draft',
      reason: `Auto-approved from live 30d strategy scan: ${signal.strategy} win_rate=${(Number(signal.win_rate || 0) * 100).toFixed(1)}% sentiment=${Number(signal.sentiment_score || 0).toFixed(2)}`,
    });

    if (approvalResult.errors) {
      errors.push({ symbol, strategy: signal.strategy, code: 'approval_failed', errors: approvalResult.errors });
      continue;
    }

    created.push(approvalResult.opportunity || opportunity);
    if (approvalResult.execution) executions.push(approvalResult.execution);
  }

  return { scan, signals: created, executions, errors };
}
