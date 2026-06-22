#!/usr/bin/env node
import process from 'node:process';
import { setTimeout as sleep } from 'node:timers/promises';
import { createOperatorStore } from '../packages/storage/src/operatorStore.mjs';
import { generateOpportunitiesFromArbitrage, generateOpportunitiesFromPredictionMarkets, generateOpportunitiesFromStrategySignals } from '../apps/api/src/opportunityGenerator.mjs';
import ExecutionEngine from '../packages/execution/src/executionEngine.mjs';

function parseArgs(argv) {
  const args = {
    stateFile: process.env.OPERATOR_STATE_PATH || 'data/operator-state.json',
    durationMinutes: Number(process.env.REALTIME_DURATION_MINUTES || 0),
    scanIntervalSeconds: Number(process.env.REALTIME_SCAN_INTERVAL_SECONDS || 60),
    executionIntervalSeconds: Number(process.env.REALTIME_EXECUTION_INTERVAL_SECONDS || 5),
    discover: true,
    refresh: false,
    products: '',
    granularity: 'ONE_HOUR',
    daysBack: 30,
    minWinRate: 0.55,
    minWeightedConfidence: 0.55,
    limit: 10,
    quoteCurrencies: 'USD,BTC',
    predictionMarkets: true,
    arbitrage: true,
    scannerPath: undefined,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = argv[i + 1];
    if (arg === '--state-file') args.stateFile = next, i += 1;
    else if (arg === '--duration-minutes') args.durationMinutes = Number(next), i += 1;
    else if (arg === '--scan-interval') args.scanIntervalSeconds = Number(next), i += 1;
    else if (arg === '--execution-interval') args.executionIntervalSeconds = Number(next), i += 1;
    else if (arg === '--products') args.products = next, i += 1;
    else if (arg === '--granularity') args.granularity = next, i += 1;
    else if (arg === '--days-back') args.daysBack = Number(next), i += 1;
    else if (arg === '--min-win-rate') args.minWinRate = Number(next), i += 1;
    else if (arg === '--min-weighted-confidence') args.minWeightedConfidence = Number(next), i += 1;
    else if (arg === '--limit') args.limit = Number(next), i += 1;
    else if (arg === '--quote-currencies') args.quoteCurrencies = next, i += 1;
    else if (arg === '--no-prediction-markets') args.predictionMarkets = false;
    else if (arg === '--no-arbitrage') args.arbitrage = false;
    else if (arg === '--scanner-path') args.scannerPath = next, i += 1;
    else if (arg === '--no-discover') args.discover = false;
    else if (arg === '--refresh') args.refresh = true;
  }

  return args;
}

function draftToRequest(draft) {
  const orders = Array.isArray(draft.orders) && draft.orders.length ? draft.orders : [{
    id: `ord-${draft.id}`,
    side: draft.side || 'buy',
    symbol: draft.symbol,
    quantity: draft.quantity || 0,
    price: draft.price || 0,
    orderType: 'market',
    timeInForce: 'GTC',
  }];

    return {
      strategyId: draft.strategyId || draft.tags?.source || draft.opportunityId || 'realtime-strategy',
      opportunityId: draft.opportunityId,
      accountId: draft.accountId || 'paper',
      mode: draft.mode || 'paper',
      confidenceScore: Number(draft.confidenceScore || draft.convictionWeight || 0.5),
      tradePlan: draft.tradePlan || null,
      tradeIntent: draft.tradeIntent || null,
      executionPurpose: draft.executionPurpose || null,
      positionSide: draft.positionSide || null,
      takeProfitPrice: draft.takeProfitPrice || null,
      stopLossPrice: draft.stopLossPrice || null,
      orders: orders.map(order => ({
        id: order.id || `ord-${draft.id}`,
        strategyId: draft.strategyId || draft.opportunityId || 'realtime-strategy',
        marketId: order.marketId || order.symbol || draft.symbol,
        symbol: order.symbol || draft.symbol,
        venue: order.venue || draft.venue || 'coinbase',
        side: (order.side || draft.side || 'buy').toLowerCase() === 'sell' ? 'sell' : 'buy',
        quantity: Number(order.quantity || draft.quantity || 0),
        price: Number(order.price || draft.price || 0),
        confidenceScore: Number(order.confidenceScore || draft.confidenceScore || draft.convictionWeight || 0.5),
        takeProfitPrice: order.takeProfitPrice || draft.takeProfitPrice || null,
        stopLossPrice: order.stopLossPrice || draft.stopLossPrice || null,
        tradePlan: order.tradePlan || draft.tradePlan || null,
        timeInForce: order.timeInForce || 'GTC',
        executionMode: order.executionMode || draft.mode || 'paper',
        orderType: order.orderType || 'market',
        notional: order.notional || draft.notional,
      feeBps: order.feeBps || 5,
      slippageBps: order.slippageBps || 10,
      createdAt: order.createdAt || new Date().toISOString(),
      updatedAt: order.updatedAt || new Date().toISOString(),
    })),
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const store = createOperatorStore({ filePath: args.stateFile });
  const engine = new ExecutionEngine({ minConfidence: args.minWeightedConfidence, requireApproval: false, requireRiskCheck: false });
  const started = Date.now();
  let scanCount = 0;
  let execCount = 0;
  let stop = false;
  let state = await store.load();

  const scanLoop = async () => {
    while (!stop) {
      const scanStarted = Date.now();
      const result = await generateOpportunitiesFromStrategySignals(state, {
        scannerPath: args.scannerPath,
        discover: args.discover,
        refresh: args.refresh,
        products: args.products,
        granularity: args.granularity,
        daysBack: args.daysBack,
        minWinRate: args.minWinRate,
        minWeightedConfidence: args.minWeightedConfidence,
        limit: args.limit,
        quoteCurrencies: args.quoteCurrencies,
      });
      const predictionResult = args.predictionMarkets
        ? await generateOpportunitiesFromPredictionMarkets(state, { limit: args.limit, agentId: 'market-research-agent' })
        : { opportunities: [], errors: [], snapshots: [] };
      const arbitrageResult = args.arbitrage
        ? await generateOpportunitiesFromArbitrage(state, { marketPageSize: 50, maxPages: 3, matchOptions: { maxMatches: 25, minScore: 0.02 } })
        : { opportunities: [], errors: [], scan: [] };
      scanCount += 1;
      await store.save(state);
      console.log(JSON.stringify({
        type: 'scan',
        scan: scanCount,
        strategyOpportunities: result.signals.length,
        predictionMarketOpportunities: predictionResult.opportunities.length,
        arbitrageOpportunities: arbitrageResult.opportunities.length,
        opportunities: result.signals.length + predictionResult.opportunities.length + arbitrageResult.opportunities.length,
        executions: result.executions.length,
        errors: result.errors.length + predictionResult.errors.length + arbitrageResult.errors.length,
        elapsedMs: Date.now() - scanStarted,
      }));
      await sleep(args.scanIntervalSeconds * 1000);
      if (args.durationMinutes > 0 && (Date.now() - started) >= args.durationMinutes * 60_000) break;
    }
  };

  const executionLoop = async () => {
    while (!stop) {
      const pending = state.executions.filter(ex => ex && (ex.status === 'draft' || ex.status === 'approved'));
      for (const draft of pending) {
        try {
          const request = draftToRequest(draft);
          const outcome = await engine.execute(request);
          execCount += 1;
          const exec = outcome.execution;
          draft.status = exec.status;
          draft.fills = exec.fills;
          draft.error = exec.error;
          draft.completedAt = exec.completedAt;
          draft.lastHeartbeatAt = exec.lastHeartbeatAt;
          draft.confidenceScore = exec.confidenceScore;
          draft.convictionWeight = exec.convictionWeight;
          draft.riskDecision = exec.riskDecision;
          draft.metadata = { ...(draft.metadata || {}), executionId: exec.id, outcomeOk: outcome.ok };
        } catch (error) {
          draft.status = 'failed';
          draft.error = error instanceof Error ? error.message : String(error);
        }
      }
      await store.save(state);
      console.log(JSON.stringify({ type: 'execution', processed: pending.length, totalExecutions: execCount }));
      await sleep(args.executionIntervalSeconds * 1000);
      if (args.durationMinutes > 0 && (Date.now() - started) >= args.durationMinutes * 60_000) break;
    }
  };

  process.on('SIGINT', () => { stop = true; });
  process.on('SIGTERM', () => { stop = true; });

  const timer = args.durationMinutes > 0 ? setTimeout(() => { stop = true; }, args.durationMinutes * 60_000) : null;
  await Promise.all([scanLoop(), executionLoop()]);
  if (timer) clearTimeout(timer);
  await store.save(state);
  console.log(JSON.stringify({
    type: 'summary',
    elapsedSeconds: round((Date.now() - started) / 1000),
    scanCount,
    executionCount: execCount,
    opportunities: state.opportunities.length,
    executions: state.executions.length,
    cpuCount: process.env.CI ? undefined : (process.env.NUMBER_OF_PROCESSORS || 0),
  }));
}

function round(value) {
  return Math.round(value * 1000) / 1000;
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
