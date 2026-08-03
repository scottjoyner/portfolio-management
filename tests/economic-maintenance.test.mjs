import test from 'node:test';
import assert from 'node:assert/strict';

import { runEconomicMaintenance } from '../apps/api/src/economicMaintenance.mjs';
import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';

const NOW = '2026-07-29T22:00:00.000Z';

function catalog() {
  return {
    data: [{
      id: 'example/value-model',
      pricing: {
        prompt: '0.000001',
        completion: '0.000002',
        request: '0',
      },
    }],
  };
}

test('maintenance refreshes pricing, retains quote history, forecasts, matures outcomes and attributes settled agent trades', async () => {
  const state = createInitialOperatorState(NOW);
  state.config.economicForecastSymbols = ['BTC-USD'];
  state.config.economicForecastHorizonMinutes = 1;
  state.config.economicForecastTtlSeconds = 30;
  state.config.maximumForecastDataAgeSeconds = 180;
  state.marketDataSnapshots = [100, 100.5, 101, 101.5].map((mid, index) => ({
    id: `md-${index + 1}`,
    symbol: 'BTC-USD',
    venue: 'coinbase',
    mid,
    bid: mid - 0.05,
    ask: mid + 0.05,
    spreadBps: 10,
    timestamp: new Date(new Date(NOW).getTime() - (4 - index) * 30_000).toISOString(),
    source: 'test-history',
  }));
  state.modelUsageLedger.push({
    id: 'quote-1',
    status: 'reconciled',
    actualCostUsd: 0.5,
    localOrRemote: 'remote',
  });
  state.economicDecisions.push({ id: 'decision-1', modelQuoteId: 'quote-1' });
  state.opportunities.push({
    id: 'opp-1',
    recommendation: 'buy',
    economicDecisionId: 'decision-1',
    botCounterfactualPnlUsd: 1,
  });
  state.executions.push({
    id: 'exec-1',
    opportunityId: 'opp-1',
    status: 'filled',
    side: 'buy',
    fills: [{ id: 'fill-1', settlementStatus: 'settled', realizedPnlUsd: 6 }],
  });

  const first = await runEconomicMaintenance(state, {
    now: NOW,
    catalog: catalog(),
    quotes: {
      'BTC-USD': { bid: 101.95, ask: 102.05, mid: 102, spreadBps: 9.8 },
    },
  });

  assert.equal(first.pricingRefreshed, true);
  assert.equal(first.marketSnapshotsAdded, 1);
  assert.equal(first.forecastsCreated, 1);
  assert.equal(first.attributionsRecorded, 1);
  assert.equal(state.modelPricingSnapshots.length, 1);
  assert.equal(state.priceForecasts.length, 1);
  assert.equal(state.agentAttributionRecords[0].incrementalValueUsd, 5);
  assert.equal(state.agentAttributionRecords[0].agentCostUsd, 0.5);
  assert.equal(state.economicMaintenance.status, 'ok');

  const secondAt = '2026-07-29T22:01:05.000Z';
  const second = await runEconomicMaintenance(state, {
    now: secondAt,
    catalog: catalog(),
    quotes: {
      'BTC-USD': { bid: 102.95, ask: 103.05, mid: 103, spreadBps: 9.7 },
    },
  });

  assert.equal(second.pricingRefreshed, false);
  assert.equal(second.marketSnapshotsAdded, 1);
  assert.equal(second.forecastOutcomesCreated, 1);
  assert.equal(state.forecastOutcomes.length, 1);
  assert.equal(state.forecastOutcomes[0].forecastId, state.priceForecasts[0].id);
  assert.equal(state.agentAttributionRecords.length, 1);
});

test('maintenance reports insufficient history instead of inventing a forecast', async () => {
  const state = createInitialOperatorState(NOW);
  state.config.economicForecastSymbols = ['ETH-USD'];
  const report = await runEconomicMaintenance(state, {
    now: NOW,
    catalog: catalog(),
    quotes: {
      'ETH-USD': { bid: 1999, ask: 2001, mid: 2000, spreadBps: 10 },
    },
  });
  assert.equal(report.forecastsCreated, 0);
  assert.equal(report.details.forecastSkips[0].blocker, 'forecast_requires_five_prices');
  assert.equal(state.priceForecasts.length, 0);
});
