import { createOpportunity, createResearchJob, ensureOpportunityState } from './opportunityFlows.mjs';
import { nextId } from '../../../packages/storage/src/operatorStore.mjs';
import { collectMarketSnapshots, PaperCryptoMarketAdapter, PolymarketWatchAdapter } from '../../../packages/connectors/src/marketDataAdapters.mjs';

const DEFAULT_STALE_AFTER_MS = 15 * 60 * 1000;

function upsertSnapshot(state, snapshot) {
  state.marketDataSnapshots ||= [];
  const idx = state.marketDataSnapshots.findIndex(row => row.id === snapshot.id || (row.symbol === snapshot.symbol && row.venue === snapshot.venue));
  if (idx >= 0) state.marketDataSnapshots[idx] = { ...state.marketDataSnapshots[idx], ...snapshot };
  else state.marketDataSnapshots.push(snapshot);
}

function adapterName(adapter) {
  return adapter?.name || adapter?.constructor?.name || 'unknown';
}

function ensureConnectorState(state) {
  ensureOpportunityState(state);
  state.connectorRuns ||= [];
  return state;
}

function recordConnectorRun(state, { kind, adapters, snapshots, errors, startedAt, completedAt }) {
  ensureConnectorState(state);
  const run = {
    id: nextId('connector-run', state.connectorRuns),
    kind,
    adapters: adapters.map(adapterName),
    status: errors.length ? (snapshots.length ? 'partial_success' : 'failed') : 'completed',
    snapshotCount: snapshots.length,
    errorCount: errors.length,
    errors,
    startedAt,
    completedAt
  };
  state.connectorRuns.push(run);
  state.audit.push({ id: nextId('audit', state.audit), action: `connector_${kind}`, actor: 'connector-system', at: completedAt, details: run.id, payload: { status: run.status, snapshotCount: run.snapshotCount, errorCount: run.errorCount } });
  return run;
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

export function connectorHealth(state, options = {}) {
  ensureConnectorState(state);
  const nowMs = options.now ? new Date(options.now).getTime() : Date.now();
  const staleAfterMs = Number(options.staleAfterMs || DEFAULT_STALE_AFTER_MS);
  const latestByVenue = new Map();
  for (const snapshot of state.marketDataSnapshots || []) {
    const current = latestByVenue.get(snapshot.venue);
    if (!current || new Date(snapshot.timestamp).getTime() > new Date(current.timestamp).getTime()) latestByVenue.set(snapshot.venue, snapshot);
  }
  const connectors = [...latestByVenue.values()].map(snapshot => {
    const ageMs = nowMs - new Date(snapshot.timestamp).getTime();
    return {
      venue: snapshot.venue,
      source: snapshot.source,
      latestSnapshotAt: snapshot.timestamp,
      ageMs,
      stale: ageMs > staleAfterMs,
      snapshotCount: state.marketDataSnapshots.filter(row => row.venue === snapshot.venue).length
    };
  });
  return {
    staleAfterMs,
    connectors,
    runs: state.connectorRuns.slice(-20).reverse(),
    lastRun: state.connectorRuns[state.connectorRuns.length - 1] || null
  };
}

export async function ingestConnectorSnapshots(state, options = {}) {
  ensureConnectorState(state);
  const startedAt = new Date().toISOString();
  const adapters = options.adapters || [new PaperCryptoMarketAdapter(), new PolymarketWatchAdapter()];
  const collected = await collectMarketSnapshots(adapters);
  for (const snapshot of collected.snapshots) upsertSnapshot(state, snapshot);
  const completedAt = new Date().toISOString();
  const run = recordConnectorRun(state, { kind: 'market_data_ingest', adapters, snapshots: collected.snapshots, errors: collected.errors, startedAt, completedAt });
  return { snapshots: collected.snapshots, errors: collected.errors, run, health: connectorHealth(state) };
}

export async function generateOpportunitiesFromConnectors(state, options = {}) {
  ensureConnectorState(state);
  const startedAt = new Date().toISOString();
  const adapters = options.adapters || [new PaperCryptoMarketAdapter(), new PolymarketWatchAdapter()];
  const { snapshots, errors } = await ingestConnectorSnapshots(state, { ...options, adapters });
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
  const completedAt = new Date().toISOString();
  const run = recordConnectorRun(state, { kind: 'opportunity_generation', adapters, snapshots, errors, startedAt, completedAt });
  return { snapshots, opportunities: created, errors, run, health: connectorHealth(state) };
}
