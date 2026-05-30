import { nextId } from '../../../packages/storage/src/operatorStore.mjs';

export function localModelCost({ runtimeHours = 0, estimatedWatts = 0, electricityRatePerKwh = 0.14, hardwareDepreciationPerHour = 0 } = {}) {
  return Number(((runtimeHours * estimatedWatts / 1000 * electricityRatePerKwh) + (hardwareDepreciationPerHour * runtimeHours)).toFixed(6));
}

export function netExpectedValue(input = {}) {
  const gross = Number(input.grossExpectedValue ?? input.expectedValue ?? 0);
  const costs = Number(input.estimatedFees || 0) + Number(input.estimatedSlippage || 0) + Number(input.estimatedGas || 0) + Number(input.agentResearchCost || 0) + Number(input.modelInferenceCost || 0);
  return Number((gross - costs).toFixed(2));
}

export function ensureOpportunityState(state) {
  state.opportunities ||= [];
  state.riskBreakdowns ||= [];
  state.researchJobs ||= [];
  state.agentBudgets ||= [];
  state.agentCostLedger ||= [];
  state.marketDataSnapshots ||= [];
  if (!state.agentBudgets.length) {
    state.agentBudgets.push(
      { agentId: 'market-research-agent', dailyTokenLimit: 200000, dailyCostLimit: 35, perJobTokenLimit: 50000, perMarketCostLimit: 12, requireApprovalAboveCost: 10, enabled: true },
      { agentId: 'liquidity-scanner', dailyTokenLimit: 150000, dailyCostLimit: 20, perJobTokenLimit: 40000, perMarketCostLimit: 8, requireApprovalAboveCost: 8, enabled: true }
    );
  }
  if (!state.marketDataSnapshots.length) {
    const now = new Date().toISOString();
    state.marketDataSnapshots.push(
      { id: 'md-btc-usd', symbol: 'BTC-USD', venue: 'coinbase-paper', assetClass: 'crypto', bid: 68250, ask: 68268, spreadBps: 2.64, volume24h: 18420000000, liquidityScore: 82, volatilityScore: 61, status: 'watching', timestamp: now, source: 'demo-market-feed' },
      { id: 'md-eth-usd', symbol: 'ETH-USD', venue: 'coinbase-paper', assetClass: 'crypto', bid: 3712, ask: 3715, spreadBps: 8.08, volume24h: 9120000000, liquidityScore: 79, volatilityScore: 58, status: 'eligible', timestamp: now, source: 'demo-market-feed' },
      { id: 'md-prediction-demo', symbol: 'PREDICTION:DEMO', venue: 'polymarket-watch', assetClass: 'prediction_market', bid: 0.42, ask: 0.45, spreadBps: 697, volume24h: 241000, liquidityScore: 71, volatilityScore: 52, status: 'research_candidate', timestamp: now, source: 'demo-market-feed' }
    );
  }
  return state;
}

export function buildRiskBreakdown(opportunity, now = new Date().toISOString()) {
  const capitalAtRiskScore = Math.min(100, Math.round(Number(opportunity.totalMoneyRisked || 0) / 25));
  const liquidityScore = Number(opportunity.liquidityScore ?? 50);
  const slippageScore = Math.min(100, Math.round(Number(opportunity.estimatedSlippage || 0) * 5));
  const confidenceScore = Math.round(Number(opportunity.confidenceScore || 0.5) * 100);
  const agentCostScore = Math.min(100, Math.round((Number(opportunity.agentResearchCost || 0) + Number(opportunity.modelInferenceCost || 0)) * 3));
  const dataFreshnessScore = Number(opportunity.dataFreshnessScore ?? 70);
  const aggregateScore = Math.round((capitalAtRiskScore + (100 - liquidityScore) + slippageScore + (100 - confidenceScore) + agentCostScore + (100 - dataFreshnessScore)) / 6);
  return {
    id: null,
    scope: 'opportunity',
    scopeId: opportunity.id,
    aggregateScore,
    capitalAtRiskScore,
    liquidityScore,
    slippageScore,
    drawdownScore: Math.min(100, Math.round(Number(opportunity.maxLoss || 0) / 10)),
    volatilityScore: Number(opportunity.volatilityScore ?? 50),
    correlationScore: Number(opportunity.correlationScore ?? 35),
    modelConfidenceScore: confidenceScore,
    dataFreshnessScore,
    agentCostScore,
    explanation: 'Risk score combines capital at risk, liquidity, slippage, confidence, data freshness, and research cost drag.',
    generatedAt: now
  };
}

export function createResearchJob(state, body = {}, now = new Date().toISOString()) {
  ensureOpportunityState(state);
  const totalTokens = Number(body.totalTokens || Number(body.promptTokens || 0) + Number(body.completionTokens || 0));
  const runtimeHours = Number(body.runtimeSeconds || 120) / 3600;
  const isLocal = body.localOrRemote === 'local';
  const estimatedLocalCost = isLocal ? localModelCost({ runtimeHours, estimatedWatts: Number(body.estimatedWatts || 250), electricityRatePerKwh: Number(body.electricityRatePerKwh || 0.14), hardwareDepreciationPerHour: Number(body.hardwareDepreciationPerHour || 0.35) }) : 0;
  const estimatedRemoteCost = isLocal ? 0 : Number((Number(body.remoteApiCost || 0) || (totalTokens / 1000000) * Number(body.costPerMillionTokens || 1.5)).toFixed(4));
  const job = {
    id: nextId('job', state.researchJobs),
    agentId: body.agentId || 'market-research-agent',
    triggerType: body.triggerType || 'operator_request',
    marketScope: body.marketScope || body.symbol || 'general',
    symbolScope: body.symbolScope || body.symbol || null,
    provider: body.provider || (isLocal ? 'local' : 'remote'),
    model: body.model || 'research-model',
    localOrRemote: isLocal ? 'local' : 'remote',
    status: body.status || 'completed',
    startedAt: now,
    completedAt: body.status === 'queued' ? null : now,
    promptTokens: Number(body.promptTokens || 0),
    completionTokens: Number(body.completionTokens || 0),
    totalTokens,
    estimatedRemoteCost,
    estimatedLocalCost,
    opportunityIdsCreated: [],
    failureReason: null
  };
  const ledger = { id: nextId('cost', state.agentCostLedger), agentId: job.agentId, jobId: job.id, model: job.model, provider: job.provider, localOrRemote: job.localOrRemote, promptTokens: job.promptTokens, completionTokens: job.completionTokens, totalTokens, remoteApiCost: estimatedRemoteCost, localComputeCost: estimatedLocalCost, allocatedOpportunityId: null, createdAt: now };
  state.researchJobs.push(job);
  state.agentCostLedger.push(ledger);
  state.audit.push({ id: nextId('audit', state.audit), action: 'research_job_created', actor: job.agentId, at: now, details: job.id, payload: { totalTokens, costLedgerId: ledger.id } });
  return { job, ledger };
}

export function createOpportunity(state, body = {}, now = new Date().toISOString()) {
  ensureOpportunityState(state);
  const job = body.researchJobId ? state.researchJobs.find(j => j.id === body.researchJobId) : null;
  const opportunity = {
    id: nextId('opp', state.opportunities),
    sourceAgentId: body.sourceAgentId || job?.agentId || 'market-research-agent',
    researchJobId: body.researchJobId || job?.id || null,
    strategyId: body.strategyId || null,
    marketType: body.marketType || 'unknown',
    venue: body.venue || 'unknown',
    symbol: body.symbol || null,
    marketSlug: body.marketSlug || null,
    title: body.title || body.market || body.symbol || 'Untitled opportunity',
    recommendation: body.recommendation || 'review',
    confidenceScore: Number(body.confidenceScore ?? 0.5),
    winProbability: Number(body.winProbability ?? 0.5),
    lossProbability: Number(body.lossProbability ?? (1 - Number(body.winProbability ?? 0.5))),
    expectedValue: Number(body.expectedValue ?? body.grossExpectedValue ?? 0),
    grossExpectedValue: Number(body.grossExpectedValue ?? body.expectedValue ?? 0),
    totalMoneyRisked: Number(body.totalMoneyRisked || 0),
    maxLoss: Number(body.maxLoss || body.totalMoneyRisked || 0),
    potentialUpside: Number(body.potentialUpside || 0),
    rewardRiskRatio: Number(body.rewardRiskRatio || 0),
    liquidityScore: Number(body.liquidityScore ?? 50),
    dataFreshnessScore: Number(body.dataFreshnessScore ?? 70),
    backtestId: body.backtestId || null,
    backtestStatus: body.backtestStatus || (body.backtestId ? 'linked' : 'backtest_missing'),
    riskBreakdownId: null,
    status: body.status || 'needs_review',
    approvalStatus: body.approvalStatus || 'needs_review',
    estimatedFees: Number(body.estimatedFees || 0),
    estimatedSlippage: Number(body.estimatedSlippage || 0),
    estimatedGas: Number(body.estimatedGas || 0),
    agentResearchCost: Number(body.agentResearchCost || 0),
    modelInferenceCost: Number(body.modelInferenceCost || 0),
    netExpectedValue: 0,
    notes: body.notes || '',
    evidence: Array.isArray(body.evidence) ? body.evidence : [],
    expiresAt: body.expiresAt || null,
    createdAt: now,
    updatedAt: now
  };
  opportunity.netExpectedValue = netExpectedValue(opportunity);
  const riskBreakdown = { ...buildRiskBreakdown(opportunity, now), id: nextId('risk', state.riskBreakdowns), scopeId: opportunity.id };
  opportunity.riskBreakdownId = riskBreakdown.id;
  state.riskBreakdowns.push(riskBreakdown);
  state.opportunities.push(opportunity);
  if (job) job.opportunityIdsCreated = [...new Set([...(job.opportunityIdsCreated || []), opportunity.id])];
  for (const row of state.agentCostLedger) if (job && row.jobId === job.id && !row.allocatedOpportunityId) row.allocatedOpportunityId = opportunity.id;
  state.audit.push({ id: nextId('audit', state.audit), action: 'opportunity_created', actor: opportunity.sourceAgentId, at: now, details: opportunity.id, payload: { venue: opportunity.venue, symbol: opportunity.symbol, netExpectedValue: opportunity.netExpectedValue } });
  return { opportunity, riskBreakdown };
}

export function decideOpportunity(state, opportunityId, body = {}, now = new Date().toISOString()) {
  ensureOpportunityState(state);
  const opportunity = state.opportunities.find(o => o.id === opportunityId);
  if (!opportunity) return { errors: ['opportunity_not_found'] };
  if (!['approved', 'rejected', 'deferred'].includes(body.status)) return { errors: ['invalid_opportunity_decision'] };
  opportunity.status = body.status;
  opportunity.approvalStatus = body.status;
  opportunity.decisionReason = body.reason || null;
  opportunity.reviewer = body.reviewer || 'operator';
  opportunity.reviewedAt = now;
  opportunity.updatedAt = now;
  state.audit.push({ id: nextId('audit', state.audit), action: `opportunity_${body.status}`, actor: opportunity.reviewer, at: now, details: opportunity.id, payload: { reason: opportunity.decisionReason } });
  return { opportunity };
}

export function summarizeAgentCosts(state) {
  ensureOpportunityState(state);
  const total = state.agentCostLedger.reduce((sum, row) => sum + Number(row.remoteApiCost || 0) + Number(row.localComputeCost || 0), 0);
  return { dailyBudgetUsd: state.agentBudgets.reduce((sum, row) => sum + Number(row.dailyCostLimit || 0), 0), spentTodayUsd: Number(total.toFixed(2)), remoteModelCostUsd: Number(state.agentCostLedger.reduce((sum, row) => sum + Number(row.remoteApiCost || 0), 0).toFixed(2)), localModelCostUsd: Number(state.agentCostLedger.reduce((sum, row) => sum + Number(row.localComputeCost || 0), 0).toFixed(2)), openResearchJobs: state.researchJobs.filter(job => ['queued', 'running'].includes(job.status)).length, costPerOpportunityUsd: state.opportunities.length ? Number((total / state.opportunities.length).toFixed(2)) : 0, localCostFormula: 'runtime_hours * estimated_watts / 1000 * electricity_rate_per_kwh + hardware_depreciation_per_hour * runtime_hours' };
}
