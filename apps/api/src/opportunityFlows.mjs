import { nextId } from '../../../packages/storage/src/operatorStore.mjs';

const JOB_STATUSES = new Set(['queued', 'running', 'completed', 'failed', 'cancelled']);
const OPPORTUNITY_STATUSES = new Set(['needs_review', 'approved', 'rejected', 'deferred', 'research_requested', 'blocked']);
const LOCAL_OR_REMOTE = new Set(['local', 'remote']);

function finiteNumber(value, fallback = 0) {
  const n = Number(value ?? fallback);
  return Number.isFinite(n) ? n : NaN;
}

function nonNegative(value, fallback = 0) {
  const n = finiteNumber(value, fallback);
  return Number.isFinite(n) && n >= 0 ? n : NaN;
}

function clampPercent(value, fallback = 0.5) {
  const n = finiteNumber(value, fallback);
  return Number.isFinite(n) ? Math.min(1, Math.max(0, n)) : NaN;
}

function invalidNumber(name, value) {
  return !Number.isFinite(value) ? `${name}_invalid_number` : null;
}

export function localModelCost({ runtimeHours = 0, estimatedWatts = 0, electricityRatePerKwh = 0.14, hardwareDepreciationPerHour = 0 } = {}) {
  const cost = (nonNegative(runtimeHours) * nonNegative(estimatedWatts) / 1000 * nonNegative(electricityRatePerKwh, 0.14)) + (nonNegative(hardwareDepreciationPerHour) * nonNegative(runtimeHours));
  return Number((Number.isFinite(cost) ? cost : 0).toFixed(6));
}

export function netExpectedValue(input = {}) {
  const gross = finiteNumber(input.grossExpectedValue ?? input.expectedValue, 0);
  const costs = nonNegative(input.estimatedFees) + nonNegative(input.estimatedSlippage) + nonNegative(input.estimatedGas) + nonNegative(input.agentResearchCost) + nonNegative(input.modelInferenceCost);
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

function costForLedger(row) {
  return Number(row.remoteApiCost || 0) + Number(row.localComputeCost || 0);
}

function projectedResearchCost(body = {}) {
  const totalTokens = nonNegative(body.totalTokens || Number(body.promptTokens || 0) + Number(body.completionTokens || 0));
  const isLocal = body.localOrRemote === 'local';
  if (isLocal) {
    const runtimeHours = nonNegative(body.runtimeSeconds || 120) / 3600;
    return localModelCost({ runtimeHours, estimatedWatts: nonNegative(body.estimatedWatts || 250), electricityRatePerKwh: nonNegative(body.electricityRatePerKwh || 0.14), hardwareDepreciationPerHour: nonNegative(body.hardwareDepreciationPerHour || 0.35) });
  }
  return Number((Number(body.remoteApiCost || 0) || (totalTokens / 1000000) * nonNegative(body.costPerMillionTokens || 1.5)).toFixed(4));
}

function validateResearchJobInput(state, body = {}) {
  const errors = [];
  const localOrRemote = body.localOrRemote || 'remote';
  if (!LOCAL_OR_REMOTE.has(localOrRemote)) errors.push('local_or_remote_invalid');
  const status = body.status || 'completed';
  if (!JOB_STATUSES.has(status)) errors.push('research_job_status_invalid');
  const promptTokens = nonNegative(body.promptTokens || 0);
  const completionTokens = nonNegative(body.completionTokens || 0);
  const totalTokens = nonNegative(body.totalTokens || promptTokens + completionTokens);
  for (const [name, value] of Object.entries({ promptTokens, completionTokens, totalTokens })) {
    const error = invalidNumber(name, value);
    if (error) errors.push(error);
  }
  if (totalTokens < promptTokens + completionTokens) errors.push('total_tokens_below_prompt_completion_sum');
  const agentId = body.agentId || 'market-research-agent';
  const budget = state.agentBudgets.find(row => row.agentId === agentId);
  const projectedCost = projectedResearchCost({ ...body, localOrRemote, totalTokens });
  if (!Number.isFinite(projectedCost)) errors.push('projected_cost_invalid_number');
  if (budget) {
    if (!budget.enabled) errors.push('agent_budget_disabled');
    if (Number(budget.perJobTokenLimit || Infinity) < totalTokens && !body.approvedBudgetOverride) errors.push('per_job_token_limit_exceeded');
    const dailyCost = state.agentCostLedger.filter(row => row.agentId === agentId).reduce((sum, row) => sum + costForLedger(row), 0);
    if (dailyCost + projectedCost > Number(budget.dailyCostLimit || Infinity) && !body.approvedBudgetOverride) errors.push('daily_cost_limit_exceeded');
    const marketScope = body.marketScope || body.symbol || 'general';
    const marketCost = state.agentCostLedger
      .filter(row => row.agentId === agentId)
      .filter(row => state.researchJobs.find(job => job.id === row.jobId)?.marketScope === marketScope)
      .reduce((sum, row) => sum + costForLedger(row), 0);
    if (marketCost + projectedCost > Number(budget.perMarketCostLimit || Infinity) && !body.approvedBudgetOverride) errors.push('per_market_cost_limit_exceeded');
    if (projectedCost > Number(budget.requireApprovalAboveCost || Infinity) && !body.approvedBudgetOverride) errors.push('research_budget_approval_required');
  }
  return errors;
}

function validateOpportunityInput(state, body = {}) {
  const errors = [];
  if (!String(body.title || body.market || body.symbol || '').trim()) errors.push('opportunity_title_required');
  if (!String(body.venue || '').trim()) errors.push('venue_required');
  if (!String(body.marketType || '').trim()) errors.push('market_type_required');
  if (body.researchJobId && !state.researchJobs.some(job => job.id === body.researchJobId)) errors.push('research_job_not_found');
  if (body.strategyId && !state.strategies.some(strategy => strategy.id === body.strategyId)) errors.push('strategy_not_found');
  if (body.backtestId && !state.backtests.some(backtest => backtest.id === body.backtestId)) errors.push('backtest_not_found');
  const fields = {
    confidenceScore: clampPercent(body.confidenceScore, 0.5),
    winProbability: clampPercent(body.winProbability, 0.5),
    lossProbability: clampPercent(body.lossProbability ?? (1 - Number(body.winProbability ?? 0.5)), 0.5),
    totalMoneyRisked: nonNegative(body.totalMoneyRisked || 0),
    maxLoss: nonNegative(body.maxLoss || body.totalMoneyRisked || 0),
    potentialUpside: nonNegative(body.potentialUpside || 0),
    liquidityScore: nonNegative(body.liquidityScore ?? 50),
    dataFreshnessScore: nonNegative(body.dataFreshnessScore ?? 70),
    estimatedFees: nonNegative(body.estimatedFees || 0),
    estimatedSlippage: nonNegative(body.estimatedSlippage || 0),
    estimatedGas: nonNegative(body.estimatedGas || 0),
    agentResearchCost: nonNegative(body.agentResearchCost || 0),
    modelInferenceCost: nonNegative(body.modelInferenceCost || 0)
  };
  for (const [name, value] of Object.entries(fields)) {
    const error = invalidNumber(name, value);
    if (error) errors.push(error);
  }
  if (fields.maxLoss > fields.totalMoneyRisked && fields.totalMoneyRisked > 0) errors.push('max_loss_exceeds_total_money_risked');
  if (Math.abs((fields.winProbability + fields.lossProbability) - 1) > 0.05) errors.push('win_loss_probability_sum_invalid');
  if (!OPPORTUNITY_STATUSES.has(body.status || 'needs_review')) errors.push('opportunity_status_invalid');
  return errors;
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
  const errors = validateResearchJobInput(state, body);
  if (errors.length) return { errors };
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
  const errors = validateOpportunityInput(state, body);
  if (errors.length) return { errors };
  const job = body.researchJobId ? state.researchJobs.find(j => j.id === body.researchJobId) : null;
  const winProbability = clampPercent(body.winProbability, 0.5);
  const opportunity = {
    id: nextId('opp', state.opportunities),
    sourceAgentId: body.sourceAgentId || job?.agentId || 'market-research-agent',
    researchJobId: body.researchJobId || job?.id || null,
    strategyId: body.strategyId || null,
    marketType: body.marketType,
    venue: body.venue,
    symbol: body.symbol || null,
    marketSlug: body.marketSlug || null,
    title: body.title || body.market || body.symbol,
    recommendation: body.recommendation || 'review',
    confidenceScore: clampPercent(body.confidenceScore, 0.5),
    winProbability,
    lossProbability: clampPercent(body.lossProbability ?? (1 - winProbability), 1 - winProbability),
    expectedValue: finiteNumber(body.expectedValue ?? body.grossExpectedValue, 0),
    grossExpectedValue: finiteNumber(body.grossExpectedValue ?? body.expectedValue, 0),
    totalMoneyRisked: nonNegative(body.totalMoneyRisked || 0),
    maxLoss: nonNegative(body.maxLoss || body.totalMoneyRisked || 0),
    potentialUpside: nonNegative(body.potentialUpside || 0),
    rewardRiskRatio: Number(body.rewardRiskRatio || 0),
    liquidityScore: nonNegative(body.liquidityScore ?? 50),
    dataFreshnessScore: nonNegative(body.dataFreshnessScore ?? 70),
    backtestId: body.backtestId || null,
    backtestStatus: body.backtestStatus || (body.backtestId ? 'linked' : 'backtest_missing'),
    riskBreakdownId: null,
    status: body.status || 'needs_review',
    approvalStatus: body.approvalStatus || body.status || 'needs_review',
    estimatedFees: nonNegative(body.estimatedFees || 0),
    estimatedSlippage: nonNegative(body.estimatedSlippage || 0),
    estimatedGas: nonNegative(body.estimatedGas || 0),
    agentResearchCost: nonNegative(body.agentResearchCost || 0),
    modelInferenceCost: nonNegative(body.modelInferenceCost || 0),
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
