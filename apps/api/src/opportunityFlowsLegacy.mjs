import { nextId } from '../../../packages/storage/src/operatorStore.mjs';

const JOB_STATUSES = new Set(['queued', 'running', 'completed', 'failed', 'cancelled']);
const OPPORTUNITY_STATUSES = new Set(['needs_review', 'approved', 'rejected', 'deferred', 'research_requested', 'blocked']);
const LOCAL_OR_REMOTE = new Set(['local', 'remote']);

function finiteNumber(value, fallback = 0) {
  const number = Number(value ?? fallback);
  return Number.isFinite(number) ? number : NaN;
}

function nonNegative(value, fallback = 0) {
  const number = finiteNumber(value, fallback);
  return Number.isFinite(number) && number >= 0 ? number : NaN;
}

function clampPercent(value, fallback = 0.5) {
  const number = finiteNumber(value, fallback);
  return Number.isFinite(number) ? Math.min(1, Math.max(0, number)) : NaN;
}

function invalidNumber(name, value) {
  return !Number.isFinite(value) ? `${name}_invalid_number` : null;
}

function researchLocality(body = {}) {
  return body.localOrRemote || 'local';
}

function researchStatus(body = {}) {
  return body.status || 'queued';
}

export function localModelCost({ runtimeHours = 0, estimatedWatts = 0, electricityRatePerKwh = 0.14, hardwareDepreciationPerHour = 0 } = {}) {
  const cost = (nonNegative(runtimeHours) * nonNegative(estimatedWatts) / 1000 * nonNegative(electricityRatePerKwh, 0.14))
    + (nonNegative(hardwareDepreciationPerHour) * nonNegative(runtimeHours));
  return Number((Number.isFinite(cost) ? cost : 0).toFixed(6));
}

export function netExpectedValue(input = {}) {
  const gross = finiteNumber(input.grossExpectedValue ?? input.expectedValue, 0);
  const costs = nonNegative(input.estimatedFees)
    + nonNegative(input.estimatedSlippage)
    + nonNegative(input.estimatedGas)
    + nonNegative(input.agentResearchCost)
    + nonNegative(input.modelInferenceCost);
  return Number((gross - costs).toFixed(2));
}

export function ensureOpportunityState(state) {
  state.opportunities ||= [];
  state.riskBreakdowns ||= [];
  state.researchJobs ||= [];
  state.agentBudgets ||= [];
  state.budgetApprovals ||= [];
  state.agentCostLedger ||= [];
  state.marketDataSnapshots ||= [];
  state.audit ||= [];
  state.executions ||= [];

  if (!state.agentBudgets.length) {
    state.agentBudgets.push(
      { agentId: 'market-research-agent', dailyTokenLimit: 200000, dailyCostLimit: 35, perJobTokenLimit: 50000, perMarketCostLimit: 12, requireApprovalAboveCost: 10, enabled: true },
      { agentId: 'liquidity-scanner', dailyTokenLimit: 150000, dailyCostLimit: 20, perJobTokenLimit: 40000, perMarketCostLimit: 8, requireApprovalAboveCost: 8, enabled: true },
    );
  }
  return state;
}

function costForLedger(row) {
  return Number(row.remoteApiCost || 0) + Number(row.localComputeCost || 0);
}

function projectedResearchCost(body = {}) {
  const totalTokens = nonNegative(body.totalTokens || Number(body.promptTokens || 0) + Number(body.completionTokens || 0));
  const isLocal = researchLocality(body) === 'local';
  if (isLocal) {
    const runtimeHours = nonNegative(body.runtimeSeconds || 120) / 3600;
    return localModelCost({
      runtimeHours,
      estimatedWatts: nonNegative(body.estimatedWatts || 250),
      electricityRatePerKwh: nonNegative(body.electricityRatePerKwh || 0.14),
      hardwareDepreciationPerHour: nonNegative(body.hardwareDepreciationPerHour || 0.35),
    });
  }
  return Number((Number(body.remoteApiCost || 0) || (totalTokens / 1_000_000) * nonNegative(body.costPerMillionTokens || 1.5)).toFixed(4));
}

function isBudgetApprovalUsable(approval, { agentId, marketScope, projectedCost, totalTokens }, now = new Date()) {
  if (!approval || approval.status !== 'approved') return false;
  if (approval.expiresAt && new Date(approval.expiresAt).getTime() < now.getTime()) return false;
  if (approval.agentId !== agentId) return false;
  if (approval.marketScope && marketScope && approval.marketScope !== marketScope) return false;
  if (Number(approval.approvedCostLimit || 0) < projectedCost) return false;
  if (Number(approval.approvedTokenLimit || 0) < totalTokens) return false;
  return true;
}

function hasApprovedBudgetOverride(state, body, context) {
  if (body.systemBudgetOverride === true || body.approvedBudgetOverride === true) return true;
  if (!body.budgetApprovalId) return false;
  const approval = state.budgetApprovals.find(row => row.id === body.budgetApprovalId);
  return isBudgetApprovalUsable(approval, context);
}

function validateResearchJobInput(state, body = {}) {
  const errors = [];
  const localOrRemote = researchLocality(body);
  if (!LOCAL_OR_REMOTE.has(localOrRemote)) errors.push('local_or_remote_invalid');
  const status = researchStatus(body);
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
  const marketScope = body.marketScope || body.symbol || 'general';
  const approvedOverride = hasApprovedBudgetOverride(state, body, { agentId, marketScope, projectedCost, totalTokens });
  if (!Number.isFinite(projectedCost)) errors.push('projected_cost_invalid_number');
  if (body.budgetApprovalId && !approvedOverride) errors.push('budget_approval_not_usable');

  if (budget) {
    if (!budget.enabled) errors.push('agent_budget_disabled');
    if (Number(budget.perJobTokenLimit || Infinity) < totalTokens && !approvedOverride) errors.push('per_job_token_limit_exceeded');
    const dailyCost = state.agentCostLedger
      .filter(row => row.agentId === agentId)
      .reduce((sum, row) => sum + costForLedger(row), 0);
    if (dailyCost + projectedCost > Number(budget.dailyCostLimit || Infinity) && !approvedOverride) errors.push('daily_cost_limit_exceeded');
    const marketCost = state.agentCostLedger
      .filter(row => row.agentId === agentId)
      .filter(row => state.researchJobs.find(job => job.id === row.jobId)?.marketScope === marketScope)
      .reduce((sum, row) => sum + costForLedger(row), 0);
    if (marketCost + projectedCost > Number(budget.perMarketCostLimit || Infinity) && !approvedOverride) errors.push('per_market_cost_limit_exceeded');
    if (projectedCost > Number(budget.requireApprovalAboveCost || Infinity) && !approvedOverride) errors.push('research_budget_approval_required');
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
    modelInferenceCost: nonNegative(body.modelInferenceCost || 0),
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
    generatedAt: now,
  };
}

export function requestBudgetApproval(state, body = {}, now = new Date().toISOString()) {
  ensureOpportunityState(state);
  const agentId = body.agentId || 'market-research-agent';
  const projectedCost = nonNegative(body.projectedCost || body.approvedCostLimit || 0);
  const projectedTokens = nonNegative(body.projectedTokens || body.approvedTokenLimit || 0);
  const errors = [];
  if (!agentId) errors.push('agent_id_required');
  if (!Number.isFinite(projectedCost) || projectedCost <= 0) errors.push('projected_cost_required');
  if (!Number.isFinite(projectedTokens) || projectedTokens <= 0) errors.push('projected_tokens_required');
  if (errors.length) return { errors };

  const approval = {
    id: nextId('budget-approval', state.budgetApprovals),
    agentId,
    marketScope: body.marketScope || null,
    opportunityId: body.opportunityId || null,
    requestedBy: body.requestedBy || 'operator',
    reason: body.reason || 'additional research budget requested',
    status: 'pending_review',
    projectedCost,
    projectedTokens,
    approvedCostLimit: 0,
    approvedTokenLimit: 0,
    reviewer: null,
    decisionReason: null,
    requestedAt: now,
    reviewedAt: null,
    expiresAt: body.expiresAt || null,
  };
  state.budgetApprovals.push(approval);
  state.audit.push({
    id: nextId('audit', state.audit),
    action: 'budget_approval_requested',
    actor: approval.requestedBy,
    at: now,
    details: approval.id,
    payload: { agentId, projectedCost, projectedTokens },
  });
  return { budgetApproval: approval };
}

export function decideBudgetApproval(state, approvalId, body = {}, now = new Date().toISOString()) {
  ensureOpportunityState(state);
  const approval = state.budgetApprovals.find(row => row.id === approvalId);
  if (!approval) return { errors: ['budget_approval_not_found'] };
  if (!['approved', 'rejected'].includes(body.status)) return { errors: ['invalid_budget_approval_decision'] };

  approval.status = body.status;
  approval.reviewer = body.reviewer || 'operator';
  approval.decisionReason = body.reason || null;
  approval.reviewedAt = now;
  approval.approvedCostLimit = body.status === 'approved' ? nonNegative(body.approvedCostLimit || approval.projectedCost) : 0;
  approval.approvedTokenLimit = body.status === 'approved' ? nonNegative(body.approvedTokenLimit || approval.projectedTokens) : 0;
  approval.expiresAt = body.expiresAt || approval.expiresAt;
  if (body.status === 'approved' && (!Number.isFinite(approval.approvedCostLimit) || !Number.isFinite(approval.approvedTokenLimit))) {
    return { errors: ['invalid_budget_approval_limits'] };
  }
  state.audit.push({
    id: nextId('audit', state.audit),
    action: `budget_approval_${body.status}`,
    actor: approval.reviewer,
    at: now,
    details: approval.id,
    payload: { approvedCostLimit: approval.approvedCostLimit, approvedTokenLimit: approval.approvedTokenLimit },
  });
  return { budgetApproval: approval };
}

export function createResearchJob(state, body = {}, now = new Date().toISOString()) {
  ensureOpportunityState(state);
  const localOrRemote = researchLocality(body);
  const status = researchStatus(body);
  const normalizedBody = { ...body, localOrRemote, status };
  const errors = validateResearchJobInput(state, normalizedBody);
  if (errors.length) return { errors };

  const totalTokens = Number(normalizedBody.totalTokens || Number(normalizedBody.promptTokens || 0) + Number(normalizedBody.completionTokens || 0));
  const runtimeHours = Number(normalizedBody.runtimeSeconds || 120) / 3600;
  const isLocal = localOrRemote === 'local';
  const estimatedLocalCost = isLocal
    ? localModelCost({
        runtimeHours,
        estimatedWatts: Number(normalizedBody.estimatedWatts || 250),
        electricityRatePerKwh: Number(normalizedBody.electricityRatePerKwh || 0.14),
        hardwareDepreciationPerHour: Number(normalizedBody.hardwareDepreciationPerHour || 0.35),
      })
    : 0;
  const estimatedRemoteCost = isLocal
    ? 0
    : Number((Number(normalizedBody.remoteApiCost || 0) || (totalTokens / 1_000_000) * Number(normalizedBody.costPerMillionTokens || 1.5)).toFixed(4));
  const terminal = ['completed', 'failed', 'cancelled'].includes(status);
  const started = status === 'running' || terminal;

  const job = {
    id: nextId('job', state.researchJobs),
    agentId: normalizedBody.agentId || 'market-research-agent',
    triggerType: normalizedBody.triggerType || 'operator_request',
    marketScope: normalizedBody.marketScope || normalizedBody.symbol || 'general',
    symbolScope: normalizedBody.symbolScope || normalizedBody.symbol || null,
    provider: normalizedBody.provider || (isLocal ? 'local' : 'remote'),
    model: normalizedBody.model || 'research-model',
    localOrRemote,
    status,
    requestedAt: now,
    queuedAt: status === 'queued' ? now : null,
    startedAt: started ? now : null,
    completedAt: terminal ? now : null,
    promptTokens: Number(normalizedBody.promptTokens || 0),
    completionTokens: Number(normalizedBody.completionTokens || 0),
    totalTokens,
    estimatedRemoteCost,
    estimatedLocalCost,
    budgetApprovalId: normalizedBody.budgetApprovalId || null,
    opportunityIdsCreated: [],
    failureReason: normalizedBody.failureReason || null,
    modelQuoteId: normalizedBody.modelQuoteId || null,
    economicDecisionId: normalizedBody.economicDecisionId || null,
    pricingSnapshotId: normalizedBody.pricingSnapshotId || null,
    localNodeId: normalizedBody.localNodeId || null,
    localNodeName: normalizedBody.localNodeName || null,
    purpose: normalizedBody.purpose || null,
  };
  const ledger = {
    id: nextId('cost', state.agentCostLedger),
    agentId: job.agentId,
    jobId: job.id,
    model: job.model,
    provider: job.provider,
    localOrRemote: job.localOrRemote,
    promptTokens: job.promptTokens,
    completionTokens: job.completionTokens,
    totalTokens,
    remoteApiCost: estimatedRemoteCost,
    localComputeCost: estimatedLocalCost,
    allocatedOpportunityId: null,
    modelQuoteId: job.modelQuoteId,
    economicDecisionId: job.economicDecisionId,
    pricingSnapshotId: job.pricingSnapshotId,
    localNodeId: job.localNodeId,
    costSource: 'pre_call_estimate',
    createdAt: now,
  };
  state.researchJobs.push(job);
  state.agentCostLedger.push(ledger);
  state.audit.push({
    id: nextId('audit', state.audit),
    action: 'research_job_created',
    actor: job.agentId,
    at: now,
    details: job.id,
    payload: {
      status: job.status,
      localOrRemote: job.localOrRemote,
      totalTokens,
      costLedgerId: ledger.id,
      budgetApprovalId: job.budgetApprovalId,
      modelQuoteId: job.modelQuoteId,
      economicDecisionId: job.economicDecisionId,
    },
  });
  return { job, ledger };
}

export function createOpportunity(state, body = {}, now = new Date().toISOString()) {
  ensureOpportunityState(state);
  const errors = validateOpportunityInput(state, body);
  if (errors.length) return { errors };
  const job = body.researchJobId ? state.researchJobs.find(row => row.id === body.researchJobId) : null;
  const winProbability = clampPercent(body.winProbability, 0.5);
  const lossProbability = clampPercent(body.lossProbability ?? (1 - winProbability), 1 - winProbability);
  const averageWin = nonNegative(body.potentialUpside || 0) / Math.max(1, nonNegative(body.totalMoneyRisked || 1));
  const averageLoss = 1;
  const kellyFraction = winProbability > lossProbability
    ? Math.max(0, (winProbability * averageWin - lossProbability * averageLoss) / averageWin)
    : 0;
  const kellyCapped = Math.min(kellyFraction, 0.25);
  const maxPositionSize = nonNegative(state.config?.maxPositionSizeUsd || 50000);
  const recommendedSize = Math.min(maxPositionSize, Math.round(nonNegative(body.totalMoneyRisked || 1000) * (1 + kellyCapped)));
  const volatilityScore = nonNegative(body.volatilityScore ?? 50);
  const holdingPeriodDays = Math.max(1, Math.ceil((100 - volatilityScore) / 15));

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
    lossProbability,
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
    tradeIntent: body.tradeIntent || null,
    executionPurpose: body.executionPurpose || null,
    positionSide: body.positionSide || null,
    entryPrice: Number(body.entryPrice || 0) || null,
    takeProfitPrice: Number(body.takeProfitPrice || 0) || null,
    stopLossPrice: Number(body.stopLossPrice || 0) || null,
    tradePlan: body.tradePlan || null,
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
    updatedAt: now,
    positionSizing: {
      kellyFraction: Number(kellyFraction.toFixed(4)),
      kellyCapped: Number(kellyCapped.toFixed(4)),
      recommendedSize,
      maxPositionSize,
      capitalAtRisk: nonNegative(body.totalMoneyRisked || 0),
      riskPerUnit: nonNegative(body.maxLoss || 0) / Math.max(1, recommendedSize),
    },
    holdingPeriodDays,
    expectedReturn: Number(((winProbability * nonNegative(body.potentialUpside || 0)) - (lossProbability * nonNegative(body.maxLoss || 0))).toFixed(2)),
    expectedRisk: Number((nonNegative(body.totalMoneyRisked || 0) * (1 - winProbability)).toFixed(2)),
    sharpeEstimate: Number(((winProbability * averageWin - lossProbability * averageLoss) / Math.max(0.01, (averageWin + averageLoss) * 0.5)).toFixed(2)),
    volatilityScore,
  };
  opportunity.netExpectedValue = netExpectedValue(opportunity);
  const riskBreakdown = { ...buildRiskBreakdown(opportunity, now), id: nextId('risk', state.riskBreakdowns), scopeId: opportunity.id };
  opportunity.riskBreakdownId = riskBreakdown.id;
  state.riskBreakdowns.push(riskBreakdown);
  state.opportunities.push(opportunity);
  if (job) job.opportunityIdsCreated = [...new Set([...(job.opportunityIdsCreated || []), opportunity.id])];
  for (const row of state.agentCostLedger) {
    if (job && row.jobId === job.id && !row.allocatedOpportunityId) row.allocatedOpportunityId = opportunity.id;
  }
  state.audit.push({
    id: nextId('audit', state.audit),
    action: 'opportunity_created',
    actor: opportunity.sourceAgentId,
    at: now,
    details: opportunity.id,
    payload: { venue: opportunity.venue, symbol: opportunity.symbol, netExpectedValue: opportunity.netExpectedValue },
  });
  return { opportunity, riskBreakdown };
}

export function decideOpportunity(state, opportunityId, body = {}, now = new Date().toISOString()) {
  ensureOpportunityState(state);
  const opportunity = state.opportunities.find(row => row.id === opportunityId);
  if (!opportunity) return { errors: ['opportunity_not_found'] };
  if (!['approved', 'rejected', 'deferred'].includes(body.status)) return { errors: ['invalid_opportunity_decision'] };

  opportunity.status = body.status;
  opportunity.approvalStatus = body.status;
  opportunity.decisionReason = body.reason || null;
  opportunity.reviewer = body.reviewer || 'operator';
  opportunity.reviewedAt = now;
  opportunity.updatedAt = now;
  state.audit.push({
    id: nextId('audit', state.audit),
    action: `opportunity_${body.status}`,
    actor: opportunity.reviewer,
    at: now,
    details: opportunity.id,
    payload: { reason: opportunity.decisionReason },
  });

  let execution = null;
  if (body.status === 'approved') {
    const direction = String(opportunity.side || '').toLowerCase() === 'sell'
      || opportunity.recommendation?.includes('short')
      || opportunity.executionPurpose === 'take_profit_exit'
      ? 'sell'
      : 'buy';
    const size = opportunity.positionSizing?.recommendedSize || opportunity.totalMoneyRisked || 1000;
    const estimatedPrice = (() => {
      if (opportunity.symbol && state.marketDataSnapshots) {
        const snapshot = state.marketDataSnapshots.find(row => row.symbol === opportunity.symbol);
        if (snapshot?.ask) return Number(snapshot.ask);
      }
      return opportunity.symbol ? 100 : 50;
    })();

    execution = {
      id: `exec-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      opportunityId: opportunity.id,
      strategyId: opportunity.strategyId || opportunity.sourceAgentId || 'opportunity-driver',
      sourceAgentId: opportunity.sourceAgentId || null,
      economicDecisionId: opportunity.economicDecisionId || null,
      modelQuoteId: opportunity.modelQuoteId || null,
      forecastId: opportunity.forecastId || null,
      executionCostSnapshotId: opportunity.executionCostSnapshotId || null,
      netExecutableEdgeUsd: opportunity.netExecutableEdgeUsd ?? null,
      symbol: opportunity.symbol || opportunity.marketSlug || 'UNKNOWN',
      venue: opportunity.venue || 'paper',
      mode: 'paper',
      status: 'draft',
      version: 1,
      side: direction,
      entryPrice: estimatedPrice,
      takeProfitPrice: opportunity.takeProfitPrice || null,
      stopLossPrice: opportunity.stopLossPrice || null,
      quantity: size / Math.max(1, estimatedPrice),
      price: estimatedPrice,
      notional: size,
      tradePlan: opportunity.tradePlan || null,
      tradeIntent: opportunity.tradeIntent || null,
      executionPurpose: opportunity.executionPurpose || null,
      positionSide: opportunity.positionSide || null,
      orders: [{
        id: `ord-${Date.now()}`,
        side: direction,
        symbol: opportunity.symbol || opportunity.marketSlug,
        quantity: size / Math.max(1, estimatedPrice),
        price: estimatedPrice,
        orderType: 'market',
        timeInForce: 'GTC',
        takeProfitPrice: opportunity.takeProfitPrice || null,
        stopLossPrice: opportunity.stopLossPrice || null,
        tradePlan: opportunity.tradePlan || null,
      }],
      fills: [],
      confidenceScore: opportunity.confidenceScore || 0.5,
      convictionWeight: opportunity.confidenceScore || 0.5,
      riskDecision: { approved: true, reason: 'opportunity_approval' },
      tags: {
        source: 'opportunity_approval',
        opportunityId: opportunity.id,
        recommendation: opportunity.recommendation,
        competitor: opportunity.modelQuoteId ? 'agent' : undefined,
      },
      createdAt: now,
      startedAt: now,
      updatedAt: now,
      lastHeartbeatAt: now,
    };
    state.executions.push(execution);
    state.audit.push({
      id: nextId('audit', state.audit),
      action: 'execution_draft_from_opportunity',
      actor: 'system',
      at: now,
      details: execution.id,
      payload: { opportunityId: opportunity.id, symbol: execution.symbol, quantity: execution.quantity },
    });
  }

  return { opportunity, execution };
}

export function summarizeAgentCosts(state) {
  ensureOpportunityState(state);
  const total = state.agentCostLedger.reduce((sum, row) => sum + Number(row.remoteApiCost || 0) + Number(row.localComputeCost || 0), 0);
  return {
    dailyBudgetUsd: state.agentBudgets.reduce((sum, row) => sum + Number(row.dailyCostLimit || 0), 0),
    spentTodayUsd: Number(total.toFixed(2)),
    remoteModelCostUsd: Number(state.agentCostLedger.reduce((sum, row) => sum + Number(row.remoteApiCost || 0), 0).toFixed(2)),
    localModelCostUsd: Number(state.agentCostLedger.reduce((sum, row) => sum + Number(row.localComputeCost || 0), 0).toFixed(2)),
    openResearchJobs: state.researchJobs.filter(job => ['queued', 'running'].includes(job.status)).length,
    pendingBudgetApprovals: state.budgetApprovals.filter(approval => approval.status === 'pending_review').length,
    costPerOpportunityUsd: state.opportunities.length ? Number((total / state.opportunities.length).toFixed(2)) : 0,
    localCostFormula: 'runtime_hours * estimated_watts / 1000 * electricity_rate_per_kwh + hardware_depreciation_per_hour * runtime_hours',
  };
}
