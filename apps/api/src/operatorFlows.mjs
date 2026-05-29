import { nextId } from '../../../packages/storage/src/operatorStore.mjs';

export function validateParameters(template, parameters = {}, instruments = []) {
  const errors = [];
  if (!template) return ['template_not_found'];
  for (const [key, rule] of Object.entries(template.parameterSchema || {})) {
    const value = parameters[key] ?? rule.default;
    if (rule.required && (value === undefined || value === null || value === '')) errors.push(`${key}_required`);
    if (rule.type === 'instrument' && value && !instruments.some(i => i.symbol === value && i.status === 'active')) errors.push(`${key}_invalid_instrument`);
    if (rule.type === 'enum' && value && !rule.values.includes(value)) errors.push(`${key}_invalid_enum`);
    if (rule.type === 'number' && value !== undefined) {
      const n = Number(value);
      if (!Number.isFinite(n)) errors.push(`${key}_not_number`);
      if (rule.min !== undefined && n < rule.min) errors.push(`${key}_below_min`);
      if (rule.max !== undefined && n > rule.max) errors.push(`${key}_above_max`);
    }
  }
  return errors;
}

export function createStrategyFromTemplate(state, body, now = new Date().toISOString()) {
  const template = state.strategyTemplates.find(t => t.id === body.templateId);
  const parameters = { ...Object.fromEntries(Object.entries(template?.parameterSchema || {}).map(([k, v]) => [k, v.default])), ...(body.parameters || {}) };
  const errors = validateParameters(template, parameters, state.instruments);
  if (errors.length) return { errors };
  const strategy = {
    id: nextId('strategy', state.strategies),
    templateId: template.id,
    name: body.name || template.name,
    version: 1,
    status: 'draft',
    riskLevel: body.riskLevel || template.riskLevel || 'medium',
    parameters,
    createdAt: now,
    updatedAt: now
  };
  state.strategies.push(strategy);
  state.audit.push({ id: nextId('audit', state.audit), action: 'strategy_created', actor: 'operator', at: now, details: strategy.id });
  return { strategy };
}

export function cloneStrategyVersion(state, strategyId, changes = {}, now = new Date().toISOString()) {
  const existing = state.strategies.find(s => s.id === strategyId);
  if (!existing) return { errors: ['strategy_not_found'] };
  const template = state.strategyTemplates.find(t => t.id === existing.templateId);
  const parameters = { ...existing.parameters, ...(changes.parameters || {}) };
  const errors = template ? validateParameters(template, parameters, state.instruments) : [];
  if (errors.length) return { errors };
  const strategy = {
    ...existing,
    id: nextId('strategy', state.strategies),
    version: Number(existing.version || 1) + 1,
    status: 'draft',
    name: changes.name || existing.name,
    riskLevel: changes.riskLevel || existing.riskLevel,
    parameters,
    createdAt: now,
    updatedAt: now,
    parentStrategyId: existing.id
  };
  state.strategies.push(strategy);
  state.audit.push({ id: nextId('audit', state.audit), action: 'strategy_version_cloned', actor: 'operator', at: now, details: `${existing.id}->${strategy.id}` });
  return { strategy };
}

export function updateStrategyStatus(state, strategyId, status, now = new Date().toISOString()) {
  const allowed = new Set(['draft', 'active', 'archived', 'blocked']);
  if (!allowed.has(status)) return { errors: ['invalid_strategy_status'] };
  const strategy = state.strategies.find(s => s.id === strategyId);
  if (!strategy) return { errors: ['strategy_not_found'] };
  strategy.status = status;
  strategy.updatedAt = now;
  state.audit.push({ id: nextId('audit', state.audit), action: 'strategy_status_updated', actor: 'operator', at: now, details: `${strategyId}:${status}` });
  return { strategy };
}

export function deterministicBacktest(strategy, body = {}) {
  const initialCapitalUsd = Number(body.initialCapitalUsd || 100000);
  const feeBps = Number(body.feeBps || 5);
  const slippageBps = Number(body.slippageBps || 10);
  const riskBoost = strategy.riskLevel === 'low' ? 1.4 : strategy.riskLevel === 'high' ? 5.8 : 3.2;
  const parameterBoost = Object.values(strategy.parameters || {}).filter(v => typeof v === 'number').reduce((acc, v) => acc + Math.min(Math.abs(v), 50) / 1000, 0);
  const totalReturnPct = Number((riskBoost + parameterBoost - (feeBps + slippageBps) / 100).toFixed(2));
  const maxDrawdownPct = Number((Math.max(0.75, Math.abs(totalReturnPct) / 3)).toFixed(2));
  const sharpe = Number((0.8 + Math.max(0, totalReturnPct) / 10).toFixed(2));
  const totalTrades = strategy.riskLevel === 'low' ? 6 : strategy.riskLevel === 'high' ? 22 : 14;
  const finalEquity = Math.round(initialCapitalUsd * (1 + totalReturnPct / 100));
  return {
    assumptions: { initialCapitalUsd, feeBps, slippageBps, dataSource: body.dataSource || 'demo-fixture', strategyVersion: strategy.version },
    metrics: { totalReturnPct, maxDrawdownPct, sharpe, totalTrades, winRatePct: strategy.riskLevel === 'low' ? 61.11 : 57.14, turnoverPct: Number((totalTrades * 4.2).toFixed(2)) },
    equityCurve: [initialCapitalUsd, Math.round(initialCapitalUsd * 1.005), Math.round(initialCapitalUsd * 0.998), Math.round(initialCapitalUsd * 1.017), finalEquity],
    trades: [
      { timestamp: '2026-01-02T10:00:00.000Z', symbol: strategy.parameters.symbol || 'DEMO', side: 'buy', quantity: 1, price: 100 },
      { timestamp: '2026-01-03T15:00:00.000Z', symbol: strategy.parameters.symbol || 'DEMO', side: 'sell', quantity: 1, price: Number((100 * (1 + totalReturnPct / 100)).toFixed(2)) }
    ]
  };
}

export function createBacktest(state, body, now = new Date().toISOString()) {
  const strategy = state.strategies.find(s => s.id === body.strategyId);
  if (!strategy) return { errors: ['strategy_not_found'] };
  const result = deterministicBacktest(strategy, body);
  const backtest = { id: nextId('bt', state.backtests), strategyId: strategy.id, status: 'completed', startedAt: now, completedAt: now, ...result, report: { artifactId: nextId('artifact', state.backtests), summary: `Backtest completed for ${strategy.name} v${strategy.version}.` } };
  state.backtests.push(backtest);
  state.audit.push({ id: nextId('audit', state.audit), action: 'backtest_completed', actor: 'operator', at: now, details: backtest.id });
  return { backtest };
}

export function requestApproval(state, body, now = new Date().toISOString()) {
  const strategy = state.strategies.find(s => s.id === body.strategyId);
  if (!strategy) return { errors: ['strategy_not_found'] };
  const backtest = body.backtestId ? state.backtests.find(b => b.id === body.backtestId) : state.backtests.find(b => b.strategyId === strategy.id && b.status === 'completed');
  const approval = { id: nextId('approval', state.approvals), strategyId: strategy.id, backtestId: backtest?.id, status: backtest ? 'pending_review' : 'blocked', tier: body.tier || 'canary', reason: backtest ? 'Ready for human review.' : 'Completed backtest evidence is required.', createdAt: now };
  state.approvals.push(approval);
  state.audit.push({ id: nextId('audit', state.audit), action: 'approval_requested', actor: 'operator', at: now, details: approval.id });
  return { approval };
}

export function decideApproval(state, approvalId, body = {}, now = new Date().toISOString()) {
  const approval = state.approvals.find(a => a.id === approvalId);
  if (!approval) return { errors: ['approval_not_found'] };
  if (!['approved', 'rejected'].includes(body.status)) return { errors: ['invalid_approval_decision'] };
  approval.status = body.status;
  approval.reason = body.reason || approval.reason;
  approval.reviewedAt = now;
  approval.reviewer = body.reviewer || 'operator';
  if (body.status === 'approved') {
    const strategy = state.strategies.find(s => s.id === approval.strategyId);
    if (strategy) strategy.status = approval.tier === 'production' ? 'active' : 'draft';
  }
  state.audit.push({ id: nextId('audit', state.audit), action: `approval_${body.status}`, actor: approval.reviewer, at: now, details: approval.id });
  return { approval };
}

export function startPaperExecution(state, body = {}, now = new Date().toISOString()) {
  if (state.killSwitch.enabled) return { errors: ['kill_switch_enabled'] };
  const strategy = state.strategies.find(s => s.id === body.strategyId);
  if (!strategy) return { errors: ['strategy_not_found'] };
  const approval = state.approvals.find(a => a.strategyId === strategy.id && a.status === 'approved');
  if (!approval) return { errors: ['approval_required'] };
  const account = state.accounts.find(a => a.id === (body.accountId || 'acct-paper-primary'));
  if (!account) return { errors: ['account_not_found'] };
  const execution = { id: nextId('paper', state.paperExecutions), strategyId: strategy.id, accountId: account.id, status: 'running', startedAt: now, stoppedAt: null, lastHeartbeatAt: now, mode: 'paper', fills: [] };
  state.paperExecutions.push(execution);
  state.audit.push({ id: nextId('audit', state.audit), action: 'paper_execution_started', actor: 'operator', at: now, details: execution.id });
  return { execution };
}

export function stopPaperExecution(state, executionId, body = {}, now = new Date().toISOString()) {
  const execution = state.paperExecutions.find(e => e.id === executionId);
  if (!execution) return { errors: ['paper_execution_not_found'] };
  execution.status = 'stopped';
  execution.stoppedAt = now;
  execution.stopReason = body.reason || 'operator_request';
  state.audit.push({ id: nextId('audit', state.audit), action: 'paper_execution_stopped', actor: 'operator', at: now, details: execution.id });
  return { execution };
}
