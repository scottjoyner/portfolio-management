import { refreshP1, wireP1Actions } from './p1.js';

const qs = selector => document.querySelector(selector);
const money = value => `$${Number(value || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
const pct = value => `${Number((value || 0) * 100).toFixed(1)}%`;
const fmt = value => JSON.stringify(value, null, 2);

let opportunityState = {
  opportunities: [],
  riskBreakdowns: [],
  researchJobs: [],
  agentCostLedger: [],
  agentBudgets: [],
  budgetApprovals: [],
  marketDataSnapshots: [],
  agentCostSummary: { spentTodayUsd: 0, dailyBudgetUsd: 0, remoteModelCostUsd: 0, localModelCostUsd: 0, costPerOpportunityUsd: 0, openResearchJobs: 0, pendingBudgetApprovals: 0 }
};

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { 'content-type': 'application/json', ...(options.headers || {}) },
    ...options,
    body: options.body ? JSON.stringify(options.body) : undefined
  });
  const data = await res.json();
  if (!res.ok && !data) throw new Error(`Request failed: ${res.status}`);
  return data;
}

async function ensureSeededOpportunityData() {
  const dashboard = await api('/api/opportunity-dashboard');
  if (dashboard.opportunities?.length) return dashboard;
  const job = await api('/api/agents/jobs', { method: 'POST', body: { agentId: 'market-research-agent', model: 'research-model', localOrRemote: 'remote', promptTokens: 12000, completionTokens: 6400, totalTokens: 18400, marketScope: 'prediction-demo' } });
  await api('/api/opportunities', {
    method: 'POST',
    body: {
      researchJobId: job.job.id,
      sourceAgentId: 'market-research-agent',
      marketType: 'prediction_market',
      venue: 'polymarket-watch',
      symbol: 'PREDICTION:DEMO',
      marketSlug: 'demo-prediction-market',
      title: 'Demo prediction-market research candidate',
      recommendation: 'review_yes',
      confidenceScore: 0.68,
      winProbability: 0.57,
      lossProbability: 0.43,
      grossExpectedValue: 68.4,
      totalMoneyRisked: 500,
      maxLoss: 500,
      potentialUpside: 420,
      liquidityScore: 71,
      dataFreshnessScore: 86,
      backtestStatus: 'historical_analog_required',
      estimatedFees: 5,
      estimatedSlippage: 10,
      agentResearchCost: 9.35,
      modelInferenceCost: 2.9,
      notes: 'Seeded by the API to prove the UI is no longer backed by static dashboard fixtures.',
      evidence: [{ type: 'agent_note', summary: 'Candidate requires operator review before any paper action.' }]
    }
  });
  await api('/api/opportunities', {
    method: 'POST',
    body: {
      sourceAgentId: 'liquidity-scanner',
      marketType: 'crypto_spot',
      venue: 'coinbase-paper',
      symbol: 'ETH-USD',
      title: 'ETH-USD mean reversion paper setup',
      recommendation: 'paper_review',
      confidenceScore: 0.61,
      winProbability: 0.54,
      lossProbability: 0.46,
      grossExpectedValue: 37.4,
      totalMoneyRisked: 1200,
      maxLoss: 180,
      potentialUpside: 310,
      liquidityScore: 79,
      dataFreshnessScore: 78,
      backtestStatus: 'deterministic_scaffold_only',
      estimatedFees: 6,
      estimatedSlippage: 5,
      agentResearchCost: 5.2,
      modelInferenceCost: 1.1,
      notes: 'Paper-only candidate until historical replay and real market data adapters are implemented.'
    }
  });
  return api('/api/opportunity-dashboard');
}

function renderSummary(summary) {
  const readiness = summary.readiness || { productionReady: false, mode: 'mock', liveTradingCertified: false, blockers: [] };
  const paperReady = summary.productionPaperReady || summary.productionPaperReadiness;
  qs('#readiness-card').innerHTML = `
    <span class="label">Readiness</span>
    <strong>${paperReady?.productionPaperReady ? 'Paper-production ready' : readiness.productionReady ? 'Production ready' : 'Paper-only / not prod ready'}</strong>
    <span>${readiness.mode || 'mock'} mode · live certified: ${readiness.liveTradingCertified ? 'yes' : 'no'}</span>
  `;
  qs('#summary-grid').innerHTML = Object.entries(summary.counts || {}).map(([key, value]) => `
    <article class="summary-card compact-kpi"><span class="label">${key}</span><strong>${value}</strong></article>
  `).join('');
  qs('#risk-json').textContent = fmt({ readiness: summary.readiness, killSwitch: summary.killSwitch, p0p1: summary.p0p1 });
  qs('#toggle-kill-switch').textContent = summary.killSwitch?.enabled ? 'Disable kill switch' : 'Enable kill switch';
}

function renderCommandQueue(summary = {}) {
  const agentCost = opportunityState.agentCostSummary || {};
  const pendingBudgetApprovals = opportunityState.budgetApprovals.filter(approval => approval.status === 'pending_review').length;
  const items = [
    { title: 'Review opportunity feed', detail: `${opportunityState.opportunities.length} API-backed candidates`, tone: 'warning' },
    { title: 'Backtesting depth', detail: 'Deterministic scaffold exists; historical replay still required', tone: 'blocked' },
    { title: 'Agent spend controls', detail: `${money(agentCost.spentTodayUsd)} spent today · ${pendingBudgetApprovals} budget approvals pending`, tone: pendingBudgetApprovals ? 'warning' : 'neutral' },
    { title: 'Approvals', detail: `${summary.counts?.approvals || 0} strategy approval records currently tracked`, tone: 'neutral' }
  ];
  qs('#command-queue').innerHTML = items.map(item => `<article class="queue-item ${item.tone}"><strong>${item.title}</strong><span>${item.detail}</span></article>`).join('');
}

function renderPortfolio(accounts = [], positions = []) {
  const nav = accounts.reduce((sum, account) => sum + Number(account.nav || 0), 0);
  const cash = accounts.reduce((sum, account) => sum + Number(account.cash || 0), 0);
  const locked = positions.reduce((sum, position) => sum + Math.abs(Number(position.quantity || 0) * Number(position.averagePrice || position.average_price || 0)) * 0.1, 0);
  const risked = opportunityState.opportunities.reduce((sum, opp) => sum + Number(opp.totalMoneyRisked || 0), 0);
  const netEv = opportunityState.opportunities.reduce((sum, opp) => sum + Number(opp.netExpectedValue || 0), 0);
  qs('#pnl-liquidity-grid').innerHTML = [
    ['Total NAV', money(nav), 'capital base'], ['Cash', money(cash), 'available'], ['Risk reserve', money(locked), 'paper positions'], ['Opportunity risk', money(risked), 'candidate capital'], ['Net EV queue', money(netEv), 'after model/research cost'], ['Research spend today', money(opportunityState.agentCostSummary.spentTodayUsd), 'operational loss']
  ].map(([label, value, hint]) => `<article class="summary-card"><span class="label">${label}</span><strong>${value}</strong><small>${hint}</small></article>`).join('');

  qs('#portfolio-cards').innerHTML = accounts.map(account => `<article class="card account-card"><h3>${account.name}</h3><p><span class="badge">${account.status}</span> ${account.provider}</p><p><strong>${money(account.nav)}</strong> NAV · ${money(account.cash)} cash</p><div class="mini-bar"><span style="width:${Math.min(100, Math.round((Number(account.cash || 0) / Math.max(1, Number(account.nav || 1))) * 100))}%"></span></div><p class="label">${account.currency || 'USD'} · paper/sandbox capital</p></article>`).join('') || '<p class="label">No accounts loaded.</p>';
  qs('#position-rows').innerHTML = positions.map(position => `<tr><td><strong>${position.symbol}</strong><br/><span class="label">${position.strategyId || 'manual/paper'}</span></td><td>${position.quantity}</td><td>${money(position.averagePrice || position.average_price)}</td><td>${position.markPrice || position.mark_price ? money(position.markPrice || position.mark_price) : 'pending'}</td><td><span class="badge">${position.status}</span></td></tr>`).join('') || '<tr><td colspan="5" class="label">No open positions yet.</td></tr>';
}

function renderMarkets() {
  const snapshots = opportunityState.marketDataSnapshots || [];
  qs('#market-strip').innerHTML = snapshots.map(market => `<article class="ticker-tile"><span>${market.symbol}</span><strong>${money(market.ask)}</strong><small>${market.spreadBps} bps · LQ ${market.liquidityScore}</small></article>`).join('');
  qs('#market-rows').innerHTML = snapshots.map(market => `<tr><td><strong>${market.symbol}</strong><br/><span class="label">${market.assetClass}</span></td><td>${market.venue}</td><td>${money(market.bid)} / ${money(market.ask)}</td><td>${market.spreadBps} bps</td><td><div class="score-line"><span style="width:${market.liquidityScore}%"></span></div>${market.liquidityScore}/100</td><td><span class="badge">${market.status}</span></td></tr>`).join('');
}

function renderRiskStack() {
  const totalRisked = opportunityState.opportunities.reduce((sum, opp) => sum + Number(opp.totalMoneyRisked || 0), 0);
  const maxLoss = opportunityState.opportunities.reduce((sum, opp) => sum + Number(opp.maxLoss || 0), 0);
  const netEv = opportunityState.opportunities.reduce((sum, opp) => sum + Number(opp.netExpectedValue || 0), 0);
  const agentCost = opportunityState.opportunities.reduce((sum, opp) => sum + Number(opp.agentResearchCost || 0) + Number(opp.modelInferenceCost || 0), 0);
  const rows = [['Candidate capital', money(totalRisked), 74], ['Max loss queue', money(maxLoss), 58], ['Net EV after costs', money(netEv), 42], ['Research/model drag', money(agentCost), 29]];
  qs('#risk-stack').innerHTML = rows.map(([label, value, width]) => `<article class="risk-row"><div><span class="label">${label}</span><strong>${value}</strong></div><div class="score-line"><span style="width:${width}%"></span></div></article>`).join('');
}

function riskForOpportunity(opp) {
  return opportunityState.riskBreakdowns.find(risk => risk.id === opp.riskBreakdownId || risk.scopeId === opp.id) || {};
}

function opportunityCard(opp) {
  const risk = riskForOpportunity(opp);
  return `<article class="card opportunity-card" data-opportunity-id="${opp.id}"><div class="card-topline"><div><p class="eyebrow">${opp.venue} · ${opp.marketType}</p><h3>${opp.title || opp.market || opp.symbol}</h3></div><span class="badge warning">${opp.approvalStatus}</span></div><div class="thesis-strip"><span>${opp.recommendation}</span><span>Risk ${risk.aggregateScore ?? 'n/a'}/100</span><span>Backtest: ${opp.backtestStatus}</span></div><div class="metric-row large-metrics"><span><b>${pct(opp.winProbability)}</b><small>win probability</small></span><span><b>${pct(opp.lossProbability)}</b><small>loss probability</small></span><span><b>${money(opp.totalMoneyRisked)}</b><small>risked</small></span><span><b>${money(opp.maxLoss)}</b><small>max loss</small></span><span><b>${money(opp.potentialUpside)}</b><small>upside</small></span><span><b>${money(opp.netExpectedValue)}</b><small>net EV</small></span></div><details><summary>Risk, backtest, evidence, and cost breakdown</summary><div class="split-grid"><pre>${fmt(risk)}</pre><pre>${fmt({ grossExpectedValue: opp.grossExpectedValue, agentResearchCost: opp.agentResearchCost, modelInferenceCost: opp.modelInferenceCost, estimatedFees: opp.estimatedFees, estimatedSlippage: opp.estimatedSlippage, netExpectedValue: opp.netExpectedValue })}</pre></div><p>${opp.notes || ''}</p></details><div class="button-row"><button data-opportunity-decision="approve" data-opportunity-id="${opp.id}">Approve</button><button data-opportunity-decision="reject" data-opportunity-id="${opp.id}">Reject</button><button data-opportunity-decision="request-research" data-opportunity-id="${opp.id}">Request more research</button></div></article>`;
}

function renderOpportunities() {
  qs('#opportunity-cards').innerHTML = opportunityState.opportunities.map(opportunityCard).join('') || '<p class="label">No opportunities yet.</p>';
  qs('#polymarket-cards').innerHTML = opportunityState.opportunities.filter(opp => opp.venue?.includes('polymarket') || opp.marketType === 'prediction_market').map(opportunityCard).join('') || '<p class="label">No Polymarket opportunities available.</p>';
  document.querySelectorAll('[data-opportunity-decision]').forEach(button => {
    button.addEventListener('click', async () => {
      const id = button.dataset.opportunityId;
      const action = button.dataset.opportunityDecision;
      if (action === 'request-research') await api(`/api/opportunities/${id}/request-research`, { method: 'POST', body: { localOrRemote: 'local', model: 'local-review-model', totalTokens: 8000, runtimeSeconds: 90, systemBudgetOverride: true } });
      else await api(`/api/opportunities/${id}/${action}`, { method: 'POST', body: { reviewer: 'operator-ui', reason: `${action} from dashboard` } });
      await refresh();
    });
  });
}

function budgetApprovalCard(approval) {
  return `<article class="card budget-approval-card" data-budget-approval-id="${approval.id}"><div class="card-topline"><div><p class="eyebrow">${approval.agentId}</p><h3>${approval.marketScope || 'general research budget'}</h3></div><span class="badge ${approval.status === 'approved' ? '' : 'warning'}">${approval.status}</span></div><div class="metric-row"><span><b>${money(approval.projectedCost)}</b><small>projected cost</small></span><span><b>${Number(approval.projectedTokens || 0).toLocaleString()}</b><small>projected tokens</small></span><span><b>${money(approval.approvedCostLimit)}</b><small>approved cost</small></span><span><b>${Number(approval.approvedTokenLimit || 0).toLocaleString()}</b><small>approved tokens</small></span></div><p>${approval.reason || ''}</p>${approval.status === 'pending_review' ? `<div class="button-row"><button data-budget-approval-action="approve" data-budget-approval-id="${approval.id}">Approve budget</button><button data-budget-approval-action="reject" data-budget-approval-id="${approval.id}">Reject</button></div>` : `<p class="label">Reviewed by ${approval.reviewer || 'n/a'} ${approval.reviewedAt || ''}</p>`}</article>`;
}

function renderBudgetApprovals() {
  const approvals = opportunityState.budgetApprovals || [];
  qs('#budget-approval-cards').innerHTML = approvals.map(budgetApprovalCard).join('') || '<p class="label">No research budget approvals yet.</p>';
  document.querySelectorAll('[data-budget-approval-action]').forEach(button => {
    button.addEventListener('click', async () => {
      const id = button.dataset.budgetApprovalId;
      const action = button.dataset.budgetApprovalAction;
      const approval = opportunityState.budgetApprovals.find(row => row.id === id);
      await api(`/api/agents/budget-approvals/${id}/decision`, {
        method: 'POST',
        body: {
          status: action === 'approve' ? 'approved' : 'rejected',
          reviewer: 'operator-ui',
          approvedCostLimit: approval?.projectedCost || 0,
          approvedTokenLimit: approval?.projectedTokens || 0,
          reason: `${action} from dashboard`
        }
      });
      await refresh();
    });
  });
}

function renderAgents() {
  const summary = opportunityState.agentCostSummary || {};
  qs('#sidebar-agent-spend').textContent = money(summary.spentTodayUsd);
  qs('#agent-cost-grid').innerHTML = [['Daily budget', money(summary.dailyBudgetUsd)], ['Spent today', money(summary.spentTodayUsd)], ['Remote model cost', money(summary.remoteModelCostUsd)], ['Local model cost', money(summary.localModelCostUsd)], ['Cost/opportunity', money(summary.costPerOpportunityUsd)], ['Pending approvals', summary.pendingBudgetApprovals || 0], ['Open research jobs', summary.openResearchJobs || 0]].map(([label, value]) => `<article class="summary-card"><span class="label">${label}</span><strong>${value}</strong></article>`).join('');
  renderBudgetApprovals();
  qs('#agent-job-rows').innerHTML = (opportunityState.researchJobs || []).map(job => `<tr><td><strong>${job.id}</strong><br/><span class="label">${job.budgetApprovalId ? `Budget: ${job.budgetApprovalId}` : 'standard budget'}</span></td><td>${job.agentId}</td><td>${job.model}</td><td><span class="badge">${job.status}</span></td><td>${Number(job.totalTokens || 0).toLocaleString()}</td><td>${money(Number(job.estimatedRemoteCost || 0) + Number(job.estimatedLocalCost || 0))}</td></tr>`).join('') || '<tr><td colspan="6" class="label">No research jobs yet.</td></tr>';
}

function renderStrategies(strategies) { qs('#strategy-rows').innerHTML = strategies.map(strategy => `<tr><td><strong>${strategy.name}</strong><br/><span class="label">${strategy.id} · v${strategy.version}</span></td><td><span class="badge">${strategy.status}</span></td><td>${strategy.riskLevel}</td><td><code>${fmt(strategy.parameters)}</code></td></tr>`).join(''); }
function renderBacktests(backtests) { qs('#backtest-cards').innerHTML = backtests.map(run => `<article class="card"><h3>${run.id}</h3><p><span class="badge">${run.status}</span></p><p><strong>${run.metrics.totalReturnPct}%</strong> total return · ${run.metrics.maxDrawdownPct}% max drawdown</p><p>Sharpe ${run.metrics.sharpe} · ${run.metrics.totalTrades} trades · ${run.metrics.winRatePct}% win rate</p><pre>${fmt(run.assumptions)}</pre></article>`).join('') || '<p class="label">No backtests yet.</p>'; }
function renderApprovals(approvals) { qs('#approval-cards').innerHTML = approvals.map(approval => `<article class="card"><h3>${approval.id}</h3><p><span class="badge">${approval.status}</span> ${approval.tier}</p><p>${approval.reason}</p><p class="label">Strategy: ${approval.strategyId}</p>${approval.status === 'pending_review' ? `<button data-approve="${approval.id}">Approve</button>` : ''}</article>`).join('') || '<p class="label">No approval requests yet.</p>'; document.querySelectorAll('[data-approve]').forEach(button => { button.addEventListener('click', async () => { await api(`/api/approvals/${button.dataset.approve}/decision`, { method: 'POST', body: { status: 'approved', reviewer: 'operator-ui' } }); await refresh(); }); }); }
function renderAudit(audit) { qs('#audit-list').innerHTML = audit.slice().reverse().map(event => `<li><strong>${event.action}</strong> — ${event.actor} — ${event.at}<br/><span class="label">${event.details || ''}</span></li>`).join(''); }

async function verifyAudit() { const result = await api('/api/audit/verify').catch(error => ({ ok: false, error: error.message })); qs('#audit-verify-json').textContent = fmt(result); qs('#audit-status-card').innerHTML = `<span class="label">Audit</span><strong>${result.ok ? 'Verified' : 'Needs attention'}</strong><span>${result.reason || result.error || 'audit check complete'}</span>`; }

async function refresh() {
  const [summary, strategies, backtests, approvals, audit, accounts, positions, dashboard] = await Promise.all([api('/api/operator/summary'), api('/api/strategies'), api('/api/backtests'), api('/api/approvals'), api('/api/audit'), api('/api/accounts'), api('/api/positions'), ensureSeededOpportunityData()]);
  opportunityState = dashboard;
  renderSummary(summary);
  renderCommandQueue(summary);
  renderPortfolio(accounts.accounts || [], positions.positions || []);
  renderMarkets();
  renderRiskStack();
  renderOpportunities();
  renderAgents();
  renderStrategies(strategies.strategies);
  renderBacktests(backtests.backtests);
  renderApprovals(approvals.approvals);
  renderAudit(audit.audit);
  await refreshP1();
  await verifyAudit();
}

qs('#refresh-dashboard').addEventListener('click', refresh);
qs('#request-budget-approval').addEventListener('click', async () => { await api('/api/agents/budget-approvals', { method: 'POST', body: { agentId: 'market-research-agent', marketScope: 'PREDICTION:DEMO', projectedCost: 12, projectedTokens: 60000, requestedBy: 'operator-ui', reason: 'UI-requested bounded research budget for prediction-market candidate' } }); await refresh(); });
qs('#create-strategy').addEventListener('click', async () => { await api('/api/strategies', { method: 'POST', body: { name: `Demo Strategy ${Date.now()}`, riskLevel: 'low', parameters: { symbol: 'SOL-USD', timeframe: '1h', lookback: 14, entryThresholdPct: 1.5 } } }); await refresh(); });
qs('#run-backtest').addEventListener('click', async () => { const strategies = await api('/api/strategies'); const strategy = strategies.strategies[0]; if (!strategy) return; await api('/api/backtests/run', { method: 'POST', body: { strategyId: strategy.id, initialCapitalUsd: 100000, feeBps: 5, slippageBps: 10 } }); await refresh(); });
qs('#request-approval').addEventListener('click', async () => { const strategies = await api('/api/strategies'); const strategy = strategies.strategies[0]; if (!strategy) return; await api('/api/approvals/request', { method: 'POST', body: { strategyId: strategy.id, tier: 'canary' } }); await refresh(); });
qs('#toggle-kill-switch').addEventListener('click', async () => { const summary = await api('/api/operator/summary'); await api('/api/kill-switch', { method: 'POST', body: { enabled: !summary.killSwitch.enabled, reason: 'operator_ui_test' } }); await refresh(); });
qs('#verify-audit').addEventListener('click', verifyAudit);
wireP1Actions(refresh);
refresh().catch(error => { qs('#readiness-card').innerHTML = `<span class="label">Error</span><strong>${error.message}</strong>`; });
