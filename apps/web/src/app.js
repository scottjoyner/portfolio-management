import { refreshP1, wireP1Actions } from './p1.js';
import { agentCostSummary, agentJobs, marketSnapshots, opportunities } from './dashboard-data.js';

const qs = selector => document.querySelector(selector);
const money = value => `$${Number(value || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
const pct = value => `${Number((value || 0) * 100).toFixed(1)}%`;
const fmt = value => JSON.stringify(value, null, 2);

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

function renderSummary(summary) {
  const readiness = summary.readiness || { productionReady: false, mode: 'mock', liveTradingCertified: false, blockers: [] };
  const paperReady = summary.productionPaperReady || summary.productionPaperReadiness;
  qs('#readiness-card').innerHTML = `
    <span class="label">Readiness</span>
    <strong>${paperReady?.productionPaperReady ? 'Paper-production ready' : readiness.productionReady ? 'Production ready' : 'Not production ready'}</strong>
    <span>${readiness.mode || 'mock'} mode · live certified: ${readiness.liveTradingCertified ? 'yes' : 'no'}</span>
  `;
  qs('#summary-grid').innerHTML = Object.entries(summary.counts || {}).map(([key, value]) => `
    <article class="summary-card">
      <span class="label">${key}</span>
      <strong>${value}</strong>
    </article>
  `).join('');
  qs('#risk-json').textContent = fmt({ readiness: summary.readiness, killSwitch: summary.killSwitch, p0p1: summary.p0p1 });
  qs('#toggle-kill-switch').textContent = summary.killSwitch?.enabled ? 'Disable kill switch' : 'Enable kill switch';
}

function renderPortfolio(accounts = [], positions = []) {
  const nav = accounts.reduce((sum, account) => sum + Number(account.nav || 0), 0);
  const cash = accounts.reduce((sum, account) => sum + Number(account.cash || 0), 0);
  const locked = positions.reduce((sum, position) => sum + Math.abs(Number(position.quantity || 0) * Number(position.averagePrice || position.average_price || 0)) * 0.1, 0);
  qs('#pnl-liquidity-grid').innerHTML = [
    ['Total NAV', money(nav)],
    ['Cash', money(cash)],
    ['Locked/Risk reserve', money(locked)],
    ['Open positions', positions.length],
    ['Daily P/L', 'pending market data'],
    ['Research spend today', money(agentCostSummary.spentTodayUsd)]
  ].map(([label, value]) => `<article class="summary-card"><span class="label">${label}</span><strong>${value}</strong></article>`).join('');

  qs('#portfolio-cards').innerHTML = accounts.map(account => `
    <article class="card">
      <h3>${account.name}</h3>
      <p><span class="badge">${account.status}</span> ${account.provider}</p>
      <p><strong>${money(account.nav)}</strong> NAV · ${money(account.cash)} cash</p>
      <p class="label">${account.currency || 'USD'} · paper/sandbox capital</p>
    </article>
  `).join('') || '<p class="label">No accounts loaded.</p>';

  qs('#position-rows').innerHTML = positions.map(position => `
    <tr>
      <td><strong>${position.symbol}</strong><br/><span class="label">${position.strategyId || 'manual/paper'}</span></td>
      <td>${position.quantity}</td>
      <td>${money(position.averagePrice || position.average_price)}</td>
      <td>${position.markPrice || position.mark_price ? money(position.markPrice || position.mark_price) : 'pending'}</td>
      <td><span class="badge">${position.status}</span></td>
    </tr>
  `).join('') || '<tr><td colspan="5" class="label">No open positions yet.</td></tr>';
}

function renderMarkets() {
  qs('#market-rows').innerHTML = marketSnapshots.map(market => `
    <tr>
      <td><strong>${market.symbol}</strong><br/><span class="label">${market.assetClass}</span></td>
      <td>${market.venue}</td>
      <td>${money(market.bid)} / ${money(market.ask)}</td>
      <td>${market.spreadBps} bps</td>
      <td>${market.liquidityScore}/100</td>
      <td><span class="badge">${market.status}</span></td>
    </tr>
  `).join('');
}

function opportunityCard(opp) {
  return `
    <article class="card opportunity-card" data-opportunity-id="${opp.id}">
      <div class="card-topline">
        <div>
          <p class="eyebrow">${opp.venue} · ${opp.marketType}</p>
          <h3>${opp.market}</h3>
        </div>
        <span class="badge warning">${opp.approvalStatus}</span>
      </div>
      <div class="metric-row">
        <span><b>${opp.recommendation}</b><small>recommendation</small></span>
        <span><b>${pct(opp.winProbability)}</b><small>win probability</small></span>
        <span><b>${money(opp.totalMoneyRisked)}</b><small>risked</small></span>
        <span><b>${money(opp.potentialUpside)}</b><small>upside</small></span>
        <span><b>${money(opp.netExpectedValue)}</b><small>net EV</small></span>
      </div>
      <details>
        <summary>Risk, backtest, and cost breakdown</summary>
        <div class="split-grid">
          <pre>${fmt({
            riskScore: opp.riskScore,
            confidenceScore: opp.confidenceScore,
            lossProbability: opp.lossProbability,
            maxLoss: opp.maxLoss,
            liquidityScore: opp.liquidityScore,
            riskBreakdown: opp.riskBreakdown
          })}</pre>
          <pre>${fmt({
            backtestStatus: opp.backtestStatus,
            grossExpectedValue: opp.grossExpectedValue,
            agentResearchCost: opp.agentResearchCost,
            modelInferenceCost: opp.modelInferenceCost,
            estimatedFees: opp.estimatedFees,
            estimatedSlippage: opp.estimatedSlippage,
            netExpectedValue: opp.netExpectedValue
          })}</pre>
        </div>
        <p>${opp.notes}</p>
      </details>
      <div class="button-row">
        <button data-opportunity-action="approve" disabled>Approve</button>
        <button data-opportunity-action="reject" disabled>Reject</button>
        <button data-opportunity-action="research" disabled>Request more research</button>
      </div>
      <p class="label">Opportunity approval API is next. Buttons are intentionally disabled until persistence exists.</p>
    </article>
  `;
}

function renderOpportunities() {
  qs('#opportunity-cards').innerHTML = opportunities.map(opportunityCard).join('');
  qs('#polymarket-cards').innerHTML = opportunities.filter(opp => opp.venue === 'polymarket').map(opportunityCard).join('') || '<p class="label">No Polymarket opportunities available.</p>';
}

function renderAgents() {
  qs('#agent-cost-grid').innerHTML = [
    ['Daily budget', money(agentCostSummary.dailyBudgetUsd)],
    ['Spent today', money(agentCostSummary.spentTodayUsd)],
    ['Remote model cost', money(agentCostSummary.remoteModelCostUsd)],
    ['Local model cost', money(agentCostSummary.localModelCostUsd)],
    ['Cost/opportunity', money(agentCostSummary.costPerOpportunityUsd)],
    ['Open research jobs', agentCostSummary.openResearchJobs]
  ].map(([label, value]) => `<article class="summary-card"><span class="label">${label}</span><strong>${value}</strong></article>`).join('');
  qs('#agent-job-rows').innerHTML = agentJobs.map(job => `
    <tr>
      <td><strong>${job.id}</strong></td>
      <td>${job.agent}</td>
      <td>${job.model}</td>
      <td><span class="badge">${job.status}</span></td>
      <td>${Number(job.totalTokens || 0).toLocaleString()}</td>
      <td>${money(job.estimatedCostUsd)}</td>
    </tr>
  `).join('');
}

function renderStrategies(strategies) {
  qs('#strategy-rows').innerHTML = strategies.map(strategy => `
    <tr>
      <td><strong>${strategy.name}</strong><br/><span class="label">${strategy.id} · v${strategy.version}</span></td>
      <td><span class="badge">${strategy.status}</span></td>
      <td>${strategy.riskLevel}</td>
      <td><code>${fmt(strategy.parameters)}</code></td>
    </tr>
  `).join('');
}

function renderBacktests(backtests) {
  qs('#backtest-cards').innerHTML = backtests.map(run => `
    <article class="card">
      <h3>${run.id}</h3>
      <p><span class="badge">${run.status}</span></p>
      <p><strong>${run.metrics.totalReturnPct}%</strong> total return · ${run.metrics.maxDrawdownPct}% max drawdown</p>
      <p>Sharpe ${run.metrics.sharpe} · ${run.metrics.totalTrades} trades · ${run.metrics.winRatePct}% win rate</p>
      <pre>${fmt(run.assumptions)}</pre>
    </article>
  `).join('') || '<p class="label">No backtests yet.</p>';
}

function renderApprovals(approvals) {
  qs('#approval-cards').innerHTML = approvals.map(approval => `
    <article class="card">
      <h3>${approval.id}</h3>
      <p><span class="badge">${approval.status}</span> ${approval.tier}</p>
      <p>${approval.reason}</p>
      <p class="label">Strategy: ${approval.strategyId}</p>
      ${approval.status === 'pending_review' ? `<button data-approve="${approval.id}">Approve</button>` : ''}
    </article>
  `).join('') || '<p class="label">No approval requests yet.</p>';
  document.querySelectorAll('[data-approve]').forEach(button => {
    button.addEventListener('click', async () => {
      await api(`/api/approvals/${button.dataset.approve}/decision`, { method: 'POST', body: { status: 'approved', reviewer: 'operator-ui' } });
      await refresh();
    });
  });
}

function renderAudit(audit) {
  qs('#audit-list').innerHTML = audit.slice().reverse().map(event => `
    <li><strong>${event.action}</strong> — ${event.actor} — ${event.at}<br/><span class="label">${event.details || ''}</span></li>
  `).join('');
}

async function verifyAudit() {
  const result = await api('/api/audit/verify').catch(error => ({ ok: false, error: error.message }));
  qs('#audit-verify-json').textContent = fmt(result);
  qs('#audit-status-card').innerHTML = `<span class="label">Audit</span><strong>${result.ok ? 'Verified' : 'Needs attention'}</strong><span>${result.reason || result.error || 'audit check complete'}</span>`;
}

async function refresh() {
  const [summary, strategies, backtests, approvals, audit, accounts, positions] = await Promise.all([
    api('/api/operator/summary'),
    api('/api/strategies'),
    api('/api/backtests'),
    api('/api/approvals'),
    api('/api/audit'),
    api('/api/accounts'),
    api('/api/positions')
  ]);
  renderSummary(summary);
  renderPortfolio(accounts.accounts || [], positions.positions || []);
  renderMarkets();
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

qs('#create-strategy').addEventListener('click', async () => {
  await api('/api/strategies', {
    method: 'POST',
    body: {
      name: `Demo Strategy ${Date.now()}`,
      riskLevel: 'low',
      parameters: { symbol: 'SOL-USD', timeframe: '1h', lookback: 14, entryThresholdPct: 1.5 }
    }
  });
  await refresh();
});

qs('#run-backtest').addEventListener('click', async () => {
  const strategies = await api('/api/strategies');
  const strategy = strategies.strategies[0];
  if (!strategy) return;
  await api('/api/backtests/run', { method: 'POST', body: { strategyId: strategy.id, initialCapitalUsd: 100000, feeBps: 5, slippageBps: 10 } });
  await refresh();
});

qs('#request-approval').addEventListener('click', async () => {
  const strategies = await api('/api/strategies');
  const strategy = strategies.strategies[0];
  if (!strategy) return;
  await api('/api/approvals/request', { method: 'POST', body: { strategyId: strategy.id, tier: 'canary' } });
  await refresh();
});

qs('#toggle-kill-switch').addEventListener('click', async () => {
  const summary = await api('/api/operator/summary');
  await api('/api/kill-switch', { method: 'POST', body: { enabled: !summary.killSwitch.enabled, reason: 'operator_ui_test' } });
  await refresh();
});

qs('#verify-audit').addEventListener('click', verifyAudit);

wireP1Actions(refresh);

refresh().catch(error => {
  qs('#readiness-card').innerHTML = `<span class="label">Error</span><strong>${error.message}</strong>`;
});
