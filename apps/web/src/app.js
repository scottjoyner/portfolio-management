const qs = selector => document.querySelector(selector);
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
  const readiness = summary.readiness;
  qs('#readiness-card').innerHTML = `
    <span class="label">Readiness</span>
    <strong>${readiness.productionReady ? 'Production ready' : 'Not production ready'}</strong>
    <span>${readiness.mode} mode · live certified: ${readiness.liveTradingCertified ? 'yes' : 'no'}</span>
  `;
  qs('#summary-grid').innerHTML = Object.entries(summary.counts).map(([key, value]) => `
    <article class="summary-card">
      <span class="label">${key}</span>
      <strong>${value}</strong>
    </article>
  `).join('');
  qs('#risk-json').textContent = fmt({ readiness: summary.readiness, killSwitch: summary.killSwitch });
  qs('#toggle-kill-switch').textContent = summary.killSwitch.enabled ? 'Disable kill switch' : 'Enable kill switch';
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
  `).join('');
}

function renderApprovals(approvals) {
  qs('#approval-cards').innerHTML = approvals.map(approval => `
    <article class="card">
      <h3>${approval.id}</h3>
      <p><span class="badge">${approval.status}</span> ${approval.tier}</p>
      <p>${approval.reason}</p>
      <p class="label">Strategy: ${approval.strategyId}</p>
    </article>
  `).join('');
}

function renderAudit(audit) {
  qs('#audit-list').innerHTML = audit.slice().reverse().map(event => `
    <li><strong>${event.action}</strong> — ${event.actor} — ${event.at}<br/><span class="label">${event.details || ''}</span></li>
  `).join('');
}

async function refresh() {
  const [summary, strategies, backtests, approvals, audit] = await Promise.all([
    api('/api/operator/summary'),
    api('/api/strategies'),
    api('/api/backtests'),
    api('/api/approvals'),
    api('/api/audit')
  ]);
  renderSummary(summary);
  renderStrategies(strategies.strategies);
  renderBacktests(backtests.backtests);
  renderApprovals(approvals.approvals);
  renderAudit(audit.audit);
}

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
  await api('/api/backtests', { method: 'POST', body: { strategyId: strategy.id, initialCapitalUsd: 100000, feeBps: 5, slippageBps: 10 } });
  await refresh();
});

qs('#request-approval').addEventListener('click', async () => {
  const strategies = await api('/api/strategies');
  const strategy = strategies.strategies[0];
  if (!strategy) return;
  await api('/api/approvals', { method: 'POST', body: { strategyId: strategy.id, tier: 'canary' } });
  await refresh();
});

qs('#toggle-kill-switch').addEventListener('click', async () => {
  const summary = await api('/api/operator/summary');
  await api('/api/kill-switch', { method: 'POST', body: { enabled: !summary.killSwitch.enabled, reason: 'operator_ui_test' } });
  await refresh();
});

refresh().catch(error => {
  qs('#readiness-card').innerHTML = `<span class="label">Error</span><strong>${error.message}</strong>`;
});
