const qs = selector => document.querySelector(selector);
const fmt = value => JSON.stringify(value, null, 2);

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { 'content-type': 'application/json', ...(options.headers || {}) },
    ...options,
    body: options.body ? JSON.stringify(options.body) : undefined
  });
  return res.json();
}

function addNavLink(nav, href, label, position = 'beforeend') {
  if (!nav || nav.querySelector(`a[href="${href}"]`)) return;
  nav.insertAdjacentHTML(position, `<a href="${href}">${label}</a>`);
}

function ensureP1Panels() {
  if (qs('#p1-panels')) return;
  const anchor = qs('#strategies');
  const wrapper = document.createElement('section');
  wrapper.id = 'p1-panels';
  wrapper.innerHTML = `
    <section class="panel" id="accounts">
      <div class="panel-heading"><div><p class="eyebrow">P1.1</p><h2>Accounts and instruments</h2></div></div>
      <div class="cards" id="account-cards"></div>
      <div class="table-wrap"><table><thead><tr><th>Symbol</th><th>Name</th><th>Class</th><th>Venue</th></tr></thead><tbody id="instrument-rows"></tbody></table></div>
    </section>
    <section class="panel" id="templates">
      <div class="panel-heading"><div><p class="eyebrow">P1.3</p><h2>Strategy templates</h2></div><button id="create-template-strategy">Create from template</button></div>
      <div class="cards" id="template-cards"></div>
    </section>
    <section class="panel" id="paper">
      <div class="panel-heading"><div><p class="eyebrow">P1.6</p><h2>Paper execution</h2></div><button id="start-paper">Start paper execution</button></div>
      <div class="cards" id="paper-cards"></div>
    </section>
    <section class="panel" id="execution-adapters">
      <div class="panel-heading"><div><p class="eyebrow">Execution Adapters</p><h2>Broker adapter registry</h2></div><button id="refresh-adapters">Refresh</button></div>
      <div class="table-wrap"><table><thead><tr><th>Adapter</th><th>Venue</th><th>Mode</th><th>Status</th></tr></thead><tbody id="adapter-rows"></tbody></table></div>
    </section>
  `;
  anchor.parentNode.insertBefore(wrapper, anchor);
  const nav = document.querySelector('.tabs');
  addNavLink(nav, '#accounts', 'Accounts', 'afterbegin');
  addNavLink(nav, '#templates', 'Templates', 'afterbegin');
  addNavLink(nav, '#paper', 'Paper');
  addNavLink(nav, '#execution-adapters', 'Adapters');
}

function renderAccounts(accounts) {
  qs('#account-cards').innerHTML = accounts.map(account => `
    <article class="card">
      <h3>${account.name}</h3>
      <p><span class="badge">${account.status}</span> ${account.provider}</p>
      <p><strong>${account.currency} ${Number(account.nav || 0).toLocaleString()}</strong> NAV</p>
      <p class="label">Cash ${Number(account.cash || 0).toLocaleString()}</p>
    </article>
  `).join('');
}

function renderInstruments(instruments) {
  qs('#instrument-rows').innerHTML = instruments.map(i => `<tr><td>${i.symbol}</td><td>${i.name}</td><td>${i.assetClass}</td><td>${i.venue}</td></tr>`).join('');
}

function renderTemplates(templates) {
  qs('#template-cards').innerHTML = templates.map(t => `
    <article class="card">
      <h3>${t.name}</h3>
      <p><span class="badge">${t.riskLevel}</span></p>
      <p>${t.description}</p>
      <pre>${fmt(t.parameterSchema)}</pre>
    </article>
  `).join('');
}

function renderPaper(executions) {
  qs('#paper-cards').innerHTML = executions.map(execution => `
    <article class="card">
      <h3>${execution.id}</h3>
      <p><span class="badge">${execution.status}</span> ${execution.mode || 'paper'}</p>
      <p class="label">Strategy ${execution.strategyId}</p>
      <button data-stop-paper="${execution.id}" ${execution.status !== 'running' ? 'disabled' : ''}>Stop</button>
    </article>
  `).join('') || '<p class="label">No paper executions yet.</p>';
  document.querySelectorAll('[data-stop-paper]').forEach(button => {
    button.addEventListener('click', async () => {
      await api(`/api/paper-executions/${button.dataset.stopPaper}/stop`, { method: 'POST', body: { reason: 'operator_ui_stop' } });
      await refreshP1();
    });
  });
}

function renderAdapters(adapters) {
  qs('#adapter-rows').innerHTML = (adapters || []).map(a => `
    <tr>
      <td>${a.name}</td>
      <td><span class="badge">${a.venue}</span></td>
      <td><span class="badge">${a.mode}</span></td>
      <td><span class="badge ${a.connected ? 'success' : 'blocked'}">${a.connected ? 'Connected' : 'Not connected'}</span></td>
    </tr>
  `).join('') || '<tr><td colspan="4" class="label">No adapters registered.</td></tr>';
}

export async function refreshP1() {
  ensureP1Panels();
  const [accounts, instruments, templates, paper, adapters] = await Promise.all([
    api('/api/accounts'),
    api('/api/instruments'),
    api('/api/strategy-templates'),
    api('/api/paper-executions'),
    api('/api/execution/adapters').catch(() => ({ adapters: [] }))
  ]);
  renderAccounts(accounts.accounts || []);
  renderInstruments(instruments.instruments || []);
  renderTemplates(templates.templates || []);
  renderPaper(paper.executions || []);
  renderAdapters(adapters.adapters || []);
}

export function wireP1Actions(baseRefresh) {
  ensureP1Panels();
  qs('#create-template-strategy')?.addEventListener('click', async () => {
    const templates = await api('/api/strategy-templates');
    const template = templates.templates?.[0];
    if (!template) return;
    await api('/api/strategies/from-template', { method: 'POST', body: { templateId: template.id, name: `${template.name} ${Date.now()}` } });
    await baseRefresh();
    await refreshP1();
  });
  qs('#refresh-adapters')?.addEventListener('click', async () => { await refreshP1(); });
  qs('#start-paper')?.addEventListener('click', async () => {
    const strategies = await api('/api/strategies');
    const approvals = await api('/api/approvals');
    const approval = approvals.approvals?.find(a => a.status === 'approved') || approvals.approvals?.[0];
    if (approval && approval.status !== 'approved') await api(`/api/approvals/${approval.id}/decision`, { method: 'POST', body: { status: 'approved', reviewer: 'operator-ui', reason: 'paper test approval' } });
    const strategyId = approval?.strategyId || strategies.strategies?.[0]?.id;
    if (!strategyId) return;
    await api('/api/paper-executions', { method: 'POST', body: { strategyId, accountId: 'acct-paper-primary' } });
    await baseRefresh();
    await refreshP1();
  });
}
