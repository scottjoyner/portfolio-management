const POLICY_ROUTE = '/api/economics/intelligence/policy';

const state = {
  view: null,
  error: null,
  saving: false,
  dirty: false,
};

const money = value => Number.isFinite(Number(value))
  ? Number(value).toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 4 })
  : '—';

const escapeHtml = value => String(value ?? '')
  .replaceAll('&', '&amp;')
  .replaceAll('<', '&lt;')
  .replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;')
  .replaceAll("'", '&#039;');

function titleWords(value) {
  return String(value || 'unknown')
    .replaceAll('_', ' ')
    .replace(/\b\w/g, letter => letter.toUpperCase());
}

async function request(path, options = {}) {
  const response = await fetch(path, {
    method: options.method || 'GET',
    headers: { accept: 'application/json', 'content-type': 'application/json' },
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok || body.ok === false) {
    const message = Array.isArray(body.errors) && body.errors.length
      ? body.errors.join(', ')
      : body.error || `${path} returned ${response.status}`;
    throw new Error(message);
  }
  return body;
}

function injectStyles() {
  if (document.getElementById('intelligence-policy-styles')) return;
  const style = document.createElement('style');
  style.id = 'intelligence-policy-styles';
  style.textContent = `
    .intelligence-policy-card { grid-column: 1 / -1; }
    .intelligence-policy-form { display:grid; gap:16px; }
    .intelligence-policy-grid { display:grid; grid-template-columns:minmax(220px,1.3fr) repeat(3,minmax(140px,1fr)); gap:12px; }
    .intelligence-policy-grid label { display:flex; flex-direction:column; gap:7px; color:var(--muted,#9aa4b2); font-size:.78rem; }
    .intelligence-policy-grid select,.intelligence-policy-grid input { width:100%; min-height:42px; border:1px solid rgba(255,255,255,.12); border-radius:8px; background:rgba(255,255,255,.04); color:inherit; padding:9px 11px; }
    .intelligence-policy-toggles { display:flex; align-items:center; justify-content:space-between; gap:18px; flex-wrap:wrap; }
    .intelligence-policy-check { display:flex; align-items:center; gap:9px; color:var(--muted,#9aa4b2); font-size:.84rem; }
    .intelligence-policy-check input { width:18px; height:18px; }
    .intelligence-policy-save { min-width:170px; }
    .intelligence-policy-status { min-height:20px; margin:0; color:var(--muted,#9aa4b2); }
    .intelligence-policy-status.error { color:#ff8b8b; }
    .intelligence-policy-facts { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:10px; }
    .intelligence-policy-fact { padding:12px; border:1px solid rgba(255,255,255,.09); border-radius:8px; background:rgba(255,255,255,.025); }
    .intelligence-policy-fact small,.intelligence-policy-fact span { display:block; color:var(--muted,#9aa4b2); }
    .intelligence-policy-fact strong { display:block; margin:4px 0; }
    .intelligence-policy-description { padding:12px 14px; border-left:3px solid rgba(255,255,255,.2); background:rgba(255,255,255,.025); border-radius:0 8px 8px 0; }
    @media (max-width:900px) { .intelligence-policy-grid,.intelligence-policy-facts { grid-template-columns:1fr 1fr; } }
    @media (max-width:620px) { .intelligence-policy-grid,.intelligence-policy-facts { grid-template-columns:1fr; } .intelligence-policy-save { width:100%; } }
  `;
  document.head.append(style);
}

function markup() {
  return `
    <article id="intelligence-routing-policy" class="economic-card wide intelligence-policy-card">
      <div class="economic-card-head">
        <div><p class="section-kicker">INTELLIGENCE ROUTING</p><h2>Local fleet versus OpenRouter</h2></div>
        <span id="intelligence-policy-badge" class="badge">loading</span>
      </div>
      <form id="intelligence-policy-form" class="intelligence-policy-form">
        <div class="intelligence-policy-grid">
          <label>Routing mode
            <select id="intelligence-policy-mode">
              <option value="local_only">Local fleet only</option>
              <option value="economic_auto">Economic auto-selection</option>
              <option value="openrouter_allowed">OpenRouter eligible</option>
            </select>
          </label>
          <label>Daily OpenRouter cap (USD)
            <input id="intelligence-policy-daily-cap" type="number" min="0" max="10000" step="0.01" inputmode="decimal" />
          </label>
          <label>Per-request cap (USD)
            <input id="intelligence-policy-request-cap" type="number" min="0" max="10000" step="0.01" inputmode="decimal" />
          </label>
          <label>Minimum value coverage
            <input id="intelligence-policy-coverage" type="number" min="1" max="100" step="0.1" inputmode="decimal" />
          </label>
        </div>
        <div id="intelligence-policy-description" class="intelligence-policy-description"></div>
        <div id="intelligence-policy-facts" class="intelligence-policy-facts"></div>
        <div class="intelligence-policy-toggles">
          <label class="intelligence-policy-check"><input id="intelligence-policy-fallback" type="checkbox" /> Fall back to the local fleet when a remote comparison is blocked</label>
          <button id="intelligence-policy-save" class="economic-action-primary intelligence-policy-save" type="submit">Save routing policy</button>
        </div>
        <p id="intelligence-policy-status" class="intelligence-policy-status" aria-live="polite"></p>
      </form>
      <p class="economic-footer-note">This policy controls model-call eligibility and budgets. It never stores the OpenRouter key in the browser and never bypasses forecast, execution-cost, usage-reconciliation, or trade-approval gates.</p>
    </article>`;
}

function injectCard() {
  injectStyles();
  if (document.getElementById('intelligence-routing-policy')) return true;
  const grid = document.querySelector('#economics .economic-grid');
  if (!grid) return false;
  grid.insertAdjacentHTML('afterbegin', markup());
  bindEvents();
  return true;
}

function modeDescription(mode) {
  if (mode === 'economic_auto') {
    return '<strong>Economic auto-selection:</strong> a remote quote is eligible only when its expected decision improvement covers its cost by the configured multiple. A blocked comparison can fall back to the local fleet.';
  }
  if (mode === 'openrouter_allowed') {
    return '<strong>OpenRouter eligible:</strong> callers may explicitly request a paid model, subject to deployment credentials, the per-request and daily caps, and the existing economic decision gate.';
  }
  return '<strong>Local fleet only:</strong> paid remote calls are blocked even when OpenRouter credentials are installed. Local model routing remains available.';
}

function setBadge(label, status = 'warn') {
  const badge = document.getElementById('intelligence-policy-badge');
  if (!badge) return;
  badge.textContent = label;
  badge.className = `badge ${status === 'ok' ? 'badge-ok' : status === 'error' ? 'badge-err' : 'badge-warn'}`;
}

function renderFacts(view) {
  const capabilities = view?.capabilities || {};
  const spend = view?.spend || {};
  const facts = [
    ['Local fleet', capabilities.localConfigured ? 'Configured' : 'Not configured', capabilities.localConfigured ? 'Endpoints are present' : 'Set LOCAL_LLM_NODES_JSON or LOCAL_LLM_ENDPOINTS'],
    ['OpenRouter runtime', capabilities.openRouterAvailable ? 'Available' : 'Unavailable', capabilities.openRouterAvailable ? 'Execution flag and key are present' : 'Requires REMOTE_LLM_EXECUTION_ENABLED=true and OPENROUTER_API_KEY'],
    ['Remote committed today', money(spend.committedUsd), `${spend.committedRequests || 0} reserved or completed request(s)`],
    ['Remote cap remaining', money(spend.remainingDailyUsd), `${money(spend.actualUsd)} provider-reported actual cost`],
  ];
  const container = document.getElementById('intelligence-policy-facts');
  if (container) container.innerHTML = facts.map(([label, value, detail]) => `
    <div class="intelligence-policy-fact"><small>${escapeHtml(label)}</small><strong>${escapeHtml(value)}</strong><span>${escapeHtml(detail)}</span></div>`).join('');
}

function render(forceInputs = false) {
  if (!injectCard()) return;
  const view = state.view;
  const policy = view?.policy;
  const status = document.getElementById('intelligence-policy-status');
  if (!policy) {
    setBadge(state.error ? 'Unavailable' : 'Loading', state.error ? 'error' : 'warn');
    if (status) {
      status.textContent = state.error || 'Loading the persisted routing policy…';
      status.className = `intelligence-policy-status${state.error ? ' error' : ''}`;
    }
    return;
  }

  if (forceInputs || !state.dirty) {
    document.getElementById('intelligence-policy-mode').value = policy.mode;
    document.getElementById('intelligence-policy-daily-cap').value = policy.remoteSpendCapUsdPerDay;
    document.getElementById('intelligence-policy-request-cap').value = policy.remoteSpendCapUsdPerRequest;
    document.getElementById('intelligence-policy-coverage').value = policy.minimumRemoteValueCoverage;
    document.getElementById('intelligence-policy-fallback').checked = policy.fallbackToLocalOnRemoteBlock !== false;
  }

  const mode = document.getElementById('intelligence-policy-mode').value;
  const description = document.getElementById('intelligence-policy-description');
  if (description) description.innerHTML = modeDescription(mode);
  renderFacts(view);

  const remoteReady = view.capabilities?.openRouterAvailable === true;
  setBadge(
    mode === 'local_only' ? 'Local only' : remoteReady ? titleWords(mode) : 'Remote unavailable',
    mode === 'local_only' || remoteReady ? 'ok' : 'warn',
  );
  const save = document.getElementById('intelligence-policy-save');
  if (save) {
    save.disabled = state.saving;
    save.textContent = state.saving ? 'Saving…' : 'Save routing policy';
  }
  if (status) {
    status.textContent = state.error
      ? state.error
      : state.saving
        ? 'Persisting policy and budget limits…'
        : state.dirty
          ? 'Unsaved changes.'
          : policy.updatedAt
            ? `Saved ${new Date(policy.updatedAt).toLocaleString()}.`
            : 'Using fail-closed defaults until you save a policy.';
    status.className = `intelligence-policy-status${state.error ? ' error' : ''}`;
  }
}

function formValue() {
  return {
    mode: document.getElementById('intelligence-policy-mode').value,
    remoteSpendCapUsdPerDay: Number(document.getElementById('intelligence-policy-daily-cap').value),
    remoteSpendCapUsdPerRequest: Number(document.getElementById('intelligence-policy-request-cap').value),
    minimumRemoteValueCoverage: Number(document.getElementById('intelligence-policy-coverage').value),
    fallbackToLocalOnRemoteBlock: document.getElementById('intelligence-policy-fallback').checked,
  };
}

async function refresh(forceInputs = false) {
  try {
    state.view = await request(POLICY_ROUTE);
    state.error = null;
    if (forceInputs) state.dirty = false;
  } catch (error) {
    state.error = error.message;
  }
  render(forceInputs);
}

async function save(event) {
  event.preventDefault();
  if (state.saving) return;
  state.saving = true;
  state.error = null;
  render();
  try {
    state.view = await request(POLICY_ROUTE, { method: 'PUT', body: formValue() });
    state.dirty = false;
  } catch (error) {
    state.error = error.message;
  } finally {
    state.saving = false;
    render(true);
  }
}

function bindEvents() {
  const form = document.getElementById('intelligence-policy-form');
  if (!form || form.dataset.bound === 'true') return;
  form.dataset.bound = 'true';
  form.addEventListener('submit', save);
  form.addEventListener('input', () => {
    state.dirty = true;
    state.error = null;
    render();
  });
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden && !state.dirty) refresh();
  });
}

if (!injectCard()) {
  const observer = new MutationObserver(() => {
    if (injectCard()) {
      observer.disconnect();
      refresh(true);
    }
  });
  observer.observe(document.body, { childList: true, subtree: true });
} else {
  refresh(true);
}

setInterval(() => {
  if (!document.hidden && !state.dirty && !state.saving) refresh();
}, 10000);
