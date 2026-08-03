(() => {
  const BEARER_KEY = 'portfolio.operatorBearer';
  const CSRF_KEY = 'portfolio.operatorCsrf';
  const nativeFetch = window.fetch.bind(window);

  function value(key) {
    try { return sessionStorage.getItem(key) || ''; } catch { return ''; }
  }

  function sameOriginApi(input) {
    try {
      const raw = typeof input === 'string' || input instanceof URL ? input : input.url;
      const url = new URL(raw, window.location.href);
      return url.origin === window.location.origin && url.pathname.startsWith('/api/');
    } catch {
      return false;
    }
  }

  function methodOf(input, init = {}) {
    return String(init.method || (typeof input === 'object' && input?.method) || 'GET').toUpperCase();
  }

  window.fetch = function portfolioAuthenticatedFetch(input, init = {}) {
    if (!sameOriginApi(input)) return nativeFetch(input, init);
    const headers = new Headers(typeof input === 'object' && input?.headers ? input.headers : undefined);
    new Headers(init.headers || {}).forEach((headerValue, key) => headers.set(key, headerValue));
    const bearer = value(BEARER_KEY);
    const csrf = value(CSRF_KEY);
    if (bearer && !headers.has('authorization')) headers.set('authorization', `Bearer ${bearer}`);
    if (csrf && !['GET', 'HEAD', 'OPTIONS'].includes(methodOf(input, init)) && !headers.has('x-csrf-token')) {
      headers.set('x-csrf-token', csrf);
    }
    return nativeFetch(input, { ...init, headers });
  };

  function redact(token) {
    if (!token) return 'not configured';
    if (token.length < 9) return 'configured';
    return `${token.slice(0, 3)}…${token.slice(-3)}`;
  }

  function styles() {
    if (document.getElementById('operator-session-styles')) return;
    const style = document.createElement('style');
    style.id = 'operator-session-styles';
    style.textContent = `
      .operator-session-launcher { position:fixed; right:18px; bottom:18px; z-index:1200; border:1px solid rgba(255,255,255,.16); border-radius:999px; background:#111827; color:#f8fafc; padding:10px 15px; box-shadow:0 12px 35px rgba(0,0,0,.35); cursor:pointer; font:600 .78rem/1 system-ui,sans-serif; }
      .operator-session-panel { position:fixed; right:18px; bottom:66px; z-index:1200; width:min(390px,calc(100vw - 36px)); border:1px solid rgba(255,255,255,.14); border-radius:12px; background:#0b1220; color:#f8fafc; padding:18px; box-shadow:0 18px 55px rgba(0,0,0,.5); font-family:system-ui,sans-serif; }
      .operator-session-panel[hidden] { display:none; }
      .operator-session-panel h2 { margin:0 0 6px; font-size:1rem; }
      .operator-session-panel p { margin:0 0 14px; color:#9ca3af; font-size:.78rem; line-height:1.45; }
      .operator-session-panel label { display:flex; flex-direction:column; gap:6px; margin-top:11px; color:#cbd5e1; font-size:.76rem; }
      .operator-session-panel input { min-height:42px; border:1px solid rgba(255,255,255,.14); border-radius:8px; background:#111827; color:#f8fafc; padding:9px 11px; }
      .operator-session-actions { display:flex; gap:9px; margin-top:15px; }
      .operator-session-actions button { flex:1; min-height:38px; border-radius:8px; border:1px solid rgba(255,255,255,.15); background:#1f2937; color:#f8fafc; cursor:pointer; }
      .operator-session-actions button[type=submit] { background:#2563eb; border-color:#2563eb; }
      .operator-session-state { margin-top:12px !important; }
    `;
    document.head.append(style);
  }

  function render() {
    styles();
    if (document.getElementById('operator-session-launcher')) return;
    const launcher = document.createElement('button');
    launcher.id = 'operator-session-launcher';
    launcher.className = 'operator-session-launcher';
    launcher.type = 'button';
    launcher.textContent = value(BEARER_KEY) ? 'Operator session ✓' : 'Operator session';

    const panel = document.createElement('section');
    panel.id = 'operator-session-panel';
    panel.className = 'operator-session-panel';
    panel.hidden = true;
    panel.innerHTML = `
      <h2>Operator API session</h2>
      <p>Used only for same-origin <code>/api/</code> requests. Values stay in this browser tab's session storage and are never written into portfolio state.</p>
      <form id="operator-session-form">
        <label>Bearer token<input id="operator-session-bearer" type="password" autocomplete="off" spellcheck="false" placeholder="OPERATOR_ADMIN_TOKEN" /></label>
        <label>CSRF token<input id="operator-session-csrf" type="password" autocomplete="off" spellcheck="false" placeholder="OPERATOR_CSRF_TOKEN" /></label>
        <div class="operator-session-actions"><button type="submit">Save and reload</button><button id="operator-session-clear" type="button">Clear</button></div>
      </form>
      <p id="operator-session-state" class="operator-session-state"></p>`;

    document.body.append(panel, launcher);
    const bearerInput = panel.querySelector('#operator-session-bearer');
    const csrfInput = panel.querySelector('#operator-session-csrf');
    const state = panel.querySelector('#operator-session-state');
    state.textContent = `Bearer: ${redact(value(BEARER_KEY))}. CSRF: ${redact(value(CSRF_KEY))}.`;

    launcher.addEventListener('click', () => {
      panel.hidden = !panel.hidden;
      if (!panel.hidden) bearerInput.focus();
    });
    panel.querySelector('#operator-session-form').addEventListener('submit', event => {
      event.preventDefault();
      try {
        if (bearerInput.value.trim()) sessionStorage.setItem(BEARER_KEY, bearerInput.value.trim());
        if (csrfInput.value.trim()) sessionStorage.setItem(CSRF_KEY, csrfInput.value.trim());
      } catch { /* browser storage may be unavailable */ }
      window.dispatchEvent(new CustomEvent('portfolio-session-changed'));
      window.location.reload();
    });
    panel.querySelector('#operator-session-clear').addEventListener('click', () => {
      try {
        sessionStorage.removeItem(BEARER_KEY);
        sessionStorage.removeItem(CSRF_KEY);
      } catch { /* browser storage may be unavailable */ }
      window.dispatchEvent(new CustomEvent('portfolio-session-changed'));
      window.location.reload();
    });
    document.addEventListener('click', event => {
      if (!panel.hidden && !panel.contains(event.target) && event.target !== launcher) panel.hidden = true;
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', render, { once: true });
  else render();
})();
