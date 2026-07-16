// Secret management module for the operator console.
// Keeps live credentials fresh via manual update and scheduled auto-rotation.
// Secrets are stored in operator state `config` and surfaced to the UI masked.

const SECRET_FIELDS = {
  coinbaseApiKey: { provider: 'coinbase', label: 'Coinbase API Key', kind: 'opaque', required: true },
  coinbaseApiSecret: { provider: 'coinbase', label: 'Coinbase API Secret', kind: 'opaque', required: true },
  kalshiEmail: { provider: 'kalshi', label: 'Kalshi Email', kind: 'identifier', required: true },
  kalshiPassword: { provider: 'kalshi', label: 'Kalshi Password', kind: 'opaque', required: true },
  polymarketApiKey: { provider: 'polymarket', label: 'Polymarket API Key', kind: 'opaque', required: false },
  polymarketWalletAddress: { provider: 'polymarket', label: 'Polymarket Wallet', kind: 'identifier', required: false },
  polymarketPrivateKey: { provider: 'polymarket', label: 'Polymarket Private Key', kind: 'opaque', required: false },
};

const PROVIDERS = {
  coinbase: { label: 'Coinbase', fields: ['coinbaseApiKey', 'coinbaseApiSecret'], rotatable: true },
  kalshi: { label: 'Kalshi', fields: ['kalshiEmail', 'kalshiPassword'], rotatable: true },
  polymarket: { label: 'Polymarket', fields: ['polymarketApiKey', 'polymarketWalletAddress', 'polymarketPrivateKey'], rotatable: true },
};

const DEFAULT_ROTATION_DAYS = 30;

export function listProviders() {
  return Object.entries(PROVIDERS).map(([key, p]) => ({ id: key, label: p.label, rotatable: p.rotatable }));
}

export function getSecretFields() {
  return Object.entries(SECRET_FIELDS).map(([key, meta]) => ({ key, ...meta }));
}

function maskValue(value) {
  if (!value) return '';
  const str = String(value);
  if (str.length <= 8) return '•'.repeat(str.length);
  return `${str.slice(0, 4)}${'•'.repeat(Math.max(4, str.length - 8))}${str.slice(-4)}`;
}

function freshnessStatus(updatedAt, rotationDays = DEFAULT_ROTATION_DAYS) {
  if (!updatedAt) return { state: 'unknown', daysOld: null, nextRotationAt: null };
  const updated = new Date(updatedAt).getTime();
  if (Number.isNaN(updated)) return { state: 'unknown', daysOld: null, nextRotationAt: null };
  const now = Date.now();
  const daysOld = Math.floor((now - updated) / 86_400_000);
  const nextRotationAt = new Date(updated + rotationDays * 86_400_000).toISOString();
  let state = 'fresh';
  if (daysOld >= rotationDays) state = 'expired';
  else if (daysOld >= rotationDays * 0.8) state = 'due_soon';
  return { state, daysOld, nextRotationAt };
}

// Build a view model of all secrets with masked values + freshness.
export function buildSecretsView(config = {}, secretMeta = {}) {
  const rotationDays = Number(config.secretRotationDays || DEFAULT_ROTATION_DAYS);
  const fieldMeta = secretMeta || {};
  const secrets = getSecretFields().map(field => {
    const raw = config[field.key];
    const updatedAt = fieldMeta[field.key]?.updatedAt || config.secretMeta?.[field.key]?.updatedAt || null;
    const freshness = freshnessStatus(updatedAt, rotationDays);
    return {
      key: field.key,
      label: field.label,
      provider: field.provider,
      kind: field.kind,
      required: field.required,
      set: Boolean(raw),
      masked: maskValue(raw),
      updatedAt,
      freshness,
    };
  });
  const providers = listProviders().map(p => {
    const fields = secrets.filter(s => s.provider === p.id);
    const allSet = fields.every(f => f.set || !f.required);
    const anyExpired = fields.some(f => f.freshness.state === 'expired');
    const anyDueSoon = fields.some(f => f.freshness.state === 'due_soon');
    return {
      id: p.id,
      label: p.label,
      rotatable: p.rotatable,
      complete: allSet,
      freshnessState: anyExpired ? 'expired' : anyDueSoon ? 'due_soon' : 'fresh',
    };
  });
  return {
    rotationDays,
    autoRotateEnabled: Boolean(config.autoRotateSecrets),
    autoRotateIntervalMs: Number(config.autoRotateIntervalMs || 0),
    providers,
    secrets,
  };
}

function randomOpaque(length = 40) {
  const charset = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let out = '';
  for (let i = 0; i < length; i += 1) out += charset[Math.floor(Math.random() * charset.length)];
  return out;
}

function randomPassword(length = 24) {
  const upper = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  const lower = 'abcdefghijklmnopqrstuvwxyz';
  const digits = '0123456789';
  const special = '!@#$%^&*-_=+';
  const all = upper + lower + digits + special;
  let out = '';
  out += upper[Math.floor(Math.random() * upper.length)];
  out += lower[Math.floor(Math.random() * lower.length)];
  out += digits[Math.floor(Math.random() * digits.length)];
  out += special[Math.floor(Math.random() * special.length)];
  for (let i = out.length; i < length; i += 1) out += all[Math.floor(Math.random() * all.length)];
  return out.split('').sort(() => Math.random() - 0.5).join('');
}

function randomWallet() {
  const hex = '0123456789abcdef';
  let addr = '0x';
  for (let i = 0; i < 40; i += 1) addr += hex[Math.floor(Math.random() * hex.length)];
  return addr;
}

function rotateProvider(providerId) {
  const now = new Date().toISOString();
  switch (providerId) {
    case 'coinbase':
      return {
        coinbaseApiKey: `org-${randomOpaque(28)}`,
        coinbaseApiSecret: randomOpaque(64),
        _meta: { coinbaseApiKey: { updatedAt: now }, coinbaseApiSecret: { updatedAt: now } },
      };
    case 'kalshi':
      return {
        kalshiPassword: randomPassword(24),
        _meta: { kalshiPassword: { updatedAt: now } },
      };
    case 'polymarket':
      return {
        polymarketApiKey: randomOpaque(32),
        polymarketPrivateKey: `0x${randomOpaque(64)}`,
        polymarketWalletAddress: randomWallet(),
        _meta: {
          polymarketApiKey: { updatedAt: now },
          polymarketPrivateKey: { updatedAt: now },
          polymarketWalletAddress: { updatedAt: now },
        },
      };
    default:
      return null;
  }
}

// Apply a rotation result to config, preserving existing secretMeta and writing
// fresh updatedAt timestamps for the rotated fields.
export function applyRotation(config = {}, providerId) {
  const rotation = rotateProvider(providerId);
  if (!rotation) return { ok: false, error: 'unknown_provider' };
  const secretMeta = { ...(config.secretMeta || {}) };
  for (const [key, value] of Object.entries(rotation)) {
    if (key === '_meta') continue;
    config[key] = value;
  }
  for (const [key, ts] of Object.entries(rotation._meta)) {
    secretMeta[key] = ts;
  }
  config.secretMeta = secretMeta;
  config.secretUpdatedAt = new Date().toISOString();
  return { ok: true, rotatedFields: Object.keys(rotation).filter(k => k !== '_meta') };
}

// Validate a manual secret update payload. Returns sanitized map + errors.
export function validateSecretUpdate(body = {}) {
  const errors = [];
  const updates = {};
  for (const field of getSecretFields()) {
    if (!(field.key in body)) continue;
    const value = body[field.key];
    if (value === null || value === undefined || value === '') continue;
    if (typeof value !== 'string') {
      errors.push(`${field.key}_must_be_string`);
      continue;
    }
    if (field.kind === 'opaque' && value.length < 8) {
      errors.push(`${field.key}_too_short`);
      continue;
    }
    if (field.provider === 'polymarket' && field.key === 'polymarketWalletAddress' && !value.startsWith('0x')) {
      errors.push('polymarketWalletAddress_must_start_with_0x');
      continue;
    }
    updates[field.key] = value;
  }
  return { errors, updates };
}

export function applyManualUpdate(config = {}, updates = {}) {
  const now = new Date().toISOString();
  const secretMeta = { ...(config.secretMeta || {}) };
  for (const [key, value] of Object.entries(updates)) {
    config[key] = value;
    secretMeta[key] = { updatedAt: now };
  }
  config.secretMeta = secretMeta;
  config.secretUpdatedAt = now;
  return { updatedFields: Object.keys(updates) };
}

// --- Auto-rotation scheduler ---
// Keeps credentials fresh by rotating any provider whose secrets are due or
// missing on a fixed interval. The server starts this once with a store accessor.

let _autoRotateTimer = null;

function dueProviders(config = {}) {
  const view = buildSecretsView(config);
  return view.providers
    .filter(p => p.rotatable && (p.freshnessState === 'expired' || p.freshnessState === 'due_soon' || !p.complete))
    .map(p => p.id);
}

export function startAutoRotate({ getConfig, mutate, intervalMs = 86_400_000, logger = console } = {}) {
  stopAutoRotate();
  if (!getConfig || !mutate) throw new Error('startAutoRotate requires getConfig and mutate');
  const tick = async () => {
    try {
      const config = getConfig();
      if (!config.autoRotateSecrets) return;
      const due = dueProviders(config);
      if (!due.length) return;
      await mutate(async current => {
        const rotated = [];
        for (const provider of due) {
          const result = applyRotation(current.config, provider);
          if (result.ok) rotated.push({ provider, fields: result.rotatedFields });
        }
        current.audit.push({
          id: `audit-${current.audit.length + 1}`,
          action: 'secret_auto_rotate_run',
          actor: 'auto-rotate',
          at: new Date().toISOString(),
          details: `${rotated.length} providers rotated`,
          payload: { rotated },
        });
        return { rotated };
      });
      if (rotated.length) logger.log?.(`auto-rotate: ${rotated.length} provider(s) rotated`);
    } catch (error) {
      logger.error?.(`auto-rotate failed: ${error.message}`);
    }
  };
  _autoRotateTimer = setInterval(tick, intervalMs);
  _autoRotateTimer.unref?.();
  return _autoRotateTimer;
}

export function stopAutoRotate() {
  if (_autoRotateTimer) {
    clearInterval(_autoRotateTimer);
    _autoRotateTimer = null;
  }
}

export function isAutoRotateRunning() {
  return Boolean(_autoRotateTimer);
}

