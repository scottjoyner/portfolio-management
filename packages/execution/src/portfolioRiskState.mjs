import { stableHash } from './overseer.mjs';

const PENDING_EXECUTION_STATUSES = new Set(['draft', 'approved', 'submitted', 'partially_filled']);
const ACCOUNT_READY_STATUSES = new Set(['connected', 'active', 'ready']);
const HIGH_WATER_ACTIONS = new Set(['portfolio_high_water_initialized', 'portfolio_high_water_advanced']);

function finite(value, fallback = null) {
  if (value === null || value === undefined || value === '') return fallback;
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function timestamp(value) {
  if (value === null || value === undefined || value === '') return null;
  const ms = new Date(value).getTime();
  return Number.isFinite(ms) ? ms : null;
}

function iso(value) {
  const ms = timestamp(value);
  return ms == null ? null : new Date(ms).toISOString();
}

function rootAccounts(root) {
  if (Array.isArray(root?.accounts)) return root.accounts.filter(row => row && typeof row === 'object');
  if (root?.accounts && typeof root.accounts === 'object') {
    return Object.entries(root.accounts).map(([accountId, row]) => ({ accountId, ...(row || {}) }));
  }
  return [];
}

function highWaterFromEvent(event) {
  if (!HIGH_WATER_ACTIONS.has(event?.action)) return null;
  const payload = event?.payload && typeof event.payload === 'object' ? event.payload : {};
  const accountId = payload.accountId || event?.details || null;
  const highWaterNavUsd = finite(payload.highWaterNavUsd ?? payload.newHighWaterNavUsd, null);
  if (!accountId || highWaterNavUsd == null || highWaterNavUsd <= 0) return null;
  const at = iso(event?.at ?? event?.timestamp ?? event?.createdAt);
  return {
    accountId,
    highWaterNavUsd,
    highWaterAt: at,
    initializedAt: payload.initializedAt || at,
    updatedAt: at,
    source: 'append_only_audit',
  };
}

export function hydratePortfolioRiskState(state) {
  if (!state || typeof state !== 'object') return null;
  const existing = state.portfolioRiskState && typeof state.portfolioRiskState === 'object'
    ? state.portfolioRiskState
    : {};
  const records = new Map();
  let updatedAt = iso(existing.updatedAt);

  for (const row of rootAccounts(existing)) {
    const accountId = row?.accountId || row?.id || null;
    const highWaterNavUsd = finite(row?.highWaterNavUsd, null);
    if (!accountId || highWaterNavUsd == null || highWaterNavUsd <= 0) continue;
    records.set(accountId, {
      ...row,
      accountId,
      highWaterNavUsd,
    });
    const rowUpdatedAt = iso(row.updatedAt ?? row.highWaterAt);
    if ((timestamp(rowUpdatedAt) || 0) > (timestamp(updatedAt) || 0)) updatedAt = rowUpdatedAt;
  }

  for (const event of state.audit || []) {
    const candidate = highWaterFromEvent(event);
    if (!candidate) continue;
    const current = records.get(candidate.accountId);
    if (!current || candidate.highWaterNavUsd > Number(current.highWaterNavUsd || 0) + 1e-9) {
      records.set(candidate.accountId, {
        ...current,
        ...candidate,
        initializedAt: current?.initializedAt || candidate.initializedAt,
      });
    } else if (candidate.highWaterNavUsd === Number(current.highWaterNavUsd)) {
      const currentAt = timestamp(current.updatedAt ?? current.highWaterAt) || 0;
      const candidateAt = timestamp(candidate.updatedAt) || 0;
      if (candidateAt > currentAt) {
        records.set(candidate.accountId, {
          ...current,
          highWaterAt: candidate.highWaterAt,
          updatedAt: candidate.updatedAt,
          source: candidate.source,
        });
      }
    }
    if ((timestamp(candidate.updatedAt) || 0) > (timestamp(updatedAt) || 0)) updatedAt = candidate.updatedAt;
  }

  state.portfolioRiskState = {
    accounts: [...records.values()].sort((a, b) => String(a.accountId).localeCompare(String(b.accountId))),
    correlationMatrix: existing.correlationMatrix && typeof existing.correlationMatrix === 'object'
      ? existing.correlationMatrix
      : {},
    volatilityBySymbol: existing.volatilityBySymbol && typeof existing.volatilityBySymbol === 'object'
      ? existing.volatilityBySymbol
      : {},
    updatedAt: updatedAt || null,
  };
  return state.portfolioRiskState;
}

export function accountHasOpenRisk(state, accountId) {
  for (const position of state?.positions || []) {
    if (String(position?.status || 'open').toLowerCase() === 'closed') continue;
    if (!position?.accountId || position.accountId === accountId) return true;
  }
  for (const execution of state?.executions || []) {
    if (!PENDING_EXECUTION_STATUSES.has(String(execution?.status || '').toLowerCase())) continue;
    if (!execution?.accountId || execution.accountId === accountId) return true;
  }
  return false;
}

export function planPortfolioHighWaterUpdates(state, now = new Date().toISOString()) {
  const riskState = hydratePortfolioRiskState(state);
  const actions = [];
  const blockedAccounts = [];
  const records = new Map((riskState?.accounts || []).map(row => [row.accountId, row]));

  for (const account of state?.accounts || []) {
    if (!account?.id || !ACCOUNT_READY_STATUSES.has(String(account.status || '').toLowerCase())) continue;
    const navUsd = finite(account.nav, null);
    if (navUsd == null || navUsd <= 0) continue;
    const current = records.get(account.id);
    const currentHighWater = finite(current?.highWaterNavUsd, null);
    if (currentHighWater == null || currentHighWater <= 0) {
      if (accountHasOpenRisk(state, account.id)) {
        blockedAccounts.push(account.id);
        continue;
      }
      actions.push({
        action: 'portfolio_high_water_initialized',
        accountId: account.id,
        previousHighWaterNavUsd: null,
        highWaterNavUsd: navUsd,
        initializedAt: now,
        at: now,
      });
      continue;
    }
    if (navUsd > currentHighWater + 1e-9) {
      actions.push({
        action: 'portfolio_high_water_advanced',
        accountId: account.id,
        previousHighWaterNavUsd: currentHighWater,
        highWaterNavUsd: navUsd,
        initializedAt: current.initializedAt || current.highWaterAt || now,
        at: now,
      });
    }
  }
  return { actions, blockedAccounts: [...new Set(blockedAccounts)].sort() };
}

function auditEventForHighWater(action, actor) {
  const id = `audit-portfolio-high-water-${stableHash({
    action: action.action,
    accountId: action.accountId,
    highWaterNavUsd: action.highWaterNavUsd,
  }).slice(0, 24)}`;
  return {
    id,
    action: action.action,
    actor,
    at: action.at,
    details: action.accountId,
    payload: {
      accountId: action.accountId,
      previousHighWaterNavUsd: action.previousHighWaterNavUsd,
      highWaterNavUsd: action.highWaterNavUsd,
      initializedAt: action.initializedAt,
    },
  };
}

export function applyPortfolioHighWaterUpdates(
  state,
  { now = new Date().toISOString(), actor = 'portfolio-allocator' } = {},
) {
  const planned = planPortfolioHighWaterUpdates(state, now);
  if (!state || typeof state !== 'object') return { changed: false, ...planned, portfolioRiskState: null };
  state.audit = Array.isArray(state.audit) ? state.audit : [];
  const knownAuditIds = new Set(state.audit.map(row => row?.id).filter(Boolean));
  const root = hydratePortfolioRiskState(state) || { accounts: [], correlationMatrix: {}, volatilityBySymbol: {}, updatedAt: null };
  const records = new Map((root.accounts || []).map(row => [row.accountId, row]));
  let changed = false;

  for (const action of planned.actions) {
    const current = records.get(action.accountId) || {};
    records.set(action.accountId, {
      ...current,
      accountId: action.accountId,
      highWaterNavUsd: action.highWaterNavUsd,
      highWaterAt: action.at,
      initializedAt: current.initializedAt || action.initializedAt || action.at,
      updatedAt: action.at,
      source: 'append_only_audit',
    });
    const event = auditEventForHighWater(action, actor);
    if (!knownAuditIds.has(event.id)) {
      state.audit.push(event);
      knownAuditIds.add(event.id);
    }
    changed = true;
  }

  state.portfolioRiskState = {
    accounts: [...records.values()].sort((a, b) => String(a.accountId).localeCompare(String(b.accountId))),
    correlationMatrix: root.correlationMatrix || {},
    volatilityBySymbol: root.volatilityBySymbol || {},
    updatedAt: changed ? now : root.updatedAt || null,
  };
  return {
    changed,
    actions: planned.actions,
    blockedAccounts: planned.blockedAccounts,
    portfolioRiskState: state.portfolioRiskState,
  };
}
