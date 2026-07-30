import {
  handleOperatorRoute as legacyHandleOperatorRoute,
  persistRouteArtifacts as legacyPersistRouteArtifacts,
  routeMatch,
} from './operatorRouterLegacy.mjs';

export { routeMatch };

const BUNDLE_COMPONENT_METHODS = new Set([
  'upsertMarketDataSnapshots',
  'upsertBudgetApproval',
  'upsertResearchJob',
  'upsertAgentCost',
  'upsertOpportunity',
  'upsertRiskBreakdown',
]);

const STATE_COLLECTIONS = {
  marketDataSnapshots: 'marketDataSnapshots',
  budgetApprovals: 'budgetApprovals',
  researchJobs: 'researchJobs',
  opportunities: 'opportunities',
  riskBreakdowns: 'riskBreakdowns',
  agentCostLedger: 'agentCostLedger',
};

function identity(value) {
  if (Array.isArray(value)) return value.map(identity).join(',');
  if (!value || typeof value !== 'object') return String(value);
  return String(
    value.id
      || value.jobId
      || value.agentId
      || value.scopeId
      || value.symbol
      || JSON.stringify(value),
  );
}

function deduplicatingStore(store) {
  if (!store || typeof store !== 'object') return store;
  const hasBundle = typeof store.upsertOpportunityBundle === 'function';
  const seen = new Set();
  return new Proxy(store, {
    get(target, property) {
      const value = Reflect.get(target, property, target);
      if (typeof value !== 'function') return value;
      if (BUNDLE_COMPONENT_METHODS.has(property)) {
        if (hasBundle) return async () => undefined;
        return async (...args) => {
          const key = `${String(property)}:${args.map(identity).join('|')}`;
          if (seen.has(key)) return undefined;
          seen.add(key);
          return value.apply(target, args);
        };
      }
      return value.bind(target);
    },
  });
}

function collectionIds(state = {}) {
  return Object.fromEntries(Object.entries(STATE_COLLECTIONS).map(([bundleKey, stateKey]) => [
    bundleKey,
    new Set((state[stateKey] || []).map(identity)),
  ]));
}

function stateDelta(state = {}, before = {}) {
  return Object.fromEntries(Object.entries(STATE_COLLECTIONS).map(([bundleKey, stateKey]) => [
    bundleKey,
    (state[stateKey] || []).filter(row => !before[bundleKey]?.has(identity(row))),
  ]));
}

function mergeBundles(...bundles) {
  const merged = {};
  for (const key of Object.keys(STATE_COLLECTIONS)) {
    const rows = bundles.flatMap(bundle => bundle?.[key] || []);
    const seen = new Set();
    merged[key] = rows.filter(row => {
      const keyValue = identity(row);
      if (seen.has(keyValue)) return false;
      seen.add(keyValue);
      return true;
    });
  }
  return merged;
}

function responseBundle(result = {}) {
  return {
    marketDataSnapshots: result.snapshots || result.marketDataSnapshots || [],
    budgetApprovals: [result.budgetApproval].filter(Boolean),
    researchJobs: [result.job, ...(result.jobs || [])].filter(Boolean),
    opportunities: [result.opportunity, ...(result.opportunities || [])].filter(Boolean),
    riskBreakdowns: [result.riskBreakdown, ...(result.riskBreakdowns || [])].filter(Boolean),
    agentCostLedger: [result.ledger, ...(result.ledgers || [])].filter(Boolean),
  };
}

async function persistOne(store, method, value) {
  if (value && typeof store?.[method] === 'function') await store[method](value);
}

async function persistMany(store, method, values = []) {
  if (!Array.isArray(values) || !values.length || typeof store?.[method] !== 'function') return;
  await store[method](values);
}

export async function persistRouteArtifacts(store, result = {}) {
  if (!result || result.errors?.length) return;
  const bundle = result.marketDataSnapshots || result.agentCostLedger
    ? result
    : responseBundle(result);
  if (typeof store?.upsertOpportunityBundle === 'function' && Object.values(bundle).some(rows => rows.length)) {
    await store.upsertOpportunityBundle(bundle);
    return;
  }

  await persistMany(store, 'upsertMarketDataSnapshots', bundle.marketDataSnapshots);
  for (const row of bundle.budgetApprovals || []) await persistOne(store, 'upsertBudgetApproval', row);
  for (const row of bundle.researchJobs || []) await persistOne(store, 'upsertResearchJob', row);
  for (const row of bundle.agentCostLedger || []) await persistOne(store, 'upsertAgentCost', row);
  for (const row of bundle.opportunities || []) await persistOne(store, 'upsertOpportunity', row);
  for (const row of bundle.riskBreakdowns || []) await persistOne(store, 'upsertRiskBreakdown', row);
}

export async function handleOperatorRoute(args) {
  const before = collectionIds(args?.state || args?.store?.state || {});
  const wrappedStore = deduplicatingStore(args?.store);
  const result = await legacyHandleOperatorRoute({ ...args, store: wrappedStore });
  if (typeof args?.store?.upsertOpportunityBundle !== 'function') {
    const responseArtifacts = responseBundle(result?.body || result || {});
    const deltaArtifacts = stateDelta(args?.state || args?.store?.state || {}, before);
    await persistRouteArtifacts(wrappedStore, mergeBundles(responseArtifacts, deltaArtifacts));
  }
  handleOperatorRoute._execEngine = legacyHandleOperatorRoute._execEngine;
  handleOperatorRoute._arbitrageCache = legacyHandleOperatorRoute._arbitrageCache;
  return result;
}

for (const property of ['_execEngine', '_arbitrageCache']) {
  Object.defineProperty(handleOperatorRoute, property, {
    configurable: true,
    enumerable: false,
    get() { return legacyHandleOperatorRoute[property]; },
    set(value) { legacyHandleOperatorRoute[property] = value; },
  });
}

export { legacyPersistRouteArtifacts };
