import { upsertProductRecord, upsertProductRecords } from './productUpserts.mjs';

export const PRODUCT_MUTATION_TYPES = [
  'marketDataSnapshot',
  'agentBudget',
  'budgetApproval',
  'researchJob',
  'opportunity',
  'riskBreakdown',
  'agentCost'
];

async function ensureReady(store) {
  if (typeof store.checkMigrations === 'function') {
    const migrations = await store.checkMigrations();
    if (!migrations.ok) throw new Error(`postgres_migrations_not_ready: ${migrations.reason || 'unknown'}`);
  }
}

async function runTransaction(store, work) {
  await ensureReady(store);
  await store.query('BEGIN');
  try {
    const result = await work();
    await store.query('COMMIT');
    return result;
  } catch (error) {
    await store.query('ROLLBACK').catch(() => {});
    throw error;
  }
}

function asQuery(store) {
  return (sql, params = []) => store.query(sql, params);
}

export async function upsertProduct(store, type, record) {
  return runTransaction(store, async () => upsertProductRecord(asQuery(store), type, record));
}

export async function upsertProducts(store, type, records = []) {
  return runTransaction(store, async () => upsertProductRecords(asQuery(store), type, records));
}

export async function upsertProductBundle(store, bundle = {}) {
  return runTransaction(store, async () => {
    const results = {};
    for (const type of PRODUCT_MUTATION_TYPES) {
      const records = bundle[type] || bundle[`${type}s`] || [];
      if (!records.length) continue;
      results[type] = await upsertProductRecords(asQuery(store), type, records);
    }
    return results;
  });
}

export async function upsertMarketDataSnapshot(store, snapshot) {
  return upsertProduct(store, 'marketDataSnapshot', snapshot);
}

export async function upsertMarketDataSnapshots(store, snapshots = []) {
  return upsertProducts(store, 'marketDataSnapshot', snapshots);
}

export async function upsertAgentBudget(store, budget) {
  return upsertProduct(store, 'agentBudget', budget);
}

export async function upsertBudgetApproval(store, approval) {
  return upsertProduct(store, 'budgetApproval', approval);
}

export async function upsertResearchJob(store, job) {
  return upsertProduct(store, 'researchJob', job);
}

export async function upsertOpportunity(store, opportunity) {
  return upsertProduct(store, 'opportunity', opportunity);
}

export async function upsertRiskBreakdown(store, riskBreakdown) {
  return upsertProduct(store, 'riskBreakdown', riskBreakdown);
}

export async function upsertAgentCost(store, cost) {
  return upsertProduct(store, 'agentCost', cost);
}

export function attachProductMutations(store) {
  return Object.assign(store, {
    upsertProduct: (type, record) => upsertProduct(store, type, record),
    upsertProducts: (type, records) => upsertProducts(store, type, records),
    upsertProductBundle: bundle => upsertProductBundle(store, bundle),
    upsertMarketDataSnapshot: snapshot => upsertMarketDataSnapshot(store, snapshot),
    upsertMarketDataSnapshots: snapshots => upsertMarketDataSnapshots(store, snapshots),
    upsertAgentBudget: budget => upsertAgentBudget(store, budget),
    upsertBudgetApproval: approval => upsertBudgetApproval(store, approval),
    upsertResearchJob: job => upsertResearchJob(store, job),
    upsertOpportunity: opportunity => upsertOpportunity(store, opportunity),
    upsertRiskBreakdown: riskBreakdown => upsertRiskBreakdown(store, riskBreakdown),
    upsertAgentCost: cost => upsertAgentCost(store, cost)
  });
}
