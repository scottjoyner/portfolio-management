import { reconcileModelUsage } from '../../../packages/economics/src/economicDecisionEngine.mjs';
import { fetchOpenRouterGenerationUsage } from './openRouterExecution.mjs';

function finite(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function positive(value, fallback) {
  const number = finite(value, fallback);
  return number > 0 ? number : fallback;
}

function dueAt(quote, now) {
  const next = quote.nextUsageReconciliationAt ? new Date(quote.nextUsageReconciliationAt).getTime() : 0;
  return !Number.isFinite(next) || next <= new Date(now).getTime();
}

export function pendingRemoteUsageQuotes(state, options = {}) {
  const now = options.now instanceof Date ? options.now.toISOString() : options.now || new Date().toISOString();
  const limit = Math.max(1, Math.floor(positive(options.limit, 10)));
  return (state?.modelUsageLedger || [])
    .filter(quote => quote.status === 'usage_pending')
    .filter(quote => quote.localOrRemote === 'remote')
    .filter(quote => quote.generationId)
    .filter(quote => quote.requiresManualReconciliation !== true)
    .filter(quote => dueAt(quote, now))
    .sort((a, b) => new Date(a.nextUsageReconciliationAt || a.completedAt || a.requestedAt || 0) - new Date(b.nextUsageReconciliationAt || b.completedAt || b.requestedAt || 0))
    .slice(0, limit);
}

export async function fetchPendingRemoteUsage(state, options = {}) {
  const now = options.now instanceof Date ? options.now.toISOString() : options.now || new Date().toISOString();
  const candidates = pendingRemoteUsageQuotes(state, { ...options, now });
  const results = [];
  for (const quote of candidates) {
    const result = await fetchOpenRouterGenerationUsage({
      generationId: quote.generationId,
      env: options.env || process.env,
      fetchImpl: options.fetchImpl || globalThis.fetch,
      timeoutMs: options.timeoutMs,
    });
    results.push({
      quoteId: quote.id,
      generationId: quote.generationId,
      checkedAt: now,
      ...result,
    });
  }
  return results;
}

export function applyPendingRemoteUsage(state, results = [], options = {}) {
  const now = options.now instanceof Date ? options.now.toISOString() : options.now || new Date().toISOString();
  const baseRetrySeconds = positive(options.baseRetrySeconds, 30);
  const maxRetrySeconds = positive(options.maxRetrySeconds, 3600);
  const maxAttempts = Math.max(1, Math.floor(positive(options.maxAttempts, 20)));
  const reconciled = [];
  const pending = [];
  const manual = [];
  const errors = [];

  for (const result of results || []) {
    const quote = state.modelUsageLedger?.find(row => row.id === result.quoteId);
    if (!quote || quote.status !== 'usage_pending' || quote.generationId !== result.generationId) {
      errors.push({ quoteId: result.quoteId, error: 'usage_reconciliation_quote_mismatch' });
      continue;
    }

    quote.usageReconciliationAttempts = Number(quote.usageReconciliationAttempts || 0) + 1;
    quote.lastUsageReconciliationAt = result.checkedAt || now;
    quote.lastUsageReconciliationError = result.error || null;

    if (result.ok && result.usage) {
      const applied = reconcileModelUsage(state, {
        quoteId: quote.id,
        generationId: quote.generationId,
        usage: result.usage,
        jobStatus: 'completed',
      }, now);
      if (applied.errors) {
        errors.push({ quoteId: quote.id, error: applied.errors[0] });
        continue;
      }
      applied.modelUsage.requiresUsageReconciliation = false;
      applied.modelUsage.requiresManualReconciliation = false;
      applied.modelUsage.uncertainProviderOutcome = false;
      applied.modelUsage.usageReconciliationStatus = 'reconciled';
      applied.modelUsage.nextUsageReconciliationAt = null;
      const job = state.researchJobs?.find(row => row.modelQuoteId === quote.id || row.id === quote.researchJobId);
      if (job) {
        job.requiresUsageReconciliation = false;
        job.requiresManualReconciliation = false;
        job.uncertainProviderOutcome = false;
        job.usageReconciliationStatus = 'reconciled';
      }
      const cost = state.agentCostLedger?.find(row => row.modelQuoteId === quote.id || (job && row.jobId === job.id));
      if (cost) {
        cost.recoveryStatus = 'provider_usage_reconciled';
        cost.requiresManualReconciliation = false;
      }
      reconciled.push({ quoteId: quote.id, generationId: quote.generationId, actualCostUsd: applied.modelUsage.actualCostUsd });
      continue;
    }

    if (quote.usageReconciliationAttempts >= maxAttempts || result.pending === false) {
      quote.requiresManualReconciliation = true;
      quote.requiresUsageReconciliation = false;
      quote.usageReconciliationStatus = 'manual_required';
      quote.nextUsageReconciliationAt = null;
      quote.failureReason = result.error || 'provider_usage_reconciliation_exhausted';
      const job = state.researchJobs?.find(row => row.modelQuoteId === quote.id || row.id === quote.researchJobId);
      if (job) {
        job.requiresManualReconciliation = true;
        job.requiresUsageReconciliation = false;
        job.usageReconciliationStatus = 'manual_required';
      }
      manual.push({ quoteId: quote.id, generationId: quote.generationId, error: quote.failureReason });
      continue;
    }

    const delaySeconds = Math.min(maxRetrySeconds, baseRetrySeconds * (2 ** Math.max(0, quote.usageReconciliationAttempts - 1)));
    quote.requiresUsageReconciliation = true;
    quote.usageReconciliationStatus = 'pending';
    quote.nextUsageReconciliationAt = new Date(new Date(now).getTime() + delaySeconds * 1000).toISOString();
    pending.push({
      quoteId: quote.id,
      generationId: quote.generationId,
      attempts: quote.usageReconciliationAttempts,
      nextUsageReconciliationAt: quote.nextUsageReconciliationAt,
      error: result.error || 'provider_usage_cost_pending',
    });
  }

  return { reconciled, pending, manual, errors };
}
