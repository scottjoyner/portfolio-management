import { reconcileModelUsage } from '../../../packages/economics/src/economicDecisionEngine.mjs';

function finite(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function positive(value, fallback) {
  const number = finite(value, fallback);
  return number > 0 ? number : fallback;
}

function iso(value = new Date()) {
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? new Date().toISOString() : date.toISOString();
}

function generationUsage(payload = {}) {
  const data = payload?.data || payload || {};
  const cost = finite(data.total_cost ?? data.usage ?? data.cost, null);
  if (cost == null || cost < 0) return null;
  return {
    prompt_tokens: finite(data.tokens_prompt ?? data.native_tokens_prompt, 0),
    completion_tokens: finite(data.tokens_completion ?? data.native_tokens_completion, 0),
    total_tokens: finite(
      data.total_tokens,
      finite(data.tokens_prompt ?? data.native_tokens_prompt, 0)
        + finite(data.tokens_completion ?? data.native_tokens_completion, 0),
    ),
    cost,
    total_cost: cost,
    native_tokens_reasoning: finite(data.native_tokens_reasoning, 0),
    native_tokens_cached: finite(data.native_tokens_cached, 0),
    upstream_inference_cost: finite(data.upstream_inference_cost, null),
    provider_name: data.provider_name || null,
    request_id: data.request_id || null,
    generation_time: finite(data.generation_time, null),
    latency: finite(data.latency, null),
  };
}

function retryDelayMs(attempt, env = process.env) {
  const base = positive(env.OPENROUTER_RECONCILIATION_RETRY_MS, 30000);
  const maximum = positive(env.OPENROUTER_RECONCILIATION_MAX_RETRY_MS, 3600000);
  return Math.min(maximum, base * (2 ** Math.max(0, attempt - 1)));
}

function maxAttempts(env = process.env) {
  return Math.max(1, Math.floor(positive(env.OPENROUTER_RECONCILIATION_MAX_ATTEMPTS, 8)));
}

function pendingRows(state = {}, now = new Date()) {
  const nowMs = now instanceof Date ? now.getTime() : new Date(now).getTime();
  return (state.modelUsageLedger || []).filter(row => {
    if (row.localOrRemote !== 'remote' || row.status !== 'usage_pending' || !row.generationId) return false;
    if (row.reconciliationStatus === 'exhausted' || row.requiresManualReconciliation === true) return false;
    const nextAt = row.nextReconciliationAt ? new Date(row.nextReconciliationAt).getTime() : 0;
    return !Number.isFinite(nextAt) || nextAt <= nowMs;
  });
}

export async function fetchOpenRouterGenerationUsage({ generationId, env = process.env, fetchImpl = globalThis.fetch } = {}) {
  if (!generationId) throw new Error('openrouter_generation_id_required');
  if (!env.OPENROUTER_API_KEY) throw new Error('openrouter_api_key_required_for_reconciliation');
  if (typeof fetchImpl !== 'function') throw new Error('fetch_unavailable');

  const controller = new AbortController();
  const timeoutMs = positive(env.OPENROUTER_RECONCILIATION_TIMEOUT_MS, 10000);
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const endpoint = env.OPENROUTER_GENERATION_URL || 'https://openrouter.ai/api/v1/generation';
  const headers = {
    accept: 'application/json',
    authorization: `Bearer ${env.OPENROUTER_API_KEY}`,
  };
  if (env.OPENROUTER_APP_URL) headers['HTTP-Referer'] = env.OPENROUTER_APP_URL;
  if (env.OPENROUTER_APP_NAME) headers['X-Title'] = env.OPENROUTER_APP_NAME;

  try {
    const response = await fetchImpl(`${endpoint}?id=${encodeURIComponent(generationId)}`, {
      method: 'GET',
      headers,
      signal: controller.signal,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const error = new Error(payload?.error?.message || `openrouter_generation_http_${response.status}`);
      error.httpStatus = response.status;
      error.retryable = response.status === 404 || response.status === 408 || response.status === 409 || response.status === 429 || response.status >= 500;
      throw error;
    }
    const usage = generationUsage(payload);
    if (!usage) {
      const error = new Error('openrouter_generation_cost_unavailable');
      error.retryable = true;
      throw error;
    }
    return { generationId, usage, metadata: payload?.data || payload };
  } catch (error) {
    if (error?.name === 'AbortError') {
      const timeout = new Error('openrouter_generation_reconciliation_timeout');
      timeout.retryable = true;
      throw timeout;
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

export async function preparePendingOpenRouterReconciliations({ state = {}, env = process.env, fetchImpl = globalThis.fetch, now = new Date() } = {}) {
  const rows = pendingRows(state, now);
  const results = [];
  for (const quote of rows) {
    try {
      const fetched = await fetchOpenRouterGenerationUsage({ generationId: quote.generationId, env, fetchImpl });
      results.push({
        quoteId: quote.id,
        generationId: quote.generationId,
        ok: true,
        usage: fetched.usage,
        metadata: {
          providerName: fetched.metadata?.provider_name || null,
          requestId: fetched.metadata?.request_id || null,
        },
      });
    } catch (error) {
      results.push({
        quoteId: quote.id,
        generationId: quote.generationId,
        ok: false,
        error: String(error?.message || error),
        retryable: error?.retryable !== false,
        httpStatus: error?.httpStatus || null,
      });
    }
  }
  return { attempted: rows.length, results, preparedAt: iso(now) };
}

function linkedRows(state, quote) {
  const job = state.researchJobs?.find(row => row.modelQuoteId === quote.id || row.id === quote.researchJobId) || null;
  const cost = state.agentCostLedger?.find(row => row.modelQuoteId === quote.id || (job && row.jobId === job.id)) || null;
  return { job, cost };
}

export function applyPendingOpenRouterReconciliations(state = {}, prepared = {}, env = process.env, now = new Date()) {
  const appliedAt = iso(now);
  const report = { attempted: Number(prepared.attempted || 0), reconciled: 0, retryScheduled: 0, exhausted: 0, missing: 0, results: [] };
  for (const result of prepared.results || []) {
    const quote = state.modelUsageLedger?.find(row => row.id === result.quoteId);
    if (!quote || quote.status !== 'usage_pending') {
      report.missing += 1;
      report.results.push({ quoteId: result.quoteId, status: 'no_longer_pending' });
      continue;
    }
    const linked = linkedRows(state, quote);
    if (result.ok) {
      const reconciled = reconcileModelUsage(state, {
        quoteId: quote.id,
        generationId: result.generationId,
        usage: result.usage,
        jobStatus: 'completed',
      }, appliedAt);
      if (reconciled.errors) {
        result.ok = false;
        result.error = reconciled.errors.join(',');
        result.retryable = false;
      } else {
        quote.reconciliationStatus = 'reconciled';
        quote.reconciliationAttempts = Number(quote.reconciliationAttempts || 0) + 1;
        quote.lastReconciliationAt = appliedAt;
        quote.nextReconciliationAt = null;
        quote.requiresManualReconciliation = false;
        quote.uncertainProviderOutcome = false;
        if (linked.job) {
          linked.job.status = 'completed';
          linked.job.failureReason = null;
          linked.job.requiresManualReconciliation = false;
          linked.job.uncertainProviderOutcome = false;
        }
        if (linked.cost) {
          linked.cost.recoveryStatus = 'reconciled';
          linked.cost.requiresManualReconciliation = false;
          linked.cost.providerError = null;
        }
        report.reconciled += 1;
        report.results.push({ quoteId: quote.id, generationId: quote.generationId, status: 'reconciled', actualCostUsd: quote.actualCostUsd });
        continue;
      }
    }

    const attempt = Number(quote.reconciliationAttempts || 0) + 1;
    const exhausted = result.retryable === false || attempt >= maxAttempts(env);
    quote.reconciliationAttempts = attempt;
    quote.lastReconciliationAt = appliedAt;
    quote.reconciliationLastError = result.error || 'openrouter_generation_reconciliation_failed';
    quote.reconciliationStatus = exhausted ? 'exhausted' : 'retry_scheduled';
    quote.nextReconciliationAt = exhausted
      ? null
      : iso(new Date(new Date(appliedAt).getTime() + retryDelayMs(attempt, env)));
    quote.requiresManualReconciliation = exhausted;
    if (linked.job) {
      linked.job.requiresManualReconciliation = exhausted;
      linked.job.failureReason = exhausted
        ? 'remote_usage_reconciliation_exhausted'
        : 'remote_usage_reconciliation_pending';
    }
    if (linked.cost) {
      linked.cost.recoveryStatus = exhausted
        ? 'usage_pending_manual_reconciliation_required'
        : 'usage_pending_reconciliation_retry_scheduled';
      linked.cost.requiresManualReconciliation = exhausted;
      linked.cost.providerError = quote.reconciliationLastError;
    }
    if (exhausted) report.exhausted += 1;
    else report.retryScheduled += 1;
    report.results.push({
      quoteId: quote.id,
      generationId: quote.generationId,
      status: quote.reconciliationStatus,
      attempt,
      nextReconciliationAt: quote.nextReconciliationAt,
      error: quote.reconciliationLastError,
    });
  }
  return report;
}
