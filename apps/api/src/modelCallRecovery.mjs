function finite(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function positive(value, fallback) {
  const number = finite(value, fallback);
  return number > 0 ? number : fallback;
}

function ageSeconds(record, now) {
  const reference = record?.startedAt || record?.requestedAt || record?.queuedAt || record?.createdAt;
  const timestamp = reference ? new Date(reference).getTime() : NaN;
  if (!Number.isFinite(timestamp)) return Infinity;
  return Math.max(0, (new Date(now).getTime() - timestamp) / 1000);
}

function nextAuditId(state, prefix, now) {
  const existing = new Set((state.audit || []).map(row => row?.id).filter(Boolean));
  const base = `${prefix}-${new Date(now).getTime()}`;
  if (!existing.has(base)) return base;
  let index = 2;
  while (existing.has(`${base}-${index}`)) index += 1;
  return `${base}-${index}`;
}

function linkedJob(state, quote) {
  return state.researchJobs?.find(row => row.id === quote.researchJobId || row.modelQuoteId === quote.id) || null;
}

function linkedCost(state, quote, job) {
  return state.agentCostLedger?.find(row => row.modelQuoteId === quote.id || (job && row.jobId === job.id)) || null;
}

function recoverQuote(state, quote, now, reason) {
  const job = linkedJob(state, quote);
  const cost = linkedCost(state, quote, job);
  const remote = quote.localOrRemote === 'remote';
  const recoveryReason = remote ? 'remote_provider_outcome_uncertain_reconciliation_required' : reason;

  quote.status = remote ? 'usage_pending' : 'failed';
  quote.failureReason = recoveryReason;
  quote.recoveredAt = now;
  quote.completedAt = now;
  quote.retryable = !remote;
  quote.requiresRequote = !remote;
  quote.requiresManualReconciliation = remote;
  quote.uncertainProviderOutcome = remote;

  if (job && job.status === 'running') {
    job.status = 'failed';
    job.failureReason = recoveryReason;
    job.recoveredAt = now;
    job.completedAt = now;
    job.retryable = !remote;
    job.requiresRequote = !remote;
    job.requiresManualReconciliation = remote;
    job.uncertainProviderOutcome = remote;
  }
  if (cost) {
    cost.recoveryStatus = remote
      ? 'usage_pending_manual_reconciliation_required'
      : 'failed_before_cost_reconciliation';
    cost.recoveredAt = now;
    cost.requiresManualReconciliation = remote;
  }
  return {
    quoteId: quote.id,
    jobId: job?.id || null,
    localOrRemote: quote.localOrRemote,
    status: quote.status,
    retryable: quote.retryable,
    requiresRequote: quote.requiresRequote,
    requiresManualReconciliation: quote.requiresManualReconciliation,
  };
}

export function recoverStaleModelCalls(state, options = {}) {
  state.modelUsageLedger ||= [];
  state.researchJobs ||= [];
  state.agentCostLedger ||= [];
  state.audit ||= [];
  const now = options.now instanceof Date ? options.now.toISOString() : options.now || new Date().toISOString();
  const env = { ...process.env, ...(options.env || {}) };
  const staleSeconds = positive(options.staleSeconds, positive(env.MODEL_CALL_STALE_SECONDS, positive(state.config?.modelCallStaleSeconds, 300)));
  const reason = options.reason || 'stale_model_call_recovered';
  const recoveredQuotes = [];
  const recoveredJobs = [];

  for (const quote of state.modelUsageLedger) {
    if (quote.status !== 'running' || ageSeconds(quote, now) <= staleSeconds) continue;
    recoveredQuotes.push(recoverQuote(state, quote, now, reason));
  }

  for (const job of state.researchJobs) {
    if (job.status !== 'running' || ageSeconds(job, now) <= staleSeconds) continue;
    const quote = state.modelUsageLedger.find(row => row.id === job.modelQuoteId || row.researchJobId === job.id);
    if (quote && quote.status !== 'running') continue;
    const remote = job.localOrRemote === 'remote';
    job.status = 'failed';
    job.failureReason = remote ? 'remote_provider_outcome_uncertain_reconciliation_required' : reason;
    job.recoveredAt = now;
    job.completedAt = now;
    job.retryable = !remote;
    job.requiresRequote = !remote;
    job.requiresManualReconciliation = remote;
    job.uncertainProviderOutcome = remote;
    recoveredJobs.push({
      jobId: job.id,
      modelQuoteId: job.modelQuoteId || null,
      localOrRemote: job.localOrRemote,
      retryable: job.retryable,
      requiresRequote: job.requiresRequote,
      requiresManualReconciliation: job.requiresManualReconciliation,
    });
  }

  const report = {
    ok: true,
    checkedAt: now,
    staleSeconds,
    recoveredQuoteCount: recoveredQuotes.length,
    recoveredOrphanJobCount: recoveredJobs.length,
    recoveredQuotes,
    recoveredJobs,
  };
  if (recoveredQuotes.length || recoveredJobs.length) {
    state.audit.push({
      id: nextAuditId(state, 'audit-model-call-recovery', now),
      action: 'stale_model_calls_recovered',
      actor: options.actor || 'economic-maintenance',
      at: now,
      details: `${recoveredQuotes.length + recoveredJobs.length} stale model calls`,
      payload: report,
    });
  }
  return report;
}
