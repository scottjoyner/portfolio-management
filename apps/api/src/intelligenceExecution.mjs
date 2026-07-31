import {
  quoteModelRequest,
  reconcileModelUsage,
} from '../../../packages/economics/src/economicDecisionEngine.mjs';
import { createIntelligenceProviderRegistry } from '../../../packages/intelligence/src/providerRegistry.mjs';
import {
  executeOpenRouter,
  RemoteUsagePendingError,
} from './openRouterExecution.mjs';

let cachedRegistry = null;
let cachedRegistryKey = null;
let cachedFetchImpl = null;

function registryKey(env) {
  return JSON.stringify({
    nodes: env.LOCAL_LLM_NODES_JSON || '',
    endpoints: env.LOCAL_LLM_ENDPOINTS || '',
    localApiKey: env.LOCAL_LLM_API_KEY ? 'configured' : 'none',
    remoteEnabled: env.REMOTE_LLM_EXECUTION_ENABLED || 'false',
    openRouterKey: env.OPENROUTER_API_KEY ? 'configured' : 'none',
    defaultPrefill: env.LOCAL_LLM_DEFAULT_PREFILL_TPS || '',
    defaultDecode: env.LOCAL_LLM_DEFAULT_DECODE_TPS || '',
    defaultWatts: env.LOCAL_LLM_DEFAULT_WATTS || '',
    electricity: env.LOCAL_LLM_ELECTRICITY_RATE_PER_KWH || '',
    depreciation: env.LOCAL_LLM_HARDWARE_DEPRECIATION_PER_HOUR || '',
  });
}

function registryFor(env, fetchImpl) {
  const key = registryKey(env);
  if (!cachedRegistry || cachedRegistryKey !== key || cachedFetchImpl !== fetchImpl) {
    cachedRegistry = createIntelligenceProviderRegistry({ env, fetchImpl });
    cachedRegistryKey = key;
    cachedFetchImpl = fetchImpl;
  }
  return cachedRegistry;
}

export function resetIntelligenceProviderRegistry() {
  cachedRegistry = null;
  cachedRegistryKey = null;
  cachedFetchImpl = null;
}

function messagesFrom(body = {}) {
  if (Array.isArray(body.messages) && body.messages.length) return body.messages;
  if (body.prompt) return [{ role: 'user', content: String(body.prompt) }];
  return [];
}

function errorStatus(errors = []) {
  if (errors.some(error => error.endsWith('_not_found'))) return 404;
  if (errors.some(error => error.includes('disabled') || error.includes('required') || error.includes('mismatch') || error.includes('already_') || error.includes('requote') || error.includes('reconciliation') || error.includes('consumed'))) return 409;
  if (errors.some(error => error.includes('unavailable') || error.includes('timeout') || error.includes('no_healthy') || error.includes('rate'))) return 503;
  return 400;
}

function resultFromErrors(errors, extra = {}) {
  return { status: errorStatus(errors), body: { ok: false, errors, ...extra } };
}

function quoteRecordForExecution(state, body) {
  return state.modelUsageLedger?.find(row => row.id === body.modelQuoteId) || null;
}

function decisionRecordForExecution(state, body) {
  return state.economicDecisions?.find(row => row.id === body.economicDecisionId) || null;
}

function validateExecution(state, body, env) {
  const quote = quoteRecordForExecution(state, body);
  const decision = decisionRecordForExecution(state, body);
  const messages = messagesFrom(body);
  const errors = [];
  if (!quote) errors.push('model_quote_not_found');
  if (!decision) errors.push('economic_decision_not_found');
  if (quote?.status === 'usage_pending') errors.push('model_usage_pending_reconciliation');
  else if (quote && quote.status !== 'quoted') errors.push('model_quote_already_consumed');
  if (decision && decision.intelligenceAllowed !== true) errors.push('intelligence_purchase_not_economic');
  if (quote && decision && decision.modelQuoteId !== quote.id) errors.push('economic_decision_model_quote_mismatch');
  if (!messages.length) errors.push('model_messages_required');
  if (quote?.localOrRemote === 'remote' && env.REMOTE_LLM_EXECUTION_ENABLED !== 'true') errors.push('remote_llm_execution_disabled');
  if (quote?.localOrRemote === 'local' && !quote.localNodeId) errors.push('local_model_quote_requires_node_route');
  return { quote, decision, messages, errors };
}

function markRunning(state, body, now, env) {
  const validation = validateExecution(state, body, env);
  if (validation.errors.length) return { errors: validation.errors };
  const { quote, decision, messages } = validation;
  const job = body.researchJobId ? state.researchJobs?.find(row => row.id === body.researchJobId) : null;
  if (body.researchJobId && !job) return { errors: ['research_job_not_found'] };
  const costRow = job ? state.agentCostLedger?.find(row => row.jobId === job.id) : null;
  const providerAttemptNumber = Number(quote.providerAttemptNumber || 0) + 1;
  const providerAttemptId = `provider-attempt:${quote.id}:${providerAttemptNumber}`;

  quote.researchJobId = job?.id || quote.researchJobId || null;
  quote.status = 'running';
  quote.startedAt = now;
  quote.providerAttemptNumber = providerAttemptNumber;
  quote.providerAttemptId = providerAttemptId;
  quote.providerAttemptStartedAt = now;
  quote.providerAttemptCompletedAt = null;
  quote.failureReason = null;
  quote.retryable = false;
  quote.requiresManualReconciliation = false;
  quote.uncertainProviderOutcome = false;
  if (job) {
    job.status = 'running';
    job.startedAt ||= now;
    job.completedAt = null;
    job.failureReason = null;
    job.retryable = false;
    job.providerAttemptId = providerAttemptId;
    job.providerAttemptNumber = providerAttemptNumber;
    job.modelQuoteId = quote.id;
    job.economicDecisionId = decision.id;
    job.pricingSnapshotId = quote.pricingSnapshotId;
    job.localNodeId = quote.localNodeId || null;
  }
  if (costRow) {
    costRow.modelQuoteId = quote.id;
    costRow.economicDecisionId = decision.id;
    costRow.pricingSnapshotId = quote.pricingSnapshotId;
    costRow.providerAttemptId = providerAttemptId;
    costRow.providerAttemptNumber = providerAttemptNumber;
    costRow.remoteApiCost = quote.localOrRemote === 'remote' ? quote.estimatedCostUsd : 0;
    costRow.localComputeCost = quote.localOrRemote === 'local' ? quote.estimatedCostUsd : 0;
    costRow.costSource = 'pre_call_estimate';
    costRow.localNodeId = quote.localNodeId || null;
  }

  return {
    execution: {
      quoteId: quote.id,
      decisionId: decision.id,
      researchJobId: job?.id || null,
      providerAttemptId,
      providerAttemptNumber,
      localOrRemote: quote.localOrRemote,
      provider: quote.provider,
      model: quote.model,
      localNodeId: quote.localNodeId || null,
      localNodeName: quote.localNodeName || null,
      messages,
      providerPreferences: quote.providerPreferences || null,
      maxCompletionTokens: Number(body.maxCompletionTokens || quote.completionTokens || 0) || undefined,
      temperature: Number.isFinite(Number(body.temperature)) ? Number(body.temperature) : 0.2,
      topP: body.topP,
      seed: body.seed,
      responseFormat: body.responseFormat,
      tools: body.tools,
      toolChoice: body.toolChoice,
      timeoutMs: body.timeoutMs,
    },
  };
}

function markFailed(state, prepared, error, now, options = {}) {
  const quote = state.modelUsageLedger?.find(row => row.id === prepared.quoteId);
  const job = prepared.researchJobId ? state.researchJobs?.find(row => row.id === prepared.researchJobId) : null;
  const costRow = job ? state.agentCostLedger?.find(row => row.jobId === job.id) : null;
  const providerOutcome = options.providerOutcome || (prepared.localOrRemote === 'remote' ? 'uncertain' : 'local_failed');
  const generationId = options.generationId || null;
  const responseSummary = options.responseSummary || null;
  const remoteNotStarted = prepared.localOrRemote === 'remote' && providerOutcome === 'not_started';
  const remoteKnownGeneration = prepared.localOrRemote === 'remote' && providerOutcome === 'generation_usage_pending';
  const remoteUncertain = prepared.localOrRemote === 'remote' && providerOutcome === 'uncertain';
  const failureReason = remoteKnownGeneration
    ? 'remote_usage_pending_reconciliation'
    : remoteUncertain
      ? 'remote_provider_outcome_uncertain_reconciliation_required'
      : error;

  if (quote) {
    quote.lastProviderAttemptId = prepared.providerAttemptId;
    quote.providerAttemptCompletedAt = now;
    quote.providerOutcome = providerOutcome;
    quote.providerError = error;
    quote.lastProviderError = error;
    if (remoteNotStarted) {
      quote.status = 'quoted';
      quote.failureReason = error;
      quote.retryable = true;
      quote.requiresRequote = false;
      quote.requiresManualReconciliation = false;
      quote.uncertainProviderOutcome = false;
      quote.completedAt = null;
    } else if (remoteKnownGeneration || remoteUncertain) {
      quote.status = 'usage_pending';
      quote.generationId = generationId || quote.generationId || null;
      quote.failureReason = failureReason;
      quote.completedAt = now;
      quote.retryable = false;
      quote.requiresRequote = false;
      quote.requiresManualReconciliation = remoteUncertain;
      quote.uncertainProviderOutcome = remoteUncertain;
      quote.reconciliationStatus = remoteKnownGeneration ? 'pending' : 'manual_required';
      quote.reconciliationAttempts = Number(quote.reconciliationAttempts || 0);
      quote.nextReconciliationAt = remoteKnownGeneration ? now : null;
      if (responseSummary) quote.providerResponseSummary = responseSummary;
    } else {
      quote.status = 'failed';
      quote.failureReason = failureReason;
      quote.completedAt = now;
      quote.retryable = false;
      quote.requiresRequote = prepared.localOrRemote === 'local';
      quote.requiresManualReconciliation = false;
      quote.uncertainProviderOutcome = false;
    }
  }
  if (job) {
    if (remoteNotStarted) {
      job.status = 'queued';
      job.failureReason = error;
      job.completedAt = null;
      job.retryable = true;
      job.requiresRequote = false;
      job.requiresManualReconciliation = false;
      job.uncertainProviderOutcome = false;
    } else {
      job.status = 'failed';
      job.failureReason = failureReason;
      job.completedAt = now;
      job.retryable = false;
      job.requiresRequote = prepared.localOrRemote === 'local';
      job.requiresManualReconciliation = remoteUncertain;
      job.uncertainProviderOutcome = remoteUncertain;
      if (generationId) job.generationId = generationId;
      if (responseSummary) job.responseSummary = responseSummary;
    }
  }
  if (costRow) {
    if (remoteNotStarted) {
      costRow.recoveryStatus = 'provider_not_started_retryable';
      costRow.requiresManualReconciliation = false;
      costRow.providerError = error;
    } else if (remoteKnownGeneration) {
      costRow.recoveryStatus = 'usage_pending_reconciliation_scheduled';
      costRow.requiresManualReconciliation = false;
      costRow.providerError = error;
      costRow.generationId = generationId;
    } else if (remoteUncertain) {
      costRow.recoveryStatus = 'usage_pending_manual_reconciliation_required';
      costRow.requiresManualReconciliation = true;
      costRow.providerError = error;
    }
  }
  return {
    errors: [failureReason],
    providerOutcome,
    retryable: remoteNotStarted,
    generationId,
    requiresManualReconciliation: remoteUncertain,
    automaticReconciliationScheduled: remoteKnownGeneration,
  };
}

function reconcileExecution(state, prepared, providerResult, now) {
  const reconciled = reconcileModelUsage(state, {
    quoteId: prepared.quoteId,
    generationId: providerResult.id,
    usage: providerResult.usage,
    jobStatus: 'completed',
  }, now);
  if (reconciled.errors) return reconciled;

  const quote = reconciled.modelUsage;
  quote.provider = providerResult.provider || quote.provider;
  quote.localNodeId = providerResult.nodeId || quote.localNodeId || null;
  quote.localNodeName = providerResult.nodeName || quote.localNodeName || null;
  quote.runtimeSecondsActual = providerResult.usage?.runtime_seconds ?? null;
  quote.queueDelaySecondsActual = providerResult.usage?.queue_delay_seconds ?? null;
  quote.prefillTokensPerSecondActual = providerResult.usage?.prefill_tokens_per_second ?? null;
  quote.decodeTokensPerSecondActual = providerResult.usage?.decode_tokens_per_second ?? null;
  quote.completedAt = now;
  quote.providerAttemptCompletedAt = now;
  quote.providerOutcome = 'completed_reconciled';
  quote.requiresManualReconciliation = false;
  quote.uncertainProviderOutcome = false;
  quote.reconciliationStatus = 'reconciled';
  quote.nextReconciliationAt = null;

  const job = prepared.researchJobId ? state.researchJobs?.find(row => row.id === prepared.researchJobId) : null;
  if (job) {
    job.localNodeId = quote.localNodeId;
    job.runtimeSecondsActual = quote.runtimeSecondsActual;
    job.queueDelaySecondsActual = quote.queueDelaySecondsActual;
    job.requiresManualReconciliation = false;
    job.uncertainProviderOutcome = false;
    job.responseSummary = {
      generationId: providerResult.id || null,
      finishReasons: (providerResult.choices || []).map(choice => choice.finish_reason).filter(Boolean),
      choiceCount: Array.isArray(providerResult.choices) ? providerResult.choices.length : 0,
      provider: providerResult.provider || null,
      nodeId: providerResult.nodeId || null,
    };
  }

  return { modelUsage: quote, researchJob: job || null };
}

function instrumentRemoteFetch(fetchImpl, env, telemetry) {
  if (typeof fetchImpl !== 'function') return fetchImpl;
  const chatUrl = env.OPENROUTER_CHAT_URL || 'https://openrouter.ai/api/v1/chat/completions';
  return async (url, init = {}) => {
    const remotePost = String(url) === chatUrl && String(init.method || 'GET').toUpperCase() === 'POST';
    if (remotePost) {
      telemetry.dispatched = true;
      telemetry.dispatchedAt = new Date().toISOString();
    }
    try {
      const response = await fetchImpl(url, init);
      if (remotePost) {
        telemetry.responseReceived = true;
        telemetry.httpStatus = Number(response?.status || 0);
        telemetry.generationId = response?.headers?.get?.('x-generation-id')
          || response?.headers?.get?.('X-Generation-Id')
          || null;
      }
      return response;
    } catch (error) {
      if (remotePost) telemetry.transportError = String(error?.message || error);
      throw error;
    }
  };
}

function classifyRemoteFailure(telemetry = {}) {
  if (!telemetry.dispatched) return 'not_started';
  if (telemetry.responseReceived && telemetry.httpStatus >= 400) return 'not_started';
  if (telemetry.generationId) return 'generation_usage_pending';
  return 'uncertain';
}

export async function discoverLocalIntelligenceNodes({ env = process.env, fetchImpl = globalThis.fetch } = {}) {
  const registry = registryFor(env, fetchImpl);
  const nodes = await registry.health();
  return {
    status: nodes.some(node => node.ok) ? 200 : 503,
    body: {
      ok: nodes.some(node => node.ok),
      localRequired: env.LOCAL_LLM_EXECUTION_REQUIRED === 'true',
      remoteEnabled: env.REMOTE_LLM_EXECUTION_ENABLED === 'true',
      nodes,
    },
  };
}

export async function quoteLocalIntelligence({ store, body = {}, env = process.env, fetchImpl = globalThis.fetch, now = new Date().toISOString() }) {
  const registry = registryFor(env, fetchImpl);
  const routed = await registry.routeLocal(body);
  if (routed.errors) return resultFromErrors(routed.errors, { nodes: routed.nodes });
  const route = routed.route;
  const result = await store.mutate(state => {
    const quoted = quoteModelRequest(state, {
      ...body,
      provider: route.provider,
      model: route.model,
      localOrRemote: 'local',
      runtimeSeconds: route.estimatedRuntimeSeconds,
      estimatedWatts: route.estimatedWatts,
      electricityRatePerKwh: route.electricityRatePerKwh,
      hardwareDepreciationPerHour: route.hardwareDepreciationPerHour,
    }, now);
    if (quoted.modelQuote) {
      quoted.modelQuote.localNodeId = route.nodeId;
      quoted.modelQuote.localNodeName = route.nodeName;
      quoted.modelQuote.localNodeKind = route.provider;
      quoted.modelQuote.localEndpoint = route.baseUrl;
      quoted.modelQuote.contextLength = route.contextLength;
      quoted.modelQuote.estimatedRuntimeSeconds = route.estimatedRuntimeSeconds;
      quoted.modelQuote.estimatedPrefillSeconds = route.estimatedPrefillSeconds;
      quoted.modelQuote.estimatedDecodeSeconds = route.estimatedDecodeSeconds;
      quoted.modelQuote.estimatedQueueSeconds = route.estimatedQueueSeconds;
      quoted.modelQuote.routeHealthLatencyMs = route.healthLatencyMs;
    }
    return quoted;
  });
  if (result.errors) return resultFromErrors(result.errors, { nodes: routed.nodes });
  return { status: 201, body: { ok: true, ...result, route, nodes: routed.nodes } };
}

export async function executeEconomicIntelligence({ store, body = {}, env = process.env, fetchImpl = globalThis.fetch, now = new Date().toISOString() }) {
  const preparedResult = await store.mutate(state => markRunning(state, body, now, env));
  if (preparedResult.errors) return resultFromErrors(preparedResult.errors);
  const prepared = preparedResult.execution;
  const telemetry = {};
  const providerFetch = prepared.localOrRemote === 'remote'
    ? instrumentRemoteFetch(fetchImpl, env, telemetry)
    : fetchImpl;
  const registry = prepared.localOrRemote === 'local' ? registryFor(env, providerFetch) : null;
  const provider = prepared.localOrRemote === 'local' ? registry.providerForQuote(prepared) : null;
  if (prepared.localOrRemote === 'local' && !provider) {
    const error = 'local_node_unavailable_requote_required';
    const failed = await store.mutate(state => markFailed(
      state,
      prepared,
      error,
      new Date().toISOString(),
      { providerOutcome: 'local_failed' },
    ));
    return resultFromErrors(failed.errors || [error], failed);
  }

  if (prepared.localOrRemote === 'local') {
    const health = await provider.health();
    const modelAvailable = !health.models?.length || health.models.includes(prepared.model);
    if (!health.ok || !modelAvailable || health.activeRequests >= health.maxConcurrent) {
      const error = !health.ok
        ? 'local_node_unavailable_requote_required'
        : !modelAvailable
          ? 'quoted_model_no_longer_available_requote_required'
          : 'local_node_busy_requote_required';
      await store.mutate(state => markFailed(state, prepared, error, new Date().toISOString(), { providerOutcome: 'local_failed' }));
      return resultFromErrors([error], { node: health });
    }
  }

  let providerResult;
  try {
    providerResult = prepared.localOrRemote === 'remote'
      ? await executeOpenRouter({ request: prepared, env, fetchImpl: providerFetch })
      : await provider.execute(prepared);
  } catch (error) {
    const message = String(error?.message || error);
    const knownGenerationId = error instanceof RemoteUsagePendingError
      ? error.generationId
      : telemetry.generationId;
    const providerOutcome = prepared.localOrRemote === 'remote'
      ? knownGenerationId
        ? 'generation_usage_pending'
        : classifyRemoteFailure(telemetry)
      : 'local_failed';
    const responseSummary = error instanceof RemoteUsagePendingError
      ? {
          generationId: error.generationId,
          finishReasons: (error.choices || []).map(choice => choice.finish_reason).filter(Boolean),
          choiceCount: Array.isArray(error.choices) ? error.choices.length : 0,
          provider: 'openrouter',
          model: error.model || prepared.model,
        }
      : null;
    const failed = await store.mutate(state => markFailed(
      state,
      prepared,
      message,
      new Date().toISOString(),
      { providerOutcome, generationId: knownGenerationId, responseSummary },
    ));
    return resultFromErrors(failed.errors || [message], {
      modelQuoteId: prepared.quoteId,
      providerAttemptId: prepared.providerAttemptId,
      providerOutcome,
      generationId: knownGenerationId || null,
      retryable: failed.retryable === true,
      automaticReconciliationScheduled: failed.automaticReconciliationScheduled === true,
      requiresManualReconciliation: failed.requiresManualReconciliation === true,
    });
  }

  const reconciledAt = new Date().toISOString();
  const reconciled = await store.mutate(state => reconcileExecution(state, prepared, providerResult, reconciledAt));
  if (reconciled.errors) return resultFromErrors(reconciled.errors);
  return {
    status: 200,
    body: {
      ok: true,
      modelResponse: {
        id: providerResult.id || null,
        model: providerResult.model || prepared.model,
        provider: providerResult.provider,
        nodeId: providerResult.nodeId || null,
        choices: providerResult.choices || [],
        usage: providerResult.usage,
      },
      modelUsage: reconciled.modelUsage,
      researchJob: reconciled.researchJob,
      providerAttemptId: prepared.providerAttemptId,
      economicDecisionRefreshRequired: true,
    },
  };
}
