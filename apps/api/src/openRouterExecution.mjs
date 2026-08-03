const DEFAULT_COMPLETION_TIMEOUT_MS = 120000;
const DEFAULT_USAGE_TIMEOUT_MS = 10000;

function finite(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function positive(value, fallback) {
  const number = finite(value, fallback);
  return number > 0 ? number : fallback;
}

function headersFor(env = process.env) {
  const headers = {
    accept: 'application/json',
    'content-type': 'application/json',
    authorization: `Bearer ${env.OPENROUTER_API_KEY}`,
  };
  if (env.OPENROUTER_APP_URL) headers['HTTP-Referer'] = env.OPENROUTER_APP_URL;
  if (env.OPENROUTER_APP_NAME) headers['X-Title'] = env.OPENROUTER_APP_NAME;
  return headers;
}

export function normalizeOpenRouterUsage(payload = {}) {
  const data = payload?.data || payload;
  const cost = finite(data?.total_cost ?? data?.cost, null);
  if (cost == null || cost < 0) return null;
  return {
    prompt_tokens: Number(data?.tokens_prompt ?? data?.native_tokens_prompt ?? data?.prompt_tokens ?? 0),
    completion_tokens: Number(data?.tokens_completion ?? data?.native_tokens_completion ?? data?.completion_tokens ?? 0),
    cost,
    native_tokens_reasoning: Number(data?.native_tokens_reasoning || 0),
    native_tokens_cached: Number(data?.native_tokens_cached || 0),
    upstream_inference_cost: finite(data?.upstream_inference_cost, null),
  };
}

export class RemoteUsagePendingError extends Error {
  constructor(message, details = {}) {
    super(message || 'provider_usage_cost_pending');
    this.name = 'RemoteUsagePendingError';
    this.code = 'provider_usage_cost_pending';
    this.generationId = details.generationId || null;
    this.model = details.model || null;
    this.choices = Array.isArray(details.choices) ? details.choices : [];
    this.partialUsage = details.partialUsage || null;
    this.reconciliationError = details.reconciliationError || null;
  }
}

export async function fetchOpenRouterGenerationUsage({
  generationId,
  env = process.env,
  fetchImpl = globalThis.fetch,
  timeoutMs,
} = {}) {
  if (!generationId) return { ok: false, pending: false, error: 'generation_id_required' };
  if (env.REMOTE_LLM_EXECUTION_ENABLED !== 'true') return { ok: false, pending: true, error: 'remote_llm_execution_disabled' };
  if (!env.OPENROUTER_API_KEY) return { ok: false, pending: true, error: 'openrouter_api_key_required' };
  if (typeof fetchImpl !== 'function') return { ok: false, pending: true, error: 'fetch_unavailable' };

  const controller = new AbortController();
  const timer = setTimeout(
    () => controller.abort(),
    positive(timeoutMs, positive(env.OPENROUTER_USAGE_TIMEOUT_MS, DEFAULT_USAGE_TIMEOUT_MS)),
  );
  try {
    const endpoint = env.OPENROUTER_GENERATION_URL || 'https://openrouter.ai/api/v1/generation';
    const response = await fetchImpl(`${endpoint}?id=${encodeURIComponent(generationId)}`, {
      method: 'GET',
      headers: headersFor(env),
      signal: controller.signal,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      return {
        ok: false,
        pending: response.status === 404 || response.status === 409 || response.status === 425 || response.status === 429 || response.status >= 500,
        generationId,
        error: payload?.error?.message || `openrouter_generation_http_${response.status}`,
      };
    }
    const usage = normalizeOpenRouterUsage(payload);
    return usage
      ? { ok: true, pending: false, generationId, usage }
      : { ok: false, pending: true, generationId, error: 'provider_usage_cost_pending' };
  } catch (error) {
    return {
      ok: false,
      pending: true,
      generationId,
      error: error?.name === 'AbortError' ? 'openrouter_usage_timeout' : String(error?.message || error),
    };
  } finally {
    clearTimeout(timer);
  }
}

export async function executeOpenRouter({ request = {}, env = process.env, fetchImpl = globalThis.fetch } = {}) {
  if (env.REMOTE_LLM_EXECUTION_ENABLED !== 'true') throw new Error('remote_llm_execution_disabled');
  if (!env.OPENROUTER_API_KEY) throw new Error('openrouter_api_key_required');
  if (typeof fetchImpl !== 'function') throw new Error('fetch_unavailable');

  const controller = new AbortController();
  const timer = setTimeout(
    () => controller.abort(),
    positive(request.timeoutMs, positive(env.OPENROUTER_COMPLETION_TIMEOUT_MS, DEFAULT_COMPLETION_TIMEOUT_MS)),
  );
  try {
    const body = {
      model: request.model,
      messages: request.messages,
      usage: { include: true },
      provider: request.providerPreferences,
      max_tokens: positive(request.maxCompletionTokens, undefined),
      temperature: finite(request.temperature, 0.2),
      response_format: request.responseFormat,
      tools: request.tools,
      tool_choice: request.toolChoice,
    };
    for (const key of Object.keys(body)) if (body[key] === undefined) delete body[key];

    const response = await fetchImpl(env.OPENROUTER_CHAT_URL || 'https://openrouter.ai/api/v1/chat/completions', {
      method: 'POST',
      headers: headersFor(env),
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload?.error?.message || `openrouter_chat_http_${response.status}`);

    let usage = normalizeOpenRouterUsage(payload.usage || {});
    if (!usage && payload.id) {
      const reconciled = await fetchOpenRouterGenerationUsage({
        generationId: payload.id,
        env,
        fetchImpl,
        timeoutMs: env.OPENROUTER_USAGE_TIMEOUT_MS,
      });
      if (reconciled.ok) usage = reconciled.usage;
      else {
        throw new RemoteUsagePendingError('provider_usage_cost_pending', {
          generationId: payload.id,
          model: payload.model || request.model,
          choices: payload.choices || [],
          partialUsage: payload.usage || null,
          reconciliationError: reconciled.error,
        });
      }
    }
    if (!usage) {
      throw new RemoteUsagePendingError('provider_usage_cost_pending', {
        generationId: payload.id || null,
        model: payload.model || request.model,
        choices: payload.choices || [],
        partialUsage: payload.usage || null,
      });
    }

    return {
      provider: 'openrouter',
      id: payload.id,
      model: payload.model || request.model,
      choices: payload.choices || [],
      usage,
    };
  } catch (error) {
    if (error instanceof RemoteUsagePendingError) throw error;
    if (error?.name === 'AbortError') throw new Error('openrouter_completion_timeout');
    throw error;
  } finally {
    clearTimeout(timer);
  }
}
