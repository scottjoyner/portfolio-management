const DEFAULT_HEALTH_TIMEOUT_MS = 4000;
const DEFAULT_COMPLETION_TIMEOUT_MS = 120000;

function finite(value, fallback = null) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function positive(value, fallback) {
  const parsed = finite(value, fallback);
  return parsed > 0 ? parsed : fallback;
}

function nonNegative(value, fallback = 0) {
  const parsed = finite(value, fallback);
  return parsed >= 0 ? parsed : fallback;
}

function round(value, digits = 8) {
  if (!Number.isFinite(value)) return null;
  const scale = 10 ** digits;
  return Math.round(value * scale) / scale;
}

function normalizeBaseUrl(value) {
  const trimmed = String(value || '').trim().replace(/\/+$/, '');
  if (!trimmed) return null;
  return trimmed.endsWith('/v1') ? trimmed : `${trimmed}/v1`;
}

function nodeIdFromUrl(value, index) {
  try {
    return new URL(value).hostname.replace(/[^a-z0-9]+/gi, '-').replace(/^-|-$/g, '') || `local-node-${index + 1}`;
  } catch {
    return `local-node-${index + 1}`;
  }
}

function parseJsonNodes(value) {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

export function parseLocalInferenceNodes(env = process.env) {
  const configured = parseJsonNodes(env.LOCAL_LLM_NODES_JSON);
  const fallback = String(env.LOCAL_LLM_ENDPOINTS || '')
    .split(',')
    .map(value => value.trim())
    .filter(Boolean)
    .map((baseUrl, index) => ({ id: nodeIdFromUrl(baseUrl, index), baseUrl }));
  const source = configured.length ? configured : fallback;
  return source.map((row, index) => {
    const baseUrl = normalizeBaseUrl(row.baseUrl || row.url || row.endpoint);
    if (!baseUrl) return null;
    return {
      id: String(row.id || row.name || nodeIdFromUrl(baseUrl, index)),
      name: String(row.name || row.id || nodeIdFromUrl(baseUrl, index)),
      kind: String(row.kind || row.provider || 'openai-compatible-local'),
      baseUrl,
      apiKey: row.apiKey || (row.apiKeyEnv ? env[row.apiKeyEnv] : null) || env.LOCAL_LLM_API_KEY || null,
      models: Array.isArray(row.models) ? row.models.map(String) : [],
      priority: finite(row.priority, 0),
      maxConcurrent: Math.max(1, Math.floor(positive(row.maxConcurrent, 1))),
      prefillTokensPerSecond: positive(row.prefillTokensPerSecond, positive(env.LOCAL_LLM_DEFAULT_PREFILL_TPS, 80)),
      decodeTokensPerSecond: positive(row.decodeTokensPerSecond, positive(env.LOCAL_LLM_DEFAULT_DECODE_TPS, 20)),
      estimatedWatts: positive(row.estimatedWatts ?? row.watts, positive(env.LOCAL_LLM_DEFAULT_WATTS, 110)),
      electricityRatePerKwh: nonNegative(row.electricityRatePerKwh, nonNegative(env.LOCAL_LLM_ELECTRICITY_RATE_PER_KWH, 0.14)),
      hardwareDepreciationPerHour: nonNegative(row.hardwareDepreciationPerHour, nonNegative(env.LOCAL_LLM_HARDWARE_DEPRECIATION_PER_HOUR, 0.18)),
      contextLength: positive(row.contextLength, positive(env.LOCAL_LLM_DEFAULT_CONTEXT_LENGTH, 32768)),
      enabled: row.enabled !== false,
      tags: Array.isArray(row.tags) ? row.tags.map(String) : [],
    };
  }).filter(Boolean).filter(row => row.enabled);
}

function authHeaders(node) {
  return node.apiKey ? { authorization: `Bearer ${node.apiKey}` } : {};
}

function payloadModels(payload) {
  const rows = Array.isArray(payload?.data) ? payload.data : Array.isArray(payload?.models) ? payload.models : [];
  return rows.map(row => String(row?.id || row?.key || row?.name || '')).filter(Boolean);
}

function normalizeUsage(payload = {}) {
  const usage = payload.usage || {};
  const timings = payload.timings || payload.stats || {};
  return {
    prompt_tokens: nonNegative(usage.prompt_tokens ?? timings.prompt_n, 0),
    completion_tokens: nonNegative(usage.completion_tokens ?? timings.predicted_n, 0),
    total_tokens: nonNegative(usage.total_tokens, nonNegative(usage.prompt_tokens, 0) + nonNegative(usage.completion_tokens, 0)),
    prompt_tokens_details: usage.prompt_tokens_details || {},
    completion_tokens_details: usage.completion_tokens_details || {},
    timings,
  };
}

function localRuntimeCost(node, runtimeSeconds) {
  const runtimeHours = nonNegative(runtimeSeconds, 0) / 3600;
  const electricityUsd = runtimeHours * node.estimatedWatts / 1000 * node.electricityRatePerKwh;
  const depreciationUsd = runtimeHours * node.hardwareDepreciationPerHour;
  return {
    cost: round(electricityUsd + depreciationUsd, 8),
    electricity_usd: round(electricityUsd, 8),
    depreciation_usd: round(depreciationUsd, 8),
  };
}

export class OpenAICompatibleLocalProvider {
  constructor(node, options = {}) {
    this.node = node;
    this.fetchImpl = options.fetchImpl || globalThis.fetch;
    this.healthTimeoutMs = positive(options.healthTimeoutMs, DEFAULT_HEALTH_TIMEOUT_MS);
    this.completionTimeoutMs = positive(options.completionTimeoutMs, DEFAULT_COMPLETION_TIMEOUT_MS);
    this.activeRequests = 0;
    this.lastHealth = null;
  }

  async health() {
    if (typeof this.fetchImpl !== 'function') return { ok: false, nodeId: this.node.id, error: 'fetch_unavailable' };
    const started = performance.now();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.healthTimeoutMs);
    try {
      const response = await this.fetchImpl(`${this.node.baseUrl}/models`, {
        method: 'GET',
        headers: { accept: 'application/json', ...authHeaders(this.node) },
        signal: controller.signal,
      });
      const payload = await response.json().catch(() => ({}));
      const discoveredModels = response.ok ? payloadModels(payload) : [];
      const models = discoveredModels.length ? discoveredModels : this.node.models;
      this.lastHealth = {
        ok: response.ok,
        nodeId: this.node.id,
        name: this.node.name,
        kind: this.node.kind,
        baseUrl: this.node.baseUrl,
        latencyMs: round(performance.now() - started, 3),
        models,
        configuredModels: this.node.models,
        activeRequests: this.activeRequests,
        maxConcurrent: this.node.maxConcurrent,
        contextLength: this.node.contextLength,
        checkedAt: new Date().toISOString(),
        error: response.ok ? null : `models_http_${response.status}`,
      };
      return this.lastHealth;
    } catch (error) {
      this.lastHealth = {
        ok: false,
        nodeId: this.node.id,
        name: this.node.name,
        kind: this.node.kind,
        baseUrl: this.node.baseUrl,
        latencyMs: round(performance.now() - started, 3),
        models: this.node.models,
        activeRequests: this.activeRequests,
        maxConcurrent: this.node.maxConcurrent,
        contextLength: this.node.contextLength,
        checkedAt: new Date().toISOString(),
        error: error?.name === 'AbortError' ? 'health_timeout' : String(error?.message || error),
      };
      return this.lastHealth;
    } finally {
      clearTimeout(timer);
    }
  }

  estimate(request = {}, health = this.lastHealth) {
    const promptTokens = nonNegative(request.promptTokens, 0);
    const completionTokens = nonNegative(request.completionTokens ?? request.maxCompletionTokens, 0);
    const prefillSeconds = promptTokens / this.node.prefillTokensPerSecond;
    const decodeSeconds = completionTokens / this.node.decodeTokensPerSecond;
    const queueSeconds = this.activeRequests * (prefillSeconds + decodeSeconds);
    const runtimeSeconds = prefillSeconds + decodeSeconds;
    const cost = localRuntimeCost(this.node, runtimeSeconds);
    const requestedModel = String(request.model || '').trim();
    const models = health?.models?.length ? health.models : this.node.models;
    const selectedModel = requestedModel || models[0] || null;
    const modelAvailable = !requestedModel || !models.length || models.includes(requestedModel);
    const contextRequired = promptTokens + completionTokens;
    return {
      nodeId: this.node.id,
      nodeName: this.node.name,
      provider: this.node.kind,
      baseUrl: this.node.baseUrl,
      model: selectedModel,
      modelAvailable,
      contextAvailable: contextRequired <= this.node.contextLength,
      contextLength: this.node.contextLength,
      promptTokens,
      completionTokens,
      estimatedPrefillSeconds: round(prefillSeconds, 3),
      estimatedDecodeSeconds: round(decodeSeconds, 3),
      estimatedRuntimeSeconds: round(runtimeSeconds, 3),
      estimatedQueueSeconds: round(queueSeconds, 3),
      estimatedWatts: this.node.estimatedWatts,
      electricityRatePerKwh: this.node.electricityRatePerKwh,
      hardwareDepreciationPerHour: this.node.hardwareDepreciationPerHour,
      estimatedCostUsd: cost.cost,
      pricingBreakdown: {
        electricityUsd: cost.electricity_usd,
        depreciationUsd: cost.depreciation_usd,
      },
      activeRequests: this.activeRequests,
      maxConcurrent: this.node.maxConcurrent,
      priority: this.node.priority,
      healthy: health?.ok === true,
      healthLatencyMs: health?.latencyMs ?? null,
    };
  }

  async execute(request = {}) {
    if (typeof this.fetchImpl !== 'function') throw new Error('fetch_unavailable');
    if (this.activeRequests >= this.node.maxConcurrent) throw new Error('local_node_busy_requote_required');
    const messages = Array.isArray(request.messages) && request.messages.length
      ? request.messages
      : request.prompt
        ? [{ role: 'user', content: String(request.prompt) }]
        : [];
    if (!messages.length) throw new Error('model_messages_required');

    const queueStarted = performance.now();
    this.activeRequests += 1;
    const queueDelaySeconds = (performance.now() - queueStarted) / 1000;
    const started = performance.now();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), positive(request.timeoutMs, this.completionTimeoutMs));
    try {
      const body = {
        model: request.model,
        messages,
        max_tokens: positive(request.maxCompletionTokens, undefined),
        temperature: finite(request.temperature, 0.2),
        top_p: finite(request.topP, undefined),
        seed: finite(request.seed, undefined),
        response_format: request.responseFormat,
        tools: request.tools,
        tool_choice: request.toolChoice,
        stream: false,
      };
      for (const key of Object.keys(body)) if (body[key] === undefined) delete body[key];
      const response = await this.fetchImpl(`${this.node.baseUrl}/chat/completions`, {
        method: 'POST',
        headers: { accept: 'application/json', 'content-type': 'application/json', ...authHeaders(this.node) },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(payload?.error?.message || `local_chat_http_${response.status}`);
      const runtimeSeconds = (performance.now() - started) / 1000;
      const usage = normalizeUsage(payload);
      const cost = localRuntimeCost(this.node, runtimeSeconds);
      usage.cost = cost.cost;
      usage.cost_details = {
        electricity_cost: cost.electricity_usd,
        hardware_depreciation_cost: cost.depreciation_usd,
      };
      usage.runtime_seconds = round(runtimeSeconds, 6);
      usage.queue_delay_seconds = round(queueDelaySeconds, 6);
      usage.estimated_watts = this.node.estimatedWatts;
      usage.prefill_tokens_per_second = finite(usage.timings?.prompt_per_second, null);
      usage.decode_tokens_per_second = finite(usage.timings?.predicted_per_second, null);
      return {
        provider: this.node.kind,
        nodeId: this.node.id,
        nodeName: this.node.name,
        baseUrl: this.node.baseUrl,
        id: payload.id || `local-${this.node.id}-${Date.now()}`,
        model: payload.model || request.model,
        choices: payload.choices || [],
        usage,
        rawTelemetry: {
          timings: payload.timings || null,
          systemFingerprint: payload.system_fingerprint || null,
        },
      };
    } catch (error) {
      if (error?.name === 'AbortError') throw new Error('local_completion_timeout');
      throw error;
    } finally {
      clearTimeout(timer);
      this.activeRequests = Math.max(0, this.activeRequests - 1);
    }
  }
}

export class OpenRouterProvider {
  constructor(options = {}) {
    this.env = options.env || process.env;
    this.fetchImpl = options.fetchImpl || globalThis.fetch;
  }

  async execute(request = {}) {
    if (this.env.REMOTE_LLM_EXECUTION_ENABLED !== 'true') throw new Error('remote_llm_execution_disabled');
    if (!this.env.OPENROUTER_API_KEY) throw new Error('openrouter_api_key_required');
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), positive(request.timeoutMs, positive(this.env.OPENROUTER_COMPLETION_TIMEOUT_MS, DEFAULT_COMPLETION_TIMEOUT_MS)));
    const headers = {
      accept: 'application/json',
      'content-type': 'application/json',
      authorization: `Bearer ${this.env.OPENROUTER_API_KEY}`,
    };
    if (this.env.OPENROUTER_APP_URL) headers['HTTP-Referer'] = this.env.OPENROUTER_APP_URL;
    if (this.env.OPENROUTER_APP_NAME) headers['X-Title'] = this.env.OPENROUTER_APP_NAME;
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
      const response = await this.fetchImpl(this.env.OPENROUTER_CHAT_URL || 'https://openrouter.ai/api/v1/chat/completions', {
        method: 'POST', headers, body: JSON.stringify(body), signal: controller.signal,
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(payload?.error?.message || `openrouter_chat_http_${response.status}`);
      let usage = payload.usage || null;
      if (!Number.isFinite(Number(usage?.cost)) && payload.id) {
        const endpoint = this.env.OPENROUTER_GENERATION_URL || 'https://openrouter.ai/api/v1/generation';
        const generation = await this.fetchImpl(`${endpoint}?id=${encodeURIComponent(payload.id)}`, { method: 'GET', headers, signal: controller.signal });
        if (generation.ok) {
          const generationPayload = await generation.json();
          const data = generationPayload?.data || generationPayload;
          usage = {
            prompt_tokens: Number(data?.tokens_prompt || data?.native_tokens_prompt || 0),
            completion_tokens: Number(data?.tokens_completion || data?.native_tokens_completion || 0),
            cost: Number(data?.total_cost),
            native_tokens_reasoning: Number(data?.native_tokens_reasoning || 0),
            native_tokens_cached: Number(data?.native_tokens_cached || 0),
            upstream_inference_cost: Number(data?.upstream_inference_cost || 0),
          };
        }
      }
      if (!Number.isFinite(Number(usage?.cost))) throw new Error('provider_usage_cost_unavailable');
      return { provider: 'openrouter', id: payload.id, model: payload.model || request.model, choices: payload.choices || [], usage };
    } catch (error) {
      if (error?.name === 'AbortError') throw new Error('openrouter_completion_timeout');
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }
}

export class IntelligenceProviderRegistry {
  constructor(options = {}) {
    this.env = options.env || process.env;
    this.fetchImpl = options.fetchImpl || globalThis.fetch;
    this.localProviders = parseLocalInferenceNodes(this.env).map(node => new OpenAICompatibleLocalProvider(node, { fetchImpl: this.fetchImpl }));
    this.remoteProvider = new OpenRouterProvider({ env: this.env, fetchImpl: this.fetchImpl });
  }

  async health() {
    return Promise.all(this.localProviders.map(provider => provider.health()));
  }

  async routeLocal(request = {}) {
    if (!this.localProviders.length) return { errors: ['local_inference_nodes_not_configured'], nodes: [] };
    const health = await this.health();
    const preferredNodeId = request.nodeId || request.preferredNodeId || null;
    const candidates = this.localProviders.map((provider, index) => {
      const estimate = provider.estimate(request, health[index]);
      const preferred = preferredNodeId && provider.node.id === preferredNodeId;
      const available = estimate.healthy
        && estimate.modelAvailable
        && estimate.contextAvailable
        && estimate.activeRequests < estimate.maxConcurrent;
      const score = (preferred ? 1_000_000 : 0)
        + (estimate.priority * 1000)
        - (estimate.estimatedQueueSeconds * 100)
        - (estimate.estimatedRuntimeSeconds * 10)
        - (estimate.estimatedCostUsd * 100000)
        - (estimate.healthLatencyMs || 0);
      return { provider, estimate, available, score, health: health[index] };
    }).sort((a, b) => b.score - a.score);
    const selected = candidates.find(row => row.available);
    if (!selected) {
      return {
        errors: ['no_healthy_local_model_route'],
        nodes: candidates.map(row => ({ ...row.estimate, error: row.health?.error || null })),
      };
    }
    return {
      provider: selected.provider,
      route: selected.estimate,
      nodes: candidates.map(row => ({ ...row.estimate, selected: row === selected, error: row.health?.error || null })),
    };
  }

  providerForQuote(quote) {
    if (quote?.localOrRemote === 'remote') return this.remoteProvider;
    const nodeId = quote?.localNodeId || quote?.nodeId;
    return this.localProviders.find(provider => provider.node.id === nodeId) || null;
  }
}

export function createIntelligenceProviderRegistry(options = {}) {
  return new IntelligenceProviderRegistry(options);
}
