// Financial-market sentiment reads for the economic decision engine.
//
// GROK VIA THE EXISTING REGISTRY, NOT A NEW CLIENT. xAI has no integration in this
// repository, so the tempting move is a fresh fetch against api.x.ai. That would be
// the worst option available: the one thing `REMOTE_LLM_EXECUTION_ENABLED=false`
// buys us today is that no remote model can influence a decision. A hand-rolled
// client is not covered by that flag, has no spend cap, and writes nothing to the
// modelUsageLedger, so its cost is invisible to the reconciliation that can
// retroactively invalidate decisions. Routing Grok through the registry makes the
// new signal inherit all of it for free, including the remote_llm_execution_disabled
// throw at providerRegistry.mjs:301.
//
// ADVISORY ONLY. Nothing here can dispatch, size, or approve a trade. The engine
// treats sentiment as one more input to a probability, and the blend is scored in
// shadow before anyone is allowed to believe it.

const DEFAULT_MODEL = 'x-ai/grok-4-fast';
const DEFAULT_TIMEOUT_MS = 20_000;
// Reasoning models (ornith-1.5-35b-a3b among them) spend most of their budget
// on reasoning_content and return an empty content string. Verified live: 400
// max_tokens produced HTTP 200, finish_reason 'stop' and nothing parseable;
// 2000 returned a valid schema-conformant reading.
const DEFAULT_MAX_COMPLETION_TOKENS = 2_000;

// Deliberately narrow. The model returns a number we then trust in a scoring path,
// so the shape is fixed and anything else is a hard failure rather than a default.
// A provider that answers prose, or answers nothing, must not silently become a
// neutral reading: a fabricated 0.0 is indistinguishable from a real "no view" and
// would quietly dilute every forecast it touched.
const SENTIMENT_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['score', 'confidence', 'horizonMinutes', 'rationale'],
  properties: {
    score: { type: 'number', minimum: -1, maximum: 1 },
    confidence: { type: 'number', minimum: 0, maximum: 1 },
    horizonMinutes: { type: 'number', exclusiveMinimum: 0 },
    rationale: { type: 'string', minLength: 1, maxLength: 600 },
    drivers: { type: 'array', maxItems: 6, items: { type: 'string', maxLength: 160 } },
  },
};

export function buildSentimentPrompt({ symbol, horizonMinutes, observations = [], now = new Date() }) {
  const recent = observations.slice(-12).map(row => ({
    price: Number(row.price ?? row.close ?? row.mid),
    timestamp: row.timestamp || row.time || row.at || null,
  }));
  return {
    messages: [
      {
        role: 'system',
        content: [
          'You assess near-term sentiment for a liquid crypto market from market data and news context.',
          'Respond with JSON only. score is direction in [-1,1], confidence is your certainty in [0,1],',
          'and horizonMinutes is how long your read is expected to hold.',
          'Judge the next move at the given horizon. Do not restate recent price action as sentiment;',
          'if the data does not support a directional view, return a score near 0 with low confidence',
          'rather than a confident guess. You do not place trades and you do not recommend entries.',
        ].join(' '),
      },
      {
        role: 'user',
        content: JSON.stringify({ symbol, horizonMinutes, asOf: now.toISOString(), recentObservations: recent }),
      },
    ],
    responseFormat: { type: 'json_schema', json_schema: { name: 'market_sentiment', strict: true, schema: SENTIMENT_SCHEMA } },
  };
}

// Fail-closed normalisation. Returns { errors } instead of a value whenever the
// payload cannot be trusted, so a caller that forgets to check still cannot act on
// a malformed read: every caller in this repo treats a missing reading as absent
// rather than as neutral.
export function normalizeSentiment(payload) {
  const errors = [];
  if (!payload || typeof payload !== 'object') return { errors: ['sentiment_payload_required'] };
  const score = Number(payload.score);
  const confidence = Number(payload.confidence);
  const horizonMinutes = Number(payload.horizonMinutes);
  if (!Number.isFinite(score) || score < -1 || score > 1) errors.push('sentiment_score_out_of_range');
  if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) errors.push('sentiment_confidence_out_of_range');
  if (!Number.isFinite(horizonMinutes) || horizonMinutes <= 0) errors.push('sentiment_horizon_invalid');
  const rationale = typeof payload.rationale === 'string' ? payload.rationale.trim() : '';
  if (!rationale) errors.push('sentiment_rationale_required');
  if (errors.length) return { errors };
  return {
    sentiment: {
      score,
      confidence,
      horizonMinutes,
      rationale,
      drivers: Array.isArray(payload.drivers)
        ? payload.drivers.filter(row => typeof row === 'string' && row.trim()).map(row => row.trim()).slice(0, 6)
        : [],
    },
  };
}

// The model is asked for JSON but providers still wrap it in prose or fences more
// often than the schema suggests, so the first fenced object is tried before giving
// up. This is a transport concession only -- it cannot manufacture a reading, since
// normalizeSentiment still rejects anything missing or out of range.
export function extractSentimentJson(text) {
  if (typeof text !== 'string' || !text.trim()) return null;
  const candidates = [text.trim()];
  const fenced = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fenced) candidates.push(fenced[1].trim());
  const braced = text.match(/\{[\s\S]*\}/);
  if (braced) candidates.push(braced[0]);
  for (const candidate of candidates) {
    try {
      const parsed = JSON.parse(candidate);
      if (parsed && typeof parsed === 'object') return parsed;
    } catch {
      // try the next shape
    }
  }
  return null;
}

/**
 * Read sentiment for one symbol, preferring local inference.
 *
 * `registry` is an IntelligenceProviderRegistry in production and a fixture
 * offline. Two shapes are accepted deliberately, because the real registry has no
 * `execute`: it exposes `routeLocal()`, which picks among configured local nodes
 * by health, queue depth and cost. Supporting only `execute` meant the adapter
 * could not actually drive the registry it was written for -- every test used a
 * duck-typed stand-in and the gap was invisible.
 *
 * LOCAL FIRST because the brief is free endpoints only. `routeLocal` is the
 * existing mechanism for that, and it is the default mode in
 * apps/api/src/intelligencePolicy.mjs ('local_only'). Remote is only attempted
 * when the caller explicitly asks for it, and the OpenRouter provider still
 * enforces REMOTE_LLM_EXECUTION_ENABLED on its own.
 */
export async function readSentiment({ registry, symbol, horizonMinutes, observations, model, env = process.env, now = new Date(), allowRemote = false }) {
  if (!registry || (typeof registry.execute !== 'function' && typeof registry.routeLocal !== 'function')) {
    return { errors: ['sentiment_registry_required'] };
  }
  const requested = Number(horizonMinutes);
  if (!Number.isFinite(requested) || requested <= 0) return { errors: ['sentiment_horizon_required'] };

  const { messages, responseFormat } = buildSentimentPrompt({ symbol, horizonMinutes: requested, observations, now });
  const request = {
    model: model || env.SENTIMENT_MODEL || DEFAULT_MODEL,
    // Carried on the request rather than buried in the prompt so an offline
    // registry can key a recorded series by symbol. OpenRouterProvider builds
    // its body from an explicit key list, so it ignores this field.
    symbol,
    messages,
    responseFormat,
    timeoutMs: Number(env.SENTIMENT_TIMEOUT_MS) > 0 ? Number(env.SENTIMENT_TIMEOUT_MS) : DEFAULT_TIMEOUT_MS,
    temperature: 0,
    // Reasoning models emit their answer after a long thinking block: a live read
    // against ornith-1.5-35b returned 200 with empty content and finish_reason
    // 'stop' because 400 max_tokens were entirely consumed by reasoning_content.
    // Without headroom here a perfectly healthy local model silently yields no
    // reading at all.
    maxCompletionTokens: Number(env.SENTIMENT_MAX_TOKENS) > 0 ? Number(env.SENTIMENT_MAX_TOKENS) : DEFAULT_MAX_COMPLETION_TOKENS,
    // Grok is not behind OpenRouter's own routing in any way we depend on, so
    // cost accounting must come from the usage block the registry already
    // requires (providerRegistry.mjs:347 throws without it).
    usage: { include: true },
  };

  let completion;
  let route = null;
  try {
    if (typeof registry.routeLocal === 'function') {
      const routed = await registry.routeLocal(request);
      if (routed?.errors?.length || !routed?.provider) {
        if (!allowRemote || typeof registry.execute !== 'function') {
          return { errors: routed?.errors || ['no_healthy_local_model_route'] };
        }
      } else {
        route = routed.route || null;
        completion = await routed.provider.execute(request);
      }
    }
    if (!completion && typeof registry.execute === 'function') {
      // Only reached when explicitly permitted; a registry that only exposes
      // execute() (OpenRouterProvider, a fixture) comes through here.
      completion = await registry.execute(request);
    }
  } catch (error) {
    // Provider refused (remote_llm_execution_disabled, missing key, timeout, HTTP
    // error, or a local node that went away mid-read). Propagate the registry's
    // own code so callers can distinguish "we were not allowed to ask" from "we
    // asked and it failed", and fail closed either way.
    return { errors: [error?.message || 'sentiment_provider_failed'] };
  }
  if (!completion) return { errors: ['sentiment_no_completion'] };

  const content = completion?.choices?.[0]?.message?.content;
  const parsed = extractSentimentJson(content);
  if (!parsed) return { errors: ['sentiment_unparseable_response'] };
  const normalized = normalizeSentiment(parsed);
  if (normalized.errors) return normalized;
  return {
    ...normalized,
    model: completion.model || request.model,
    usage: completion.usage || null,
    costUsd: Number.isFinite(Number(completion?.usage?.cost)) ? Number(completion.usage.cost) : null,
    route: route ? { nodeId: route.nodeId ?? null, estimatedCostUsd: route.estimatedCostUsd ?? 0 } : null,
  };
}

/**
 * Offline stand-in with the same surface as IntelligenceProviderRegistry.
 *
 * The harness needs thousands of reads and must not spend money or touch the
 * network, so this serves a recorded series keyed by timestamp. It is deliberately
 * unable to express optimism: if a fixture has no reading for a timestamp it
 * returns an error, which is what forces the harness to score an incomplete
 * sentiment series honestly instead of treating gaps as neutral.
 */
export function createFixtureSentimentRegistry(readings = [], options = {}) {
  const byKey = new Map();
  for (const row of readings) {
    if (row && row.key != null) byKey.set(String(row.key), row.value);
  }
  const calls = [];
  return {
    calls,
    get callCount() {
      return calls.length;
    },
    async execute(request = {}) {
      const key = String(options.keyFor ? options.keyFor(request) : (request.symbol ?? request.messages?.[1]?.content ?? ''));
      calls.push(key);
      const value = byKey.get(key);
      if (value == null) throw new Error('fixture_sentiment_unavailable');
      if (value instanceof Error) throw value;
      return {
        provider: 'fixture',
        id: `fixture-${calls.length}`,
        model: request.model || 'fixture',
        choices: [{ message: { content: JSON.stringify(value) } }],
        usage: { cost: 0, prompt_tokens: 0, completion_tokens: 0 },
      };
    },
  };
}