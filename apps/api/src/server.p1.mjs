import http from 'node:http';
import { Readable } from 'node:stream';

import {
  createInitialState,
  handleRequest as legacyHandleRequest,
} from './server.p1Legacy.mjs';
import { assertRuntimeEnv } from '../../../packages/config/src/runtimeEnv.mjs';
import { createOperatorStore } from '../../../packages/storage/src/operatorStoreFactory.mjs';
import { startAutoRotate, stopAutoRotate } from './secrets.mjs';
import {
  evaluateRemoteIntelligencePolicy,
  intelligenceRoutingPolicyView,
  normalizeIntelligenceRoutingPolicy,
  validateIntelligenceRoutingPolicy,
} from './intelligencePolicy.mjs';

const JSON_TYPE = 'application/json; charset=utf-8';
const POLICY_ROUTE = '/api/economics/intelligence/policy';
const MODEL_QUOTE_ROUTE = '/api/economics/model-quotes';
const INTELLIGENCE_EXECUTE_ROUTE = '/api/economics/intelligence/execute';
const POLICY_UI_ASSET = '<script type="module" src="/ui/intelligence-policy.js"></script>';

export { createInitialState };

function requestNow(options = {}) {
  const value = options.now instanceof Date ? options.now : options.now ? new Date(options.now) : new Date();
  return Number.isNaN(value.getTime()) ? new Date().toISOString() : value.toISOString();
}

function researchMutation(pathname, method) {
  return method === 'POST'
    && (pathname === '/api/agents/jobs' || /^\/api\/opportunities\/[^/]+\/request-research$/.test(pathname));
}

async function readBody(req) {
  let data = '';
  for await (const chunk of req) {
    data += chunk;
    if (data.length > 1_000_000) throw new Error('request_body_too_large');
  }
  if (!data.trim()) return {};
  try { return JSON.parse(data); } catch { throw new Error('invalid_json'); }
}

function replacementRequest(req, body, url = req.url, method = req.method) {
  const stream = new Readable({ read() {} });
  stream.method = method;
  stream.url = url;
  stream.headers = { ...(req.headers || {}), 'content-type': 'application/json' };
  stream.push(JSON.stringify(body || {}));
  stream.push(null);
  return stream;
}

function parsedBody(response) {
  try { return JSON.parse(response.body); } catch { return {}; }
}

function jsonFrom(response, status, body) {
  return {
    status,
    headers: { ...(response.headers || {}), 'content-type': JSON_TYPE },
    body: JSON.stringify(body, null, 2),
  };
}

function withPolicyUiAsset(response, method, pathname) {
  if (method !== 'GET' || !['/', '/ui/index.html'].includes(pathname) || response.status !== 200) return response;
  if (!String(response.headers?.['content-type'] || '').includes('text/html') || response.body.includes(POLICY_UI_ASSET)) return response;
  return { ...response, body: response.body.replace('</body>', `${POLICY_UI_ASSET}\n</body>`) };
}

function requestEnvironment(options = {}) {
  return { ...process.env, ...(options.env || {}) };
}

async function policyState(store) {
  return typeof store?.load === 'function' ? store.load() : store?.state || createInitialState();
}

async function policyAuthProbe(req, options) {
  return legacyHandleRequest(replacementRequest(req, {}, '/api/config', 'GET'), options);
}

async function handlePolicyRequest(req, options, store, method) {
  const env = requestEnvironment(options);
  if (method === 'GET') {
    const authenticated = await legacyHandleRequest(replacementRequest(req, {}, '/api/config', 'GET'), options);
    if (authenticated.status >= 400) return authenticated;
    const metadata = parsedBody(authenticated);
    const view = intelligenceRoutingPolicyView(await policyState(store), env, options.now || new Date());
    return jsonFrom(authenticated, 200, {
      ok: true,
      ...view,
      requestId: metadata.requestId,
      actor: metadata.actor,
      role: metadata.role,
    });
  }

  if (!['PUT', 'POST'].includes(method)) {
    const authenticated = await policyAuthProbe(req, options);
    if (authenticated.status >= 400) return authenticated;
    return jsonFrom(authenticated, 405, { ok: false, error: 'method_not_allowed' });
  }

  const body = await readBody(req);
  const validation = validateIntelligenceRoutingPolicy(body);
  if (!validation.ok) {
    const authenticated = await policyAuthProbe(req, options);
    if (authenticated.status >= 400) return authenticated;
    const metadata = parsedBody(authenticated);
    return jsonFrom(authenticated, 400, {
      ok: false,
      errors: validation.errors,
      requestId: metadata.requestId,
      actor: metadata.actor,
      role: metadata.role,
    });
  }

  const policy = normalizeIntelligenceRoutingPolicy({
    ...body,
    updatedAt: requestNow(options),
    updatedBy: body.updatedBy || 'operator',
  });
  const persisted = await legacyHandleRequest(replacementRequest(req, {
    intelligenceRoutingPolicy: policy,
  }, '/api/config', 'POST'), options);
  if (persisted.status >= 400) return persisted;
  const metadata = parsedBody(persisted);
  const view = intelligenceRoutingPolicyView(await policyState(store), env, options.now || new Date());
  return jsonFrom(persisted, 200, {
    ok: true,
    ...view,
    requestId: metadata.requestId,
    actor: metadata.actor,
    role: metadata.role,
  });
}

async function markQuoteRoutingDecision(store, quoteId, decision, status = null) {
  if (!quoteId || typeof store?.mutate !== 'function') return;
  await store.mutate(state => {
    const quote = state.modelUsageLedger?.find(row => row.id === quoteId);
    if (!quote) return { errors: ['model_quote_not_found'] };
    quote.routingPolicyDecision = decision;
    quote.expectedDecisionImprovementUsd = decision.expectedDecisionImprovementUsd;
    quote.routingPolicyMode = decision.policy.mode;
    if (status) {
      quote.status = status;
      quote.failureReason = decision.blockers?.[0] || 'intelligence_routing_policy_blocked';
    }
    return { modelQuote: quote };
  });
}

async function quoteAutomaticLocal(req, options, body, policy, details = {}) {
  const localOut = await legacyHandleRequest(replacementRequest(req, {
    ...body,
    localOrRemote: 'local',
    model: body.localModel || body.model,
  }), options);
  if (localOut.status >= 400) return localOut;
  const localResponse = parsedBody(localOut);
  localResponse.routingDecision = {
    selected: 'local',
    automatic: true,
    policy,
    ...details,
  };
  return jsonFrom(localOut, localOut.status, localResponse);
}

async function handleModelQuoteRequest(req, options, store) {
  const body = await readBody(req);
  const env = requestEnvironment(options);
  const stateBefore = await policyState(store);
  const policy = normalizeIntelligenceRoutingPolicy(stateBefore.config?.intelligenceRoutingPolicy);
  const automatic = body.localOrRemote === 'auto';

  if (automatic && (policy.mode === 'local_only' || (policy.mode === 'openrouter_allowed' && body.preferRemote !== true))) {
    return quoteAutomaticLocal(req, options, body, policy, {
      reason: policy.mode === 'local_only' ? 'intelligence_policy_local_only' : 'remote_not_preferred',
    });
  }

  const requested = automatic
    ? { ...body, localOrRemote: 'remote', model: body.remoteModel || body.model }
    : body;
  const out = await legacyHandleRequest(replacementRequest(req, requested), options);
  if (out.status >= 400) {
    if (automatic && policy.fallbackToLocalOnRemoteBlock) {
      const remoteFailure = parsedBody(out);
      return quoteAutomaticLocal(req, options, body, policy, {
        reason: 'remote_quote_unavailable',
        remoteStatus: out.status,
        remoteBlockers: remoteFailure.errors || [remoteFailure.error || 'remote_quote_unavailable'],
      });
    }
    return out;
  }
  if (requested.localOrRemote === 'local') return out;

  const response = parsedBody(out);
  const quote = response.modelQuote;
  if (!quote) return out;
  const state = await policyState(store);
  const decision = evaluateRemoteIntelligencePolicy(state, {
    estimatedCostUsd: quote.estimatedCostUsd,
    expectedDecisionImprovementUsd: body.expectedDecisionImprovementUsd ?? body.expectedValueOfInformationUsd,
    excludeQuoteId: quote.id,
  }, env, options.now || new Date());

  if (decision.allowed) {
    await markQuoteRoutingDecision(store, quote.id, decision);
    response.modelQuote.routingPolicyDecision = decision;
    response.routingDecision = { selected: 'remote', automatic, ...decision };
    return jsonFrom(out, out.status, response);
  }

  await markQuoteRoutingDecision(store, quote.id, decision, automatic ? 'comparison_only' : 'policy_blocked');
  if (automatic && policy.fallbackToLocalOnRemoteBlock) {
    return quoteAutomaticLocal(req, options, body, decision.policy, {
      reason: 'remote_policy_blocked',
      remoteComparisonQuoteId: quote.id,
      remoteBlockers: decision.blockers,
    });
  }

  return jsonFrom(out, 409, {
    ok: false,
    error: 'remote_intelligence_policy_blocked',
    errors: decision.blockers,
    routingDecision: decision,
    modelQuoteId: quote.id,
    requestId: response.requestId,
    actor: response.actor,
    role: response.role,
  });
}

async function handleIntelligenceExecution(req, options, store) {
  const body = await readBody(req);
  const state = await policyState(store);
  const quote = state.modelUsageLedger?.find(row => row.id === body.modelQuoteId);
  if (!quote || quote.localOrRemote !== 'remote') {
    return legacyHandleRequest(replacementRequest(req, body), options);
  }

  const economicDecision = state.economicDecisions?.find(row => row.id === body.economicDecisionId);
  const policyDecision = evaluateRemoteIntelligencePolicy(state, {
    estimatedCostUsd: quote.authoritativeCostUsd ?? quote.estimatedCostUsd,
    expectedDecisionImprovementUsd: quote.expectedDecisionImprovementUsd
      ?? quote.routingPolicyDecision?.expectedDecisionImprovementUsd
      ?? economicDecision?.expectedDecisionImprovementUsd,
    excludeQuoteId: quote.id,
  }, requestEnvironment(options), options.now || new Date());

  if (!policyDecision.allowed) {
    const authenticated = await legacyHandleRequest(replacementRequest(req, {}, '/api/economics/dashboard', 'GET'), options);
    if (authenticated.status >= 400) return authenticated;
    const metadata = parsedBody(authenticated);
    return jsonFrom(authenticated, 409, {
      ok: false,
      error: 'remote_intelligence_policy_blocked',
      errors: policyDecision.blockers,
      routingDecision: policyDecision,
      modelQuoteId: quote.id,
      requestId: metadata.requestId,
      actor: metadata.actor,
      role: metadata.role,
    });
  }

  return legacyHandleRequest(replacementRequest(req, body), options);
}

export async function handleRequest(req, options = {}) {
  const url = new URL(req.url || '/', 'http://localhost');
  const method = req.method || 'GET';
  const store = options.store || createOperatorStore(options);
  const delegatedOptions = options.store ? options : { ...options, store };

  if (method === 'GET' && url.pathname === '/metrics') {
    const alias = Object.create(req);
    alias.url = '/metrics.prom';
    return legacyHandleRequest(alias, delegatedOptions);
  }

  if (url.pathname === POLICY_ROUTE) {
    return handlePolicyRequest(req, delegatedOptions, store, method);
  }

  if (method === 'POST' && url.pathname === MODEL_QUOTE_ROUTE) {
    return handleModelQuoteRequest(req, delegatedOptions, store);
  }

  if (method === 'POST' && url.pathname === INTELLIGENCE_EXECUTE_ROUTE) {
    return handleIntelligenceExecution(req, delegatedOptions, store);
  }

  if (researchMutation(url.pathname, method)) {
    const body = await readBody(req);
    if (!body.localOrRemote && body.modelQuoteId) {
      const state = await policyState(store);
      const quote = state.modelUsageLedger?.find(row => row.id === body.modelQuoteId);
      if (quote && ['local', 'remote'].includes(quote.localOrRemote)) {
        body.localOrRemote = quote.localOrRemote;
        body.provider ||= quote.provider;
        body.model ||= quote.model;
      }
    }
    body.localOrRemote ||= 'local';
    body.status ||= 'queued';
    body.requestedAt ||= requestNow(options);
    return legacyHandleRequest(replacementRequest(req, body), delegatedOptions);
  }

  const out = await legacyHandleRequest(req, delegatedOptions);
  return withPolicyUiAsset(out, method, url.pathname);
}

export function startServer(port = Number(process.env.PORT || 3000), options = {}) {
  assertRuntimeEnv({ ...process.env, ...(options.env || {}) });
  const store = createOperatorStore(options);
  const server = http.createServer(async (req, res) => {
    try {
      const out = await handleRequest(req, { ...options, store });
      res.writeHead(out.status, out.headers);
      res.end(out.body);
    } catch (error) {
      res.writeHead(500, { 'content-type': JSON_TYPE });
      res.end(JSON.stringify({ ok: false, error: error.message || 'internal_error' }, null, 2));
    }
  });
  server.listen(port);
  startAutoRotate({
    getConfig: () => store.state?.config || {},
    mutate: fn => store.mutate(fn),
    intervalMs: Number(process.env.SECRET_AUTO_ROTATE_MS || 86_400_000),
  });
  server.once('close', stopAutoRotate);
  return server;
}

if (process.argv[1] === new URL(import.meta.url).pathname) startServer();
