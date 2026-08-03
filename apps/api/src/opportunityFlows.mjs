import * as legacy from './opportunityFlowsLegacy.mjs';

export * from './opportunityFlowsLegacy.mjs';

export function createResearchJob(state, body = {}, now = new Date().toISOString()) {
  const requestedAt = body.requestedAt || body.queuedAt || now;
  return legacy.createResearchJob(state, body, requestedAt);
}
