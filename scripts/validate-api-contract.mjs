#!/usr/bin/env node
import { existsSync, readFileSync } from 'node:fs';

const files = {
  operatorContract: 'docs/API_CONTRACT_P0_P1.md',
  economicContract: 'docs/API_CONTRACT_ECONOMICS.md',
  openapi: 'docs/openapi.p0p1.json',
  server: 'apps/api/src/server.p1.mjs',
  serverLegacy: 'apps/api/src/server.p1Legacy.mjs',
  operatorRouter: 'apps/api/src/operatorRouter.mjs',
  operatorRouterLegacy: 'apps/api/src/operatorRouterLegacy.mjs',
  economicRouter: 'apps/api/src/economicRouter.mjs',
  intelligence: 'apps/api/src/intelligenceExecution.mjs',
};

const errors = [];
for (const path of Object.values(files)) if (!existsSync(path)) errors.push(`missing_file:${path}`);
if (errors.length) {
  process.stderr.write(`${JSON.stringify({ ok: false, errors }, null, 2)}\n`);
  process.exit(1);
}

const operatorContract = readFileSync(files.operatorContract, 'utf8');
const economicContract = readFileSync(files.economicContract, 'utf8');
const openapi = JSON.parse(readFileSync(files.openapi, 'utf8'));
const implementation = [
  files.server,
  files.serverLegacy,
  files.operatorRouter,
  files.operatorRouterLegacy,
  files.economicRouter,
  files.intelligence,
].map(path => readFileSync(path, 'utf8')).join('\n');

const operatorRoutes = [
  '/health', '/ready', '/ready/production-paper', '/metrics', '/metrics.prom',
  '/api/operator/summary', '/api/system-truth', '/api/competition', '/api/positions',
  '/api/audit/verify', '/api/accounts', '/api/instruments', '/api/strategy-templates',
  '/api/strategies/from-template', '/api/backtests/run', '/api/approvals/request',
  '/api/paper-executions', '/api/kill-switch/stop-paper', '/api/opportunity-dashboard',
  '/api/opportunities', '/api/risk-breakdowns', '/api/agents/jobs', '/api/agents/budgets',
  '/api/agents/budget-approvals', '/api/agents/costs', '/api/market-data/snapshots',
  '/api/connectors/market-data/ingest', '/api/opportunities/generate-from-connectors',
  '/api/polymarket/opportunities',
];

const economicRoutes = [
  '/api/economics/dashboard',
  '/api/economics/model-pricing',
  '/api/economics/model-pricing/refresh',
  '/api/economics/intelligence/nodes',
  '/api/economics/model-quotes',
  '/api/economics/intelligence/execute',
  '/api/economics/model-usage/reconcile',
  '/api/economics/maintenance/run',
  '/api/economics/forecasts',
  '/api/economics/execution-costs',
  '/api/economics/coinbase/refresh',
  '/api/economics/decisions/evaluate',
  '/api/economics/attribution',
  '/api/economics/calibration',
];

for (const route of operatorRoutes) {
  if (!operatorContract.includes(route)) errors.push(`operator_markdown_missing:${route}`);
  if (!openapi.paths?.[route]) errors.push(`openapi_missing:${route}`);
  if (!implementation.includes(route)) errors.push(`implementation_missing:${route}`);
}
for (const route of economicRoutes) {
  if (!economicContract.includes(route)) errors.push(`economic_markdown_missing:${route}`);
  if (!openapi.paths?.[route]) errors.push(`openapi_missing:${route}`);
  if (!implementation.includes(route)) errors.push(`implementation_missing:${route}`);
}

const parameterized = [
  ['/api/strategies/:id/clone', '/api/strategies/{id}/clone', operatorContract],
  ['/api/strategies/:id/status', '/api/strategies/{id}/status', operatorContract],
  ['/api/backtests/:id/report', '/api/backtests/{id}/report', operatorContract],
  ['/api/approvals/:id/decision', '/api/approvals/{id}/decision', operatorContract],
  ['/api/paper-executions/:id/stop', '/api/paper-executions/{id}/stop', operatorContract],
  ['/api/paper-executions/:id/signal', '/api/paper-executions/{id}/signal', operatorContract],
  ['/api/opportunities/:id', '/api/opportunities/{id}', operatorContract],
  ['/api/opportunities/:id/approve', '/api/opportunities/{id}/approve', operatorContract],
  ['/api/opportunities/:id/request-research', '/api/opportunities/{id}/request-research', operatorContract],
  ['/api/agents/budget-approvals/:id/decision', '/api/agents/budget-approvals/{id}/decision', operatorContract],
  ['/api/economics/forecasts/:id/outcome', '/api/economics/forecasts/{id}/outcome', economicContract],
];
for (const [markdownRoute, openapiRoute, contract] of parameterized) {
  if (!contract.includes(markdownRoute)) errors.push(`markdown_missing:${markdownRoute}`);
  if (!openapi.paths?.[openapiRoute]) errors.push(`openapi_missing:${openapiRoute}`);
}

const requiredSafetyTokens = [
  'REMOTE_LLM_EXECUTION_ENABLED',
  'LOCAL_LLM_EXECUTION_REQUIRED',
  'economicDecisionRefreshRequired',
  'provider execution occurs outside',
  'does not expose configured API keys',
];
const combinedContract = `${operatorContract}\n${economicContract}\n${implementation}`.toLowerCase();
for (const token of requiredSafetyTokens) {
  if (!combinedContract.includes(token.toLowerCase())) errors.push(`safety_contract_missing:${token}`);
}

if (errors.length) {
  process.stderr.write(`${JSON.stringify({ ok: false, errors }, null, 2)}\n`);
  process.exit(1);
}
process.stdout.write(`${JSON.stringify({ ok: true, operatorRoutes: operatorRoutes.length, economicRoutes: economicRoutes.length, openapiVersion: openapi.info?.version }, null, 2)}\n`);
