import { existsSync, readFileSync } from 'node:fs';

const contractPath = 'docs/API_CONTRACT_P0_P1.md';
const openapiPath = 'docs/openapi.p0p1.json';
const serverPath = 'apps/api/src/server.p1.mjs';
const routerPath = 'apps/api/src/operatorRouter.mjs';

for (const path of [contractPath, openapiPath, serverPath, routerPath]) {
  if (!existsSync(path)) {
    console.error(`api contract validation failed: missing ${path}`);
    process.exit(1);
  }
}

const contract = readFileSync(contractPath, 'utf8');
const openapi = JSON.parse(readFileSync(openapiPath, 'utf8'));
const server = readFileSync(serverPath, 'utf8');
const router = readFileSync(routerPath, 'utf8');

const requiredRoutes = [
  '/api/accounts',
  '/api/instruments',
  '/api/strategy-templates',
  '/api/strategies/from-template',
  '/api/backtests/run',
  '/api/approvals/request',
  '/api/paper-executions',
  '/api/kill-switch/stop-paper'
];

for (const route of requiredRoutes) {
  if (!contract.includes(route)) {
    console.error(`api contract validation failed: markdown contract missing ${route}`);
    process.exit(1);
  }
  if (!openapi.paths?.[route]) {
    console.error(`api contract validation failed: openapi contract missing ${route}`);
    process.exit(1);
  }
  if (!server.includes(route) && !router.includes(route)) {
    console.error(`api contract validation failed: implementation missing ${route}`);
    process.exit(1);
  }
}

const requiredPatterns = [
  ['/api/strategies/:id/clone', '/api/strategies/{id}/clone'],
  ['/api/strategies/:id/status', '/api/strategies/{id}/status'],
  ['/api/backtests/:id/report', '/api/backtests/{id}/report'],
  ['/api/approvals/:id/decision', '/api/approvals/{id}/decision'],
  ['/api/paper-executions/:id/stop', '/api/paper-executions/{id}/stop'],
  ['/api/paper-executions/:id/signal', '/api/paper-executions/{id}/signal']
];

for (const [markdownRoute, openapiRoute] of requiredPatterns) {
  if (!contract.includes(markdownRoute)) {
    console.error(`api contract validation failed: markdown contract missing ${markdownRoute}`);
    process.exit(1);
  }
  if (!openapi.paths?.[openapiRoute]) {
    console.error(`api contract validation failed: openapi contract missing ${openapiRoute}`);
    process.exit(1);
  }
}

console.log('api contract validation ok');
