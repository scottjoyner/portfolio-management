import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const html = readFileSync('apps/web/src/index.html', 'utf8');
const app = readFileSync('apps/web/src/app.js', 'utf8');
const css = readFileSync('apps/web/src/styles.css', 'utf8');

test('expanded dashboard exposes all operator navigation sections', () => {
  for (const section of ['overview', 'portfolio', 'live-markets', 'strategies', 'backtests', 'opportunities', 'polymarket', 'agents', 'risk', 'approvals', 'paper', 'audit']) {
    assert.match(html, new RegExp(`id="${section}"`));
    assert.match(html, new RegExp(`#${section}`));
  }
});

test('dashboard uses cockpit layout rather than flat admin layout', () => {
  for (const token of ['app-frame', 'sidebar', 'command-bar', 'market-strip', 'command-queue', 'risk-stack', 'cockpit-hero']) {
    assert.match(html + css + app, new RegExp(token));
  }
});

test('dashboard renders opportunity risk and cost fields from API-backed UI code', () => {
  for (const token of ['totalMoneyRisked', 'maxLoss', 'potentialUpside', 'grossExpectedValue', 'netExpectedValue', 'agentResearchCost', 'modelInferenceCost']) {
    assert.match(app, new RegExp(token));
  }
  assert.doesNotMatch(app, /dashboard-data\.js/);
});

test('dashboard exposes budget approval controls in the agents panel', () => {
  for (const token of ['budget-approval-cards', 'request-budget-approval', 'budgetApprovals', '/api/agents/budget-approvals']) {
    assert.match(html + app, new RegExp(token.replaceAll('/', '\\/')));
  }
});

test('dashboard keeps live execution visibly blocked', () => {
  assert.match(html, /live blocked/i);
  assert.match(html, /live orders blocked/i);
});
