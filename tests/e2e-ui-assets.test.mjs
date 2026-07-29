import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const html = readFileSync('apps/web/src/index.html', 'utf8');
const app = readFileSync('apps/web/src/app.js', 'utf8');
const css = readFileSync('apps/web/src/styles.css', 'utf8');

test('competition console exposes the core operator workflow', () => {
  for (const section of ['overview', 'race', 'trades', 'signals', 'learning', 'costs', 'risk', 'system']) {
    assert.match(html, new RegExp(`id="${section}"`));
    assert.match(html, new RegExp(`#${section}`));
  }
});

test('competition console uses a cockpit layout', () => {
  for (const token of ['app-frame', 'sidebar', 'command-bar', 'market-strip', 'command-queue', 'risk-stack', 'cockpit-hero']) {
    assert.match(html + css + app, new RegExp(token));
  }
});

test('dashboard ranks net results after paid-agent costs', () => {
  for (const token of ['net_equity_usd', 'operating_cost_usd', 'agent_cost_coverage_ratio', 'agent_break_even_gap_usd', 'valid_for_ranking']) {
    assert.match(app, new RegExp(token));
  }
  assert.match(app, /\/api\/competition/);
  assert.match(app, /\/api\/agents\/costs/);
});

test('dashboard surfaces guarded budget controls', () => {
  for (const token of ['budget-approval-rows', 'request-budget-approval', '/api/agents/budget-approvals']) {
    assert.match(html + app, new RegExp(token.replaceAll('/', '\\/')));
  }
  assert.match(html, /disabled title=/);
});

test('dashboard keeps live execution visibly blocked', () => {
  assert.match(html, /live orders blocked/i);
  assert.match(html, /paper \/ guarded/i);
});
