import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const html = readFileSync('apps/web/src/index.html', 'utf8');
const app = readFileSync('apps/web/src/app.js', 'utf8');
const economics = readFileSync('apps/web/src/economics.js', 'utf8');
const css = readFileSync('apps/web/src/styles.css', 'utf8');
const economicsCss = readFileSync('apps/web/src/economics.css', 'utf8');
const combined = html + app + economics + css + economicsCss;

test('daily operations console exposes the core operator workflow', () => {
  for (const section of ['overview', 'trades', 'positions', 'signals', 'race', 'agent', 'system']) {
    assert.match(html, new RegExp(`id="${section}"`));
    assert.match(html, new RegExp(`#${section}`));
  }
  assert.match(html, /\/ui\/economics\.js/);
  assert.match(economics, /id="economics"/);
  assert.match(economics, /data-economic-nav/);
  assert.match(economics, /\/ui\/economics\.css/);
});

test('default view answers daily trading questions before showing the race', () => {
  for (const token of ['daily-brief-title', 'command-queue', 'execution-pipeline', 'position-preview', 'activity-feed']) {
    assert.match(combined, new RegExp(token));
  }
  assert.match(html, /What the bot and agent are doing/i);
  assert.match(app, /Trading evidence needs operator review/);
});

test('execution view traces owner, lifecycle, orders, fills, and events', () => {
  for (const token of ['execution-status-filter', 'execution-owner-filter', 'execution-search', 'execution-list', 'executionStages', 'executionEvents']) {
    assert.match(combined, new RegExp(token));
  }
  assert.match(app, /\/api\/execution\/events/);
});

test('positions and decisions are first-class daily views', () => {
  for (const token of ['position-rows', 'position-summary-chips', 'decision-list', 'decision-summary-chips']) {
    assert.match(combined, new RegExp(token));
  }
  assert.match(app, /\/api\/positions/);
  assert.match(app, /\/api\/market-data\/live-quotes/);
});

test('competition remains cost-adjusted and fail-closed', () => {
  for (const token of ['net_equity_usd', 'operating_cost_usd', 'agent_cost_coverage_ratio', 'agent_break_even_gap_usd', 'valid_for_ranking']) {
    assert.match(app, new RegExp(token));
  }
  assert.match(app, /No trustworthy winner yet/);
  assert.match(app, /\/api\/competition/);
});

test('economic view exposes the complete two-phase lifecycle', () => {
  for (const token of [
    'economic-lifecycle',
    'economic-summary',
    'economic-forecast',
    'economic-intelligence',
    'economic-edge',
    'economic-maintenance',
    'economic-attribution',
    'economic-governance',
    'economic-decisions',
  ]) {
    assert.match(economics, new RegExp(token));
  }
  for (const token of [
    'maximumIntelligenceSpendUsd',
    'netExecutableEdgeUsd',
    'incrementalPnlUsd',
    'unreconciledQuotes',
    'modelUsageReconciled',
    'pendingAttribution',
    'decisionPhase',
    'modelCostSource',
  ]) {
    assert.match(economics, new RegExp(token));
  }
  assert.match(economics, /\/api\/economics\/dashboard/);
  assert.match(economics, /Provider-reported actual cost becomes authoritative/);
  assert.match(economics, /pre-call purchase decision never authorizes the trade/i);
});

test('economics UI exposes guarded maintenance actions but no unscoped intelligence purchase', () => {
  assert.match(economics, /\/api\/economics\/maintenance\/run/);
  assert.match(economics, /\/api\/economics\/model-pricing\/refresh/);
  assert.match(economics, /Run maintenance/);
  assert.match(economics, /Refresh pricing/);
  assert.doesNotMatch(economics, /\/api\/economics\/intelligence\/execute/);
});

test('economic evidence is surfaced in the daily safety strip and attention queue', () => {
  assert.match(economics, /strip-economics/);
  assert.match(economics, /data-economic-attention/);
  assert.match(economics, /Economic engine needs/);
  assert.match(economicsCss, /economic-enabled\.safety-strip/);
});

test('economics layout remains responsive and readable', () => {
  assert.match(economicsCss, /economic-summary-grid/);
  assert.match(economicsCss, /economic-lifecycle/);
  assert.match(economicsCss, /economic-table-wrap/);
  assert.match(economicsCss, /@media \(max-width: 620px\)/);
});

test('dashboard keeps paid-agent budget and live execution controls guarded', () => {
  for (const token of ['budget-approval-rows', 'request-budget-approval', '/api/agents/budget-approvals']) {
    assert.match(combined, new RegExp(token.replaceAll('/', '\\/')));
  }
  assert.match(html, /disabled title=/);
  assert.match(html, /live orders blocked/i);
  assert.match(html, /paper \/ guarded/i);
  assert.match(combined, /Local operator execution \(non-canonical\)/);
});
