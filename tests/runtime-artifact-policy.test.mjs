import test from 'node:test';
import assert from 'node:assert/strict';
import { findForbiddenTrackedPaths, isForbiddenTrackedPath } from '../scripts/validate-runtime-artifacts.mjs';

test('runtime artifact policy blocks live state and nested runtime trees', () => {
  const paths = [
    'data/hermes_agent_ledger.json',
    'data/state_backups/paper_trader_v4_20260723_153406.json',
    'portfolio-management/data/operator-state.json',
    'data/competition_state.json',
    'src/safe.py',
  ];
  assert.deepEqual(findForbiddenTrackedPaths(paths), paths.slice(0, 4).sort());
});

test('runtime artifact policy permits sanitized fixtures', () => {
  assert.equal(isForbiddenTrackedPath('tests/fixtures/data/hermes_agent_ledger.json'), false);
  assert.equal(isForbiddenTrackedPath('data/fixtures/competition_state.json'), false);
  assert.equal(isForbiddenTrackedPath('docs/example.env'), false);
});
