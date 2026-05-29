import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { FileOperatorStore, MemoryOperatorStore, createInitialOperatorState, nextId } from '../packages/storage/src/operatorStore.mjs';

test('memory store mutates state without durable status', async () => {
  const store = new MemoryOperatorStore(createInitialOperatorState());
  const result = await store.mutate(async state => {
    const strategy = { id: nextId('strategy', state.strategies), name: 'Memory Strategy', version: 1, status: 'draft', riskLevel: 'low', parameters: {}, createdAt: 'now', updatedAt: 'now' };
    state.strategies.push(strategy);
    return strategy;
  });
  const loaded = await store.load();
  assert.equal(result.name, 'Memory Strategy');
  assert.equal(loaded.strategies.at(-1).name, 'Memory Strategy');
  assert.equal(store.getStatus().durable, false);
});

test('file store persists state across store instances', async () => {
  const root = mkdtempSync(join(tmpdir(), 'operator-store-'));
  const filePath = join(root, 'operator-state.json');
  try {
    const first = new FileOperatorStore(filePath);
    await first.mutate(async state => {
      state.killSwitch = { enabled: true, reason: 'persistence_test', updatedAt: '2026-05-29T00:00:00.000Z' };
      state.audit.push({ id: nextId('audit', state.audit), action: 'kill_switch_enabled', actor: 'test', at: '2026-05-29T00:00:00.000Z', details: 'persistence_test' });
    });

    const second = new FileOperatorStore(filePath);
    const loaded = await second.load();
    assert.equal(loaded.killSwitch.enabled, true);
    assert.equal(loaded.killSwitch.reason, 'persistence_test');
    assert.ok(loaded.audit.some(event => event.action === 'kill_switch_enabled'));
    assert.equal(second.getStatus().durable, true);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('file store normalizes missing collections', async () => {
  const root = mkdtempSync(join(tmpdir(), 'operator-store-'));
  const filePath = join(root, 'operator-state.json');
  try {
    const store = new FileOperatorStore(filePath, { bootstrap: false });
    await store.save({ schemaVersion: 1, strategies: [] });
    const loaded = await store.load();
    assert.deepEqual(loaded.strategies, []);
    assert.ok(Array.isArray(loaded.backtests));
    assert.ok(Array.isArray(loaded.audit));
    assert.equal(typeof loaded.killSwitch.enabled, 'boolean');
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
