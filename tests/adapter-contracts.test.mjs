import test from 'node:test';
import assert from 'node:assert/strict';
import { AdapterError, BrokerExecutionAdapter, assertAdapterCertification, createFailClosedAdapter } from '../packages/adapters/src/contracts.mjs';

test('base broker adapter blocks live execution when disabled', async () => {
  const adapter = new BrokerExecutionAdapter({ name: 'base', liveEnabled: false });
  await assert.rejects(() => adapter.submitLiveOrder({ symbol: 'BTC-USD' }), error => {
    assert.equal(error.code, 'live_execution_disabled');
    return true;
  });
});

test('base broker adapter requires paper certification for paper orders', async () => {
  const adapter = new BrokerExecutionAdapter({ name: 'base', liveEnabled: false });
  await assert.rejects(() => adapter.submitPaperOrder({ symbol: 'BTC-USD' }), error => {
    assert.equal(error.code, 'adapter_not_certified');
    return true;
  });
});

test('certification gate rejects mismatched, expired, and blocked certifications', () => {
  const adapter = { name: 'adapter-a' };
  assert.throws(() => assertAdapterCertification(adapter, { adapterName: 'adapter-b', status: 'certified_paper' }), /adapter_certification_mismatch/);
  assert.throws(() => assertAdapterCertification(adapter, { adapterName: 'adapter-a', status: 'blocked' }), /adapter_certification_blocked/);
  assert.throws(() => assertAdapterCertification(adapter, { adapterName: 'adapter-a', status: 'certified_paper', expiresAt: '2020-01-01T00:00:00.000Z' }), /adapter_certification_expired/);
});

test('certification gate requires certified_live and liveEnabled for live use', () => {
  const adapter = { name: 'adapter-a' };
  assert.throws(() => assertAdapterCertification(adapter, { adapterName: 'adapter-a', status: 'certified_paper', liveEnabled: false }, { requireLive: true }), /adapter_live_not_certified/);
  assert.equal(assertAdapterCertification(adapter, { adapterName: 'adapter-a', status: 'certified_live', liveEnabled: true }, { requireLive: true }), true);
});

test('fail-closed adapter supports preview and certified paper but rejects live', async () => {
  const adapter = createFailClosedAdapter('test-adapter');
  const preview = await adapter.previewOrder({ symbol: 'BTC-USD', side: 'buy', quantity: 1 });
  assert.equal(preview.ok, true);
  const paper = await adapter.submitPaperOrder({ symbol: 'BTC-USD', side: 'buy', quantity: 1 });
  assert.equal(paper.mode, 'paper');
  await assert.rejects(() => adapter.submitLiveOrder({ symbol: 'BTC-USD' }), error => {
    assert.equal(error.code, 'live_execution_disabled');
    return true;
  });
});

test('adapter errors carry code and details', () => {
  const error = new AdapterError('adapter_down', 'Adapter unavailable', { retry: true });
  assert.equal(error.code, 'adapter_down');
  assert.equal(error.details.retry, true);
});
