import test from 'node:test';
import assert from 'node:assert/strict';
import { AdapterError, BrokerExecutionAdapter, createFailClosedAdapter } from '../packages/adapters/src/contracts.mjs';

test('base broker adapter blocks live execution when disabled', async () => {
  const adapter = new BrokerExecutionAdapter({ name: 'base', liveEnabled: false });
  await assert.rejects(() => adapter.submitLiveOrder({ symbol: 'BTC-USD' }), error => {
    assert.equal(error.code, 'live_execution_disabled');
    return true;
  });
});

test('fail-closed adapter supports preview and paper but rejects live', async () => {
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
