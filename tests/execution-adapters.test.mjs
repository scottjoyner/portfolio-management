import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';

describe('AdapterRegistry', () => {
  let AdapterRegistry;

  before(async () => {
    const mod = await import('../packages/adapters/src/adapterRegistry.mjs');
    AdapterRegistry = mod.AdapterRegistry;
  });

  it('creates empty registry', () => {
    const reg = new AdapterRegistry();
    assert.ok(reg);
    assert.equal(reg.listAdapters().length, 0);
  });

  it('registerAdapter stores an adapter', () => {
    const reg = new AdapterRegistry();
    const adapter = { name: 'test', venue: 'paper', mode: 'paper', connected: false };
    reg.registerAdapter('test', adapter);
    assert.equal(reg.getAdapter('test'), adapter);
  });

  it('getOrCreate creates paper adapter by default', () => {
    const reg = new AdapterRegistry();
    const adapter = reg.getOrCreate('anything');
    assert.ok(adapter);
    assert.equal(adapter.venue, 'paper');
    assert.equal(adapter.mode, 'paper');
  });

  it('getOrCreate creates coinbase adapter', () => {
    const reg = new AdapterRegistry();
    const adapter = reg.getOrCreate('coinbase');
    assert.ok(adapter);
    assert.equal(adapter.venue, 'coinbase');
  });

  it('getAdapterForVenue returns correct adapter', () => {
    const reg = new AdapterRegistry();
    const adapter = reg.getAdapterForVenue('paper');
    assert.ok(adapter);
    assert.equal(adapter.venue, 'paper');

    const cb = reg.getAdapterForVenue('coinbase', 'paper');
    assert.ok(cb);
    assert.equal(cb.venue, 'coinbase');
    assert.equal(cb.mode, 'paper');
  });

  it('listAdapters returns registered adapters', () => {
    const reg = new AdapterRegistry();
    reg.registerAdapter('a1', { name: 'a1', venue: 'paper', mode: 'paper', connected: true });
    reg.registerAdapter('a2', { name: 'a2', venue: 'coinbase', mode: 'live', connected: false });
    const list = reg.listAdapters();
    assert.equal(list.length, 2);
  });

  it('connectAll attempts connection on all adapters', async () => {
    const reg = new AdapterRegistry();
    let connected = false;
    reg.registerAdapter('test', {
      name: 'test',
      venue: 'paper',
      mode: 'paper',
      connected: false,
      async connect() { connected = true; return true; },
    });
    await reg.connectAll();
    assert.equal(connected, true);
  });
});

describe('PaperBrokerAdapter', () => {
  let mod;

  before(async () => {
    mod = await import('../packages/adapters/src/adapterRegistry.mjs');
  });

  it('created via registry, returns accounts', async () => {
    const reg = new mod.AdapterRegistry();
    const paper = reg.getOrCreate('paper-test');
    assert.ok(paper);

    const accounts = await paper.getAccounts();
    assert.ok(accounts.length > 0);
    assert.equal(accounts[0].provider, 'paper');
    assert.equal(accounts[0].currency, 'USD');
  });

  it('previewOrder rejects insufficient cash', async () => {
    const reg = new mod.AdapterRegistry();
    const paper = reg.getOrCreate('paper-test2');
    const result = await paper.previewOrder({
      side: 'buy',
      quantity: 1e6,
      price: 1000,
      marketId: 'BTC-USD',
      venue: 'paper',
    });
    assert.equal(result.ok, false);
    assert.ok(result.errors.includes('insufficient_paper_cash'));
  });

  it('submitOrder creates fills', async () => {
    const reg = new mod.AdapterRegistry();
    const paper = reg.getOrCreate('paper-test3');
    const result = await paper.submitOrder({
      marketId: 'BTC-USD',
      venue: 'paper',
      side: 'buy',
      quantity: 0.1,
      price: 50000,
      orderType: 'market',
      timeInForce: 'GTC',
      strategyId: 'test',
      executionMode: 'paper',
      id: 'test-ord',
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });
    assert.equal(result.ok, true);
    assert.equal(result.status, 'filled');
    assert.equal(result.fills.length, 1);
    assert.equal(result.fills[0].quantity, 0.1);
  });

  it('getPositions returns tracked positions', async () => {
    const reg = new mod.AdapterRegistry();
    const paper = reg.getOrCreate('paper-test4');

    // Buy BTC
    await paper.submitOrder({
      marketId: 'BTC-USD', venue: 'paper', side: 'buy', quantity: 1, price: 50000,
      orderType: 'market', timeInForce: 'GTC', strategyId: 'test', executionMode: 'paper',
      id: 'ord1', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
    });

    const positions = await paper.getPositions();
    const btc = positions.find(p => p.symbol === 'BTC-USD');
    assert.ok(btc);
    assert.equal(btc.quantity, 1);
  });
});
