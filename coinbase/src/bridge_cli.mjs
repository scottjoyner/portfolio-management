#!/usr/bin/env node
/* Bridge script: calls coinbase CLI instead of Python SDK.
   Usage: node bridge_cli.mjs '{"action":"list_accounts"}'
   Compatible with the same JSON format as bridge_execution.py.
*/
import { execSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

const CLI = 'coinbase';
const KEY_FILE = join(homedir(), '.coinbase', 'api_key.json');

// Auto-configure CLI if not already set up
function ensureEnv() {
  try {
    execSync(`${CLI} env live --status`, { encoding: 'utf-8', timeout: 5000 });
    return; // already configured
  } catch { /* not configured */ }
  if (existsSync(KEY_FILE)) {
    try {
      execSync(`${CLI} env live --key-file ${KEY_FILE} --allow-plaintext-secrets`, { encoding: 'utf-8', timeout: 10000 });
    } catch { /* config failed */ }
  }
}

function run(args, parseJson = true) {
  const stdout = execSync(`${CLI} ${args}`, { encoding: 'utf-8', timeout: 30000 });
  if (!parseJson) return stdout.trim();
  try { return JSON.parse(stdout.trim()); } catch { return stdout.trim(); }
}

function handle(action, payload = {}) {
  switch (action) {
    case 'list_accounts':
    case 'health': {
      const data = run('balance');
      const accounts = (data.accounts || []).map(a => ({
        currency: a.currency,
        balance: parseFloat(a.available_balance?.value || 0),
        available: parseFloat(a.available_balance?.value || 0),
        hold: parseFloat(a.hold?.value || 0),
      }));
      return { ok: true, data: accounts };
    }

    case 'best_bid_ask': {
      const productIds = payload.product_ids || [];
      const data = run('products best-bid-ask');
      let pricebooks = data.pricebooks || [];
      if (productIds.length > 0) {
        pricebooks = pricebooks.filter(p => productIds.includes(p.product_id));
      }
      return { ok: true, data: { pricebooks } };
    }

    case 'get_products': {
      const data = run('products list');
      const products = (data.products || []).map(p => ({
        product_id: p.product_id,
        price: p.price,
        price_percentage_change_24h: p.price_percentage_change_24h,
        volume_24h: p.volume_24h,
        status: p.status,
      }));
      return { ok: true, data: products };
    }

    case 'get_product': {
      const pid = payload.product_id;
      if (!pid) return { ok: false, error: 'product_id_required' };
      const data = run(`products get ${pid}`);
      return { ok: true, data: typeof data === 'object' ? data : { product_id: pid } };
    }

    case 'list_orders': {
      const flags = [];
      if (payload.product_id) flags.push(`product_id=${payload.product_id}`);
      flags.push(`order_status=${payload.order_status || 'OPEN'}`);
      flags.push(`limit=${payload.limit || 50}`);
      const data = run(`orders list ${flags.join(' ')}`);
      const orders = (data.orders || []).map(o => ({
        order_id: o.order_id,
        product_id: o.product_id,
        side: o.side,
        status: o.status,
        size: o.size || '0',
        filled_size: o.filled_size || '0',
        price: o.price || '0',
        average_filled_price: o.average_filled_price || o.price || '0',
        filled_value: o.filled_value || '0',
        client_order_id: o.client_order_id || '',
        leaves_quantity: o.leaves_quantity || '0',
        created_time: o.created_time || o.created_at || '',
      }));
      return { ok: true, data: orders };
    }

    case 'get_order': {
      const oid = payload.order_id;
      if (!oid) return { ok: false, error: 'order_id_required' };
      const data = run(`orders get ${oid}`);
      return { ok: true, data: typeof data === 'object' ? data : { order_id: oid } };
    }

    case 'list_fills': {
      const flags = [];
      if (payload.order_id) flags.push(`order_id=${payload.order_id}`);
      if (payload.product_id) flags.push(`product_id=${payload.product_id}`);
      flags.push(`limit=${payload.limit || 100}`);
      const data = run(`orders fills ${flags.join(' ')}`);
      const fills = (data.fills || []).map(f => ({
        fill_id: f.fill_id || f.entry_id || '',
        order_id: f.order_id || '',
        product_id: f.product_id || '',
        side: f.side || '',
        liquidity: f.liquidity_indicator || '',
        size: f.size || '0',
        price: f.price || '0',
        fee: f.commission || f.fee || '0',
        value: f.filled_value || f.value || '0',
        created_at: f.created_at || f.created_at_time || '',
      }));
      return { ok: true, data: fills };
    }

    case 'preview_order': {
      const side = (payload.side || 'buy').toUpperCase();
      const productId = payload.product_id;
      const flags = [`product_id=${productId}`, `side=${side}`, 'type=market'];
      if (payload.base_size) flags.push(`base_size=${payload.base_size}`);
      if (payload.quote_size) flags.push(`quote_size=${payload.quote_size}`);
      const data = run(`orders preview ${flags.join(' ')}`);
      return { ok: true, data: typeof data === 'object' ? data : { preview: 'ok' } };
    }

    case 'submit_order': {
      const side = (payload.side || 'buy').toUpperCase();
      const productId = payload.product_id;
      const flags = [`product_id=${productId}`, `side=${side}`, 'type=market'];
      if (payload.base_size) flags.push(`base_size=${payload.base_size}`);
      if (payload.quote_size) flags.push(`quote_size=${payload.quote_size}`);
      const data = run(`orders create ${flags.join(' ')}`);
      return { ok: true, data: typeof data === 'object' ? data : { order_id: `cb-${Date.now()}` } };
    }

    case 'get_product_book': {
      const pid = payload.product_id;
      if (!pid) return { ok: false, error: 'product_id_required' };
      const data = run(`products book ${pid}`);
      const book = data?.pricebook || data;
      return { ok: true, data: { bids: book.bids || [], asks: book.asks || [] } };
    }

    case 'get_candles': {
      const pid = payload.product_id;
      if (!pid) return { ok: false, error: 'product_id_required' };
      const flags = [];
      const toRfc3339 = (u) => new Date(u * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z');
      if (payload.start_unix) flags.push(`start==${toRfc3339(payload.start_unix)}`);
      if (payload.end_unix) flags.push(`end==${toRfc3339(payload.end_unix)}`);
      if (payload.granularity) flags.push(`granularity=${payload.granularity}`);
      if (payload.limit) flags.push(`limit=${payload.limit}`);
      const data = run(`products candles ${pid} ${flags.join(' ')}`);
      const candles = (data.candles || []).map(c => ({
        start: c.start,
        open: String(c.open || 0),
        high: String(c.high || 0),
        low: String(c.low || 0),
        close: String(c.close || 0),
        volume: String(c.volume || 0),
      }));
      return { ok: true, data: candles };
    }

    default:
      return { ok: false, error: `unknown_action: ${action}` };
  }
}

function main() {
  ensureEnv();
  if (process.argv.length < 3) {
    console.log(JSON.stringify({ ok: false, error: 'no_command_provided' }));
    process.exit(1);
  }
  try {
    const cmd = JSON.parse(process.argv[2]);
    const result = handle(cmd.action, cmd.payload || {});
    console.log(JSON.stringify(result));
  } catch (e) {
    console.log(JSON.stringify({ ok: false, error: `bridge_error: ${e.message}` }));
    process.exit(1);
  }
}

main();
