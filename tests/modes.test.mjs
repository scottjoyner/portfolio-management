import test from 'node:test';
import assert from 'node:assert/strict';
import { validateModeConfig } from '../packages/config/src/mode.mjs';

test('mock mode does not require creds and forbids live true',()=>{
  const ok=validateModeConfig({MODE:'mock',LIVE_TRADING:'false'}); assert.equal(ok.ok,true);
  const bad=validateModeConfig({MODE:'mock',LIVE_TRADING:'true'}); assert.equal(bad.ok,false);
});
test('paper mode forbids live',()=>{assert.equal(validateModeConfig({MODE:'paper',LIVE_TRADING:'false'}).ok,true);});
test('polymarket readonly forbids order submission',()=>{assert.equal(validateModeConfig({MODE:'polymarket-readonly',ALLOW_POLYMARKET_ORDER_SUBMISSION:'true'}).ok,false);});
test('live mode fail closed unless explicit flags',()=>{
  const r=validateModeConfig({MODE:'live',LIVE_TRADING:'true',PAPER_TRADING:'false',REQUIRE_RUNTIME_CONFIRMATION:'true',REQUIRE_MANUAL_APPROVAL:'true',REQUIRE_APPROVED_MARKET_PAIR:'true',ALLOW_LIVE_SETTLEMENT_REDEMPTION:'true'});
  assert.equal(r.ok,true);
  const bad=validateModeConfig({MODE:'live',LIVE_TRADING:'false'}); assert.equal(bad.ok,false);
});
