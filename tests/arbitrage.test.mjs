import test from 'node:test';
import assert from 'node:assert/strict';
import { computeCrossVenueArb } from '../packages/core/src/arbitrage.mjs';

const lvl=(p,s)=>({priceMicros:p,size:s});

test('true arb before fees',()=>{const r=computeCrossVenueArb({yesA:lvl(400000,10),noB:lvl(500000,10),yesB:lvl(550000,10),noA:lvl(460000,10),feeBps:0,slippageBps:0,settlementRiskBps:0,staleMs:10,maxAgeMs:100});assert.equal(r.ok,true);});
test('false arb after fees',()=>{const r=computeCrossVenueArb({yesA:lvl(490000,10),noB:lvl(500000,10),yesB:lvl(550000,10),noA:lvl(460000,10),feeBps:100,slippageBps:50,settlementRiskBps:0,staleMs:10,maxAgeMs:100});assert.equal(r.ok,false);});
test('false arb due stale',()=>{const r=computeCrossVenueArb({yesA:lvl(400000,10),noB:lvl(500000,10),yesB:lvl(550000,10),noA:lvl(460000,10),feeBps:0,slippageBps:0,settlementRiskBps:0,staleMs:1000,maxAgeMs:100});assert.equal(r.reason,'stale_orderbook');});
