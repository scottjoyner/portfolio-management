import test from 'node:test';
import assert from 'node:assert/strict';
import { matchMarkets } from '../packages/core/src/matching.mjs';
const base={title:'Will CPI be above 3.0% in Dec 2026?',resolutionSource:'BLS',closeTime:'2026-12-12T00:00:00Z',threshold:'3.0',timezone:'UTC',marketType:'binary'};
test('exact equivalent match',()=>{const r=matchMarkets(base,{...base});assert.ok(r.confidence>0.8);assert.equal(r.flags.length,0);});
test('different threshold flagged',()=>{const r=matchMarkets(base,{...base,threshold:'2.5'});assert.ok(r.flags.includes('different_threshold'));});
