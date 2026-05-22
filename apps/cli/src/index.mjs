import { validateModeConfig } from '../../../packages/config/src/mode.mjs';
const cmd=process.argv[2]||'doctor';
const args=Object.fromEntries(process.argv.slice(3).filter(x=>x.includes('=')).map(x=>x.split('=')));
const mode=args['--mode']||'mock';
const env={MODE:mode,LIVE_TRADING:'false',PAPER_TRADING:'true',REQUIRE_RUNTIME_CONFIRMATION:'true',REQUIRE_MANUAL_APPROVAL:'true',REQUIRE_APPROVED_MARKET_PAIR:'true',ALLOW_LIVE_SETTLEMENT_REDEMPTION:'false',ALLOW_POLYMARKET_ORDER_SUBMISSION:'false'};
const cfg=validateModeConfig(env);
if(!cfg.ok){console.log(JSON.stringify({ok:false,reasons:cfg.reasons},null,2));process.exit(2);}
const map={doctor:{ok:true,mode,liveTrading:'not certified'},discover:{ok:true,mode,markets:2},'match:propose':{ok:true,pairs:[{id:'pair-1',status:'proposed'}]},'arb:scan':{ok:true,opportunities:[{id:'opp-1',edgeBps:125}]},'arb:paper':{ok:true,executed:true,mode:'paper'}};
console.log(JSON.stringify(map[cmd]||{ok:false,error:'unknown_command'},null,2));
