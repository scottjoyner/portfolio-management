export const fixtures={
  safeA:{title:'Will CPI exceed 3.0% in Dec 2026?',resolutionSource:'BLS',closeTime:'2026-12-12T00:00:00Z',resolveTime:'2026-12-13T00:00:00Z',threshold:'3.0',timezone:'UTC',marketType:'binary',cancellable:false},
  safeB:{title:'Will CPI exceed 3.0% in Dec 2026?',resolutionSource:'BLS',closeTime:'2026-12-12T00:00:00Z',resolveTime:'2026-12-13T00:00:00Z',threshold:'3.0',timezone:'UTC',marketType:'binary',cancellable:false},
  unsafeB:{title:'Will CPI exceed 2.5% in Dec 2026?',resolutionSource:'Survey',closeTime:'2026-12-11T00:00:00Z',resolveTime:'2026-12-14T00:00:00Z',threshold:'2.5',timezone:'EST',marketType:'binary',cancellable:true},
  riskInput:{killSwitch:false,unresolvedRecon:false,pairApproved:true,complianceApproved:true,orderBookAgeMs:100,maxOrderbookStalenessMs:500,edgeBps:150,minEdgeBps:100,balanceSufficient:true,notionalMicros:1000000,maxNotionalMicros:2000000,live:false,runtimeConfirmed:false,credentialsPresent:false,venueModeExplicit:false,liveTrading:false,paperTrading:true},
  reconInput:{orders:[{id:'o1',status:'filled'}],fills:[{orderId:'o1',size:1}],positions:[{sizeSigned:1}],balances:[{venue:'kalshi',amount:100}]}
};
