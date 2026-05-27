export class InMemoryRepo {
  constructor(){this.marketPairs=[];this.audit=[];}
  addPair(p){this.marketPairs.push(p);return p;}
  approvePair(id,actor){const p=this.marketPairs.find(x=>x.id===id); if(!p) throw Error('not found'); p.status='approved'; this.audit.push({type:'approve_pair',id,actor,ts:Date.now()}); return p;}
}
