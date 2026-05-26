import http from 'node:http';
export function startServer(port=3000){
  const s=http.createServer((req,res)=>{const p=req.url;res.setHeader('content-type','application/json');
    if(p==='/health') res.end(JSON.stringify({ok:true}));
    else if(p==='/metrics') res.end(JSON.stringify({discovered_markets:0,blocked_executions:0}));
    else res.end(JSON.stringify({ok:true,route:p}));
  });
  s.listen(port); return s;
}
