import { readdirSync, statSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
const bad=[];
function walk(d){for(const n of readdirSync(d)){if(n==='node_modules'||n==='.git') continue; const p=join(d,n); const s=statSync(p); if(s.isDirectory()) walk(p); else if(p.startsWith('scripts/')) {continue;} else if(/\.(ts|js|mjs)$/.test(n)){const t=String(readFileSync(p)); if(t.includes('TODO_UNSAFE_MARKER')) bad.push(p);} }}
walk('.');
if(bad.length){console.error('lint failed',bad);process.exit(1);} console.log('lint ok');
