import { readdirSync, statSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
const bad=[];
const SKIP_DIRS=new Set(['node_modules','.git','.venv','.venv_test']);
function walk(d){
  let entries;
  try { entries = readdirSync(d); } catch { return; }
  for (const n of entries){
    if (SKIP_DIRS.has(n)) continue;
    const p=join(d,n);
    let s;
    try { s=statSync(p); } catch { continue; } // dangling symlink or unreadable entry: not a lint failure
    if (s.isDirectory()) walk(p);
    else if (p.startsWith('scripts/')) {continue;}
    else if (/\.(ts|js|mjs)$/.test(n)){const t=String(readFileSync(p)); if(t.includes('TODO_UNSAFE_MARKER')) bad.push(p);}
  }
}
walk('.');
if(bad.length){console.error('lint failed',bad);process.exit(1);} console.log('lint ok');
