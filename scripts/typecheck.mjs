#!/usr/bin/env node
import { readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

const ignored = new Set(['.git', 'node_modules', '.venv', 'venv', 'archive', 'data', 'state', 'dist', 'build']);
const files = [];
function walk(directory) {
  for (const name of readdirSync(directory)) {
    if (ignored.has(name)) continue;
    const path = join(directory, name);
    const stat = statSync(path);
    if (stat.isDirectory()) walk(path);
    else if (/\.(mjs|js)$/i.test(name)) files.push(path);
  }
}
walk('.');

const errors = [];
for (const path of files) {
  const checked = spawnSync(process.execPath, ['--check', path], { encoding: 'utf8' });
  if (checked.status !== 0) errors.push({ path, error: String(checked.stderr || checked.stdout).trim() });
}
if (errors.length) {
  process.stderr.write(`${JSON.stringify({ ok: false, errors }, null, 2)}\n`);
  process.exit(1);
}
process.stdout.write(`${JSON.stringify({ ok: true, checkedFiles: files.length }, null, 2)}\n`);
