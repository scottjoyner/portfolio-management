#!/usr/bin/env node
import { lstatSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

const ignored = new Set([
  '.git',
  'node_modules',
  '.venv',
  'venv',
  '.cb_sdk_env',
  'archive',
  'data',
  'state',
  'dist',
  'build',
  '.pytest_cache',
  '.mypy_cache',
  '.ruff_cache',
]);
const files = [];
const skipped = [];

function walk(directory) {
  let names;
  try {
    names = readdirSync(directory);
  } catch (error) {
    skipped.push({ path: directory, reason: error.code || error.message });
    return;
  }
  for (const name of names) {
    if (ignored.has(name)) continue;
    const path = join(directory, name);
    try {
      const link = lstatSync(path);
      if (link.isSymbolicLink()) {
        skipped.push({ path, reason: 'symbolic_link' });
        continue;
      }
      const stat = statSync(path);
      if (stat.isDirectory()) walk(path);
      else if (/\.(mjs|js)$/i.test(name)) files.push(path);
    } catch (error) {
      skipped.push({ path, reason: error.code || error.message });
    }
  }
}
walk('.');

const errors = [];
for (const path of files) {
  const checked = spawnSync(process.execPath, ['--check', path], { encoding: 'utf8' });
  if (checked.status !== 0) errors.push({ path, error: String(checked.stderr || checked.stdout).trim() });
}
if (errors.length) {
  process.stderr.write(`${JSON.stringify({ ok: false, errors, skipped }, null, 2)}\n`);
  process.exit(1);
}
process.stdout.write(`${JSON.stringify({ ok: true, checkedFiles: files.length, skipped }, null, 2)}\n`);
