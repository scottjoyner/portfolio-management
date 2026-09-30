#!/usr/bin/env node
import { readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

export function collectTestFiles(directory = 'tests') {
  const files = [];
  function walk(current) {
    for (const name of readdirSync(current).sort()) {
      const path = join(current, name);
      const stat = statSync(path);
      if (stat.isDirectory()) walk(path);
      else if (name.endsWith('.test.mjs')) files.push(path);
    }
  }
  walk(directory);
  return files;
}

export function hashShardPath(value) {
  let result = 2166136261;
  for (const char of value) {
    result ^= char.charCodeAt(0);
    result = Math.imul(result, 16777619);
  }
  return result >>> 0;
}

export function selectShard(files, shardIndex, shardTotal) {
  return files.filter(path => hashShardPath(path) % shardTotal === shardIndex);
}

function main() {
  const shardIndex = Number(process.env.NODE_TEST_SHARD_INDEX ?? process.argv[2] ?? 0);
  const shardTotal = Number(process.env.NODE_TEST_SHARD_TOTAL ?? process.argv[3] ?? 1);
  if (!Number.isInteger(shardIndex) || !Number.isInteger(shardTotal) || shardTotal < 1 || shardIndex < 0 || shardIndex >= shardTotal) {
    process.stderr.write(`${JSON.stringify({ ok: false, error: 'invalid_node_test_shard', shardIndex, shardTotal }, null, 2)}\n`);
    process.exit(1);
  }

  const files = collectTestFiles('tests');
  const selected = selectShard(files, shardIndex, shardTotal);
  if (!selected.length) {
    process.stdout.write(`${JSON.stringify({ ok: true, shardIndex, shardTotal, selected: [] }, null, 2)}\n`);
    process.exit(0);
  }

  process.stdout.write(`${JSON.stringify({ shardIndex, shardTotal, selected }, null, 2)}\n`);
  const result = spawnSync(process.execPath, ['--test', '--test-concurrency=1', ...selected], {
    stdio: 'inherit',
    env: process.env,
  });
  process.exit(result.status ?? 1);
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) main();
