import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { collectTestFiles, selectShard } from '../scripts/run-node-test-shard.mjs';

const packageJson = JSON.parse(readFileSync('package.json', 'utf8'));

function walkTestFiles(directory) {
  const found = [];
  for (const name of readdirSync(directory).sort()) {
    const path = join(directory, name);
    if (statSync(path).isDirectory()) found.push(...walkTestFiles(path));
    else if (name.endsWith('.test.mjs')) found.push(path);
  }
  return found;
}

test('npm test must not depend on shell globstar expansion', () => {
  const testScript = packageJson.scripts.test;
  assert.ok(
    !testScript.includes('**'),
    'npm test must not use a "**" glob: bash expands it as "*" without globstar, silently skipping top-level test files',
  );
  assert.match(testScript, /run-node-test-shard\.mjs/, 'npm test must run tests through the same shard runner CI uses');
});

test('npm test selects every test file on disk', () => {
  const onDisk = walkTestFiles('tests');
  const selected = selectShard(collectTestFiles('tests'), 0, 1);
  assert.deepEqual(
    [...selected].sort(),
    [...onDisk].sort(),
    'the default (single) shard must cover every .test.mjs file, so npm test cannot false-green by running nothing',
  );
  assert.ok(onDisk.length > 1, 'expected a non-trivial test suite to be discovered');
});

test('CI shard partition is total and disjoint', () => {
  const files = collectTestFiles('tests');
  const shards = [0, 1, 2, 3].map(index => new Set(selectShard(files, index, 4)));
  for (let i = 0; i < shards.length; i += 1) {
    for (let j = i + 1; j < shards.length; j += 1) {
      assert.deepEqual([...shards[i]].filter(path => shards[j].has(path)), [], `shards ${i} and ${j} must not overlap`);
    }
  }
  const union = new Set(shards.flatMap(shard => [...shard]));
  assert.deepEqual([...union].sort(), [...files].sort(), 'the four CI shards must cover every test file exactly once');
});
