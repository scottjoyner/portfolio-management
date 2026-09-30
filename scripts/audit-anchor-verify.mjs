#!/usr/bin/env node
import fs from 'node:fs/promises';
import path from 'node:path';
import { parseArgs } from 'node:util';
import { compareLocalAndExternalRoots } from '../packages/audit/src/anchor.mjs';

/**
 * Compare the local audit chain against the externally published roots (G-006).
 *
 * Run this before certifying a release. A mismatch means the local audit log
 * was truncated or rebuilt, and it blocks certification. This is the check that
 * a purely local hash chain cannot provide, because a rebuilt chain verifies
 * perfectly on its own.
 *
 * Required: --chain <audit-events.json> --anchors <dir|file>
 * Optional: ANCHOR_VERIFICATION_PUBLIC_KEY to also verify anchor signatures.
 */

const { values } = parseArgs({
  options: {
    chain: { type: 'string' },
    anchors: { type: 'string' }
  }
});

function fail(reason, detail = {}) {
  process.stderr.write(`${JSON.stringify({ ok: false, blocking: true, reason, ...detail }, null, 2)}\n`);
  process.exit(1);
}

if (!values.chain || !values.anchors) fail('anchor_verify_args_required');

const events = JSON.parse(await fs.readFile(values.chain, 'utf8'));

async function loadAnchors(target) {
  const stat = await fs.stat(target).catch(() => null);
  if (!stat) fail('anchor_destination_unreadable', { detail: target });
  if (stat.isFile()) return JSON.parse(await fs.readFile(target, 'utf8'));
  const names = (await fs.readdir(target)).filter(name => name.startsWith('anchor-') && name.endsWith('.json'));
  const records = [];
  for (const name of names.sort()) {
    records.push(JSON.parse(await fs.readFile(path.join(target, name), 'utf8')));
  }
  if (!records.length) fail('anchor_none_published', { detail: target });
  return records;
}

const anchors = await loadAnchors(values.anchors);
const result = compareLocalAndExternalRoots({
  localEvents: events,
  anchors,
  publicKeyPem: process.env.ANCHOR_VERIFICATION_PUBLIC_KEY || null
});

if (!result.ok) {
  process.stderr.write(`${JSON.stringify(result, null, 2)}\n`);
  process.exit(1);
}

process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
