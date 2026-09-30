#!/usr/bin/env node
import fs from 'node:fs/promises';
import path from 'node:path';
import { parseArgs } from 'node:util';
import {
  appendAnchor,
  buildAnchorRecord,
  signAnchorRecord
} from '../packages/audit/src/anchor.mjs';

/**
 * Export an audit-chain root to an append-only external destination (G-006).
 *
 * The destination is a directory the deployment owner points at a
 * WORM-capable or object-locked store. This script only ever creates new files
 * named by sequence number and refuses to overwrite an existing one, so the
 * destination can enforce immutability on its side.
 *
 * Required: --chain <audit-events.json> BACKUP... no:
 *   ANCHOR_DESTINATION (directory), ANCHOR_SIGNING_PRIVATE_KEY
 * Optional: --previous <anchors.json>, --release-sha <sha>, --at <iso>
 */

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`anchor_env_required:${name}`);
  return value;
}

const { values } = parseArgs({
  options: {
    chain: { type: 'string' },
    previous: { type: 'string' },
    'release-sha': { type: 'string' },
    at: { type: 'string' }
  }
});

async function main() {
  if (!values.chain) throw new Error('anchor_chain_path_required');
  const destination = required('ANCHOR_DESTINATION');
  const events = JSON.parse(await fs.readFile(values.chain, 'utf8'));
  const previousPath = values.previous;
  const previousExists = previousPath
    ? await fs.access(previousPath).then(() => true, () => false)
    : false;
  const previousAnchors = previousExists ? JSON.parse(await fs.readFile(previousPath, 'utf8')) : [];
  const previous = previousAnchors.length
    ? previousAnchors.sort((a, b) => Number(b.sequenceNumber) - Number(a.sequenceNumber))[0]
    : null;

  const record = buildAnchorRecord({
    events,
    previousAnchor: previous,
    anchoredAt: values.at || new Date().toISOString(),
    releaseSha: values['release-sha'] || null
  });
  record.signature = signAnchorRecord(record, required('ANCHOR_SIGNING_PRIVATE_KEY'));

  const merged = appendAnchor(previousAnchors, record);
  const target = path.join(destination, `anchor-${String(record.sequenceNumber).padStart(8, '0')}.json`);
  await fs.mkdir(destination, { recursive: true });

  try {
    // wx fails if the path already exists, which is what keeps the destination
    // append-only even if this script is pointed at a directory it does not own.
    await fs.writeFile(target, `${JSON.stringify(record, null, 2)}\n`, { flag: 'wx', mode: 0o444 });
  } catch (error) {
    if (error.code === 'EEXIST') throw new Error(`anchor_destination_not_append_only:${target}`);
    throw error;
  }

  if (values.previous) {
    await fs.writeFile(values.previous, `${JSON.stringify(merged, null, 2)}\n`, { mode: 0o444 });
  }

  process.stdout.write(`${JSON.stringify({
    ok: true,
    sequenceNumber: record.sequenceNumber,
    anchorHash: record.anchorHash,
    auditRootHash: record.auditRootHash,
    auditEventCount: record.auditEventCount,
    destination: target
  }, null, 2)}\n`);
}

main().catch(error => {
  process.stderr.write(`${JSON.stringify({ ok: false, error: error.message }, null, 2)}\n`);
  process.exit(1);
});
