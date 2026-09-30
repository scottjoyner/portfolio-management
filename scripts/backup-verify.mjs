#!/usr/bin/env node
import fs from 'node:fs/promises';
import { verifyRestoredDump } from '../packages/backup/src/manifest.mjs';

/**
 * Verify a restored backup against its signed manifest (G-005).
 *
 * This is the scheduled restore verification: restore into a disposable
 * database, then run this. A restore that completes without error is not
 * evidence that the restore was correct, so the restored bytes are checked
 * against the manifest checksum and the manifest signature is checked against
 * the deployment's verification key.
 *
 * Required: --manifest <path> --dump <path>, or
 *   BACKUP_VERIFICATION_PUBLIC_KEY for the signature check.
 * Optional: --encryption-key <base64> when the manifest records encryption.
 * Exits non-zero on any problem so it can gate certification.
 */

import { parseArgs } from 'node:util';

function fail(reason, detail = {}) {
  process.stderr.write(`${JSON.stringify({ ok: false, reason, ...detail }, null, 2)}\n`);
  process.exit(1);
}

const { values } = parseArgs({
  options: {
    manifest: { type: 'string' },
    dump: { type: 'string' },
    'encryption-key': { type: 'string' },
    'stored-ciphertext': { type: 'boolean', default: false }
  }
});

if (!values.manifest || !values.dump) fail('backup_verify_args_required');
if (!process.env.BACKUP_VERIFICATION_PUBLIC_KEY) fail('backup_verification_key_required');

let manifest;
let restoredBytes;
try {
  manifest = JSON.parse(await fs.readFile(values.manifest, 'utf8'));
} catch (error) {
  fail('backup_manifest_unreadable', { detail: error.message });
}
try {
  restoredBytes = await fs.readFile(values.dump);
} catch (error) {
  fail('backup_dump_unreadable', { detail: error.message });
}

const result = verifyRestoredDump(manifest, restoredBytes, process.env.BACKUP_VERIFICATION_PUBLIC_KEY, {
  encryptionKey: values['encryption-key'] || process.env.BACKUP_ENCRYPTION_KEY || null,
  // The manifest checksum describes the plaintext. Pass --stored-ciphertext when
  // handing over the bytes as they sit in the destination, and the key is
  // applied here instead of by the caller.
  encryptedInput: Boolean(values['stored-ciphertext'])
});

if (!result.ok) {
  process.stderr.write(`${JSON.stringify({ ok: false, reason: 'backup_restore_verification_failed', problems: result.problems }, null, 2)}\n`);
  process.exit(1);
}

process.stdout.write(`${JSON.stringify({
  ok: true,
  releaseSha: manifest.releaseSha,
  createdAt: manifest.createdAt,
  dumpSha256: manifest.dump.sha256,
  sizeBytes: manifest.dump.sizeBytes,
  encrypted: Boolean(manifest.dump.encryption)
}, null, 2)}\n`);
