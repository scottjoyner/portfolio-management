#!/usr/bin/env node
import fs from 'node:fs/promises';
import {
  applyRetentionPolicy,
  buildBackupManifest,
  encryptDump,
  signManifest
} from '../packages/backup/src/manifest.mjs';
import { createUploaderFromConfig, uploadBackup } from '../packages/backup/src/uploader.mjs';

/**
 * Export a logical backup off the PostgreSQL volume with a signed manifest.
 *
 * G-005. The destination and credentials are always supplied by the deployment
 * owner through the environment; this script has no default destination,
 * because a backup that quietly stayed on the volume is indistinguishable from
 * one that worked.
 *
 * Required: BACKUP_SIGNING_PRIVATE_KEY, BACKUP_DUMP_PATH
 * Destination: exactly one of BACKUP_FILESYSTEM_ROOT or
 *   BACKUP_S3_ENDPOINT + BACKUP_S3_REGION + BACKUP_S3_BUCKET +
 *   BACKUP_S3_ACCESS_KEY_ID + BACKUP_S3_SECRET_ACCESS_KEY
 * Optional: BACKUP_ENCRYPTION_KEY (base64 32 bytes), BACKUP_RELEASE_SHA,
 *   BACKUP_OPERATOR, BACKUP_SOURCE_DATABASE, BACKUP_KEEP_DAILY/WEEKLY/MONTHLY
 */

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`backup_env_required:${name}`);
  return value;
}

function destinationFromEnv() {
  const hasFilesystem = Boolean(process.env.BACKUP_FILESYSTEM_ROOT);
  const s3Keys = ['BACKUP_S3_ENDPOINT', 'BACKUP_S3_REGION', 'BACKUP_S3_BUCKET', 'BACKUP_S3_ACCESS_KEY_ID', 'BACKUP_S3_SECRET_ACCESS_KEY'];
  const hasS3 = s3Keys.every(name => process.env[name]);
  if (hasFilesystem && hasS3) throw new Error('backup_destination_ambiguous:filesystem_and_s3');
  if (!hasFilesystem && !hasS3) throw new Error('backup_destination_required');
  if (hasFilesystem) return { kind: 'filesystem', root: process.env.BACKUP_FILESYSTEM_ROOT };
  return {
    kind: 's3',
    endpoint: process.env.BACKUP_S3_ENDPOINT,
    region: process.env.BACKUP_S3_REGION,
    bucket: process.env.BACKUP_S3_BUCKET,
    accessKeyId: process.env.BACKUP_S3_ACCESS_KEY_ID,
    secretAccessKey: process.env.BACKUP_S3_SECRET_ACCESS_KEY,
    sessionToken: process.env.BACKUP_S3_SESSION_TOKEN || null
  };
}

async function main() {
  const dumpPath = required('BACKUP_DUMP_PATH');
  const dumpBytes = await fs.readFile(dumpPath);
  const releaseSha = process.env.BACKUP_RELEASE_SHA || 'unknown';
  const createdAt = new Date().toISOString();

  let payload = dumpBytes;
  let encryption = null;
  if (process.env.BACKUP_ENCRYPTION_KEY) {
    const encrypted = encryptDump(dumpBytes, process.env.BACKUP_ENCRYPTION_KEY);
    payload = encrypted.ciphertext;
    encryption = encrypted;
  }

  const manifest = buildBackupManifest({
    dumpPath,
    dumpBytes,
    releaseSha,
    operator: process.env.BACKUP_OPERATOR || 'unknown',
    sourceDatabase: process.env.BACKUP_SOURCE_DATABASE || 'portfolio',
    migrationCount: Number(process.env.BACKUP_MIGRATION_COUNT || 0),
    createdAt,
    encryption
  });
  manifest.signature = signManifest(manifest, required('BACKUP_SIGNING_PRIVATE_KEY'));

  const uploader = createUploaderFromConfig(destinationFromEnv());
  const stamp = createdAt.replace(/[-:]/g, '').replace(/\..+/, 'Z');
  const uploaded = await uploadBackup(uploader, manifest, payload, {
    dumpKey: `dumps/${stamp}/portfolio.dump`,
    manifestKey: `manifests/${stamp}/portfolio.manifest.json`
  });

  const existing = await uploader.list().catch(() => []);
  const known = existing.filter(key => key.startsWith('manifests/'));
  const retention = applyRetentionPolicy(
    known.map(key => ({ createdAt: key.split('/')[1] ? `${key.split('/')[1].slice(0, 4)}-${key.split('/')[1].slice(4, 6)}-${key.split('/')[1].slice(6, 8)}T00:00:00.000Z` : new Date().toISOString(), key })),
    {
      keepDaily: process.env.BACKUP_KEEP_DAILY,
      keepWeekly: process.env.BACKUP_KEEP_WEEKLY,
      keepMonthly: process.env.BACKUP_KEEP_MONTHLY
    }
  );

  process.stdout.write(`${JSON.stringify({
    ok: true,
    destination: uploader.kind,
    uploaded,
    encrypted: Boolean(encryption),
    retention: { kept: retention.keep.size, expired: retention.expired.length },
    manifest
  }, null, 2)}\n`);
}

main().catch(error => {
  process.stderr.write(`${JSON.stringify({ ok: false, error: error.message, detail: error.detail ?? null }, null, 2)}\n`);
  process.exit(1);
});
