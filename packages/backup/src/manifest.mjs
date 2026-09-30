import crypto from 'node:crypto';
import { createReadStream } from 'node:fs';

/**
 * Destination-neutral logical-backup manifest for G-005.
 *
 * A manifest is the only thing that travels off the PostgreSQL volume, so it
 * has to carry everything needed to prove later that a restore is legitimate:
 * what was dumped, how big it was, what it hashes to, who produced it, and a
 * signature from the key that owns this deployment.
 *
 * The signature is deliberately required. A checksum proves the file has not
 * changed since the manifest was written; the signature proves the manifest
 * itself was not rewritten by anyone who can write to the destination. A
 * destination that accepts unsigned manifests is not a backup system.
 */

export const MANIFEST_SCHEMA_VERSION = 1;

export const REQUIRED_OWNERSHIP_FIELDS = [
  'releaseSha',
  'operator',
  'sourceDatabase',
  'createdAt'
];

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort()
      .map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(',')}}`;
  }
  return JSON.stringify(value === undefined ? null : value);
}

export function canonicalManifestJson(manifest) {
  return canonicalJson(manifest);
}

function sha256File(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const stream = createReadStream(filePath);
    stream.on('error', reject);
    stream.on('data', chunk => hash.update(chunk));
    stream.on('end', () => resolve(hash.digest('hex')));
  });
}

export function backupSha256(buffer) {
  return crypto.createHash('sha256').update(buffer).digest('hex');
}

export function signManifest(manifest, privateKeyPem) {
  const unsigned = { ...manifest };
  delete unsigned.signature;
  const key = crypto.createPrivateKey(privateKeyPem);
  if (key.asymmetricKeyType !== 'ed25519') {
    throw new Error(`backup_manifest_key_unsupported:${key.asymmetricKeyType}`);
  }
  return crypto.sign(null, Buffer.from(canonicalJson(unsigned), 'utf8'), key).toString('base64');
}

export function verifyManifestSignature(manifest, publicKeyPem) {
  if (!manifest?.signature) return { ok: false, reason: 'backup_manifest_signature_missing' };
  const unsigned = { ...manifest };
  delete unsigned.signature;
  let key;
  try {
    key = crypto.createPublicKey(publicKeyPem);
  } catch {
    return { ok: false, reason: 'backup_manifest_key_invalid' };
  }
  if (key.asymmetricKeyType !== 'ed25519') {
    return { ok: false, reason: `backup_manifest_key_unsupported:${key.asymmetricKeyType}` };
  }
  const valid = crypto.verify(
    null,
    Buffer.from(canonicalJson(unsigned), 'utf8'),
    key,
    Buffer.from(String(manifest.signature), 'base64')
  );
  return valid
    ? { ok: true, reason: 'backup_manifest_signature_valid' }
    : { ok: false, reason: 'backup_manifest_signature_invalid' };
}

/**
 * Encrypt a dump with AES-256-GCM. The manifest records the iv and auth tag so
 * a restore can prove it decrypted to the bytes the checksum describes.
 */
export function encryptDump(buffer, keyBase64) {
  const key = Buffer.from(keyBase64, 'base64');
  if (key.length !== 32) throw new Error(`backup_encryption_key_invalid_length:${key.length}`);
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const ciphertext = Buffer.concat([cipher.update(buffer), cipher.final()]);
  return {
    ciphertext,
    iv: iv.toString('base64'),
    authTag: cipher.getAuthTag().toString('base64'),
    algorithm: 'aes-256-gcm'
  };
}

export function decryptDump(ciphertext, keyBase64, ivBase64, authTagBase64) {
  const key = Buffer.from(keyBase64, 'base64');
  if (key.length !== 32) throw new Error(`backup_encryption_key_invalid_length:${key.length}`);
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(ivBase64, 'base64'));
  decipher.setAuthTag(Buffer.from(authTagBase64, 'base64'));
  return Buffer.concat([decipher.update(ciphertext), decipher.final()]);
}

/**
 * Build a manifest from the dump. `ownership` is not optional: a backup whose
 * provenance is unknown cannot be used to certify a release.
 */
export function buildBackupManifest({
  dumpPath,
  dumpBytes,
  releaseSha,
  operator,
  sourceDatabase,
  migrationCount,
  createdAt,
  encryption = null,
  schemaVersion = MANIFEST_SCHEMA_VERSION
}) {
  if (!dumpPath) throw new Error('backup_manifest_dump_path_required');
  if (!releaseSha) throw new Error('backup_manifest_release_sha_required');
  if (!operator) throw new Error('backup_manifest_operator_required');
  if (!sourceDatabase) throw new Error('backup_manifest_source_database_required');
  if (!createdAt) throw new Error('backup_manifest_created_at_required');
  if (dumpBytes == null) throw new Error('backup_manifest_dump_bytes_required');

  const manifest = {
    schemaVersion,
    releaseSha,
    operator,
    sourceDatabase,
    createdAt,
    migrationCount: Number(migrationCount ?? 0),
    dump: {
      fileName: dumpPath.split('/').pop(),
      sizeBytes: dumpBytes.length,
      sha256: backupSha256(dumpBytes),
      encryption: encryption
        ? { algorithm: encryption.algorithm, iv: encryption.iv, authTag: encryption.authTag }
        : null
    }
  };
  return manifest;
}

/**
 * Apply a retention policy. Retention is expressed as counts rather than ages
 * because a backup that failed to upload must not age out before anyone looks
 * at it.
 */
export function applyRetentionPolicy(manifests, policy = {}) {
  const keepDaily = Number(policy.keepDaily ?? 7);
  const keepWeekly = Number(policy.keepWeekly ?? 4);
  const keepMonthly = Number(policy.keepMonthly ?? 6);
  if (keepDaily < 1 || keepWeekly < 0 || keepMonthly < 0) {
    throw new Error('backup_retention_policy_invalid');
  }
  const ordered = [...manifests].sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)));
  const keep = new Set();
  const buckets = { day: new Set(), week: new Set(), month: new Set() };
  for (const manifest of ordered) {
    if (keep.size >= keepDaily + keepWeekly + keepMonthly) break;
    const at = new Date(manifest.createdAt);
    if (Number.isNaN(at.getTime())) continue;
    const day = at.toISOString().slice(0, 10);
    const week = isoWeek(at);
    const month = at.toISOString().slice(0, 7);
    let stored = false;
    if (!buckets.day.has(day) && keep.size < keepDaily) {
      buckets.day.add(day);
      keep.add(manifest);
      stored = true;
    }
    if (!buckets.week.has(week) && keep.size < keepDaily + keepWeekly) {
      buckets.week.add(week);
      keep.add(manifest);
      stored = true;
    }
    if (!buckets.month.has(month) && keep.size < keepDaily + keepWeekly + keepMonthly) {
      buckets.month.add(month);
      keep.add(manifest);
      stored = true;
    }
    void stored;
  }
  return { keep, expired: ordered.filter(manifest => !keep.has(manifest)) };
}

function isoWeek(date) {
  const target = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const dayNumber = target.getUTCDay() || 7;
  target.setUTCDate(target.getUTCDate() + 4 - dayNumber);
  const yearStart = new Date(Date.UTC(target.getUTCFullYear(), 0, 1));
  return `${target.getUTCFullYear()}-W${String(Math.ceil(((target - yearStart) / 86400000 + 1) / 7)).padStart(2, '0')}`;
}

/**
 * Verify a restored dump against its manifest. A restore that completes but
 * produces different bytes is a failed restore, and this is the check that
 * catches it.
 *
 * `restoredBytes` may be the decrypted plaintext or, when `encryptedInput` is
 * set, the stored ciphertext. The manifest checksum always describes the
 * plaintext, because that is what a restore has to reproduce; when ciphertext is
 * passed the decryption key is therefore required and applied here rather than
 * being the caller's problem.
 */
export function verifyRestoredDump(manifest, restoredBytes, publicKeyPem, options = {}) {
  const { encryptionKey = null, encryptedInput = false } =
    typeof options === 'string' ? { encryptionKey: options } : (options || {});
  const problems = [];

  if (manifest?.schemaVersion !== MANIFEST_SCHEMA_VERSION) {
    problems.push(`backup_manifest_schema_unsupported:${manifest?.schemaVersion}`);
  }
  for (const field of REQUIRED_OWNERSHIP_FIELDS) {
    if (!manifest?.[field]) problems.push(`backup_manifest_ownership_missing:${field}`);
  }
  if (!manifest?.dump?.sha256) problems.push('backup_manifest_checksum_missing');
  if (encryptedInput && !encryptionKey) {
    problems.push('backup_manifest_encryption_key_absent');
  }
  if (publicKeyPem) {
    const signature = verifyManifestSignature(manifest, publicKeyPem);
    if (!signature.ok) problems.push(signature.reason);
  } else {
    problems.push('backup_manifest_verification_key_absent');
  }

  let plaintext = restoredBytes;
  if (encryptedInput && encryptionKey && manifest?.dump?.encryption) {
    try {
      plaintext = decryptDump(
        restoredBytes,
        encryptionKey,
        manifest.dump.encryption.iv,
        manifest.dump.encryption.authTag
      );
    } catch {
      problems.push('backup_restore_decryption_failed');
      plaintext = null;
    }
  }

  if (plaintext != null && manifest?.dump?.sha256) {
    if (backupSha256(plaintext) !== manifest.dump.sha256) {
      problems.push('backup_restored_checksum_mismatch');
    }
    if (Number(manifest.dump.sizeBytes) !== plaintext.length) {
      problems.push('backup_restored_size_mismatch');
    }
  }

  return { ok: problems.length === 0, problems };
}

export async function hashDumpFile(filePath) {
  return sha256File(filePath);
}
