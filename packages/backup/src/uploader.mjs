import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

/**
 * Pluggable backup destinations for G-005.
 *
 * The uploader interface is intentionally tiny -- put(bytes) and get(key) -- so
 * that the deployment owner can supply an S3-compatible bucket, a mounted
 * filesystem, or an object store without changing any caller. What this module
 * refuses to do is pretend a backup left the volume when it did not. Every
 * uploader reports the bytes it wrote and the key it wrote them to, and
 * `uploadBackup` refuses to report success unless the destination read back
 * what was written.
 */

export const SUPPORTED_DESTINATIONS = ['filesystem', 's3'];

export class BackupUploadError extends Error {
  constructor(code, detail = {}) {
    super(code);
    this.code = code;
    this.detail = detail;
  }
}

function assertSafeKey(key) {
  if (!key || typeof key !== 'string') throw new BackupUploadError('backup_destination_key_invalid');
  if (key.includes('..') || key.startsWith('/') || key.includes('\\')) {
    throw new BackupUploadError('backup_destination_key_unsafe');
  }
  return key;
}

export function createFilesystemUploader({ root }) {
  if (!root) throw new BackupUploadError('backup_filesystem_root_required');
  const base = path.resolve(root);
  return {
    kind: 'filesystem',
    async put(key, bytes) {
      const target = path.join(base, assertSafeKey(key));
      await fs.mkdir(path.dirname(target), { recursive: true });
      await fs.writeFile(target, bytes, { mode: 0o600 });
      return { key, sizeBytes: bytes.length, path: target };
    },
    async get(key) {
      return fs.readFile(path.join(base, assertSafeKey(key)));
    },
    async list() {
      const found = [];
      const walk = async dir => {
        for (const entry of await fs.readdir(dir, { withFileTypes: true })) {
          const full = path.join(dir, entry.name);
          if (entry.isDirectory()) await walk(full);
          else found.push(path.relative(base, full));
        }
      };
      try {
        await walk(base);
      } catch (error) {
        if (error.code !== 'ENOENT') throw error;
      }
      return found.sort();
    }
  };
}

function hmac(key, value) {
  return crypto.createHmac('sha256', key).update(value, 'utf8').digest();
}

function sha256Hex(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

function amzDate(now) {
  const iso = new Date(now).toISOString().replace(/[:-]|\.\d{3}/g, '');
  return { amzDate: iso, dateStamp: iso.slice(0, 8) };
}

/**
 * Minimal AWS SigV4. S3-compatible stores differ in quirks but agree on the
 * signature scheme, so signing here and letting the caller supply endpoint,
 * region, and credentials keeps this working across MinIO, Ceph, R2, and AWS.
 */
export function signS3Request({
  method = 'PUT',
  endpoint,
  region,
  bucket,
  key,
  payloadHash,
  accessKeyId,
  secretAccessKey,
  sessionToken = null,
  now = new Date()
}) {
  const { amzDate: timestamp, dateStamp } = amzDate(now);
  const canonicalUri = `/${bucket}/${key}`.split('/').map(encodeRfc3986).join('/');
  const canonicalHost = new URL(endpoint).host;

  const headers = {
    host: canonicalHost,
    'x-amz-content-sha256': payloadHash,
    'x-amz-date': timestamp
  };
  if (sessionToken) headers['x-amz-security-token'] = sessionToken;

  const signedHeaderNames = Object.keys(headers).sort();
  const canonicalHeaders = signedHeaderNames.map(name => `${name}:${headers[name]}\n`).join('');
  const signedHeaders = signedHeaderNames.join(';');

  const canonicalRequest = [method, canonicalUri, '', canonicalHeaders, signedHeaders, payloadHash].join('\n');
  const scope = `${dateStamp}/${region}/s3/aws4_request`;
  const stringToSign = [
    'AWS4-HMAC-SHA256',
    timestamp,
    scope,
    sha256Hex(canonicalRequest)
  ].join('\n');

  let signingKey = hmac(`AWS4${secretAccessKey}`, dateStamp);
  signingKey = hmac(signingKey, region);
  signingKey = hmac(signingKey, 's3');
  signingKey = hmac(signingKey, 'aws4_request');
  const signature = crypto.createHmac('sha256', signingKey).update(stringToSign, 'utf8').digest('hex');

  return {
    url: `${endpoint.replace(/\/$/, '')}${canonicalUri}`,
    headers: {
      ...headers,
      authorization: `AWS4-HMAC-SHA256 Credential=${accessKeyId}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`
    }
  };
}

function encodeRfc3986(value) {
  return encodeURIComponent(value).replace(
    /[!'()*]/g,
    char => `%${char.charCodeAt(0).toString(16).toUpperCase()}`
  );
}

export function createS3Uploader({ endpoint, region, bucket, accessKeyId, secretAccessKey, sessionToken = null, fetchImpl = fetch }) {
  for (const [name, value] of Object.entries({ endpoint, region, bucket, accessKeyId, secretAccessKey })) {
    if (!value) throw new BackupUploadError(`backup_s3_${name}_required`);
  }
  if (typeof fetchImpl !== 'function') throw new BackupUploadError('backup_s3_fetch_unavailable');

  const request = async (method, key, body) => {
    const payloadHash = body ? sha256Hex(body) : sha256Hex('');
    const signed = signS3Request({
      method,
      endpoint,
      region,
      bucket,
      key,
      payloadHash,
      accessKeyId,
      secretAccessKey,
      sessionToken
    });
    return fetchImpl(signed.url, { method, headers: signed.headers, body });
  };

  return {
    kind: 's3',
    async put(key, bytes) {
      const response = await request('PUT', assertSafeKey(key), bytes);
      if (!response.ok) {
        throw new BackupUploadError('backup_s3_put_failed', { status: response.status, key });
      }
      return { key, sizeBytes: bytes.length, url: `${endpoint.replace(/\/$/, '')}/${bucket}/${key}` };
    },
    async get(key) {
      const response = await request('GET', assertSafeKey(key), null);
      if (!response.ok) {
        throw new BackupUploadError('backup_s3_get_failed', { status: response.status, key });
      }
      return Buffer.from(await response.arrayBuffer());
    },
    async list() {
      const url = `${endpoint.replace(/\/$/, '')}/${bucket}?list-type=2`;
      const response = await fetchImpl(url, { method: 'GET' });
      if (!response.ok) throw new BackupUploadError('backup_s3_list_failed', { status: response.status });
      const body = await response.json();
      return (body.Contents || []).map(entry => entry.Key).sort();
    }
  };
}

/**
 * Build an uploader from deployment configuration. The destination is always
 * explicit: a missing or unknown destination is an error, never a silent
 * fallback to the local volume, because a backup that quietly stayed put is
 * indistinguishable from a backup that worked.
 */
export function createUploaderFromConfig(config = {}) {
  const kind = String(config.kind || '').trim();
  if (!kind) throw new BackupUploadError('backup_destination_kind_required');
  if (!SUPPORTED_DESTINATIONS.includes(kind)) {
    throw new BackupUploadError(`backup_destination_kind_unsupported:${kind}`);
  }
  return kind === 'filesystem'
    ? createFilesystemUploader(config)
    : createS3Uploader(config);
}

/**
 * Upload and read back. A destination that accepts the write but returns
 * different bytes is treated as a failure, because a backup that cannot be
 * proven restorable is not a backup.
 *
 * The readback is compared against the bytes actually uploaded, not against
 * manifest.dump.sha256. Those differ when the dump is encrypted: the manifest
 * describes the plaintext so that a restore can be verified after decryption,
 * while this check is about transport integrity of the stored object. Restore
 * verification is verifyRestoredDump's job, not this function's.
 */
export async function uploadBackup(uploader, manifest, dumpBytes, options = {}) {
  const { manifestKey, dumpKey } = options;
  const key = assertSafeKey(dumpKey);
  const written = await uploader.put(key, dumpBytes);
  const readBack = await uploader.get(key);
  if (!readBack || readBack.length !== dumpBytes.length) {
    throw new BackupUploadError('backup_upload_readback_size_mismatch', {
      expected: dumpBytes.length,
      actual: readBack?.length ?? null
    });
  }
  if (sha256Hex(readBack) !== sha256Hex(dumpBytes)) {
    throw new BackupUploadError('backup_upload_readback_checksum_mismatch', { key });
  }
  if (manifestKey) {
    await uploader.put(assertSafeKey(manifestKey), Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`, 'utf8'));
  }
  return { ok: true, kind: uploader.kind, ...written, readBackVerified: true };
}
