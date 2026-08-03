import { randomUUID } from 'node:crypto';

function iso(value = new Date()) {
  return value instanceof Date ? value.toISOString() : new Date(value).toISOString();
}

function json(value, fallback = {}) {
  if (value === null || value === undefined) return fallback;
  return typeof value === 'string' ? JSON.parse(value) : value;
}

function mapJob(row) {
  if (!row) return null;
  return {
    id: row.id,
    jobType: row.job_type,
    scope: row.scope,
    status: row.status,
    priority: Number(row.priority || 100),
    scheduledAt: iso(row.scheduled_at),
    startedAt: row.started_at ? iso(row.started_at) : null,
    completedAt: row.completed_at ? iso(row.completed_at) : null,
    attempt: Number(row.attempt || 0),
    maxAttempts: Number(row.max_attempts || 5),
    leaseOwner: row.lease_owner || null,
    leaseExpiresAt: row.lease_expires_at ? iso(row.lease_expires_at) : null,
    idempotencyKey: row.idempotency_key,
    payload: json(row.payload_json, {}),
    result: json(row.result_json, null),
    lastError: row.last_error || null,
    createdAt: iso(row.created_at),
    updatedAt: iso(row.updated_at),
  };
}

export class RuntimeJobQueue {
  constructor(store) {
    this.store = store;
  }

  async query(sql, params = []) {
    return this.store.query(sql, params);
  }

  async enqueue(input = {}) {
    const now = iso(input.now || new Date());
    const job = {
      id: input.id || `runtime-job-${randomUUID()}`,
      jobType: String(input.jobType || '').trim(),
      scope: String(input.scope || 'global'),
      priority: Number(input.priority ?? 100),
      scheduledAt: iso(input.scheduledAt || now),
      maxAttempts: Math.max(1, Number(input.maxAttempts ?? 5)),
      idempotencyKey: String(input.idempotencyKey || '').trim(),
      payload: input.payload || {},
    };
    if (!job.jobType) throw new Error('runtime_job_type_required');
    if (!job.idempotencyKey) throw new Error('runtime_job_idempotency_key_required');

    return this.store.withTransaction(async () => {
      const inserted = await this.query(
        `INSERT INTO runtime_jobs (
           id, job_type, scope, status, priority, scheduled_at, attempt, max_attempts,
           lease_owner, lease_expires_at, idempotency_key, payload_json, created_at, updated_at
         ) VALUES ($1,$2,$3,'queued',$4,$5,0,$6,NULL,NULL,$7,$8,$9,$9)
         ON CONFLICT (idempotency_key) DO NOTHING
         RETURNING *`,
        [job.id, job.jobType, job.scope, job.priority, job.scheduledAt, job.maxAttempts, job.idempotencyKey, JSON.stringify(job.payload), now],
      );
      if (inserted.rows?.[0]) return { job: mapJob(inserted.rows[0]), created: true };
      const existing = await this.query('SELECT * FROM runtime_jobs WHERE idempotency_key = $1', [job.idempotencyKey]);
      return { job: mapJob(existing.rows?.[0]), created: false };
    }, { lockWrites: false });
  }

  async claim({ workerId, jobTypes = [], leaseSeconds = 300, now = new Date() } = {}) {
    if (!workerId) throw new Error('runtime_job_worker_id_required');
    const claimedAt = iso(now);
    const leaseExpiresAt = iso(new Date(new Date(claimedAt).getTime() + Math.max(30, Number(leaseSeconds || 300)) * 1000));
    const types = Array.isArray(jobTypes) ? jobTypes.filter(Boolean) : [];

    return this.store.withTransaction(async () => {
      const result = await this.query(
        `WITH candidate AS (
           SELECT id
           FROM runtime_jobs
           WHERE status IN ('queued', 'retry')
             AND scheduled_at <= $1
             AND (lease_expires_at IS NULL OR lease_expires_at < $1)
             AND ($2::text[] = '{}'::text[] OR job_type = ANY($2::text[]))
           ORDER BY priority ASC, scheduled_at ASC, created_at ASC
           FOR UPDATE SKIP LOCKED
           LIMIT 1
         )
         UPDATE runtime_jobs AS job
         SET status = 'running',
             attempt = job.attempt + 1,
             started_at = COALESCE(job.started_at, $1),
             lease_owner = $3,
             lease_expires_at = $4,
             last_error = NULL,
             updated_at = $1
         FROM candidate
         WHERE job.id = candidate.id
         RETURNING job.*`,
        [claimedAt, types, String(workerId), leaseExpiresAt],
      );
      return mapJob(result.rows?.[0]);
    }, { lockWrites: false, isolation: 'READ COMMITTED' });
  }

  async heartbeat({ jobId, workerId, leaseSeconds = 300, now = new Date() } = {}) {
    if (!jobId || !workerId) throw new Error('runtime_job_heartbeat_identity_required');
    const heartbeatAt = iso(now);
    const leaseExpiresAt = iso(new Date(new Date(heartbeatAt).getTime() + Math.max(30, Number(leaseSeconds || 300)) * 1000));
    const result = await this.query(
      `UPDATE runtime_jobs
       SET lease_expires_at = $3, updated_at = $4
       WHERE id = $1 AND lease_owner = $2 AND status = 'running'
       RETURNING *`,
      [jobId, workerId, leaseExpiresAt, heartbeatAt],
    );
    return mapJob(result.rows?.[0]);
  }

  async complete({ jobId, workerId, result: jobResult = {}, now = new Date() } = {}) {
    if (!jobId || !workerId) throw new Error('runtime_job_completion_identity_required');
    const completedAt = iso(now);
    const result = await this.query(
      `UPDATE runtime_jobs
       SET status = 'completed', completed_at = $3, result_json = $4,
           lease_owner = NULL, lease_expires_at = NULL, updated_at = $3
       WHERE id = $1 AND lease_owner = $2 AND status = 'running'
       RETURNING *`,
      [jobId, workerId, completedAt, JSON.stringify(jobResult || {})],
    );
    return mapJob(result.rows?.[0]);
  }

  async fail({ jobId, workerId, error, retryDelaySeconds = 60, now = new Date() } = {}) {
    if (!jobId || !workerId) throw new Error('runtime_job_failure_identity_required');
    const failedAt = iso(now);
    const retryAt = iso(new Date(new Date(failedAt).getTime() + Math.max(1, Number(retryDelaySeconds || 60)) * 1000));
    const message = String(error?.message || error || 'runtime_job_failed').slice(0, 4000);
    const result = await this.query(
      `UPDATE runtime_jobs
       SET status = CASE WHEN attempt >= max_attempts THEN 'failed' ELSE 'retry' END,
           scheduled_at = CASE WHEN attempt >= max_attempts THEN scheduled_at ELSE $4 END,
           completed_at = CASE WHEN attempt >= max_attempts THEN $3 ELSE NULL END,
           last_error = $5,
           lease_owner = NULL,
           lease_expires_at = NULL,
           updated_at = $3
       WHERE id = $1 AND lease_owner = $2 AND status = 'running'
       RETURNING *`,
      [jobId, workerId, failedAt, retryAt, message],
    );
    return mapJob(result.rows?.[0]);
  }

  async cancel({ jobId, reason = 'operator_cancelled', now = new Date() } = {}) {
    if (!jobId) throw new Error('runtime_job_id_required');
    const cancelledAt = iso(now);
    const result = await this.query(
      `UPDATE runtime_jobs
       SET status = 'cancelled', completed_at = $2, last_error = $3,
           lease_owner = NULL, lease_expires_at = NULL, updated_at = $2
       WHERE id = $1 AND status NOT IN ('completed', 'failed', 'cancelled')
       RETURNING *`,
      [jobId, cancelledAt, String(reason).slice(0, 4000)],
    );
    return mapJob(result.rows?.[0]);
  }

  async recoverExpiredLeases({ now = new Date(), retryDelaySeconds = 5 } = {}) {
    const recoveredAt = iso(now);
    const retryAt = iso(new Date(new Date(recoveredAt).getTime() + Math.max(1, Number(retryDelaySeconds || 5)) * 1000));
    const result = await this.query(
      `UPDATE runtime_jobs
       SET status = CASE WHEN attempt >= max_attempts THEN 'failed' ELSE 'retry' END,
           scheduled_at = CASE WHEN attempt >= max_attempts THEN scheduled_at ELSE $2 END,
           completed_at = CASE WHEN attempt >= max_attempts THEN $1 ELSE NULL END,
           last_error = COALESCE(last_error, 'runtime_job_lease_expired'),
           lease_owner = NULL,
           lease_expires_at = NULL,
           updated_at = $1
       WHERE status = 'running' AND lease_expires_at < $1
       RETURNING *`,
      [recoveredAt, retryAt],
    );
    return (result.rows || []).map(mapJob);
  }

  async get(jobId) {
    const result = await this.query('SELECT * FROM runtime_jobs WHERE id = $1', [jobId]);
    return mapJob(result.rows?.[0]);
  }

  async list({ jobType, scope, status, limit = 100 } = {}) {
    const result = await this.query(
      `SELECT * FROM runtime_jobs
       WHERE ($1::text IS NULL OR job_type = $1)
         AND ($2::text IS NULL OR scope = $2)
         AND ($3::text IS NULL OR status = $3)
       ORDER BY created_at DESC
       LIMIT $4`,
      [jobType || null, scope || null, status || null, Math.max(1, Math.min(1000, Number(limit || 100)))],
    );
    return (result.rows || []).map(mapJob);
  }
}
