import test from 'node:test';
import assert from 'node:assert/strict';

import { RuntimeJobQueue } from '../packages/storage/src/runtimeJobQueue.mjs';

function row(overrides = {}) {
  return {
    id: 'runtime-job-1',
    job_type: 'economic-maintenance',
    scope: 'global',
    status: 'queued',
    priority: 10,
    scheduled_at: '2026-07-30T16:00:00.000Z',
    started_at: null,
    completed_at: null,
    attempt: 0,
    max_attempts: 5,
    lease_owner: null,
    lease_expires_at: null,
    idempotency_key: 'economic-maintenance:1',
    payload_json: { requestedAt: '2026-07-30T16:00:00.000Z' },
    result_json: null,
    last_error: null,
    created_at: '2026-07-30T16:00:00.000Z',
    updated_at: '2026-07-30T16:00:00.000Z',
    ...overrides,
  };
}

class FakeStore {
  constructor(responses = []) {
    this.responses = [...responses];
    this.calls = [];
    this.transactions = [];
  }

  async query(sql, params = []) {
    this.calls.push({ sql, params });
    return this.responses.shift() || { rows: [] };
  }

  async withTransaction(operation, options = {}) {
    this.transactions.push(options);
    return operation(this, { fake: true });
  }
}

test('enqueue is idempotent and returns the existing durable job on conflict', async () => {
  const store = new FakeStore([
    { rows: [] },
    { rows: [row()] },
  ]);
  const queue = new RuntimeJobQueue(store);

  const result = await queue.enqueue({
    jobType: 'economic-maintenance',
    idempotencyKey: 'economic-maintenance:1',
    scheduledAt: '2026-07-30T16:00:00.000Z',
    now: '2026-07-30T16:00:00.000Z',
  });

  assert.equal(result.created, false);
  assert.equal(result.job.id, 'runtime-job-1');
  assert.match(store.calls[0].sql, /ON CONFLICT \(idempotency_key\) DO NOTHING/);
  assert.match(store.calls[1].sql, /WHERE idempotency_key = \$1/);
  assert.equal(store.transactions[0].lockWrites, false);
});

test('claim uses row locking, skip locked, and an expiring worker lease', async () => {
  const store = new FakeStore([
    { rows: [row({
      status: 'running',
      attempt: 1,
      lease_owner: 'worker-a',
      lease_expires_at: '2026-07-30T16:05:00.000Z',
      started_at: '2026-07-30T16:00:00.000Z',
    })] },
  ]);
  const queue = new RuntimeJobQueue(store);

  const claimed = await queue.claim({
    workerId: 'worker-a',
    jobTypes: ['economic-maintenance'],
    leaseSeconds: 300,
    now: '2026-07-30T16:00:00.000Z',
  });

  assert.equal(claimed.status, 'running');
  assert.equal(claimed.leaseOwner, 'worker-a');
  assert.equal(claimed.attempt, 1);
  assert.match(store.calls[0].sql, /FOR UPDATE SKIP LOCKED/);
  assert.match(store.calls[0].sql, /lease_owner = \$3/);
  assert.equal(store.transactions[0].isolation, 'READ COMMITTED');
  assert.equal(store.transactions[0].lockWrites, false);
});

test('heartbeat and completion require the active lease owner', async () => {
  const store = new FakeStore([
    { rows: [row({ status: 'running', lease_owner: 'worker-a', lease_expires_at: '2026-07-30T16:06:00.000Z' })] },
    { rows: [row({ status: 'completed', lease_owner: null, lease_expires_at: null, completed_at: '2026-07-30T16:01:00.000Z', result_json: { forecastsCreated: 1 } })] },
  ]);
  const queue = new RuntimeJobQueue(store);

  const heartbeat = await queue.heartbeat({
    jobId: 'runtime-job-1',
    workerId: 'worker-a',
    leaseSeconds: 300,
    now: '2026-07-30T16:01:00.000Z',
  });
  const completed = await queue.complete({
    jobId: 'runtime-job-1',
    workerId: 'worker-a',
    result: { forecastsCreated: 1 },
    now: '2026-07-30T16:01:00.000Z',
  });

  assert.equal(heartbeat.leaseOwner, 'worker-a');
  assert.equal(completed.status, 'completed');
  assert.deepEqual(completed.result, { forecastsCreated: 1 });
  assert.match(store.calls[0].sql, /lease_owner = \$2 AND status = 'running'/);
  assert.match(store.calls[1].sql, /status = 'completed'/);
});

test('failure retries below max attempts and dead-letters exhausted jobs', async () => {
  const store = new FakeStore([
    { rows: [row({ status: 'retry', attempt: 2, lease_owner: null, last_error: 'temporary_failure' })] },
    { rows: [row({ status: 'failed', attempt: 5, lease_owner: null, completed_at: '2026-07-30T16:02:00.000Z', last_error: 'permanent_failure' })] },
  ]);
  const queue = new RuntimeJobQueue(store);

  const retry = await queue.fail({
    jobId: 'runtime-job-1',
    workerId: 'worker-a',
    error: new Error('temporary_failure'),
    now: '2026-07-30T16:01:00.000Z',
  });
  const failed = await queue.fail({
    jobId: 'runtime-job-1',
    workerId: 'worker-a',
    error: new Error('permanent_failure'),
    now: '2026-07-30T16:02:00.000Z',
  });

  assert.equal(retry.status, 'retry');
  assert.equal(failed.status, 'failed');
  assert.match(store.calls[0].sql, /attempt >= max_attempts/);
  assert.match(store.calls[0].sql, /THEN 'failed' ELSE 'retry'/);
});

test('expired leases are recovered without double-running a still-valid lease', async () => {
  const store = new FakeStore([
    { rows: [row({ status: 'retry', attempt: 1, lease_owner: null, lease_expires_at: null, last_error: 'runtime_job_lease_expired' })] },
  ]);
  const queue = new RuntimeJobQueue(store);
  const recovered = await queue.recoverExpiredLeases({
    now: '2026-07-30T16:10:00.000Z',
    retryDelaySeconds: 5,
  });

  assert.equal(recovered.length, 1);
  assert.equal(recovered[0].status, 'retry');
  assert.match(store.calls[0].sql, /WHERE status = 'running' AND lease_expires_at < \$1/);
});
