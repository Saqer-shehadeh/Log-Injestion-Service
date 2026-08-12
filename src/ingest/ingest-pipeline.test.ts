import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import type { Pool } from 'pg';
import {
  BackpressureError,
  IngestPipeline,
  IngestUnavailableError,
  RollupDelta,
} from './ingest-pipeline';

/**
 * Fakes for the pg COPY pipeline. `pg-copy-streams`'s `from(sql)` just builds a
 * query-submittable object; the streaming happens on whatever `client.query()`
 * returns. These fakes control exactly when a COPY succeeds or fails without a
 * live Postgres, and record the statements issued so transaction framing can be
 * asserted.
 */
class FakeCopyStream extends EventEmitter {
  public written: string[] = [];
  constructor(private opts: { shouldFail?: boolean; delayMs?: number } = {}) {
    super();
  }
  write(chunk: string) {
    this.written.push(chunk);
    return true;
  }
  end() {
    const settle = () => {
      if (this.opts.shouldFail) this.emit('error', new Error('simulated COPY failure'));
      else this.emit('finish');
    };
    if (this.opts.delayMs) setTimeout(settle, this.opts.delayMs);
    else setImmediate(settle);
  }
}

class FakeClient {
  public released = false;
  constructor(
    public statements: string[],
    public copyStreams: FakeCopyStream[],
    private streamFactory: () => FakeCopyStream
  ) {}
  query(arg: unknown, _values?: unknown[]): any {
    if (typeof arg === 'string') {
      this.statements.push(arg.trim().split(/\s+/).slice(0, 2).join(' '));
      return Promise.resolve({ rows: [] });
    }
    const s = this.streamFactory();
    this.copyStreams.push(s);
    return s;
  }
  release() {
    this.released = true;
  }
}

class FakePool {
  public connectCount = 0;
  public statements: string[] = [];
  public copyStreams: FakeCopyStream[] = [];
  public clients: FakeClient[] = [];
  constructor(private streamFactory: () => FakeCopyStream = () => new FakeCopyStream()) {}
  async connect() {
    this.connectCount++;
    const c = new FakeClient(this.statements, this.copyStreams, this.streamFactory);
    this.clients.push(c);
    return c;
  }
}

function rollupOf(service: string, level: string, count: number): Map<string, RollupDelta> {
  const bucketMs = Date.UTC(2026, 7, 11, 10, 0, 0);
  return new Map([[`${bucketMs}|${service}|${level}`, { bucketMs, service, level, count }]]);
}

const rows = (n: number) => Array.from({ length: n }, (_, i) => `row${i}\n`);

test('IngestPipeline durability contract', async (t) => {
  await t.test('submit() does not resolve until the batch has COMMITted', async () => {
    const pool = new FakePool(() => new FakeCopyStream({ delayMs: 30 }));
    const pipeline = new IngestPipeline(pool as unknown as Pool, { maxBatchDelayMs: 1 });
    pipeline.start();

    let resolved = false;
    const p = pipeline.submit(rows(3), rollupOf('checkout', 'info', 3)).then(() => {
      resolved = true;
    });

    // Still mid-COPY: the caller must not have been told its rows are durable.
    await new Promise((r) => setTimeout(r, 10));
    assert.strictEqual(resolved, false, 'resolved before COMMIT');

    await p;
    assert.strictEqual(resolved, true);
    assert.ok(
      pool.statements.includes('COMMIT'),
      `expected a COMMIT, saw: ${pool.statements.join(', ')}`
    );
    pipeline.stop();
  });

  await t.test('COPY and the rollup upsert share one transaction', async () => {
    const pool = new FakePool();
    const pipeline = new IngestPipeline(pool as unknown as Pool, { maxBatchDelayMs: 1 });
    pipeline.start();

    await pipeline.submit(rows(2), rollupOf('auth', 'error', 2));

    // BEGIN ... INSERT (rollup) ... COMMIT, in that order, on one connection.
    assert.deepStrictEqual(pool.statements, ['BEGIN', 'INSERT INTO', 'COMMIT']);
    assert.strictEqual(pool.connectCount, 1);
    pipeline.stop();
  });

  await t.test('a failing COPY rejects its waiters and rolls back — never a silent success', async () => {
    const pool = new FakePool(() => new FakeCopyStream({ shouldFail: true }));
    const pipeline = new IngestPipeline(pool as unknown as Pool, {
      maxBatchDelayMs: 1,
      maxAttempts: 2,
      retryBackoffMs: 1,
    });
    pipeline.start();

    await assert.rejects(
      () => pipeline.submit(rows(2), rollupOf('checkout', 'warn', 2)),
      IngestUnavailableError
    );
    assert.ok(pool.statements.includes('ROLLBACK'));
    assert.ok(!pool.statements.includes('COMMIT'));
    pipeline.stop();
  });

  await t.test('a transient failure is retried and then reported as committed', async () => {
    let attempt = 0;
    const pool = new FakePool(() => new FakeCopyStream({ shouldFail: ++attempt === 1 }));
    const pipeline = new IngestPipeline(pool as unknown as Pool, {
      maxBatchDelayMs: 1,
      maxAttempts: 3,
      retryBackoffMs: 1,
    });
    pipeline.start();

    await pipeline.submit(rows(2), rollupOf('auth', 'info', 2));

    assert.strictEqual(attempt, 2, 'expected one failure then one successful retry');
    assert.ok(pool.statements.includes('COMMIT'));
    pipeline.stop();
  });

  await t.test('rows from concurrent requests are merged into one transaction', async () => {
    const pool = new FakePool(() => new FakeCopyStream({ delayMs: 15 }));
    const pipeline = new IngestPipeline(pool as unknown as Pool, { maxBatchDelayMs: 1 });
    pipeline.start();

    // First submit starts a flush; the next two land in the following batch.
    const a = pipeline.submit(rows(2), rollupOf('checkout', 'info', 2));
    await new Promise((r) => setTimeout(r, 5));
    const b = pipeline.submit(rows(2), rollupOf('checkout', 'info', 2));
    const c = pipeline.submit(rows(2), rollupOf('auth', 'error', 2));

    await Promise.all([a, b, c]);

    // 3 requests, but strictly fewer transactions — they were grouped.
    assert.ok(pool.connectCount < 3, `expected grouping, got ${pool.connectCount} transactions`);
    assert.strictEqual(pipeline.pendingRows(), 0);
    pipeline.stop();
  });
});

test('IngestPipeline backpressure', async (t) => {
  await t.test('rejects with BackpressureError instead of growing without bound', async () => {
    // A COPY that never settles, so nothing drains and the bound is reachable.
    const pool = new FakePool(() => new FakeCopyStream({ delayMs: 60_000 }));
    const pipeline = new IngestPipeline(pool as unknown as Pool, {
      maxBatchDelayMs: 1,
      maxPendingRows: 10,
    });
    pipeline.start();

    void pipeline.submit(rows(8), rollupOf('checkout', 'info', 8)).catch(() => {});
    await assert.rejects(
      () => pipeline.submit(rows(8), rollupOf('checkout', 'info', 8)),
      BackpressureError
    );
    assert.ok(pipeline.pendingRows() <= 10);
    pipeline.stop();
  });

  await t.test('BackpressureError carries a Retry-After hint', () => {
    const err = new BackpressureError(3);
    assert.strictEqual(err.retryAfterSeconds, 3);
  });
});

test('IngestPipeline.shutdown()', async (t) => {
  await t.test('commits queued rows and resolves their waiters before returning', async () => {
    const pool = new FakePool();
    // Long delay so nothing flushes on its own before shutdown() is called.
    const pipeline = new IngestPipeline(pool as unknown as Pool, { maxBatchDelayMs: 60_000 });
    pipeline.start();

    let resolved = false;
    const p = pipeline.submit(rows(5), rollupOf('checkout', 'info', 5)).then(() => {
      resolved = true;
    });
    await new Promise((r) => setTimeout(r, 5));
    assert.strictEqual(resolved, false, 'should still be waiting before shutdown');

    await pipeline.shutdown();
    await p;

    assert.strictEqual(resolved, true, 'queued rows must be committed during drain');
    assert.ok(pool.statements.includes('COMMIT'));
    assert.strictEqual(pipeline.pendingRows(), 0);
  });

  await t.test('waits for an in-flight transaction to finish', async () => {
    const pool = new FakePool(() => new FakeCopyStream({ delayMs: 40 }));
    const pipeline = new IngestPipeline(pool as unknown as Pool, { maxBatchDelayMs: 1 });
    pipeline.start();

    const p = pipeline.submit(rows(3), rollupOf('auth', 'info', 3));
    await new Promise((r) => setTimeout(r, 10)); // let the flush start

    const startedAt = Date.now();
    await pipeline.shutdown();
    assert.ok(Date.now() - startedAt >= 20, 'shutdown returned before the in-flight COPY finished');

    await p;
    assert.strictEqual(pipeline.pendingRows(), 0);
  });

  await t.test('refuses new work once shutdown has begun', async () => {
    const pool = new FakePool();
    const pipeline = new IngestPipeline(pool as unknown as Pool, { maxBatchDelayMs: 1 });
    pipeline.start();

    await pipeline.shutdown();

    await assert.rejects(
      () => pipeline.submit(rows(1), rollupOf('checkout', 'info', 1)),
      IngestUnavailableError
    );
  });

  await t.test('an idle pipeline shuts down without opening a transaction', async () => {
    const pool = new FakePool();
    const pipeline = new IngestPipeline(pool as unknown as Pool, { maxBatchDelayMs: 1 });
    pipeline.start();

    await pipeline.shutdown();
    assert.strictEqual(pool.connectCount, 0);
  });
});
