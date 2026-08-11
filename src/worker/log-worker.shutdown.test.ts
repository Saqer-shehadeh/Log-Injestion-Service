import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import type { Pool } from 'pg';
import { RingBuffer } from '../buffer/ring-buffer';
import { LogWorker } from './log-worker';

/**
 * Fakes for the pg COPY pipeline. `pg-copy-streams`'s `from(sql)` just
 * builds a query-submittable object; the actual streaming happens on
 * whatever `client.query(...)` returns. These fakes let us control exactly
 * when a "COPY" succeeds or fails, without a live Postgres connection —
 * they never touch the real ring-buffer/COPY implementation.
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
      if (this.opts.shouldFail) {
        this.emit('error', new Error('simulated COPY failure'));
      } else {
        this.emit('finish');
      }
    };
    if (this.opts.delayMs) {
      setTimeout(settle, this.opts.delayMs);
    } else {
      setImmediate(settle);
    }
  }
}

class FakeClient {
  public released = false;
  constructor(private streamFactory: () => FakeCopyStream) {}
  query(_copyFromArg: unknown) {
    return this.streamFactory();
  }
  release() {
    this.released = true;
  }
}

class FakePool {
  public connectCount = 0;
  constructor(private streamFactory: () => FakeCopyStream) {}
  async connect() {
    this.connectCount++;
    return new FakeClient(this.streamFactory);
  }
}

test('LogWorker.shutdown()', async (t) => {
  await t.test('drains all remaining buffered entries and drops them only after a successful COPY', async () => {
    const buffer = new RingBuffer<string>(100);
    for (let i = 0; i < 5; i++) buffer.push(`row${i}\n`);

    const pool = new FakePool(() => new FakeCopyStream());
    const worker = new LogWorker(buffer, pool as unknown as Pool, 4000, 50);

    // shutdown() called directly, without start() — exercises the "drain
    // whatever's sitting in the buffer" path in isolation.
    await worker.shutdown();

    assert.strictEqual(buffer.size(), 0);
    assert.strictEqual(pool.connectCount, 1); // 5 rows fit in a single batch (baseBatchSize=4000)
  });

  await t.test('does not drop entries when a COPY fails during drain, and retries until it succeeds', async () => {
    const buffer = new RingBuffer<string>(100);
    for (let i = 0; i < 3; i++) buffer.push(`row${i}\n`);

    let attempt = 0;
    const pool = new FakePool(() => {
      attempt++;
      return new FakeCopyStream({ shouldFail: attempt === 1 }); // fail once, then succeed
    });
    const worker = new LogWorker(buffer, pool as unknown as Pool, 4000, 50);

    assert.strictEqual(buffer.size(), 3);
    await worker.shutdown();

    assert.strictEqual(attempt, 2, 'expected exactly one failed attempt and one successful retry');
    assert.strictEqual(buffer.size(), 0, 'entries should only be dropped once COPY finally succeeds');
  });

  await t.test('waits for an in-flight COPY (started before shutdown) to finish before draining further', async () => {
    const buffer = new RingBuffer<string>(100);
    for (let i = 0; i < 5; i++) buffer.push(`row${i}\n`);

    let calls = 0;
    const pool = new FakePool(() => {
      calls++;
      return new FakeCopyStream({ delayMs: 40 }); // simulate a slow COPY
    });
    // baseBatchSize is 5 here so the 5 buffered rows immediately satisfy the
    // loop's minimum-batch condition. With the default 4000 the loop would
    // (correctly) hold this partial batch for up to maxFlushDelayMs, and no
    // COPY would be in flight when shutdown() is called — which is a valid
    // behaviour, but not the one this test is about.
    const worker = new LogWorker(buffer, pool as unknown as Pool, 5, 50);

    // start() synchronously fires the first flush cycle, which peeks the
    // batch and increments inFlight before this call returns.
    worker.start();

    const startedAt = Date.now();
    await worker.shutdown();
    const elapsed = Date.now() - startedAt;

    assert.ok(
      elapsed >= 30,
      `expected shutdown() to block on the ~40ms in-flight COPY, only took ${elapsed}ms`
    );
    assert.strictEqual(buffer.size(), 0);
    assert.strictEqual(calls, 1, 'no second flush should have been started once shutdown began');
  });

  await t.test('shutdown() on an already-idle worker (empty buffer) resolves immediately without any COPY', async () => {
    const buffer = new RingBuffer<string>(100);
    const pool = new FakePool(() => new FakeCopyStream());
    const worker = new LogWorker(buffer, pool as unknown as Pool, 4000, 50);

    await worker.shutdown();

    assert.strictEqual(pool.connectCount, 0);
  });
});
