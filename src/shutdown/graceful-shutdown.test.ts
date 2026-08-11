import test from 'node:test';
import assert from 'node:assert/strict';
import { createGracefulShutdown } from './graceful-shutdown';

function makeDeps(overrides: Partial<{
  closeDelayMs: number;
  workerShutdownDelayMs: number;
  workerShutdownNeverResolves: boolean;
  poolEndDelayMs: number;
  timeoutMs: number;
}> = {}) {
  const calls: string[] = [];
  const exitCodes: number[] = [];
  const errors: unknown[][] = [];

  const fastify = {
    close: async () => {
      if (overrides.closeDelayMs) await new Promise((r) => setTimeout(r, overrides.closeDelayMs));
      calls.push('fastify.close');
    },
  };

  const worker = {
    shutdown: async () => {
      if (overrides.workerShutdownNeverResolves) {
        await new Promise(() => {}); // hang forever, to exercise the timeout path
      }
      if (overrides.workerShutdownDelayMs) {
        await new Promise((r) => setTimeout(r, overrides.workerShutdownDelayMs));
      }
      calls.push('worker.shutdown');
    },
  };

  const pgPool = {
    end: async () => {
      if (overrides.poolEndDelayMs) await new Promise((r) => setTimeout(r, overrides.poolEndDelayMs));
      calls.push('pgPool.end');
    },
  };

  const shutdown = createGracefulShutdown({
    fastify,
    worker,
    pgPool,
    timeoutMs: overrides.timeoutMs ?? 200,
    log: () => {},
    logError: (...args: unknown[]) => errors.push(args),
    exit: (code: number) => exitCodes.push(code),
  });

  return { shutdown, calls, exitCodes, errors };
}

test('graceful shutdown', async (t) => {
  await t.test('SIGTERM runs the full shutdown sequence in order and exits 0', async () => {
    const { shutdown, calls, exitCodes } = makeDeps();
    await shutdown('SIGTERM');
    assert.deepStrictEqual(calls, ['fastify.close', 'worker.shutdown', 'pgPool.end']);
    assert.deepStrictEqual(exitCodes, [0]);
  });

  await t.test('SIGINT runs the same shutdown sequence and exits 0', async () => {
    const { shutdown, calls, exitCodes } = makeDeps();
    await shutdown('SIGINT');
    assert.deepStrictEqual(calls, ['fastify.close', 'worker.shutdown', 'pgPool.end']);
    assert.deepStrictEqual(exitCodes, [0]);
  });

  await t.test('shutdown is idempotent: a second signal while shutdown is in progress does not restart it', async () => {
    const { shutdown, calls, exitCodes } = makeDeps({ closeDelayMs: 20, workerShutdownDelayMs: 20 });

    // Fire SIGTERM, then SIGINT shortly after, before the first has finished.
    const first = shutdown('SIGTERM');
    await new Promise((r) => setTimeout(r, 5));
    const second = shutdown('SIGINT');

    await Promise.all([first, second]);

    // Each step of the sequence must have run exactly once, not twice.
    assert.deepStrictEqual(calls, ['fastify.close', 'worker.shutdown', 'pgPool.end']);
    assert.deepStrictEqual(exitCodes, [0]);
  });

  await t.test('fastify.close() runs before worker.shutdown() (stop accepting requests before draining)', async () => {
    const { shutdown, calls } = makeDeps({ workerShutdownDelayMs: 10 });
    await shutdown('SIGTERM');
    assert.ok(calls.indexOf('fastify.close') < calls.indexOf('worker.shutdown'));
  });

  await t.test('pgPool.end() runs after worker.shutdown() (pool stays open until draining is done)', async () => {
    const { shutdown, calls } = makeDeps({ workerShutdownDelayMs: 10 });
    await shutdown('SIGTERM');
    assert.ok(calls.indexOf('worker.shutdown') < calls.indexOf('pgPool.end'));
  });

  await t.test('a stuck worker.shutdown() (e.g. Postgres never comes back) is bounded by the timeout and forces exit(1)', async () => {
    const { shutdown, calls, exitCodes, errors } = makeDeps({
      workerShutdownNeverResolves: true,
      timeoutMs: 30,
    });

    // Deliberately not awaited: worker.shutdown() never resolves in this
    // scenario (simulating Postgres never coming back), and in production
    // the timeout's exit(1) call is a real process.exit() that terminates
    // the process outright, so nothing ever needs the outer promise to
    // settle. Here we just wait past the timeout and assert on the
    // (faked) exit call instead.
    void shutdown('SIGTERM');
    await new Promise((r) => setTimeout(r, 60));

    assert.deepStrictEqual(exitCodes, [1]);
    assert.deepStrictEqual(calls, ['fastify.close']); // never got past worker.shutdown()
    assert.ok(errors.length >= 1, 'expected a clear timeout error to be logged');
    assert.ok(String(errors[0][0]).toLowerCase().includes('did not complete'));
  });
});
