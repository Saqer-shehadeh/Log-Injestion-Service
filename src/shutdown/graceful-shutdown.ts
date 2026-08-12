/**
 * Orchestrates graceful shutdown of the service on SIGTERM/SIGINT.
 *
 * Sequence:
 *   1. Stop accepting new HTTP requests  (fastify.close())
 *   2. Stop accepting new ingest work, commit everything already queued or in
 *      flight, and answer every request still waiting on it — either with a
 *      successful commit or an explicit failure, never silently
 *      (worker.shutdown())
 *   3. Close the PostgreSQL pool          (pgPool.end())
 *   4. Exit
 *
 * The whole sequence is bounded by a timeout so the process cannot hang
 * forever if Postgres never becomes available again.
 *
 * Dependencies are passed in (rather than imported directly) so this can be
 * unit tested with lightweight fakes instead of a real Fastify/pg instance.
 */

export interface GracefulShutdownDeps {
  fastify: { close: () => Promise<unknown> };
  worker: { shutdown: () => Promise<void> };
  pgPool: { end: () => Promise<unknown> };
  /** Total time budget for the whole shutdown sequence. Default 10s. */
  timeoutMs?: number;
  log?: (...args: unknown[]) => void;
  logError?: (...args: unknown[]) => void;
  /** Injectable for tests; defaults to process.exit. */
  exit?: (code: number) => void;
}

export type ShutdownFn = (signal: string) => Promise<void>;

export function createGracefulShutdown(deps: GracefulShutdownDeps): ShutdownFn {
  const {
    fastify,
    worker,
    pgPool,
    timeoutMs = 10_000,
    log = console.log,
    logError = console.error,
    exit = process.exit,
  } = deps;

  // Idempotency guard: the first call wins and its promise is reused for
  // any later call (e.g. SIGTERM followed by SIGINT), so a second signal
  // never starts a second shutdown sequence.
  let shutdownPromise: Promise<void> | null = null;

  async function runShutdown(signal: string): Promise<void> {
    log(`[shutdown] Received ${signal}. Starting graceful shutdown...`);

    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      logError(
        `[shutdown] Did not complete within ${timeoutMs}ms (Postgres unavailable or drain stuck). Forcing exit.`
      );
      exit(1);
    }, timeoutMs);

    try {
      // 1. Stop accepting new HTTP requests/connections.
      await fastify.close();
      log('[shutdown] HTTP server closed; no new requests will be accepted.');

      // 2. Worker: stop new flushes, wait for in-flight COPY, drain buffer.
      await worker.shutdown();
      log('[shutdown] Worker drained remaining buffered logs.');

      // 3. Close the PostgreSQL pool now that nothing else needs it.
      await pgPool.end();
      log('[shutdown] PostgreSQL pool closed.');

      if (settled) return; // timeout already fired and forced exit; nothing left to do
      settled = true;
      clearTimeout(timer);
      log('[shutdown] Graceful shutdown complete.');
      exit(0);
    } catch (err) {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      logError('[shutdown] Error during graceful shutdown:', err);
      exit(1);
    }
  }

  return function shutdown(signal: string): Promise<void> {
    if (!shutdownPromise) {
      shutdownPromise = runShutdown(signal);
    } else {
      log(`[shutdown] Received ${signal} while shutdown already in progress; ignoring.`);
    }
    return shutdownPromise;
  };
}
