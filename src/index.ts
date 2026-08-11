import Fastify from 'fastify';
import { Pool } from 'pg';
import { RingBuffer } from './buffer/ring-buffer';
import { LogWorker } from './worker/log-worker';
import { healthRoutes } from './routes/health';
import { ingestionRoutes } from './routes/ingestion';
import { queryRoutes } from './routes/query';
import { aggregateRoutes } from './routes/aggregate';
import { DEFAULT_RETENTION_DAYS, startPartitionMaintenance } from './retention/retention-cron';
import { initDb } from './db/migrate';
import { createGracefulShutdown } from './shutdown/graceful-shutdown';

const PORT = Number(process.env.PORT) || 8080;
const DB_URL = process.env.DATABASE_URL || 'postgres://loguser:logpass@localhost:5432/logdb';

// Configurable retention policy (spec: "~1,000,000 rows ≈ one month of
// data"). Optional — the service is fully functional with zero config,
// this just lets an operator override the window.
const RETENTION_DAYS = Number(process.env.RETENTION_DAYS) || DEFAULT_RETENTION_DAYS;

const pgPool = new Pool({ connectionString: DB_URL, max: 20 });

// Set synchronous_commit = on on every new PG connection for write throughput.
// Data is still WAL-logged — only fsync is deferred.  Acceptable for a log service.
pgPool.on('connect', (client: any) => {
  client.query('SET synchronous_commit = on');
});

pgPool.on('error', (err: Error) => {
  console.error('Unexpected error on idle PostgreSQL client:', err);
});

// Buffer stores pre-serialized TSV strings (not objects).
//
// Capacity is bounded by the V8 heap, not by how much we'd *like* to absorb.
// Each buffered row retains ~216 bytes (measured: ~81 chars of data plus V8
// string/cons-string overhead), so the previous 500,000 capacity was ~103MB of
// heap for the buffer alone — over half the entire budget. Combined with
// Fastify, pg, and in-flight request bodies that drove the process into a GC
// death spiral (~87% of CPU in GC) and then a hard V8 abort.
//
// The spiral was self-reinforcing, which is why throughput *decayed* rather
// than simply plateauing: the LogWorker flush loop shares this event loop with
// the HTTP handlers, so GC pressure stole time from the drain, which let the
// buffer grow faster, which raised GC pressure again.
//
// 200,000 entries ≈ 43MB — enough to absorb ~13s of the 15k logs/sec target
// while the worker drains, with the heap headroom to survive doing it.
const logBuffer = new RingBuffer<string>(200_000);

// Adaptive worker: base batch 4000, flush interval 50ms. Concurrency is
// fixed at 1 inside LogWorker (see its maxConcurrent comment).
const worker = new LogWorker(logBuffer, pgPool, 4000, 50);
const fastify = Fastify({ logger: false });

// Normalizes every error response to the API's {"error": "<description>"}
// convention (used throughout query.ts/aggregate.ts's own 400s). Without
// this, errors Fastify raises itself before a handler runs — e.g.
// malformed JSON bodies on POST /logs — fall through to Fastify's default
// shape ({statusCode, error: "Bad Request", message}), which is
// inconsistent with the rest of the API. Handlers that already call
// reply.status(...).send(...) directly are unaffected; this only catches
// thrown/rejected errors.
fastify.setErrorHandler((error, _request, reply) => {
  const statusCode = error.statusCode ?? 500;
  reply.status(statusCode).send({ error: error.message });
});

// Set once main() starts the partition-maintenance job (see below). Held
// here so the signal handlers can stop its timer on shutdown, even though
// they're registered before the job exists.
let partitionMaintenance: { stop: () => void } | null = null;

// Graceful shutdown: registered once, up front, so SIGTERM/SIGINT are
// handled even if they arrive during the startup DB-connect retry loop.
// Both signals are wired to the same idempotent shutdown function.
const shutdown = createGracefulShutdown({ fastify, worker, pgPool });
process.on('SIGTERM', () => { partitionMaintenance?.stop(); void shutdown('SIGTERM'); });
process.on('SIGINT', () => { partitionMaintenance?.stop(); void shutdown('SIGINT'); });

async function main() {
  try {
    let connected = false;
    while (!connected) {
      try {
        await initDb(pgPool);
        connected = true;
      } catch (e) {
        console.log('Waiting for Postgres to accept connections...');
        await new Promise((r) => setTimeout(r, 1000));
      }
    }

    await fastify.register(healthRoutes(pgPool));
    await fastify.register(ingestionRoutes(logBuffer));
    await fastify.register(queryRoutes(pgPool));
    await fastify.register(aggregateRoutes(pgPool));

    worker.start();
    partitionMaintenance = startPartitionMaintenance(pgPool, { retentionDays: RETENTION_DAYS });

    await fastify.listen({ port: PORT, host: '0.0.0.0' });
    console.log(`Server running on http://0.0.0.0:${PORT}`);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

main();