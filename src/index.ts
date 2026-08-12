import Fastify from 'fastify';
import { Pool } from 'pg';
import { IngestPipeline } from './ingest/ingest-pipeline';
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

// synchronous_commit is set as the server default in docker-compose.yml, so
// there is deliberately no per-connection SET here. The previous fire-and-forget
// `client.query()` in a 'connect' handler raced the first real query on that
// connection (pg's "client.query() when the client is already executing a
// query is deprecated" warning) and cost a round-trip on every new connection,
// to re-apply a value the server already had.
pgPool.on('error', (err: Error) => {
  console.error('Unexpected error on idle PostgreSQL client:', err);
});

// Ingestion is group-commit: a request is answered only once the transaction
// carrying its rows has committed. Memory is therefore bounded by in-flight
// request concurrency (rows live for the ~20-30ms until their batch commits),
// not by a capacity constant that has to be guessed against the V8 heap.
// maxPendingRows is a safety ceiling for the case where Postgres stalls
// entirely; at ~200 bytes retained per serialized row it is ~10MB.
// See src/ingest/ingest-pipeline.ts for the full rationale.
const pipeline = new IngestPipeline(pgPool, {
  minBatchRows: 4000,
  maxBatchDelayMs: 20,
  maxPendingRows: 50_000,
});
// CPU profiling under sustained ingestion put `secure-json-parse` at 24.7% of
// total process CPU — more than every line of this codebase combined, and the
// single largest consumer. Fastify's default JSON body parser runs two
// prototype-pollution regexes across the *entire* raw body before parsing it;
// at ingestion rates that is megabytes/sec of extra scanning per second.
//
// Setting both actions to 'ignore' makes secure-json-parse return straight
// after JSON.parse and skip both scans (see its index.js line 37).
//
// Why that is safe *here* specifically: those scans protect code that later
// merges parsed input into an existing object (Object.assign, a recursive
// merge, target[key] = value). This service never does that. JSON.parse itself
// does not invoke setters — a "__proto__" key becomes an ordinary own property
// on the parsed object and cannot reach Object.prototype. The only things read
// off the parsed body are fixed, named fields; `attributes` is validated to
// contain nothing but string/number/boolean values and is then re-serialized
// with JSON.stringify into a JSONB column. There is no merge anywhere on the
// path, so there is nothing for a poisoned key to pollute.
const fastify = Fastify({
  logger: false,
  onProtoPoisoning: 'ignore',
  onConstructorPoisoning: 'ignore',
});

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
const shutdown = createGracefulShutdown({ fastify, worker: pipeline, pgPool });
process.on('SIGTERM', () => { partitionMaintenance?.stop(); void shutdown('SIGTERM'); });
process.on('SIGINT', () => { partitionMaintenance?.stop(); void shutdown('SIGINT'); });

async function main() {
  try {
    let connected = false;
    while (!connected) {
      try {
        // Pre-create partitions across the whole retention window, not just
        // today onward, so month-old timestamps land in real prunable
        // partitions instead of the catch-all DEFAULT partition.
        await initDb(pgPool, { partitionBehindDays: RETENTION_DAYS });
        connected = true;
      } catch (e) {
        console.log('Waiting for Postgres to accept connections...');
        await new Promise((r) => setTimeout(r, 1000));
      }
    }

    await fastify.register(healthRoutes(pgPool));
    await fastify.register(ingestionRoutes(pipeline));
    await fastify.register(queryRoutes(pgPool));
    await fastify.register(aggregateRoutes(pgPool));

    pipeline.start();
    partitionMaintenance = startPartitionMaintenance(pgPool, { retentionDays: RETENTION_DAYS });

    await fastify.listen({ port: PORT, host: '0.0.0.0' });
    console.log(`Server running on http://0.0.0.0:${PORT}`);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

main();