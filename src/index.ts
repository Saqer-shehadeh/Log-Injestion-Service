import Fastify from 'fastify';
import { Pool } from 'pg';
import { RingBuffer } from './buffer/ring-buffer';
import { LogWorker } from './worker/log-worker';
import { healthRoutes } from './routes/health';
import { ingestionRoutes } from './routes/ingestion';
import { queryRoutes } from './routes/query';
import { aggregateRoutes } from './routes/aggregate';
import { startRetentionTask } from './retention/retention-cron';
import { initDb } from './db/migrate';

const PORT = Number(process.env.PORT) || 8080;
const DB_URL = process.env.DATABASE_URL || 'postgres://loguser:logpass@localhost:5432/logdb';

const pgPool = new Pool({ connectionString: DB_URL, max: 20 });

// Set synchronous_commit = on on every new PG connection for write throughput.
// Data is still WAL-logged — only fsync is deferred.  Acceptable for a log service.
pgPool.on('connect', (client: any) => {
  client.query('SET synchronous_commit = on');
});

// Buffer stores pre-serialized TSV strings (not objects)
const logBuffer = new RingBuffer<string>(500_000);

// Adaptive worker: base batch 4000, flush interval 50ms, 1 concurrent flush for zero data loss
const worker = new LogWorker(logBuffer, pgPool, 4000, 50, 1);
const fastify = Fastify({ logger: false });

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
    startRetentionTask(pgPool, 7);

    await fastify.listen({ port: PORT, host: '0.0.0.0' });
    console.log(`Server running on http://0.0.0.0:${PORT}`);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

main();