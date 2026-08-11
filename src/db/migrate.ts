import { Pool } from 'pg';
import { DEFAULT_PARTITION_AHEAD_DAYS, ensureUpcomingPartitions } from './partitions';

/**
 * Note on DB-level performance settings: they are all declared on the
 * `postgres` service's command line in docker-compose.yml, not here.
 *
 * This function used to open with an `ALTER SYSTEM SET ...` block, but
 * node-postgres sends a multi-statement string as a single simple query, which
 * Postgres wraps in an implicit transaction — and ALTER SYSTEM is illegal
 * inside one. So it threw "ALTER SYSTEM cannot run inside a transaction block"
 * on every single boot, got swallowed by its own catch, and never applied
 * anything (postgresql.auto.conf stayed empty). It also set values that
 * contradicted docker-compose.yml, so the two would have fought had it worked.
 * Keeping the tuning in exactly one place avoids both problems.
 */
export async function initDb(pgPool: Pool, options: { partitionAheadDays?: number } = {}) {
  const query = `
    CREATE TABLE IF NOT EXISTS logs (
        id BIGSERIAL,
        timestamp TIMESTAMPTZ NOT NULL,
        level VARCHAR(10) NOT NULL,
        service VARCHAR(100) NOT NULL,
        message TEXT NOT NULL,
        attributes JSONB DEFAULT '{}'::jsonb,
        PRIMARY KEY (timestamp, id)
    ) PARTITION BY RANGE (timestamp);

    CREATE TABLE IF NOT EXISTS logs_default PARTITION OF logs DEFAULT;

    CREATE INDEX IF NOT EXISTS idx_logs_query ON logs (service, level, timestamp DESC);
    CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs (timestamp DESC);
  `;
  await pgPool.query(query);

  // Daily range partitions: ensure "today" and the next few days already
  // exist before the server starts accepting ingestion traffic, so writes
  // never land in the DEFAULT partition under normal operation. This is
  // awaited here (not left to the periodic maintenance job) specifically
  // so it completes before main() registers routes / starts the worker.
  await ensureUpcomingPartitions(pgPool, options.partitionAheadDays ?? DEFAULT_PARTITION_AHEAD_DAYS);

  console.log('Database migrations applied successfully.');
}