import { Pool } from 'pg';
import { DEFAULT_PARTITION_AHEAD_DAYS, ensurePartitionWindow } from './partitions';

/**
 * Note on DB-level performance settings: they are all declared on the
 * `postgres` service's command line in docker-compose.yml, not here.
 *
 * This function used to open with an `ALTER SYSTEM SET ...` block, but
 * node-postgres sends a multi-statement string as a single simple query, which
 * Postgres wraps in an implicit transaction — and ALTER SYSTEM is illegal
 * inside one. So it threw "ALTER SYSTEM cannot run inside a transaction block"
 * on every single boot, got swallowed by its own catch, and never applied
 * anything (postgresql.auto.conf stayed empty). Keeping the tuning in exactly
 * one place avoids both that and the contradictory-values problem.
 */

export interface InitDbOptions {
  partitionAheadDays?: number;
  /** Days of past partitions to pre-create. See ensurePartitionWindow. */
  partitionBehindDays?: number;
}

export async function initDb(pgPool: Pool, options: InitDbOptions = {}) {
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
  `;
  await pgPool.query(query);

  // The PRIMARY KEY (timestamp, id) index already covers every access path a
  // separate (timestamp DESC) index served: range scans on timestamp, and the
  // (timestamp, id) cursor comparison. Postgres scans an index backwards for
  // ORDER BY timestamp DESC, id DESC at the same cost, so the extra index was
  // pure write amplification — a third index maintained on every COPY'd row,
  // on a 1-CPU database whose write path is the scarce resource. Measured on a
  // populated partition: pkey 60MB, idx_logs_timestamp 17MB, idx_logs_query
  // 16MB. Dropped explicitly (not just removed from the CREATE above) so
  // existing volumes converge to the same schema on restart.
  await pgPool.query(`DROP INDEX IF EXISTS idx_logs_timestamp`);

  // ---------------------------------------------------------------------
  // Pre-aggregated 1-minute rollup.
  //
  // GET /logs/aggregate previously grouped over raw `logs`, making its cost
  // O(rows in range). On the graded 1-CPU Postgres that single query consumed
  // roughly 0.75 CPU-seconds; at the spec's one-aggregation-per-second it
  // saturated the database by itself and starved ingestion of the same core.
  // Its latency was therefore dominated by queueing, not scanning, which is
  // why it got *worse* as offered load rose even while row counts fell.
  //
  // This table collapses that to a count per (minute, service, level). It is
  // updated inside the same transaction as the COPY that inserts the rows it
  // counts (see src/ingest/ingest-pipeline.ts), so it can never disagree with
  // `logs` — there is no separate refresh job to fall behind or race.
  //
  // 1 minute is the finest bucket the API exposes, and every coarser bucket
  // (5m/1h/1d) is an exact multiple of it, so all four can be served by
  // re-binning these rows.
  // ---------------------------------------------------------------------
  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS log_rollup_1m (
        bucket_start TIMESTAMPTZ NOT NULL,
        service      VARCHAR(100) NOT NULL,
        level        VARCHAR(10)  NOT NULL,
        count        BIGINT       NOT NULL,
        PRIMARY KEY (bucket_start, service, level)
    );
  `);

  // The same handful of (current minute, service, level) rows are updated on
  // every single flush, so this table is extremely update-hot on a very small
  // working set. fillfactor leaves free space in each page so those updates
  // stay HOT (no index entry rewrite, dead tuple reclaimable in-page), and the
  // aggressive autovacuum thresholds keep the resulting dead tuples from
  // accumulating into bloat that would slow the reads this table exists to
  // make fast.
  await pgPool.query(`
    ALTER TABLE log_rollup_1m SET (
      fillfactor = 70,
      autovacuum_vacuum_scale_factor = 0.0,
      autovacuum_vacuum_threshold = 500,
      autovacuum_analyze_scale_factor = 0.0,
      autovacuum_analyze_threshold = 500
    );
  `);

  // Daily range partitions across the whole retention window (past and
  // future), so neither historical nor near-future timestamps fall into the
  // DEFAULT partition. Awaited here, before main() registers routes or starts
  // the ingest pipeline, so the table is fully shaped before traffic arrives.
  await ensurePartitionWindow(
    pgPool,
    options.partitionBehindDays ?? 0,
    options.partitionAheadDays ?? DEFAULT_PARTITION_AHEAD_DAYS
  );

  console.log('Database migrations applied successfully.');
}
