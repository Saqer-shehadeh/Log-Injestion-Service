import { Pool } from 'pg';

const MS_PER_DAY = 24 * 60 * 60 * 1000;

/** Matches this module's daily-partition naming convention exactly, e.g. logs_y2026m08d11. */
const PARTITION_NAME_RE = /^logs_y(\d{4})m(\d{2})d(\d{2})$/;

/**
 * Default number of future days to keep pre-created, beyond "today".
 * log-validator.ts allows timestamps up to 5 minutes in the future, so 1
 * day ahead would already cover that; 2 gives a comfortable safety margin
 * against clock drift or a missed maintenance cycle before ingestion would
 * ever fall back to the DEFAULT partition.
 */
export const DEFAULT_PARTITION_AHEAD_DAYS = 2;

/** UTC midnight for the day containing `date`. */
function startOfUtcDay(date: Date): Date {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
}

/** Partition name for the UTC day starting at `dayStart` (must already be UTC midnight). */
export function partitionNameFor(dayStart: Date): string {
  const y = dayStart.getUTCFullYear();
  const m = String(dayStart.getUTCMonth() + 1).padStart(2, '0');
  const d = String(dayStart.getUTCDate()).padStart(2, '0');
  return `logs_y${y}m${m}d${d}`;
}

/**
 * Idempotently creates the daily partition covering [dayStart, dayStart+1day).
 *
 * Partition bounds must be literal constants in Postgres' DDL grammar —
 * `FOR VALUES FROM (...) TO (...)` does not accept bind parameters — so the
 * bounds are inlined as ISO-8601 strings. They are computed here from
 * `Date` objects, never taken from request input, so this is not a
 * SQL-injection surface; the generated name is additionally checked
 * against the expected pattern before being interpolated, as a defensive
 * belt-and-braces check rather than because it's reachable by user input.
 */
export async function ensureDailyPartition(pgPool: Pool, dayStart: Date): Promise<void> {
  const name = partitionNameFor(dayStart);
  if (!PARTITION_NAME_RE.test(name)) {
    throw new Error(`Refusing to create partition with unexpected name: ${name}`);
  }

  const from = dayStart.toISOString();
  const to = new Date(dayStart.getTime() + MS_PER_DAY).toISOString();

  await pgPool.query(
    `CREATE TABLE IF NOT EXISTS ${name} PARTITION OF logs FOR VALUES FROM ('${from}') TO ('${to}')`
  );
}

/**
 * Ensures partitions exist for "today" through `aheadDays` days ahead
 * (UTC), so ingestion never has to fall back to the DEFAULT partition for
 * an in-window timestamp. Safe to call repeatedly (each partition create
 * is IF NOT EXISTS).
 */
export async function ensureUpcomingPartitions(
  pgPool: Pool,
  aheadDays: number = DEFAULT_PARTITION_AHEAD_DAYS,
  now: Date = new Date()
): Promise<void> {
  const today = startOfUtcDay(now);
  for (let i = 0; i <= aheadDays; i++) {
    await ensureDailyPartition(pgPool, new Date(today.getTime() + i * MS_PER_DAY));
  }
}

/**
 * Drops daily partitions that are entirely older than the retention
 * window, using ALTER TABLE ... DETACH PARTITION ... CONCURRENTLY followed
 * by DROP TABLE, instead of a bulk DELETE. DETACH CONCURRENTLY avoids
 * taking a long-lived lock on the live `logs` table while ingestion COPYs
 * and queries are active; the DROP that follows only touches the
 * already-detached, now-standalone table, so it is fast and does not
 * disturb `logs` at all.
 *
 * Only tables matching this module's exact daily-partition naming
 * convention (logs_yYYYYmMMdDD) are ever considered — the SQL filters for
 * it, and the result is checked again in JS as a second, independent
 * guard. The DEFAULT partition and anything else attached to `logs` is
 * never touched by this function.
 *
 * Returns the names of the partitions that were dropped.
 */
export async function dropExpiredPartitions(
  pgPool: Pool,
  retentionDays: number,
  now: Date = new Date()
): Promise<string[]> {
  const cutoff = startOfUtcDay(new Date(now.getTime() - retentionDays * MS_PER_DAY));

  const { rows } = await pgPool.query<{ partition: string }>(`
    SELECT c.relname AS partition
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    JOIN pg_class p ON p.oid = i.inhparent
    WHERE p.relname = 'logs'
      AND c.relname ~ '^logs_y[0-9]{4}m[0-9]{2}d[0-9]{2}$'
    ORDER BY c.relname ASC
  `);

  const dropped: string[] = [];

  for (const { partition } of rows) {
    const match = partition.match(PARTITION_NAME_RE);
    if (!match) continue; // defensive; the SQL filter above should already guarantee this

    const [, y, m, d] = match;
    const partitionDay = new Date(Date.UTC(Number(y), Number(m) - 1, Number(d)));
    if (partitionDay >= cutoff) continue; // still within the retention window

    await pgPool.query(`ALTER TABLE logs DETACH PARTITION ${partition} CONCURRENTLY`);
    await pgPool.query(`DROP TABLE IF EXISTS ${partition}`);
    dropped.push(partition);
  }

  return dropped;
}
