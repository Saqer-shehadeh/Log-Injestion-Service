import { Pool } from 'pg';
import { DEFAULT_PARTITION_AHEAD_DAYS, dropExpiredPartitions, ensureUpcomingPartitions } from '../db/partitions';

/**
 * Project spec: "Handle approximately 1,000,000 stored log records. Assume
 * those records represent approximately one month of data." 30 days is the
 * retention window that matches that sizing target
 */
export const DEFAULT_RETENTION_DAYS = 30;

const DEFAULT_CHECK_INTERVAL_MS = 60 * 60 * 1000; // hourly

export interface PartitionMaintenanceOptions {
  retentionDays?: number;
  aheadDays?: number;
  checkIntervalMs?: number;
}

export interface PartitionMaintenanceHandle {
  /** Stops the periodic timer. Idempotent — safe to call more than once. */
  stop: () => void;
}

/**
 * Background job that keeps the daily-partitioned `logs` table healthy:
 *
 *  - creates partitions for today + a few days ahead, so ingestion never
 *    silently falls back to the DEFAULT partition (see ensureUpcomingPartitions)
 *  - drops partitions entirely outside the retention window via
 *    DETACH ... CONCURRENTLY + DROP TABLE — no bulk DELETE, no long-lived
 *    locks on the live table, no dead-tuple/WAL bloat from a giant DELETE
 *    (see dropExpiredPartitions)
 *
 * `initDb` already creates the initial set of partitions synchronously
 * before the server starts accepting HTTP traffic; this job keeps that
 * topped up over time and performs the ongoing retention drop. Both steps
 * are independently try/caught — a transient failure in one (e.g. a
 * momentary DB hiccup) does not stop the other from running, and the next
 * scheduled run retries automatically.
 */
export function startPartitionMaintenance(
  pgPool: Pool,
  options: PartitionMaintenanceOptions = {}
): PartitionMaintenanceHandle {
  const retentionDays = options.retentionDays ?? DEFAULT_RETENTION_DAYS;
  const aheadDays = options.aheadDays ?? DEFAULT_PARTITION_AHEAD_DAYS;
  const checkIntervalMs = options.checkIntervalMs ?? DEFAULT_CHECK_INTERVAL_MS;

  async function run(): Promise<void> {
    try {
      await ensureUpcomingPartitions(pgPool, aheadDays);
    } catch (err) {
      console.error('[partitions] Failed to ensure upcoming partitions:', err);
    }

    try {
      const dropped = await dropExpiredPartitions(pgPool, retentionDays);
      if (dropped.length > 0) {
        console.log(`[retention] Dropped ${dropped.length} expired partition(s): ${dropped.join(', ')}`);
      }
    } catch (err) {
      console.error('[retention] Failed to drop expired partitions:', err);
    }
  }

  const timer = setInterval(() => {
    void run();
  }, checkIntervalMs);

  // Also run once immediately. `initDb` already ensured the initial
  // partitions exist, so this is mostly about performing an early
  // retention pass (e.g. after downtime) rather than duplicating startup
  // work — both operations are idempotent either way.
  void run();

  return {
    stop: () => clearInterval(timer),
  };
}
