import { Pool } from 'pg';

export function startRetentionTask(pgPool: Pool, retentionDays: number = 7, checkIntervalMs: number = 3600000) {
  setInterval(async () => {
    try {
      const cutoff = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000);
      await pgPool.query(`DELETE FROM logs WHERE timestamp < $1;`, [cutoff.toISOString()]);
      console.log(`[Retention] Cleaned up logs older than ${cutoff.toISOString()}`);
    } catch (err) {
      console.error('[Retention] Error during cleanup:', err);
    }
  }, checkIntervalMs);
}