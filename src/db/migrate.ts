import { Pool } from 'pg';

export async function initDb(pgPool: Pool) {
  // Apply DB-level performance settings
  try {
    await pgPool.query(`
      ALTER SYSTEM SET synchronous_commit = 'off';
      ALTER SYSTEM SET max_wal_size = '2GB';
      ALTER SYSTEM SET checkpoint_completion_target = '0.9';
      SELECT pg_reload_conf();
    `);
  } catch (e) {
    console.log('Could not apply ALTER SYSTEM settings:', e);
  }

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
  console.log('Database migrations applied successfully.');
}