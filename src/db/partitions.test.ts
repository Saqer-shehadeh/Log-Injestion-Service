import test from 'node:test';
import assert from 'node:assert/strict';
import type { Pool } from 'pg';
import {
  partitionNameFor,
  ensureDailyPartition,
  ensureUpcomingPartitions,
  dropExpiredPartitions,
} from './partitions';

/** Records every query issued; returns canned rows for the pg_inherits lookup. */
class FakePgPool {
  public queries: { sql: string }[] = [];
  constructor(private selectRows: { partition: string }[] = []) {}
  async query(sql: string) {
    this.queries.push({ sql });
    if (sql.includes('pg_inherits')) {
      return { rows: this.selectRows };
    }
    return { rows: [] };
  }
}

test('partitionNameFor', async (t) => {
  await t.test('zero-pads single-digit month and day', () => {
    assert.strictEqual(partitionNameFor(new Date(Date.UTC(2026, 0, 5))), 'logs_y2026m01d05');
  });

  await t.test('formats a normal date', () => {
    assert.strictEqual(partitionNameFor(new Date(Date.UTC(2026, 7, 11))), 'logs_y2026m08d11');
  });
});

test('ensureDailyPartition', async (t) => {
  await t.test('issues one idempotent CREATE TABLE with the correct [start, start+1day) bounds', async () => {
    const pool = new FakePgPool();
    await ensureDailyPartition(pool as unknown as Pool, new Date(Date.UTC(2026, 7, 11)));

    assert.strictEqual(pool.queries.length, 1);
    const sql = pool.queries[0].sql;
    assert.ok(sql.includes('CREATE TABLE IF NOT EXISTS logs_y2026m08d11'));
    assert.ok(sql.includes('PARTITION OF logs'));
    assert.ok(sql.includes("FOR VALUES FROM ('2026-08-11T00:00:00.000Z')"));
    assert.ok(sql.includes("TO ('2026-08-12T00:00:00.000Z')"));
  });
});

test('ensureUpcomingPartitions', async (t) => {
  await t.test('creates today through aheadDays ahead, inclusive, in order', async () => {
    const pool = new FakePgPool();
    const now = new Date(Date.UTC(2026, 7, 11, 23, 59)); // late in the day, still Aug 11 UTC

    await ensureUpcomingPartitions(pool as unknown as Pool, 2, now);

    assert.strictEqual(pool.queries.length, 3); // today, +1, +2
    assert.ok(pool.queries[0].sql.includes('logs_y2026m08d11'));
    assert.ok(pool.queries[1].sql.includes('logs_y2026m08d12'));
    assert.ok(pool.queries[2].sql.includes('logs_y2026m08d13'));
  });

  await t.test('defaults to DEFAULT_PARTITION_AHEAD_DAYS when aheadDays is omitted', async () => {
    const pool = new FakePgPool();
    await ensureUpcomingPartitions(pool as unknown as Pool, undefined, new Date(Date.UTC(2026, 7, 11)));
    assert.strictEqual(pool.queries.length, 3); // today + 2 days ahead by default
  });
});

test('dropExpiredPartitions', async (t) => {
  await t.test('drops only partitions fully older than the retention window', async () => {
    // now = 2026-08-31 UTC (day boundary), retentionDays = 30 -> cutoff = 2026-08-01 UTC
    const now = new Date(Date.UTC(2026, 7, 31));
    const pool = new FakePgPool([
      { partition: 'logs_y2026m07d15' }, // well before cutoff -> drop
      { partition: 'logs_y2026m07d31' }, // day before cutoff -> drop
      { partition: 'logs_y2026m08d01' }, // exactly at cutoff -> keep
      { partition: 'logs_y2026m08d15' }, // within window -> keep
    ]);

    const dropped = await dropExpiredPartitions(pool as unknown as Pool, 30, now);

    assert.deepStrictEqual(dropped, ['logs_y2026m07d15', 'logs_y2026m07d31']);

    const detachCalls = pool.queries.filter((q) => q.sql.includes('DETACH PARTITION'));
    const dropCalls = pool.queries.filter((q) => q.sql.trim().startsWith('DROP TABLE'));
    assert.strictEqual(detachCalls.length, 2, 'expected exactly one DETACH per dropped partition');
    assert.strictEqual(dropCalls.length, 2, 'expected exactly one DROP TABLE per dropped partition');
    assert.ok(detachCalls[0].sql.includes('logs_y2026m07d15'));
    assert.ok(detachCalls[0].sql.includes('CONCURRENTLY'));
    assert.ok(dropCalls[0].sql.includes('logs_y2026m07d15'));

    // partitions inside the retention window are never referenced by any query
    assert.ok(!pool.queries.some((q) => q.sql.includes('logs_y2026m08d01')));
    assert.ok(!pool.queries.some((q) => q.sql.includes('logs_y2026m08d15')));
  });

  await t.test('never touches a table that is not this module\'s daily-partition naming pattern, even if returned', async () => {
    const now = new Date(Date.UTC(2026, 7, 31));
    const pool = new FakePgPool([
      { partition: 'logs_default' }, // must never be dropped by this function
      { partition: 'logs_y2026m07d01' },
    ]);

    const dropped = await dropExpiredPartitions(pool as unknown as Pool, 30, now);

    assert.deepStrictEqual(dropped, ['logs_y2026m07d01']);
    assert.ok(!pool.queries.some((q) => q.sql.includes('logs_default')));
  });

  await t.test('returns an empty list and issues no DETACH/DROP when nothing is expired', async () => {
    const now = new Date(Date.UTC(2026, 7, 5));
    const pool = new FakePgPool([{ partition: 'logs_y2026m08d01' }]);

    const dropped = await dropExpiredPartitions(pool as unknown as Pool, 30, now);

    assert.deepStrictEqual(dropped, []);
    assert.strictEqual(pool.queries.length, 1); // only the SELECT against pg_inherits
  });
});
