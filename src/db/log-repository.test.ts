import test from 'node:test';
import assert from 'node:assert/strict';
import type { Pool } from 'pg';
import { buildLogsQuery, buildAggregateQuery, queryLogs, aggregateLogs } from './log-repository';

class FakePgPool {
  public lastSql = '';
  public lastValues: unknown[] = [];
  constructor(private rows: any[] = []) {}
  async query(sql: string, values: unknown[]) {
    this.lastSql = sql;
    this.lastValues = values;
    return { rows: this.rows };
  }
}

test('buildLogsQuery', async (t) => {
  await t.test('no filters: only ORDER BY + LIMIT, limit+1 as the sole parameter', () => {
    const { sql, values } = buildLogsQuery({ attrFilters: [], limit: 100 });
    assert.ok(!sql.includes('WHERE'));
    assert.ok(sql.includes('ORDER BY timestamp DESC, id DESC'));
    assert.deepStrictEqual(values, [101]);
  });

  await t.test('combines service, level, since, until, q, attr filters, and cursor in $-order', () => {
    const { sql, values } = buildLogsQuery({
      service: 'checkout',
      level: 'error',
      since: '2026-08-01T00:00:00Z',
      until: '2026-08-02T00:00:00Z',
      q: 'declined',
      attrFilters: [{ key: 'user_id', value: '42' }],
      cursor: { timestamp: '2026-08-01T12:00:00Z', id: '500' },
      limit: 50,
    });

    assert.ok(sql.includes('service = $1'));
    assert.ok(sql.includes('level = $2'));
    assert.ok(sql.includes('timestamp >= $3'));
    assert.ok(sql.includes('timestamp < $4'));
    assert.ok(sql.includes('message ILIKE $5'));
    assert.ok(sql.includes("attributes->>'user_id' = $6"));
    assert.ok(sql.includes('(timestamp, id) < ($7, $8)'));
    assert.deepStrictEqual(values, [
      'checkout',
      'error',
      '2026-08-01T00:00:00Z',
      '2026-08-02T00:00:00Z',
      '%declined%',
      '42',
      '2026-08-01T12:00:00Z',
      '500',
      51,
    ]);
  });

  await t.test('throws on an invalid attribute key instead of building unsafe SQL', () => {
    assert.throws(
      () => buildLogsQuery({ attrFilters: [{ key: "foo' OR 1=1 --", value: 'x' }], limit: 100 }),
      /Invalid attribute key format/
    );
  });
});

test('queryLogs pagination', async (t) => {
  await t.test('hasMore=false and no trimming when fewer than limit+1 rows come back', async () => {
    const rows = [{ id: '1' }, { id: '2' }];
    const pool = new FakePgPool(rows);
    const result = await queryLogs(pool as unknown as Pool, { attrFilters: [], limit: 10 });
    assert.strictEqual(result.hasMore, false);
    assert.strictEqual(result.rows.length, 2);
  });

  await t.test(
    'hasMore=true trims to exactly `limit` rows, and the last row is the last *returned* row — not the discarded probe row',
    async () => {
      // Simulate the DB returning limit(=3)+1 rows, ordered as the real query would be.
      const rows = [{ id: '10' }, { id: '9' }, { id: '8' }, { id: '7' }]; // '7' is the probe row
      const pool = new FakePgPool(rows);

      const result = await queryLogs(pool as unknown as Pool, { attrFilters: [], limit: 3 });

      assert.strictEqual(result.hasMore, true);
      assert.deepStrictEqual(
        result.rows.map((r: any) => r.id),
        ['10', '9', '8']
      );
      // The row a caller would anchor next_cursor to (rows[rows.length-1])
      // must be '8' (the last row actually returned), never '7' (the
      // probe row) — anchoring to '7' would cause it to be skipped
      // entirely on the next page, since the next query excludes rows
      // equal to the cursor.
      assert.strictEqual(result.rows[result.rows.length - 1].id, '8');
    }
  );
});

test('buildAggregateQuery', async (t) => {
  await t.test('since/until are always $1/$2; group is NULL when group_by is omitted', () => {
    const { sql, values } = buildAggregateQuery({
      since: '2026-08-01T00:00:00Z',
      until: '2026-08-02T00:00:00Z',
      bucketInterval: '1 hour',
      attrFilters: [],
    });

    assert.ok(sql.includes('timestamp >= $1'));
    assert.ok(sql.includes('timestamp < $2'));
    assert.ok(sql.includes(', NULL AS "group"'));
    assert.deepStrictEqual(values, ['2026-08-01T00:00:00Z', '2026-08-02T00:00:00Z']);
  });

  await t.test('group_by adds the column to SELECT and GROUP BY', () => {
    const { sql } = buildAggregateQuery({
      since: '2026-08-01T00:00:00Z',
      until: '2026-08-02T00:00:00Z',
      bucketInterval: '1 minute',
      attrFilters: [],
      groupBy: 'service',
    });

    assert.ok(sql.includes(', service AS "group"'));
    assert.ok(sql.includes('GROUP BY start , service'));
  });

  await t.test('attr filters and q append after since/until, in order', () => {
    const { sql, values } = buildAggregateQuery({
      since: '2026-08-01T00:00:00Z',
      until: '2026-08-02T00:00:00Z',
      bucketInterval: '1 day',
      attrFilters: [{ key: 'region', value: 'eu-west' }],
      q: 'declined',
    });

    assert.ok(sql.includes("attributes->>'region' = $3"));
    assert.ok(sql.includes('message ILIKE $4'));
    assert.deepStrictEqual(values, [
      '2026-08-01T00:00:00Z',
      '2026-08-02T00:00:00Z',
      'eu-west',
      '%declined%',
    ]);
  });

  await t.test('throws on an invalid attribute key instead of building unsafe SQL', () => {
    assert.throws(
      () =>
        buildAggregateQuery({
          since: '2026-08-01T00:00:00Z',
          until: '2026-08-02T00:00:00Z',
          bucketInterval: '1 minute',
          attrFilters: [{ key: "foo' OR 1=1 --", value: 'x' }],
        }),
      /Invalid attribute key format/
    );
  });
});

test('aggregateLogs', async (t) => {
  await t.test('returns whatever rows the pool query resolves with', async () => {
    const rows = [{ start: new Date('2026-08-01T00:00:00Z'), group: null, count: 5 }];
    const pool = new FakePgPool(rows);
    const result = await aggregateLogs(pool as unknown as Pool, {
      since: '2026-08-01T00:00:00Z',
      until: '2026-08-02T00:00:00Z',
      bucketInterval: '1 hour',
      attrFilters: [],
    });
    assert.deepStrictEqual(result, rows);
  });
});
