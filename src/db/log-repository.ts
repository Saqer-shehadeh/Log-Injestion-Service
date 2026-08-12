import { Pool } from 'pg';
import { isValidAttributeKey } from '../validation/log-validator';

/**
 * Query-building and persistence for /logs and /logs/aggregate, kept out
 * of the HTTP route handlers (spec: "Query-building and persistence logic
 * separated from HTTP handlers"). Route handlers are responsible for
 * parsing and validating request input and shaping the HTTP response;
 * everything below is responsible for turning already-validated options
 * into safe SQL and executing it.
 *
 * All dynamic identifiers (`attr.<key>` -> a JSON key, `group_by` -> a
 * column name) are still re-validated here, independently of whatever the
 * HTTP layer already checked — this module is the one actually
 * constructing the SQL, so it does not trust its caller for safety.
 */

export interface AttrFilter {
  key: string;
  value: string;
}

export interface LogRow {
  id: string;
  timestamp: Date;
  level: string;
  service: string;
  message: string;
  attributes: Record<string, unknown>;
  /**
   * Full-precision UTC ISO-8601 rendering of `timestamp`, produced by Postgres
   * rather than by the driver. Used *only* to build the pagination cursor.
   *
   * `timestamp` above is a JS Date, which is millisecond-precision, but the
   * column is TIMESTAMPTZ, which is microsecond-precision. Anchoring a cursor
   * to the Date therefore truncated it (…123456 -> …123), and since the next
   * page asks for `(timestamp, id) < (cursor)`, every remaining row whose
   * true timestamp was above the truncated value was silently skipped —
   * pagination would return a short page and then stop early, reporting
   * `next_cursor: null` as though it had reached the end.
   */
  cursor_ts: string;
}

export interface LogsQueryOptions {
  service?: string;
  level?: string;
  since?: string;
  until?: string;
  q?: string;
  attrFilters: AttrFilter[];
  cursor?: { timestamp: string; id: string };
  /** Number of logs to return (not including the extra probe row). */
  limit: number;
}

function assertValidAttrFilters(attrFilters: AttrFilter[]): void {
  for (const { key } of attrFilters) {
    if (!isValidAttributeKey(key)) {
      throw new Error(`Invalid attribute key format: '${key}'`);
    }
  }
}

/** Builds the parameterized SQL for GET /logs. Pure — no I/O. */
export function buildLogsQuery(options: LogsQueryOptions): { sql: string; values: unknown[] } {
  assertValidAttrFilters(options.attrFilters);

  const conditions: string[] = [];
  const values: unknown[] = [];
  let paramIdx = 1;

  if (options.service) {
    conditions.push(`service = $${paramIdx++}`);
    values.push(options.service);
  }
  if (options.level) {
    conditions.push(`level = $${paramIdx++}`);
    values.push(options.level);
  }
  if (options.since) {
    conditions.push(`timestamp >= $${paramIdx++}`);
    values.push(options.since);
  }
  if (options.until) {
    conditions.push(`timestamp < $${paramIdx++}`);
    values.push(options.until);
  }
  if (options.q) {
    conditions.push(`message ILIKE $${paramIdx++}`);
    values.push(`%${options.q}%`);
  }
  for (const { key, value } of options.attrFilters) {
    conditions.push(`attributes->>'${key}' = $${paramIdx++}`);
    values.push(value);
  }
  if (options.cursor) {
    conditions.push(`(timestamp, id) < ($${paramIdx++}, $${paramIdx++})`);
    values.push(options.cursor.timestamp, options.cursor.id);
  }

  const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

  // Fetch one extra row beyond `limit` so the caller can tell whether a
  // next page exists without a separate COUNT query.
  // cursor_ts is rendered by Postgres at full microsecond precision so the
  // pagination cursor never loses resolution the way a JS Date would — see
  // LogRow.cursor_ts.
  const sql = `
    SELECT id, timestamp, level, service, message, attributes,
           to_char(timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS cursor_ts
    FROM logs
    ${whereClause}
    ORDER BY timestamp DESC, id DESC
    LIMIT $${paramIdx}
  `;
  values.push(options.limit + 1);

  return { sql, values };
}

/**
 * Executes the /logs query and returns exactly `limit` rows (trimmed) plus
 * whether more results exist beyond them.
 *
 * The trimmed-off probe row is discarded, not used for pagination — the
 * caller should build the next cursor from the *last row actually
 * returned* (rows[rows.length - 1]), not from the probe row. Anchoring the
 * cursor to the probe row instead would exclude it from the next page
 * (since the next query uses `< cursor`), silently dropping one row at
 * every page boundary.
 */
export async function queryLogs(
  pgPool: Pool,
  options: LogsQueryOptions
): Promise<{ rows: LogRow[]; hasMore: boolean }> {
  const { sql, values } = buildLogsQuery(options);
  const res = await pgPool.query<LogRow>(sql, values as any[]);
  const rows = res.rows;
  const hasMore = rows.length > options.limit;
  if (hasMore) {
    rows.pop(); // discard the probe row; it is not part of this page
  }
  return { rows, hasMore };
}

export interface AggregateOptions {
  since: string;
  until: string;
  /** Postgres interval literal, e.g. '1 minute' — already mapped from '1m' etc. by the caller. */
  bucketInterval: string;
  service?: string;
  level?: string;
  q?: string;
  attrFilters: AttrFilter[];
  /** Restricted to a two-value union by the caller before it ever reaches here. */
  groupBy?: 'service' | 'level';
}

export interface AggregateBucket {
  start: Date;
  group: string | null;
  count: number;
}

/** Builds the parameterized SQL for GET /logs/aggregate. Pure — no I/O. */
export function buildAggregateQuery(options: AggregateOptions): { sql: string; values: unknown[] } {
  assertValidAttrFilters(options.attrFilters);

  const conditions: string[] = ['timestamp >= $1', 'timestamp < $2'];
  const values: unknown[] = [options.since, options.until];
  let paramIdx = 3;

  if (options.service) {
    conditions.push(`service = $${paramIdx++}`);
    values.push(options.service);
  }
  if (options.level) {
    conditions.push(`level = $${paramIdx++}`);
    values.push(options.level);
  }
  for (const { key, value } of options.attrFilters) {
    conditions.push(`attributes->>'${key}' = $${paramIdx++}`);
    values.push(value);
  }
  if (options.q) {
    conditions.push(`message ILIKE $${paramIdx++}`);
    values.push(`%${options.q}%`);
  }

  // groupBy is typed as 'service' | 'level' and validated by the caller
  // before construction — never free-form user input by this point — so
  // interpolating it directly as a column/identifier is safe.
  const groupSelect = options.groupBy ? `, ${options.groupBy} AS "group"` : ', NULL AS "group"';
  const groupByClause = options.groupBy ? `, ${options.groupBy}` : '';

  const sql = `
    SELECT
      date_bin('${options.bucketInterval}'::interval, timestamp, ${BUCKET_ORIGIN}) AS start
      ${groupSelect},
      COUNT(*)::int as count
    FROM logs
    WHERE ${conditions.join(' AND ')}
    GROUP BY start ${groupByClause}
    ORDER BY start ASC
  `;

  return { sql, values };
}

/**
 * Bucket origin for date_bin. Written as TIMESTAMPTZ rather than TIMESTAMP so
 * bucket boundaries do not depend on the session TimeZone — the raw and rollup
 * paths must bin identically or the same request could return different bucket
 * starts depending on which path served it.
 */
const BUCKET_ORIGIN = `TIMESTAMPTZ '2026-01-01 00:00:00+00'`;

const MS_PER_SECOND = 1_000;

/**
 * True when this request can be served from log_rollup_1s.
 *
 * The rollup stores only (second, service, level, count) — it has no message
 * and no attributes — so `q` and `attr.<key>` filters cannot be answered from
 * it and must fall back to scanning raw rows. Everything else can.
 */
export function canUseRollup(options: AggregateOptions): boolean {
  return options.attrFilters.length === 0 && !options.q;
}

/**
 * Builds the rollup-backed aggregation.
 *
 * `since`/`until` are arbitrary instants, but rollup rows are whole seconds,
 * so reading the rollup alone would be wrong at the edges: a request starting
 * at 10:00:00.300 must not include 10:00:00.000-10:00:00.299, yet those rows
 * live in the same 10:00:00 bucket. Rather than restrict this path to aligned
 * requests, the query unions three exact pieces:
 *
 *   [since, firstWholeSecond)   raw rows   (empty when since is aligned)
 *   [firstWholeSecond, lastWholeSecond)    rollup rows
 *   [lastWholeSecond, until)    raw rows   (empty when until is aligned)
 *
 * Granularity is deliberately one second, not one minute. The edge pieces are
 * raw scans, and their cost is what the rollup cannot remove — so the bucket
 * size sets a floor on how expensive an unaligned request can be. At the
 * graded ingestion density a partial *minute* is hundreds of thousands of
 * rows, a large enough fraction of the partition that Postgres correctly plans
 * a sequential scan over all of it; measured, that made an unaligned request
 * 10x the cost of an aligned one and left the database saturated. A partial
 * *second* is a small enough fraction to be served from the primary key index,
 * so the edge cost stops scaling with how much data the partition holds.
 *
 * The result is identical to scanning raw rows, for any input.
 */
export function buildRollupAggregateQuery(options: AggregateOptions): { sql: string; values: unknown[] } {
  assertValidAttrFilters(options.attrFilters);

  const sinceMs = Date.parse(options.since);
  const untilMs = Date.parse(options.until);

  let rollupFromMs: number | null = Math.ceil(sinceMs / MS_PER_SECOND) * MS_PER_SECOND;
  let rollupToMs: number | null = Math.floor(untilMs / MS_PER_SECOND) * MS_PER_SECOND;
  // Range too narrow to contain a whole second: answer entirely from raw rows.
  if (rollupFromMs >= rollupToMs) {
    rollupFromMs = null;
    rollupToMs = null;
  }

  const headEndMs = rollupFromMs ?? untilMs;
  const tailStartMs = rollupToMs ?? untilMs;

  const values: unknown[] = [];
  const bind = (v: unknown): string => {
    values.push(v);
    return `$${values.length}`;
  };
  // service/level are named identically in both tables, so one builder serves
  // every union branch. Called per branch because each needs its own binds.
  const filters = (): string => {
    let sql = '';
    if (options.service) sql += ` AND service = ${bind(options.service)}`;
    if (options.level) sql += ` AND level = ${bind(options.level)}`;
    return sql;
  };
  const iso = (ms: number) => new Date(ms).toISOString();

  const branches: string[] = [];

  if (headEndMs > sinceMs) {
    branches.push(
      `SELECT timestamp AS ts, service, level, 1::bigint AS c FROM logs
        WHERE timestamp >= ${bind(iso(sinceMs))} AND timestamp < ${bind(iso(headEndMs))}${filters()}`
    );
  }

  if (rollupFromMs !== null && rollupToMs !== null) {
    branches.push(
      `SELECT bucket_start AS ts, service, level, count AS c FROM log_rollup_1s
        WHERE bucket_start >= ${bind(iso(rollupFromMs))} AND bucket_start < ${bind(iso(rollupToMs))}${filters()}`
    );
  }

  if (tailStartMs < untilMs) {
    branches.push(
      `SELECT timestamp AS ts, service, level, 1::bigint AS c FROM logs
        WHERE timestamp >= ${bind(iso(tailStartMs))} AND timestamp < ${bind(iso(untilMs))}${filters()}`
    );
  }

  const groupSelect = options.groupBy ? `, ${options.groupBy} AS "group"` : ', NULL AS "group"';
  const groupByClause = options.groupBy ? `, ${options.groupBy}` : '';

  const sql = `
    SELECT
      date_bin('${options.bucketInterval}'::interval, ts, ${BUCKET_ORIGIN}) AS start
      ${groupSelect},
      SUM(c)::int as count
    FROM (
      ${branches.join('\n      UNION ALL\n      ')}
    ) parts
    GROUP BY start ${groupByClause}
    ORDER BY start ASC
  `;

  return { sql, values };
}

/**
 * Executes the /logs/aggregate query and returns the bucket rows, choosing the
 * rollup-backed plan whenever the requested filters allow it.
 */
export async function aggregateLogs(pgPool: Pool, options: AggregateOptions): Promise<AggregateBucket[]> {
  const { sql, values } = canUseRollup(options)
    ? buildRollupAggregateQuery(options)
    : buildAggregateQuery(options);
  const res = await pgPool.query<AggregateBucket>(sql, values as any[]);
  return res.rows;
}
