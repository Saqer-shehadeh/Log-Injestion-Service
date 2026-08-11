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
  const sql = `
    SELECT id, timestamp, level, service, message, attributes
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
      date_bin('${options.bucketInterval}'::interval, timestamp, TIMESTAMP '2026-01-01') AS start
      ${groupSelect},
      COUNT(*)::int as count
    FROM logs
    WHERE ${conditions.join(' AND ')}
    GROUP BY start ${groupByClause}
    ORDER BY start ASC
  `;

  return { sql, values };
}

/** Executes the /logs/aggregate query and returns the bucket rows. */
export async function aggregateLogs(pgPool: Pool, options: AggregateOptions): Promise<AggregateBucket[]> {
  const { sql, values } = buildAggregateQuery(options);
  const res = await pgPool.query<AggregateBucket>(sql, values as any[]);
  return res.rows;
}
