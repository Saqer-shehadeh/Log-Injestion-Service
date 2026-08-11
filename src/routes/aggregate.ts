import { FastifyPluginAsync } from 'fastify';
import { Pool } from 'pg';
import { isValidAttributeKey } from '../validation/log-validator';

const VALID_LEVELS = new Set(['debug', 'info', 'warn', 'error']);

export const aggregateRoutes = (pgPool: Pool): FastifyPluginAsync => async (fastify) => {
  fastify.get('/logs/aggregate', async (request, reply) => {
    const q = request.query as any;

    // --- Required parameter checks ---

    if (!q.since || !q.until || !q.bucket) {
      return reply.status(400).send({ error: 'Missing required parameters: since, until, bucket' });
    }

    if (isNaN(Date.parse(q.since))) {
      return reply.status(400).send({ error: 'Invalid since timestamp' });
    }

    if (isNaN(Date.parse(q.until))) {
      return reply.status(400).send({ error: 'Invalid until timestamp' });
    }

    if (Date.parse(q.until) < Date.parse(q.since)) {
      return reply.status(400).send({ error: 'until must not be earlier than since' });
    }

    if (q.level !== undefined && !VALID_LEVELS.has(q.level)) {
      return reply.status(400).send({ error: `Invalid level: '${q.level}'` });
    }

    const bucketMap: Record<string, string> = {
      '1m': '1 minute',
      '5m': '5 minutes',
      '1h': '1 hour',
      '1d': '1 day'
    };

    const interval = bucketMap[q.bucket];
    if (!interval) {
      return reply.status(400).send({ error: 'Invalid bucket size. Supported: 1m, 5m, 1h, 1d' });
    }

    // --- Build filters ---

    const conditions: string[] = [
      `timestamp >= $1`,
      `timestamp < $2`
    ];
    const values: any[] = [q.since, q.until];
    let paramIdx = 3;

    if (q.service) {
      conditions.push(`service = $${paramIdx++}`);
      values.push(q.service);
    }

    if (q.level) {
      conditions.push(`level = $${paramIdx++}`);
      values.push(q.level);
    }

    // attr.<key> filters (spec: same filters as GET /logs)
    for (const key of Object.keys(q)) {
      if (key.startsWith('attr.')) {
        const attrKey = key.slice(5);
        if (!isValidAttributeKey(attrKey)) {
          return reply.status(400).send({ error: `Invalid attribute key format: '${attrKey}'` });
        }
        conditions.push(`attributes->>'${attrKey}' = $${paramIdx++}`);
        values.push(q[key]);
      }
    }

    // Message search filter (spec: same filters as GET /logs)
    if (q.q) {
      conditions.push(`message ILIKE $${paramIdx++}`);
      values.push(`%${q.q}%`);
    }

    // --- Group by ---

    const groupByField = q.group_by;
    if (groupByField && groupByField !== 'service' && groupByField !== 'level') {
      return reply.status(400).send({ error: 'group_by must be either "service" or "level"' });
    }

    const groupSelect = groupByField ? `, ${groupByField} AS "group"` : ', NULL AS "group"';
    const groupByClause = groupByField ? `, ${groupByField}` : '';

    const query = `
      SELECT 
        date_bin('${interval}'::interval, timestamp, TIMESTAMP '2026-01-01') AS start
        ${groupSelect},
        COUNT(*)::int as count
      FROM logs
      WHERE ${conditions.join(' AND ')}
      GROUP BY start ${groupByClause}
      ORDER BY start ASC
    `;

    try {
      const res = await pgPool.query(query, values);
      return reply.status(200).send({ buckets: res.rows });
    } catch (err: any) {
      return reply.status(400).send({ error: err.message });
    }
  });
};