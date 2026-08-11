import { FastifyPluginAsync } from 'fastify';
import { Pool } from 'pg';
import { isValidAttributeKey } from '../validation/log-validator';

const VALID_LEVELS = new Set(['debug', 'info', 'warn', 'error']);

export const queryRoutes = (pgPool: Pool): FastifyPluginAsync => async (fastify) => {
  fastify.get('/logs', async (request, reply) => {
    const q = request.query as any;

    // --- Parameter validation (spec compliance) ---

    let limit = 50;
    if (q.limit !== undefined) {
      limit = parseInt(q.limit, 10);
      if (isNaN(limit) || limit < 1 || limit > 1000) {
        return reply.status(400).send({ error: 'Limit must be a number between 1 and 1000' });
      }
    }

    if (q.level !== undefined && !VALID_LEVELS.has(q.level)) {
      return reply.status(400).send({ error: `Invalid level: '${q.level}'` });
    }

    if (q.since !== undefined && isNaN(Date.parse(q.since))) {
      return reply.status(400).send({ error: 'Invalid since timestamp' });
    }

    if (q.until !== undefined && isNaN(Date.parse(q.until))) {
      return reply.status(400).send({ error: 'Invalid until timestamp' });
    }

    if (q.since && q.until && Date.parse(q.until) < Date.parse(q.since)) {
      return reply.status(400).send({ error: 'until must not be earlier than since' });
    }

    // --- Build query ---

    const conditions: string[] = [];
    const values: any[] = [];
    let paramIdx = 1;

    if (q.service) {
      conditions.push(`service = $${paramIdx++}`);
      values.push(q.service);
    }

    if (q.level) {
      conditions.push(`level = $${paramIdx++}`);
      values.push(q.level);
    }

    if (q.since) {
      conditions.push(`timestamp >= $${paramIdx++}`);
      values.push(q.since);
    }

    if (q.until) {
      conditions.push(`timestamp < $${paramIdx++}`);
      values.push(q.until);
    }

    if (q.q) {
      conditions.push(`message ILIKE $${paramIdx++}`);
      values.push(`%${q.q}%`);
    }

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

    if (q.cursor) {
      try {
        const decoded = Buffer.from(q.cursor, 'base64').toString('utf-8');
        const pipeIndex = decoded.lastIndexOf('|');
        if (pipeIndex === -1) {
          return reply.status(400).send({ error: 'Invalid cursor format' });
        }
        const cTimestamp = decoded.substring(0, pipeIndex);
        const cId = decoded.substring(pipeIndex + 1);
        if (isNaN(Date.parse(cTimestamp)) || isNaN(Number(cId))) {
          return reply.status(400).send({ error: 'Invalid cursor format' });
        }
        conditions.push(`(timestamp, id) < ($${paramIdx++}, $${paramIdx++})`);
        values.push(cTimestamp, cId);
      } catch (err) {
        return reply.status(400).send({ error: 'Invalid cursor format' });
      }
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    
    const query = `
      SELECT id, timestamp, level, service, message, attributes
      FROM logs
      ${whereClause}
      ORDER BY timestamp DESC, id DESC
      LIMIT $${paramIdx}
    `;
    values.push(limit + 1);

    try {
      const res = await pgPool.query(query, values);
      const rows = res.rows;
      let nextCursor: string | null = null;

      if (rows.length > limit) {
        const lastItem = rows.pop();
        const cursorVal = `${lastItem.timestamp.toISOString()}|${lastItem.id}`;
        nextCursor = Buffer.from(cursorVal).toString('base64');
      }

      return reply.status(200).send({ logs: rows, next_cursor: nextCursor });
    } catch (err: any) {
      return reply.status(400).send({ error: err.message });
    }
  });
};