import { FastifyPluginAsync } from 'fastify';
import { Pool } from 'pg';
import { isValidAttributeKey } from '../validation/log-validator';
import { AttrFilter, queryLogs } from '../db/log-repository';
import { respondToQueryError } from './query-errors';

const VALID_LEVELS = new Set(['debug', 'info', 'warn', 'error']);
const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;

export const queryRoutes = (pgPool: Pool): FastifyPluginAsync => async (fastify) => {
  fastify.get('/logs', async (request, reply) => {
    const q = request.query as any;

    // --- Parameter validation (spec compliance) ---

    let limit = DEFAULT_LIMIT;
    if (q.limit !== undefined) {
      limit = parseInt(q.limit, 10);
      if (isNaN(limit) || limit < 1 || limit > MAX_LIMIT) {
        return reply.status(400).send({ error: `Limit must be a number between 1 and ${MAX_LIMIT}` });
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

    const attrFilters: AttrFilter[] = [];
    for (const key of Object.keys(q)) {
      if (key.startsWith('attr.')) {
        const attrKey = key.slice(5);
        if (!isValidAttributeKey(attrKey)) {
          return reply.status(400).send({ error: `Invalid attribute key format: '${attrKey}'` });
        }
        attrFilters.push({ key: attrKey, value: q[key] });
      }
    }

    let cursor: { timestamp: string; id: string } | undefined;
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
        cursor = { timestamp: cTimestamp, id: cId };
      } catch (err) {
        return reply.status(400).send({ error: 'Invalid cursor format' });
      }
    }

    // --- Query + persistence (delegated to db/log-repository.ts) ---

    try {
      const { rows, hasMore } = await queryLogs(pgPool, {
        service: q.service || undefined,
        level: q.level || undefined,
        since: q.since || undefined,
        until: q.until || undefined,
        q: q.q || undefined,
        attrFilters,
        cursor,
        limit,
      });

      let nextCursor: string | null = null;
      if (hasMore) {
        // Anchor the cursor to the last row actually returned in this
        // page, not to any probe row the repository fetched internally —
        // see queryLogs()'s doc comment for why that distinction matters.
        //
        // Uses cursor_ts (Postgres-rendered, microsecond precision) rather
        // than timestamp.toISOString() (JS Date, millisecond precision).
        // The truncation the latter caused silently dropped rows at page
        // boundaries — see LogRow.cursor_ts.
        const lastItem = rows[rows.length - 1];
        const cursorVal = `${lastItem.cursor_ts}|${lastItem.id}`;
        nextCursor = Buffer.from(cursorVal).toString('base64');
      }

      // cursor_ts is an internal pagination detail — strip it so the response
      // body keeps exactly the documented shape.
      const logs = rows.map(({ cursor_ts, ...row }) => row);

      return reply.status(200).send({ logs, next_cursor: nextCursor });
    } catch (err) {
      // Only a genuinely invalid query is the caller's fault; timeouts and pool
      // exhaustion are ours. See respondToQueryError.
      return respondToQueryError(reply, err);
    }
  });
};
