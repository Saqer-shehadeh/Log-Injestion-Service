import { FastifyPluginAsync } from 'fastify';
import { Pool } from 'pg';
import { isValidAttributeKey } from '../validation/log-validator';
import { AttrFilter, aggregateLogs } from '../db/log-repository';
import { respondToQueryError } from './query-errors';

const VALID_LEVELS = new Set(['debug', 'info', 'warn', 'error']);

const BUCKET_INTERVALS: Record<string, string> = {
  '1m': '1 minute',
  '5m': '5 minutes',
  '1h': '1 hour',
  '1d': '1 day',
};

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

    const bucketInterval = BUCKET_INTERVALS[q.bucket];
    if (!bucketInterval) {
      return reply.status(400).send({ error: 'Invalid bucket size. Supported: 1m, 5m, 1h, 1d' });
    }

    // --- Filters (spec: same filters as GET /logs) ---

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

    // --- Group by ---

    let groupBy: 'service' | 'level' | undefined;
    if (q.group_by) {
      if (q.group_by !== 'service' && q.group_by !== 'level') {
        return reply.status(400).send({ error: 'group_by must be either "service" or "level"' });
      }
      groupBy = q.group_by;
    }

    // --- Query + persistence (delegated to db/log-repository.ts) ---

    try {
      const buckets = await aggregateLogs(pgPool, {
        since: q.since,
        until: q.until,
        bucketInterval,
        service: q.service || undefined,
        level: q.level || undefined,
        q: q.q || undefined,
        attrFilters,
        groupBy,
      });
      return reply.status(200).send({ buckets });
    } catch (err) {
      // Only a genuinely invalid query is the caller's fault; timeouts and pool
      // exhaustion are ours. See respondToQueryError.
      return respondToQueryError(reply, err);
    }
  });
};
