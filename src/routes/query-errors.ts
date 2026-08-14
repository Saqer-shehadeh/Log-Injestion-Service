import { FastifyReply } from 'fastify';
import { InvalidQueryError } from '../db/log-repository';

/**
 * Maps an error raised while serving GET /logs or GET /logs/aggregate onto the
 * right HTTP status.
 *
 * Both handlers used to answer every failure with `400 { error: message }`.
 * That is correct for a malformed request and wrong for everything else: a
 * statement timeout, an exhausted connection pool, or an unreachable database
 * are all server-side conditions, and reporting them as 4xx tells the caller to
 * fix a request that was never the problem. It also mislabels the service's own
 * saturation as client error volume.
 *
 * The mapping:
 *
 *   InvalidQueryError            -> 400  the caller really did send bad input
 *   Postgres 57014 (canceled)    -> 503  statement_timeout fired; the query was
 *                                        cancelled server-side, so the caller
 *                                        may reasonably retry later
 *   pool acquire timeout         -> 503  every read connection is busy
 *   anything else                -> 500  a genuine, unclassified server fault
 *
 * 503 carries `Retry-After`, which is what the project spec designates for shed
 * load, and unlike a 400 it does not invite the caller to rewrite a valid query.
 */

/** Postgres SQLSTATE for a statement cancelled by statement_timeout. */
const PG_QUERY_CANCELED = '57014';

/** node-pg's message when `connectionTimeoutMillis` elapses waiting for a slot. */
const POOL_ACQUIRE_TIMEOUT = 'timeout exceeded when trying to connect';

export function respondToQueryError(reply: FastifyReply, err: unknown): FastifyReply {
  if (err instanceof InvalidQueryError) {
    return reply.status(400).send({ error: err.message });
  }

  const code = (err as { code?: string } | null)?.code;
  const message = err instanceof Error ? err.message : String(err);

  if (code === PG_QUERY_CANCELED) {
    return reply
      .status(503)
      .header('Retry-After', '1')
      .send({ error: 'Query exceeded the server time limit; please narrow the range and retry' });
  }

  if (message.includes(POOL_ACQUIRE_TIMEOUT)) {
    return reply
      .status(503)
      .header('Retry-After', '1')
      .send({ error: 'Query capacity is saturated; please retry' });
  }

  console.error('[query] unexpected error while serving request:', err);
  return reply.status(500).send({ error: 'Internal error while executing query' });
}
