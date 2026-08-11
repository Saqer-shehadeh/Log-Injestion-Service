import { FastifyPluginAsync } from 'fastify';
import { Pool } from 'pg';

export const healthRoutes = (pgPool: Pool): FastifyPluginAsync => async (fastify) => {
  fastify.get('/health', async (request, reply) => {
    try {
      await pgPool.query('SELECT 1');
      return reply.status(200).send({ status: 'ok' });
    } catch (err) {
      return reply.status(500).send({ status: 'unhealthy', error: 'Database disconnected' });
    }
  });
};