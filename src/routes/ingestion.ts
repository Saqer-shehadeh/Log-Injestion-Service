import { FastifyPluginAsync } from 'fastify';
import { RingBuffer } from '../buffer/ring-buffer';
import { validateLogEntry } from '../validation/log-validator';

// Single-pass regex for COPY escape — 1 scan instead of 4
const COPY_ESCAPE_RE = /[\\\t\n\r]/g;
const COPY_ESCAPE_MAP: Record<string, string> = {
  '\\': '\\\\',
  '\t': '\\t',
  '\n': '\\n',
  '\r': '\\r',
};

function escapeCopyValue(value: string): string {
  return value.replace(COPY_ESCAPE_RE, (ch) => COPY_ESCAPE_MAP[ch]);
}


export const ingestionRoutes = (buffer: RingBuffer<string>): FastifyPluginAsync => async (fastify) => {
  let reqCount = 0;

  fastify.post('/logs', async (request, reply) => {
    const requestStart = performance.now();

    const body = request.body as any;

    if (!body || typeof body !== 'object' || !Array.isArray(body.logs)) {
      return reply.status(400).send({ error: 'Request body must be an object with a "logs" array' });
    }

    const logsArray = body.logs;
    if (logsArray.length === 0) {
      return reply.status(400).send({ error: 'Logs array cannot be empty' });
    }

    let accepted = 0;
    const rejected: Array<{ index: number; reason: string }> = [];

    const nowMs = Date.now();
    for (let i = 0; i < logsArray.length; i++) {
      const result = validateLogEntry(logsArray[i], nowMs);

      if (result.valid && result.log) {
        // Pre-serialize to TSV at ingestion time.
        // Distributes CPU work across many small requests
        // instead of concentrating it in one massive flush loop.
        const attrs = result.log.attributes;
        const row =
          escapeCopyValue(result.log.timestamp) + '\t' +
          result.log.level + '\t' +                           // levels are clean enum values, no escape needed
          escapeCopyValue(result.log.service) + '\t' +
          escapeCopyValue(result.log.message) + '\t' +
          escapeCopyValue(
            attrs && typeof attrs === 'object' && Object.keys(attrs).length > 0
              ? JSON.stringify(attrs)
              : '{}'
          ) + '\n';

        if (buffer.push(row)) {
          accepted++;
        } else {
          rejected.push({ index: i, reason: 'Buffer full (Backpressure active)' });
        }
      } else {
        rejected.push({ index: i, reason: result.reason || 'Invalid entry' });
      }
    }

    if (accepted === 0) {
      return reply.status(400).send({ accepted: 0, rejected });
    }

    // Sampled diagnostic logging: every 200 requests
    if (++reqCount % 200 === 0) {
      console.log({
        ingestionTime: `${(performance.now() - requestStart).toFixed(2)}ms`,
        accepted,
        rejected: rejected.length,
        bufferSize: buffer.size()
      });
    }

    return reply.status(200).send({ accepted, rejected });
  });
};