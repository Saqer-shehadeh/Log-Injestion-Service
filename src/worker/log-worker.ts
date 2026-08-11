import { Pool, PoolClient } from 'pg';
import { from as copyFrom } from 'pg-copy-streams';
import { RingBuffer } from '../buffer/ring-buffer';

export interface ValidatedLog {
  timestamp: string;
  level: string;
  service: string;
  message: string;
  attributes: Record<string, any>;
}

export class LogWorker {
  private isRunning = false;
  private inFlight = 0;
  private flushCount = 0;
  private readonly bufferCapacity: number;
  private readonly maxConcurrent: number;

  constructor(
    private buffer: RingBuffer<string>,
    private pgPool: Pool,
    private baseBatchSize: number = 4000,
    private flushIntervalMs: number = 50,
    maxConcurrent: number = 1
  ) {
    // For correctness with peekBatch/drop, maxConcurrent is set to 1 to prevent
    // multiple in-flight flushes from peeking/dropping the same buffer region.
    this.maxConcurrent = 1;
    this.bufferCapacity = buffer.getCapacity();
  }

  public start() {
    this.isRunning = true;
    this.loop();
  }


  private async loop() {
    while (this.isRunning) {
      try {
        const size = this.buffer.size();

        if (size > 0 && this.inFlight < this.maxConcurrent) {
          const batchSize = this.getAdaptiveBatchSize(size);
          this.fireFlush(batchSize);
          await this.yieldTick();
        } else {
          await this.sleep(size > 0 ? 5 : this.flushIntervalMs);
        }
      } catch (err) {
        console.error('LogWorker loop error:', err);
        await this.sleep(50);
      }
    }
  }


  private getAdaptiveBatchSize(bufferSize: number): number {
    const occupancy = bufferSize / this.bufferCapacity;

    if (occupancy > 0.8) {
      return Math.min(bufferSize, 8000);
    }
    if (occupancy > 0.5) {
      return Math.min(bufferSize, 6000);
    }
    return Math.min(bufferSize, this.baseBatchSize);
  }


  private fireFlush(batchSize: number): void {
    // Correctness Invariant: Use peekBatch to inspect items without removing them.
    // Items remain in the RingBuffer until PostgreSQL COPY succeeds.
    const batch = this.buffer.peekBatch(batchSize);
    if (batch.length === 0) return;

    this.inFlight++;
    this.flushBatch(batch)
      .catch((err) => {
        console.error('Flush pipeline error:', err);
      })
      .finally(() => {
        this.inFlight--;
      });
  }


  private async flushBatch(batch: string[]): Promise<void> {
    const flushStart = performance.now();
    let client: PoolClient | null = null;

    try {
      client = await this.pgPool.connect();
      const stream = client.query(
        copyFrom(
          `COPY logs (timestamp, level, service, message, attributes) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')`
        )
      );

      await new Promise<void>((resolve, reject) => {
        stream.on('error', reject);
        stream.on('finish', resolve);

        // Send pre-serialized rows in a single write buffer
        stream.write(batch.join(''));
        stream.end();
      });

      // Correctness Invariant: Drop items from RingBuffer ONLY after successful COPY
      this.buffer.drop(batch.length);

      if (++this.flushCount % 10 === 0) {
        console.log({
          batchSize: batch.length,
          flushTime: `${(performance.now() - flushStart).toFixed(2)}ms`,
          remaining: this.buffer.size(),
          inFlight: this.inFlight
        });
      }

    } catch (err) {
      console.error('Flush pipeline error (batch retained in buffer for retry):', err);
      // Wait briefly on failure before retrying to prevent aggressive spinning during DB outages
      await this.sleep(100);
    } finally {
      if (client) client.release();
    }
  }


  private yieldTick(): Promise<void> {
    return new Promise((resolve) => setImmediate(resolve));
  }


  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }


  public stop() {
    this.isRunning = false;
  }
}