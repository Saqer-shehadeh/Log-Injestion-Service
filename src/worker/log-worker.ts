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

  // Not configurable. peekBatch()/drop() share a single read cursor on the
  // RingBuffer, so two concurrent flushes would peek overlapping regions
  // and could drop() the wrong one, corrupting the buffer. Running more
  // than one flush at a time would require the RingBuffer to hand out
  // non-overlapping batch leases instead of a shared cursor — it doesn't
  // today, so this stays pinned at 1.
  private readonly maxConcurrent = 1;

  // Set once graceful shutdown begins. Checked once per poll iteration in the
  // main loop (negligible cost) — never checked on the request/ingestion hot
  // path, since ingestion only ever touches RingBuffer.push(), not this class.
  private shuttingDown = false;

  constructor(
    private buffer: RingBuffer<string>,
    private pgPool: Pool,
    private baseBatchSize: number = 4000,
    private flushIntervalMs: number = 50
  ) {
    this.bufferCapacity = buffer.getCapacity();
  }

  public start() {
    this.isRunning = true;
    this.loop();
  }


  private async loop() {
    while (this.isRunning) {
      try {
        // Once shutdown has begun, stop starting new flush cycles. Any flush
        // already in flight (started before shuttingDown flipped) is left to
        // finish on its own — shutdown() below waits for it via `inFlight`.
        if (this.shuttingDown) {
          await this.sleep(this.flushIntervalMs);
          continue;
        }

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


  /**
   * Graceful shutdown lifecycle. Unlike stop() (which just halts the loop
   * immediately), this:
   *   1. Stops the loop from starting any new flush ("no new flushes").
   *   2. Waits for a flush that was already in flight to finish its COPY.
   *   3. Drains whatever remains in the RingBuffer using the exact same
   *      safe peekBatch -> flushBatch -> drop sequence the normal flush
   *      path uses (flushBatch is reused as-is; drop() only ever runs
   *      after a successful COPY, same as always).
   *   4. Only then stops the loop for good.
   *
   * This does not touch flushBatch(), peekBatch(), or drop() — it just
   * drives the existing, already-correct primitives sequentially instead
   * of via the loop's fire-and-forget scheduling.
   */
  public async shutdown(): Promise<void> {
    // Step 1: prevent the loop from starting any further flushes.
    this.shuttingDown = true;

    // Step 2: wait for a flush already in flight (if any) to finish. It will
    // call drop() itself on success, or leave the batch in the buffer on
    // failure — same as normal operation.
    while (this.inFlight > 0) {
      await this.sleep(10);
    }

    // Step 3: drain remaining buffered entries sequentially. Each iteration
    // reuses flushBatch() as-is: peek -> COPY -> drop only on success. If a
    // flush fails (e.g. Postgres unreachable), flushBatch() already leaves
    // the batch in the buffer and backs off briefly before returning, so the
    // next iteration retries the same data — identical retry semantics to
    // the normal flush path. The caller (index.ts) bounds total shutdown
    // time with an overall timeout, so this loop cannot hang forever.
    while (this.buffer.size() > 0) {
      const batchSize = this.getAdaptiveBatchSize(this.buffer.size());
      const batch = this.buffer.peekBatch(batchSize);
      if (batch.length === 0) break;
      await this.flushBatch(batch);
    }

    // Step 4: fully stop the loop now that draining is complete.
    this.isRunning = false;
  }
}