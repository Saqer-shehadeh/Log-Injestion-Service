export class RingBuffer<T> {
  private buffer: Array<T | null>;
  private capacity: number;
  private writeIndex = 0;
  private readIndex = 0;
  private count = 0;

  constructor(capacity: number) {
    this.capacity = capacity;
    this.buffer = new Array(capacity).fill(null);
  }


  public push(item: T): boolean {
    if (this.count >= this.capacity) return false;

    this.buffer[this.writeIndex] = item;
    this.writeIndex = (this.writeIndex + 1) % this.capacity;
    this.count++;

    return true;
  }


  /**
   * Read items without removing them.
   * Optimized: uses Array.slice when no wrap-around (native V8 speed),
   * pre-allocates result array when wrapping.
   */
  public peekBatch(maxBatchSize: number): T[] {
    const batchCount = Math.min(this.count, maxBatchSize);
    if (batchCount === 0) return [];

    const end = this.readIndex + batchCount;

    if (end <= this.capacity) {
      // No wrap — native slice, single allocation
      return this.buffer.slice(this.readIndex, end) as T[];
    }

    // Wrap-around — pre-allocate exact size
    const items = new Array<T>(batchCount);
    const firstLen = this.capacity - this.readIndex;

    for (let i = 0; i < firstLen; i++) {
      items[i] = this.buffer[this.readIndex + i] as T;
    }
    const secondLen = batchCount - firstLen;
    for (let i = 0; i < secondLen; i++) {
      items[firstLen + i] = this.buffer[i] as T;
    }

    return items;
  }


  /**
   * Remove items after successful database insert.
   * Optimized: uses Array.fill(null) instead of per-element loop.
   */
  public drop(count: number): void {
    const removeCount = Math.min(count, this.count);
    if (removeCount === 0) return;

    const end = this.readIndex + removeCount;

    if (end <= this.capacity) {
      this.buffer.fill(null, this.readIndex, end);
    } else {
      // Wrap-around
      this.buffer.fill(null, this.readIndex, this.capacity);
      this.buffer.fill(null, 0, end - this.capacity);
    }

    this.readIndex = end % this.capacity;
    this.count -= removeCount;
  }


  /**
   * Atomically read and remove a batch.
   */
  public popBatch(maxBatchSize: number): T[] {
    const items = this.peekBatch(maxBatchSize);
    this.drop(items.length);
    return items;
  }


  public isFull(): boolean {
    return this.count >= this.capacity;
  }


  public size(): number {
    return this.count;
  }


  public getCapacity(): number {
    return this.capacity;
  }
}