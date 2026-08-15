FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM node:20-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
COPY package*.json ./
RUN npm ci --only=production
COPY --from=builder /app/dist ./dist

EXPOSE 8080

# libuv sizes its threadpool from the *host's* CPU count, not the cgroup quota,
# so in a 0.5-CPU container it would spin up threads this process can never
# afford to run — they just burn the CPU quota fighting each other. Same reason
# max_parallel_workers_per_gather is pinned to 0 for Postgres in
# docker-compose.yml: `nproc` reports 8, the quota is a fraction of one core.
ENV UV_THREADPOOL_SIZE=2

# These three flags are chosen together: V8 derives the young generation from
# the heap cap, so they cannot be reasoned about one at a time.
#
# --max-old-space-size: heap cap sits well below the 256MB container limit so
#   RSS (heap + Node/V8/libuv overhead + in-flight request buffers) stays clear
#   of the cgroup ceiling. It was 200MB, which let RSS reach ~224MB before V8
#   aborted with "Ineffective mark-compacts near heap limit" — the abort fired
#   first, so the container never showed as OOMKilled and the cause was easy to
#   miss. See src/index.ts for the ring-buffer sizing this has to accommodate.
#
# --max-semi-space-size: that abort fix had a second-order cost. V8 sizes the
#   young generation relative to the heap cap, so capping the old space also
#   shrank the semi-space to ~1MB (heap_size_limit reported 163MB = 160 + 3x1).
#   The ingest path allocates a COPY row string, a JSON.stringify(attributes)
#   string and a rollup Map key per row — ~43,000 short-lived strings/sec at
#   14,200 logs/sec, none surviving the request. Into a 1MB semi-space that is
#   a scavenge every 30ms, and the profiler put the garbage collector at 25.8%
#   of a 0.5-CPU budget the app had already saturated (95% used, while Postgres
#   sat at 30% of its own). Measured over 40s at 15,000 logs/sec:
#
#     old=160 (semi ~1MB)   1,346 scavenges   33.6/s
#     old=136 semi=8          173 scavenges    4.3/s
#     old=112 semi=16          96 scavenges    2.4/s
#
#   Across five paired runs with rotated order, semi=16 improved latency p95
#   (980ms -> 755ms), aggregate p95 (368ms -> 210ms) and throughput (13,794 ->
#   14,470 logs/sec), each 5/5 runs at p<0.05. Peak RSS 82.9MiB of 256MB.
#
#   Old space drops 160 -> 112 to hold the total heap limit at 160MB, BELOW the
#   previous 163MB. Keeping old=160 alongside semi=16 was measured too: same
#   score, same peak RSS, but it raises the limit to 208MB and walks back
#   toward the abort described above. The extra old space buys nothing.
#
# --v8-pool-size: same CPU-count reasoning as UV_THREADPOOL_SIZE above, for
#   V8's own background (GC/compiler) threads.
CMD ["node", "--max-old-space-size=112", "--max-semi-space-size=16", "--v8-pool-size=1", "dist/index.js"]