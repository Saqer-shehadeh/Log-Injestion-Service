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

# --max-old-space-size: heap cap sits well below the 256MB container limit so
#   RSS (heap + Node/V8/libuv overhead + in-flight request buffers) stays clear
#   of the cgroup ceiling. It was 200MB, which let RSS reach ~224MB before V8
#   aborted with "Ineffective mark-compacts near heap limit" — the abort fired
#   first, so the container never showed as OOMKilled and the cause was easy to
#   miss. See src/index.ts for the ring-buffer sizing this has to accommodate.
# --v8-pool-size: same CPU-count reasoning as UV_THREADPOOL_SIZE above, for
#   V8's own background (GC/compiler) threads.
CMD ["node", "--max-old-space-size=160", "--v8-pool-size=1", "dist/index.js"]