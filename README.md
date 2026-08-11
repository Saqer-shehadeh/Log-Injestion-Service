# Log Ingestion and Query Service

A log ingestion, query, and aggregation service — a simplified Datadog/Grafana Loki — built with Node.js, Fastify, TypeScript, and PostgreSQL. Targets **15,000+ logs/sec** sustained ingestion under **0.5 CPU / 256 MB RAM** (application) and **1 CPU / 1 GB RAM** (PostgreSQL).

## Contents

- [Setup and Usage](#setup-and-usage)
- [API Documentation](#api-documentation)
- [Optional Features](#optional-features)
- [Architecture](#architecture)
- [Schema and Index Design](#schema-and-index-design)
- [Attribute Storage Strategy](#attribute-storage-strategy)
- [Retention Strategy](#retention-strategy)
- [CI](#ci)
- [Load-Test Methodology and Measured Performance](#load-test-methodology-and-measured-performance)
- [Known Limitations](#known-limitations)

---

## Setup and Usage

### Prerequisites

- Docker and Docker Compose (the `docker compose` CLI plugin).

### Starting the service

```bash
docker compose up --build
```

This is the entire setup — no `.env` file, flags, or manual steps required. On startup the app container waits for Postgres, applies schema migrations and creates the initial set of daily partitions, then starts listening on `http://localhost:8080`. `GET /health` only returns `200` once all of that has completed.

```bash
curl -i http://localhost:8080/health
```

### Configuration (all optional)

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8080` | HTTP port the app listens on. |
| `DATABASE_URL` | `postgres://loguser:logpass@postgres:5432/logdb` | Postgres connection string. |
| `RETENTION_DAYS` | `30` | How many days of logs to retain before their partition is dropped (see [Retention Strategy](#retention-strategy)). Matches the project spec's "~1,000,000 rows ≈ one month of data" sizing target. |

None of these need to be set — `docker compose up` with no configuration yields the fully working service.

### Running tests

```bash
npm test              # typecheck + unit tests (fast, no Docker required)
npm run typecheck      # tsc --noEmit only
npm run test:unit      # unit tests only (src/**/*.test.ts, via node --test)
npm run test:contract  # docker compose up + hits all 4 required endpoints, then tears down
```

### Running the load benchmark

```bash
npm run benchmark   # against an already-running stack (docker compose up first)
npm run loadtest     # full teardown + rebuild + docker compose up + benchmark, one command
```

---

## API Documentation

### `GET /health`

Returns `200` with a small JSON body once the database connection is established, migrations are applied, and the service is ready to accept logs. Used by the load generator to know when to start.

### `POST /logs` — Ingest logs

Accepts a batch (a batch of one is valid):

```json
{
  "logs": [
    {
      "timestamp": "2026-07-20T14:32:01.123Z",
      "level": "error",
      "service": "checkout",
      "message": "payment declined",
      "attributes": { "user_id": "42", "region": "eu-west", "retries": 3 }
    }
  ]
}
```

**Validation** (per entry — one invalid entry never fails the batch):

| Field | Rule |
|---|---|
| `timestamp` | Required, valid ISO-8601, not more than 5 minutes in the future |
| `level` | Required, one of `debug`, `info`, `warn`, `error` |
| `service` | Required, non-empty string |
| `message` | Required, non-empty string |
| `attributes` | Optional, flat object; values must be string/number/boolean; nested objects and arrays are rejected |

**Response** — `200` when at least one entry is accepted:

```json
{ "accepted": 9, "rejected": [{ "index": 3, "reason": "Invalid level: 'critical'" }] }
```

`400` when every entry is rejected, the body isn't valid JSON, or the top-level shape isn't `{ "logs": [...] }`. Every error response (including framework-level ones like malformed JSON) uses `{"error": "<description>"}`.

Ingestion returns as soon as accepted entries are durably queued in the in-memory ring buffer — not after they've been `COPY`'d into Postgres. See [Known Limitations](#known-limitations) for what that trades off.

### `GET /logs` — Query logs

All parameters are optional and freely combinable:

| Parameter | Meaning |
|---|---|
| `service` | Exact match |
| `level` | Exact match |
| `since` | Inclusive start of time range |
| `until` | Exclusive end of time range |
| `attr.<key>` | Attribute equality, compared as strings (e.g. `attr.user_id=42`) |
| `q` | Case-insensitive substring match on `message` |
| `limit` | Default **100**, max **1000** |
| `cursor` | Opaque cursor from a previous response's `next_cursor` |

Results are sorted `timestamp DESC`, tie-broken by `id DESC` for a deterministic order.

```json
{
  "logs": [
    { "id": "109800", "timestamp": "2026-07-20T14:32:01.123Z", "level": "error", "service": "checkout", "message": "payment declined", "attributes": { "user_id": "42" } }
  ],
  "next_cursor": "MjAyNi0wOC0wMVQyMDowMTo0NS44ODRafDEwOTc5NQ=="
}
```

`next_cursor` is `null` when there are no more results. `400 {"error": "<description>"}` on invalid timestamps, `until` earlier than `since`, an unsupported level, a non-numeric or out-of-range `limit`, or a malformed cursor.

### `GET /logs/aggregate` — Aggregate logs

Supports the same filters as `GET /logs` (`service`, `level`, `attr.<key>`, `q`), plus:

| Parameter | Required | Meaning |
|---|---|---|
| `since` | Yes | Inclusive start |
| `until` | Yes | Exclusive end |
| `bucket` | Yes | `1m`, `5m`, `1h`, or `1d` |
| `group_by` | No | `service` or `level` |

```json
{
  "buckets": [
    { "start": "2026-07-20T14:00:00Z", "group": "checkout", "count": 118 },
    { "start": "2026-07-20T14:00:00Z", "group": "auth", "count": 42 }
  ]
}
```

One row per bucket/group combination, ordered by `start` ascending, `group: null` when `group_by` is omitted. Empty buckets are omitted. `400` on any invalid or missing required parameter, same error format as `GET /logs`.

---

## Optional Features

**None are implemented.** No authentication/API keys, no multi-tenancy, no rate limiting.

`docker compose up` with no environment file, flags, or manual setup produces the complete, unauthenticated core service: all four required endpoints reachable immediately, no credentials required, no rate limit or quota applied. There is no `AUTH_ENABLED` variable to set — that entire code path doesn't exist in this build. This is intentional: the project brief prioritizes a reliable, performant core over incomplete extras, and per the spec's own rule, since no optional features are implemented, only the single zero-config, unauthenticated configuration needs to work — which is what's graded and what CI validates (see [CI](#ci)).

---

## Architecture

```
POST /logs
  → validateLogEntry() (per-entry validation)
  → serialize accepted entries to COPY-ready TSV (src/routes/ingestion.ts)
  → RingBuffer<string>.push()            ← HTTP response returned here
  → [async] LogWorker polls the buffer, adaptively batches (4,000–8,000 rows)
  → peekBatch() → COPY logs FROM STDIN (pg-copy-streams) → drop() only on success
  → row is now committed and immediately visible to GET /logs and GET /logs/aggregate
```

Ingestion and persistence are deliberately decoupled by the ring buffer: the HTTP handler never waits on Postgres, only on an in-memory array push. That's what makes sustained high throughput possible inside a 0.5 CPU app container. The worker never removes anything from the buffer until the corresponding `COPY` has actually succeeded (`peekBatch` → `COPY` → `drop`, never `pop`-before-persist), and a failed `COPY` leaves the batch in place to retry rather than losing it. Concurrency is intentionally capped at one in-flight `COPY` — the buffer uses a single shared read cursor, so running two flushes at once could race on which one advances it.

Query-building and execution for `/logs` and `/logs/aggregate` live in `src/db/log-repository.ts`, kept separate from the Fastify route handlers (`src/routes/query.ts`, `src/routes/aggregate.ts`), which are only responsible for parsing/validating request input and shaping the HTTP response.

On `SIGTERM`/`SIGINT` (`src/shutdown/graceful-shutdown.ts`): stop accepting new HTTP connections → stop the worker from starting new flushes → wait for any in-flight `COPY` to finish → drain whatever remains in the buffer through the same safe `peekBatch`/`COPY`/`drop` sequence → close the Postgres pool → exit. The whole sequence is bounded by a 10-second timeout so a stuck shutdown (e.g. Postgres never coming back) can't hang the process forever.

---

## Schema and Index Design

```sql
CREATE TABLE logs (
    id BIGSERIAL,
    timestamp TIMESTAMPTZ NOT NULL,
    level VARCHAR(10) NOT NULL,
    service VARCHAR(100) NOT NULL,
    message TEXT NOT NULL,
    attributes JSONB DEFAULT '{}'::jsonb,
    PRIMARY KEY (timestamp, id)
) PARTITION BY RANGE (timestamp);
```

The table is range-partitioned by `timestamp` into one partition per UTC day (`logs_yYYYYmMMdDD`, e.g. `logs_y2026m08d11`), plus a `DEFAULT` partition (`logs_default`) that exists only as a safety net — see [Retention Strategy](#retention-strategy) for why daily partitions specifically, and how they're managed.

Two indexes, declared on the partitioned parent so every partition (including ones created after the fact) automatically gets a matching local index:

- **`idx_logs_query (service, level, timestamp DESC)`** — serves `service`/`level`-filtered queries and the `group_by=service|level` aggregation path.
- **`idx_logs_timestamp (timestamp DESC)`** — serves time-range-only queries and backs the `(timestamp, id)` cursor comparison used for pagination.

**No GIN index on `attributes`.** A GIN index would make `attr.<key>` lookups index-assisted, but it adds real per-row index-maintenance cost to every `COPY`, paid on every single ingested row on a 1-CPU Postgres instance — directly opposed to the 15k+/sec ingestion target. Given the actual graded scale (~1,000,000 rows across ~30 days, i.e. tens of thousands of rows/day per partition — see [Retention Strategy](#retention-strategy)), a sequential per-row evaluation of `attributes->>'key'` over an already time/service/level-narrowed, partition-pruned result set is fast enough without it. `GET /logs/aggregate` always requires `since`/`until`, so its attribute/message filters are always partition-pruned first; `GET /logs` does not require a time range (per spec, all its filters are optional), so an unbounded `attr.<key>`-only or `q`-only query on that endpoint is the one case that scans across all live partitions — see [Known Limitations](#known-limitations).

---

## Attribute Storage Strategy

Attributes are stored as a single `JSONB` column rather than a separate EAV (entity-attribute-value) table or per-key columns.

- **Why JSONB over EAV:** the log entry's attribute set is arbitrary and per-service — an EAV table (`log_id, key, value`) would multiply row count by the average attribute count on every single ingested log, which directly fights the 15k+/sec `COPY` throughput target. A single JSONB column keeps ingestion to exactly one row per log entry.
- **Ingestion:** attributes are validated (flat object, string/number/boolean values only, no nesting/arrays) during per-entry validation, then serialized to a JSON string as part of the same COPY-ready TSV row built at request time — no extra round-trip or separate write.
- **Querying:** `attr.<key>=value` is translated to `attributes->>'<key>' = $value`. The `->>` operator extracts the value as `text`, matching the spec's requirement that attribute filters be "compared as strings" regardless of whether the original value was a string, number, or boolean.
- **Safety:** `<key>` becomes part of the SQL expression itself (a JSON key), not a bound parameter — Postgres has no way to parameterize a JSON key name. It's validated against an allow-list pattern (`^[a-zA-Z0-9_.-]+$`) before ever being interpolated, both in the HTTP layer and again, independently, in `src/db/log-repository.ts` (the layer that actually builds the SQL). `<value>` is always a bound parameter. Covered by `src/routes/attr-injection.test.ts`.
- **Trade-off:** no GIN index (see [Schema and Index Design](#schema-and-index-design)) — attribute lookups are sequential-scan-per-row rather than index-assisted, offset by partition pruning and the `service`/`level` composite index narrowing the row set first.

---

## Retention Strategy

Retention is **partition-drop-based**, not row-`DELETE`-based, specifically to satisfy "expired-data deletion without long-running locks, excessive table bloat, or major ingestion disruption" while ingestion is actively running.

- The `logs` table is partitioned by day (`logs_yYYYYmMMdDD`). `src/db/partitions.ts` creates each day's partition ahead of time — on every boot (`initDb`, synchronously, before the server starts accepting requests) and then hourly (`src/retention/retention-cron.ts`) — keeping "today" plus the next 2 days always pre-created, comfortably covering the 5-minute future-skew window ingestion validation allows.
- The same hourly job drops any partition entirely older than `RETENTION_DAYS` (default **30**, matching the spec's "~1,000,000 rows ≈ one month" sizing) via `ALTER TABLE logs DETACH PARTITION ... CONCURRENTLY` followed by `DROP TABLE`. `DETACH ... CONCURRENTLY` avoids taking a long-lived lock on the live `logs` table while `COPY`s and queries are in flight; the subsequent drop only touches the already-detached, now-standalone table.
- This is a deliberate alternative to `DELETE FROM logs WHERE timestamp < cutoff`, which would generate WAL, dead tuples requiring autovacuum, and lock contention proportional to however much data has accumulated — all real costs on a 1 CPU / 1 GB Postgres instance, and exactly the kind of thing that would show up as ingestion disruption during a retention pass.
- Retention granularity is whole partitions (days), not to-the-second — a partition is dropped once its entire range falls outside the window, so retained data can be up to ~1 day older than the exact `RETENTION_DAYS` boundary at the edge.
- The `DEFAULT` partition (`logs_default`) is never touched by retention or considered for dropping — it exists purely as a safety net for any timestamp outside the pre-created range and should stay empty in normal operation.

---

## CI

`.github/workflows/ci.yml` runs on every push/PR:

1. **Build, typecheck, unit tests** — `npm ci`, `tsc --noEmit`, `npm run build` (verifies the production build — test files are excluded from `dist/` via `tsconfig.json`), then `npm run test:unit` (`src/**/*.test.ts` via Node's built-in test runner).
2. **Contract smoke test** (`scripts/contract-smoke-test.sh`, gated on job 1 passing) — `docker compose up` with zero configuration, waits for `/health`, then exercises all four required endpoints: a mixed valid/invalid `POST /logs` batch, a malformed-JSON `POST /logs`, `GET /logs` (success + an invalid-parameter case), and `GET /logs/aggregate` (success + a missing-required-parameter case). Tears the stack down afterward regardless of outcome.

No optional features are implemented (see [Optional Features](#optional-features)), so per the spec's own carve-out only this single unauthenticated configuration needs to be validated in CI.

---

## Load-Test Methodology and Measured Performance

**Methodology:**

- `benchmark.js` (autocannon) drives `POST /logs` at a fixed configuration — 1 connection, 1 pipelining, 50 logs/request, 60s duration — and reports `logs/sec` derived from `HTTP requests/sec × 50`, plus autocannon's latency percentiles, error/timeout/non-2xx counts.
- `benchmark-query.js` separately polls `GET /logs/aggregate` once per second, printing per-request latency, for exercising the "1 aggregation request/sec during ingestion" and "<1s p95" targets.
- Run either against a stack already started with `docker compose up`, or use `npm run loadtest` for a one-command teardown/rebuild/benchmark cycle.

**Numbers:** this run was prepared in a sandboxed environment without Docker available, so the figures below have **not** been re-measured against the current build (partitioned schema, retention-drop, `limit=100` default, the repository refactor, etc.) and are intentionally left as a template rather than restated as fact. Fill this in from your own run before submitting:

| Metric | Result |
|---|---|
| Test environment (CPU/RAM limits, OS) | _run `docker compose up` with the limits in `docker-compose.yml`, record host details_ |
| Dataset size at test time | _row count from `SELECT count(*) FROM logs;`_ |
| Batch size | 50 logs/request (`benchmark.js`'s `LOGS_PER_REQUEST`) |
| Ingestion rate | _logs/sec from `npm run benchmark`_ |
| Query rate | _requests/sec from `benchmark-query.js`_ |
| Query latency (p50 / p95 / p99) | _from `benchmark-query.js` output_ |
| Resource usage during test | _`docker stats` output for both containers_ |
| Bottlenecks discovered | _fill in_ |
| Optimizations applied | pre-serialization to TSV at ingestion time; single-chunk `COPY` streaming (`batch.join('')`); adaptive batch sizing (4,000 baseline, up to 8,000 under high occupancy); daily partitioning + partition-drop retention instead of bulk `DELETE` |

To reproduce: `docker compose up --build -d`, then `npm run benchmark`, and separately `node benchmark-query.js` (Ctrl-C to stop) while the ingestion benchmark runs, to get concurrent ingest+query numbers. `docker stats` in another terminal for resource usage.

---

## Known Limitations

- **Ack-before-durable.** `POST /logs` returns `200` once accepted entries are in the in-memory ring buffer, not once they're committed to Postgres. A process crash between accept and the next `COPY` loses that data despite having returned success. This is the trade-off that makes the buffer/async-flush architecture capable of 15k+/sec within a 0.5 CPU app container; a graceful `SIGTERM`/`SIGINT` (see [Architecture](#architecture)) drains the buffer before exiting, but an ungraceful termination (OOM kill, `kill -9`, power loss) does not.
- **Bounded buffer.** The ring buffer holds up to 500,000 entries. If Postgres is unavailable long enough for it to fill, further ingestion requests start getting entries rejected (still `200` with a `rejected` array if some entries in the request still fit, `400` only if the whole request is rejected) rather than queuing indefinitely.
- **Unbounded `GET /logs` queries.** Per spec, all filters on `GET /logs` are optional — a query using only `attr.<key>` or `q` with no `since`/`until` scans across every live partition rather than being pruned to one. `GET /logs/aggregate` always requires a time range, so it doesn't have this exposure.
- **No GIN index on `attributes`** (deliberate — see [Schema and Index Design](#schema-and-index-design)): attribute/message filtering is sequential-scan-per-row rather than index-assisted, relying on `service`/`level`/time-range narrowing and partition pruning to stay fast at the project's target scale.
- **Retention granularity is daily**, not to-the-second — see [Retention Strategy](#retention-strategy).
- **Single in-flight `COPY`.** `LogWorker` intentionally never runs more than one flush at a time — the ring buffer uses a single shared read cursor, so concurrent flushes could race on advancing it. Raising this would require reworking the buffer to hand out non-overlapping batch leases rather than a shared cursor.
- **No authentication, multi-tenancy, or rate limiting** — by design (see [Optional Features](#optional-features)); anyone able to reach the service can read and write all data.
