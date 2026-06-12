# Design — mastra-pg-pubsub

A PostgreSQL-backed PubSub implementation for [Mastra](https://mastra.ai), extending the
`PubSub` abstract class from `@mastra/core/events`. At-least-once delivery, consumer
groups, replay from offset, dead-lettering, and low-latency wakeups via `LISTEN/NOTIFY` —
all on a database you already run.

## Goals

- Drop-in `pubsub` for `new Mastra({ pubsub })` with full base-class semantics.
- At-least-once delivery with ack/nack, visibility timeouts, and bounded redelivery.
- Competing-consumer groups (`options.group`) and fan-out (groupless) subscribers.
- Native replay: `getHistory`, `subscribeWithReplay`, `subscribeFromOffset`.
- Low latency through `LISTEN/NOTIFY`, with polling as the correctness backstop.
- Single runtime dependency: `pg`. `@mastra/core` is a peer dependency.

## Non-goals

- Native batching (`supportsNativeBatching` stays `false`; `options.batch` is ignored).
- Push delivery mode (`supportedModes` is `['pull']`).
- Exactly-once delivery (consumers must be idempotent; event `id` enables dedupe).

## Schema

Everything lives in a dedicated PostgreSQL schema (default `mastra_pubsub`), created
lazily under an advisory lock, or explicitly via `migrate()`.

```
topics         topic TEXT PK, next_index BIGINT          -- per-topic monotonic counter
events         seq BIGSERIAL PK, id UUID UNIQUE, topic TEXT, index BIGINT,
               type TEXT, run_id TEXT, data JSONB, created_at TIMESTAMPTZ
               UNIQUE (topic, index)
subscriptions  id TEXT PK, topic TEXT, is_group BOOL, last_seen_at TIMESTAMPTZ
deliveries     event_seq BIGINT FK→events, subscription_id TEXT FK→subscriptions,
               delivery_attempt INT, visible_at TIMESTAMPTZ
               PK (subscription_id, event_seq)
dead_events    (optional, when deadLetter: true) copy of event + subscription + attempts
```

## Data flow

```mermaid
flowchart LR
  P[publish] -->|tx: bump topic counter,\ninsert event + deliveries,\nNOTIFY| DB[(PostgreSQL)]
  DB -->|NOTIFY wakeup / poll| L[consume loop\nper subscription]
  L -->|claim batch:\nFOR UPDATE SKIP LOCKED\n+ visibility timeout| DB
  L -->|event, ack, nack| CB[EventCallback]
  CB -->|ack: DELETE delivery| DB
  CB -->|nack: visible_at = now()+nackDelay| DB
```

### publish (single transaction)

1. `INSERT ... ON CONFLICT` bump `topics.next_index`, returning the per-topic `index`.
2. Insert the event row (implementation assigns `id` = UUID, `createdAt`, `index`).
3. Fan out: insert one `deliveries` row per active subscription on the topic.
4. `NOTIFY <channel>, topic` (channel name derived from the schema, payload is the topic).

Subscribers created after publish do not receive prior events (standard pubsub), but can
get them through replay.

### subscribe

- `options.group` set → shared subscription row `id = group`; every group member claims
  from the same delivery rows, so each event reaches exactly one member
  (`FOR UPDATE SKIP LOCKED`). Multiple local callbacks on the same group round-robin.
- No group → private subscription row (`__private:<instanceId>:<uuid>`); each callback
  gets every event (fan-out).
- One consume loop per (topic, subscription id): claim a batch, extend `visible_at` by
  `ackDeadlineMs`, increment `delivery_attempt`, deliver sequentially.
- `ack()` deletes the delivery row. `nack()` makes it visible again after `nackDelayMs`.
  No call → redelivered when the visibility timeout lapses (crash safety).
- `delivery_attempt > maxDeliveryAttempts` → dropped (optionally copied to `dead_events`),
  with a `logger.warn`.

### Loops, lifecycle, maintenance

- A dedicated `LISTEN` connection wakes the loops for a topic the moment something is
  published; `pollIntervalMs` polling is the backstop that also picks up visibility-timeout
  redeliveries. `listen: false` degrades to pure polling.
- `flush()` resolves when all in-flight publishes and callback deliveries settle. Callback
  errors are logged, never thrown.
- `close()` stops loops, releases the listener, deletes private subscriptions, ends the
  pool (only if the library created it).
- Periodic maintenance (every `cleanupIntervalMs`): trim each topic to `maxEventsPerTopic`
  (never deleting events with pending deliveries) and prune private subscriptions whose
  `last_seen_at` heartbeat went stale (`staleSubscriptionMs`), so dead fan-out subscribers
  don't accumulate backlog.

### Replay

- `getHistory(topic, offset = 0)` → `SELECT ... WHERE topic = $1 AND index >= $2 ORDER BY index`.
- `subscribeWithReplay` / `subscribeFromOffset`: register the live subscription first (so
  nothing is missed), then deliver history, deduping the boundary by event `index`.

## Configuration

```ts
interface PostgresPubSubConfig {
  connectionString?: string;     // or...
  pool?: pg.Pool;                // bring-your-own pool (never ended by close())
  schema?: string;               // default 'mastra_pubsub'
  pollIntervalMs?: number;       // default 1000
  ackDeadlineMs?: number;        // visibility timeout, default 30_000
  nackDelayMs?: number;          // default 0
  maxDeliveryAttempts?: number;  // default 5; Infinity = unbounded; 0 → Infinity (warn once)
  batchSize?: number;            // claim batch size, default 32
  maxEventsPerTopic?: number;    // retention, default 10_000; 0 = keep everything
  cleanupIntervalMs?: number;    // default 60_000; 0 = disable maintenance
  staleSubscriptionMs?: number;  // default 300_000
  listen?: boolean;              // default true (LISTEN/NOTIFY wakeups)
  deadLetter?: boolean;          // default false; keep exhausted events in dead_events
  logger?: { debug?: LogFn; warn?: LogFn; error?: LogFn }; // silent by default
}
```

Defaults mirror `@mastra/redis-streams` where a counterpart exists
(`maxDeliveryAttempts`, retention, reclaim-style timeouts, optional silent logger).

## Delivery guarantees

| Property | Guarantee |
| --- | --- |
| Delivery | At-least-once (ack to settle; crash → redelivery after `ackDeadlineMs`) |
| Ordering | Per topic, by `index`, within a single consumer; retries may reorder |
| Groups | Each event delivered to exactly one member per group |
| Fan-out | Every groupless subscriber receives every event |
| Replay | Full history up to retention, addressable by per-topic `index` |
| Idempotency | Stable event `id` (UUID) for consumer-side dedupe |

## Engineering constraints

- TypeScript executed natively by Node ≥ 22.13 (erasable-syntax-only; no enums/namespaces;
  `.ts` relative imports with `rewriteRelativeImportExtensions` for the build).
- ESM-only. Tests with `node:test`; coverage with the built-in coverage reporter,
  enforced thresholds. Build = `tsc` to `dist/` (the only transpile step).
- Lint/format: Biome. Postgres for dev/test via `docker compose` (latest image).
- E2E: real Mastra instance + real OpenAI agent (`OPENAI_API_KEY` from `.env`) proving
  cross-instance delivery, semantics, and replay against a live database.
