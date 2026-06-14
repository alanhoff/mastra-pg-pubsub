# Design - mastra-pg-pubsub

A PostgreSQL-backed PubSub implementation for [Mastra](https://mastra.ai), extending the `PubSub` abstract class from `@mastra/core/events`. At-least-once delivery, consumer groups, replay from offset, dead-lettering, and low-latency wakeups via `LISTEN/NOTIFY` all run on a database you already operate.

## Goals

- Drop-in `pubsub` for `new Mastra({ pubsub })` with full base-class semantics.
- At-least-once delivery with ack/nack, visibility timeouts, and bounded redelivery.
- Competing-consumer groups (`options.group`) and fan-out subscribers.
- Native replay: `getHistory`, `subscribeWithReplay`, `subscribeFromOffset`.
- Lazy migration/start on database use, with idle resource stop after final unsubscribe.
- Mastra-shaped logging and current-span observability integration.
- Single runtime dependency: `pg`. `@mastra/core` is a peer dependency.

## Non-Goals

- Native batching (`supportsNativeBatching` stays `false`; `options.batch` is ignored).
- Push delivery mode (`supportedModes` is `['pull']`).
- Exactly-once delivery (consumers must be idempotent; event `id` enables dedupe).

## Schema

Everything lives in a dedicated PostgreSQL schema, defaulting to `pg_pubsub`, created lazily under an advisory lock or explicitly via `migrate()`.

```text
topics         topic TEXT PK, next_index BIGINT
events         seq BIGSERIAL PK, id UUID UNIQUE, topic TEXT, index BIGINT,
               type TEXT, run_id TEXT, data JSONB, created_at TIMESTAMPTZ
               UNIQUE (topic, index)
subscriptions  id TEXT PK, topic TEXT, is_group BOOL, last_seen_at TIMESTAMPTZ
deliveries     event_seq BIGINT FK->events, subscription_id TEXT FK->subscriptions,
               delivery_attempt INT, visible_at TIMESTAMPTZ
               PK (subscription_id, event_seq)
dead_events    optional copy of event + subscription + attempts when deadLetter is true
```

PostgreSQL reserves the `pg_` prefix. When the default schema is absent, the migration enables the session flag needed to create this package-owned schema; when the schema already exists, ordinary roles only need table-creation privileges inside it. Other custom `pg_` names are rejected; users can select an ordinary schema name when their database policy requires one. Production least-privilege deployments should run `migrate()` from a release/migration role and run app workers with DML privileges on the pre-created schema rather than broad runtime DDL privileges.

## Data Flow

```mermaid
flowchart LR
  P[publish] -->|tx: bump topic counter,\ninsert event + deliveries,\nNOTIFY| DB[(PostgreSQL)]
  DB -->|NOTIFY wakeup / poll| L[consume loop\nper subscription]
  L -->|claim batch:\nFOR UPDATE SKIP LOCKED\n+ visibility timeout| DB
  L -->|event, ack, nack| CB[EventCallback]
  CB -->|ack: DELETE delivery| DB
  CB -->|nack: visible_at = now()+nackDelay| DB
```

## Publish

1. `INSERT ... ON CONFLICT` bumps `topics.next_index`, returning the per-topic `index`.
2. Insert the event row with UUID `id`, `createdAt`, and `index`.
3. Insert one `deliveries` row per active subscription on the topic.
4. `NOTIFY <channel>, topic`, where the channel name is derived from the schema.

Subscribers created after publish do not receive prior events, but can get them through replay.

## Subscribe

- `options.group` set: shared subscription row derived from `(topic, group)`; each event reaches exactly one group member through `FOR UPDATE SKIP LOCKED`.
- No group: private subscription row (`__private:<instanceId>:<uuid>`); each callback receives every event published after it subscribes.
- One consume loop exists per `(topic, subscription id)`.
- `ack()` deletes the delivery row. `nack()` makes it visible again after `nackDelayMs`.
- Missing settlement causes redelivery after `ackDeadlineMs`.
- `delivery_attempt > maxDeliveryAttempts` drops the event and optionally copies it to `dead_events`.

## Lifecycle

- Constructors are side-effect free.
- `publish`, `subscribe`, `getHistory`, `subscribeWithReplay`, `subscribeFromOffset`, `start`, `init`, and `migrate` start lazily when they need the database.
- Startup migrates the schema once and starts maintenance when `cleanupIntervalMs > 0`.
- Unsubscribing stops the consume loop for that subscription. When no local subscriptions remain, the adapter closes the listener, clears the maintenance timer, and resets start state so future database use restarts lazily.
- `flush()` drains in-flight publishes and deliveries for this adapter's active subscription ids. Private subscription ids are local to one process; group subscription ids are shared across group members, so group `flush()` is a group-wide drain check rather than a strictly process-local check.
- `close()` is terminal and idempotent. It stops loops, releases the listener, deletes private subscriptions, and ends only pools created by this library.

## Replay

- `getHistory(topic, offset = 0)` returns events ordered by per-topic `index`.
- `subscribeWithReplay` and `subscribeFromOffset` create the live subscription row first, replay history while the consume loop is paused, settle replayed delivery rows, then start live delivery.
- Setup failures clean up the paused private subscription.

## Configuration

```ts
interface PostgresPubSubConfig {
  connectionString?: string;
  pool?: pg.Pool;
  schema?: string; // default 'pg_pubsub'
  pollIntervalMs?: number; // positive safe integer
  ackDeadlineMs?: number; // positive safe integer
  nackDelayMs?: number; // non-negative safe integer
  maxDeliveryAttempts?: number; // positive safe integer, Infinity, or 0 -> Infinity
  batchSize?: number; // positive safe integer
  maxEventsPerTopic?: number; // non-negative safe integer
  cleanupIntervalMs?: number; // non-negative safe integer
  staleSubscriptionMs?: number; // positive safe integer
  listen?: boolean;
  deadLetter?: boolean;
  logger?: IMastraLogger | false;
}
```

## Observability

The adapter emits payload-safe logs plus Mastra observability spans/events:

- `logger` accepts the same shape as `new Mastra({ logger })`.
- If `logger` is omitted, the adapter resolves the current span and uses `span.observabilityInstance.getLogger()`.
- `logger: false` silences PubSub logs.
- Operation spans are created as generic child spans of the current span when one exists.
- Delivery callbacks run inside the delivery span context so downstream work inherits the correct async context.

Emitted context is allow-listed scalar metadata such as topic, event id/type/index, run id, subscription id/kind, attempts, counts, status, and duration. Event payload `data`, connection strings, raw database rows, secrets, and arbitrary user objects are excluded. Errors are sanitized to fields such as `error.name`. Applications that encode tenant, user, or business-sensitive values in topics, groups, run ids, or other identifiers should hash or redact those identifiers before handing them to PubSub.

## Delivery Guarantees

| Property | Guarantee |
| --- | --- |
| Delivery | At least once; `ack()` settles, missing ack redelivers after `ackDeadlineMs`. |
| Ordering | Per topic by `index`; retries may interleave with newer events. |
| Groups | Each event delivered to exactly one member per group. |
| Fan-out | Every groupless subscriber receives every event published after subscription. |
| Replay | History is addressable by per-topic `index` until retention trims it. |
| Idempotency | Stable event `id` for consumer-side dedupe. |

## Engineering Constraints

- TypeScript executed natively by Node >= 22.13 in tests; build uses `tsc` to `dist/`.
- ESM-only.
- Tests use `node:test`; coverage uses the built-in coverage reporter.
- Lint/format: Biome.
- Postgres for dev/test via Docker Compose.
- Cluster tests prove fan-out, consumer groups, and history across child-process Mastra instances.
- E2E tests prove key-free PubSub semantics against live Postgres on every CI run; the OpenAI-backed Mastra durable-agent case is gated by `OPENAI_API_KEY`.
