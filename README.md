# mastra-pg-pubsub

[![CI](https://github.com/alanhoff/mastra-pg-pubsub/actions/workflows/ci.yml/badge.svg)](https://github.com/alanhoff/mastra-pg-pubsub/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/mastra-pg-pubsub.svg)](https://www.npmjs.com/package/mastra-pg-pubsub)
[![license](https://img.shields.io/npm/l/mastra-pg-pubsub.svg)](./LICENSE)

PostgreSQL-backed [`PubSub`](https://mastra.ai/reference/pubsub/base) for Mastra. It gives Mastra apps at-least-once delivery, consumer groups, replay by offset, optional dead-lettering, and low-latency `LISTEN/NOTIFY` wakeups using a database you already operate.

## Why

Use this when you want Mastra agent/workflow events to survive process restarts and coordinate across multiple Node processes without adding Redis, NATS, or a cloud queue.

- **At-least-once delivery** with ack/nack and visibility timeouts.
- **Consumer groups** for competing workers (`subscribe(..., { group })`).
- **Fan-out** for groupless subscribers.
- **Replay** via `getHistory`, `subscribeWithReplay`, and `subscribeFromOffset`.
- **Crash recovery** through durable delivery rows and visibility timeout redelivery.
- **Low latency** through Postgres `LISTEN/NOTIFY`, with polling as the correctness backstop.
- **Lazy lifecycle**: SQL migration and maintenance start on first database use.
- **Idle shutdown**: listener and maintenance resources stop when the final local subscriber is removed.

## Install

```sh
npm install mastra-pg-pubsub @mastra/core @mastra/loggers
```

`@mastra/core` is a peer dependency. `@mastra/loggers` is used by the example below; any Mastra-compatible logger works. `pg` is installed as this package's runtime dependency. If your app already owns a `pg.Pool`, pass it in instead of a connection string.

## Quickstart

```ts
import { Agent } from '@mastra/core/agent';
import { Mastra } from '@mastra/core/mastra';
import { PinoLogger } from '@mastra/loggers';
import { PostgresPubSub } from 'mastra-pg-pubsub';

const logger = new PinoLogger({
  name: 'mastra',
  level: 'debug',
});

const pubsub = new PostgresPubSub({
  connectionString: process.env.DATABASE_URL!,
  logger: logger.child({ module: 'pubsub' }),
});

const mastra = new Mastra({
  pubsub,
  logger,
  agents: {
    assistant: new Agent({
      id: 'assistant',
      name: 'Assistant',
      instructions: 'You are helpful.',
      model: 'openai/gpt-4o-mini',
    }),
  },
});

export { mastra };
```

You can also use it directly through the Mastra PubSub contract:

```ts
await pubsub.subscribe('agent.stream.run-123', (event, ack, nack) => {
  try {
    console.log(event.type, event.index, event.data);
    ack?.();
  } catch {
    nack?.();
  }
});

await pubsub.publish('agent.stream.run-123', {
  type: 'chunk',
  data: { text: 'hello' },
  runId: 'run-123',
});
```

## Replay

```ts
const history = await pubsub.getHistory('agent.stream.run-123', 10);

await pubsub.subscribeWithReplay('agent.stream.run-123', (event, ack) => {
  ack?.();
});

await pubsub.subscribeFromOffset('agent.stream.run-123', 42, (event, ack) => {
  ack?.();
});
```

Replay registers the live subscription first, then replays history, deduping the boundary by event `index` so no event is missed or delivered twice at the transition.

## Configuration

Provide exactly one of `connectionString` or `pool`.

| Option | Default | Description |
| --- | ---: | --- |
| `connectionString` | - | PostgreSQL connection string. The adapter owns and closes its pool. |
| `pool` | - | Bring-your-own `pg.Pool`; never closed by `PostgresPubSub.close()`. |
| `schema` | `pg_pubsub` | Schema for all tables. Must match `^[a-z_][a-z0-9_]*$`. Other custom names that start with `pg_` are rejected. |
| `pollIntervalMs` | `1000` | Backstop polling interval and redelivery detection bound. |
| `ackDeadlineMs` | `30000` | Visibility timeout before unacked deliveries can be reclaimed. |
| `nackDelayMs` | `0` | Delay before a nacked delivery becomes visible again. |
| `maxDeliveryAttempts` | `5` | Attempts before drop/dead-letter. `Infinity` disables the cap; `0` is treated as `Infinity`. |
| `batchSize` | `32` | Deliveries claimed per consume-loop tick. |
| `maxEventsPerTopic` | `10000` | Retention cap per topic. `0` keeps everything. |
| `cleanupIntervalMs` | `60000` | Maintenance interval. `0` disables maintenance. |
| `staleSubscriptionMs` | `300000` | Age before stale private subscriptions are pruned. |
| `listen` | `true` | Enable `LISTEN/NOTIFY` wakeups. `false` uses polling only. |
| `deadLetter` | `false` | Copy exhausted events to `dead_events`. |
| `logger` | current span logger | Same logger shape accepted by `new Mastra({ logger })`. Pass `false` to force silence. |

The default schema is created automatically during migration when it is missing. PostgreSQL reserves the `pg_` prefix, so first-time creation of `pg_pubsub` requires an elevated migration role allowed to set `allow_system_table_mods`; once the schema exists, ordinary roles only need table-creation privileges on that schema. Use `schema` when your database policy requires an ordinary application schema name.

## Lifecycle

No lifecycle wiring is required. Any method that touches the database starts the adapter lazily by running the migration and starting maintenance if enabled:

- `publish`
- `subscribe`
- `getHistory`
- `subscribeWithReplay`
- `subscribeFromOffset`
- explicit `start`, `init`, or `migrate`

When the last local subscriber is removed, the adapter stops idle resources: consume loops are stopped as part of unsubscribe, the `LISTEN` connection is closed, and the maintenance timer is cleared. The pool stays open so later database use can restart lazily. `close()` remains the explicit terminal cleanup path and ends only pools created by this library.

`flush()` resolves when all in-flight publishes and locally-owned deliveries settle. Callback errors are logged, never thrown. If locally-owned deliveries remain unsettled after the bounded drain window, `flush()` rejects so callers do not mistake a stuck subscriber for a clean drain.

## Observability

The adapter emits payload-safe logs and Mastra observability spans/events. If `logger` is provided, it is used directly. If `logger` is omitted, the adapter resolves the current Mastra span with `resolveCurrentSpan()` and uses `span.observabilityInstance.getLogger()`. Pass `logger: false` to silence PubSub logs.

```ts
import { resolveCurrentSpan } from '@mastra/core/observability';

const span = resolveCurrentSpan();
const observability = span?.observabilityInstance;
```

Emitted context is allow-listed scalar metadata: topics, event ids, event types, indexes, run ids, subscription ids/kinds, attempts, counts, status, and durations. Event `data`, connection strings, raw database rows, and arbitrary payload objects are not logged or attached to spans. Error telemetry is sanitized to metadata such as `error.name`.

Span and event names use the `pg_pubsub.*` prefix, including `pg_pubsub.lifecycle.start`, `pg_pubsub.lifecycle.idle_stop`, `pg_pubsub.migrate`, `pg_pubsub.publish`, `pg_pubsub.delivery`, `pg_pubsub.flush`, `pg_pubsub.listener.*`, and `pg_pubsub.maintenance.*`.

## Delivery Guarantees

| Property | Guarantee |
| --- | --- |
| Delivery | At least once; `ack()` settles, missing ack redelivers after `ackDeadlineMs`. |
| Ordering | Per-topic `index` order for normal delivery; retries can interleave with newer events. |
| Groups | Each event is delivered to one member per group. |
| Fan-out | Each groupless subscriber receives every event published after it subscribes. |
| Replay | Historical events are ordered by per-topic `index` and available until retention trims them. |
| Idempotency | Event `id` is stable across redeliveries for consumer-side dedupe. |
| Lifecycle | Lazy start on database use; idle resource stop after final local unsubscribe; explicit `close()` is idempotent. |

This is intentionally **not exactly-once** delivery. Consumers that perform side effects should dedupe by `event.id` or a domain idempotency key.

## Architecture

```mermaid
flowchart LR
  P[publish] -->|tx: bump topic counter, insert event + deliveries, NOTIFY| DB[(PostgreSQL)]
  DB -->|NOTIFY wakeup / poll| L[consume loop per subscription]
  L -->|claim batch: FOR UPDATE SKIP LOCKED + visibility timeout| DB
  L -->|event, ack, nack| CB[EventCallback]
  CB -->|ack: DELETE delivery| DB
  CB -->|nack: visible_at = now()+nackDelay| DB
```

The schema is created lazily under a Postgres advisory lock or explicitly with `await pubsub.migrate()`.

## Local Development

```sh
npm install
npm run db:up
npm test
npm run test:cluster
npm run test:coverage
npm run typecheck
npm run lint
npm run build
```

`npm test` is key-free and uses the pinned Postgres service from `docker-compose.yml` on port `5544`. `npm run test:cluster` runs the process-level cluster proof: multiple child Node processes each create a real `Mastra` container with this adapter, then verify fan-out, competing-consumer groups, and history through the shared Postgres schema.

### Real E2E Tests

The e2e suite includes one real Mastra durable-agent stream backed by OpenAI and Postgres memory, plus no-OpenAI delivery semantics tests. The real agent test intentionally validates the durable-agent stream API and topic shape for the locked `@mastra/core` version; refresh it when upgrading Mastra.

```sh
OPENAI_API_KEY=... # or put it in .env
npm run db:up
npm run test:e2e
```

The script loads `.env` when present with Node's `--env-file-if-exists=.env`, so an exported `OPENAI_API_KEY` also works. Keep `.env` out of git.

## Package Contents

`npm pack --dry-run` should include only the built `dist/` files plus package metadata, README, changelog, and license. Source, tests, local notes, and `.env` are not published.
