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
- **Small runtime surface**: one runtime dependency (`pg`), ESM, strict TypeScript, Node-native tests.

## Install

```sh
npm install mastra-pg-pubsub @mastra/core
```

`@mastra/core` is a peer dependency. `pg` is installed as this package's runtime dependency. If your app already owns a `pg.Pool`, pass it in instead of a connection string.

## Quickstart

```ts
import { Agent } from '@mastra/core/agent';
import { Mastra } from '@mastra/core/mastra';
import { PostgresPubSub } from 'mastra-pg-pubsub';

const pubsub = new PostgresPubSub({
  connectionString: process.env.DATABASE_URL,
});

const mastra = new Mastra({
  pubsub,
  agents: {
    assistant: new Agent({
      id: 'assistant',
      name: 'Assistant',
      instructions: 'You are helpful.',
      model: 'openai/gpt-4o-mini',
    }),
  },
});

pubsub.wireMastraLifecycle(mastra);

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

## Replay examples

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
| `connectionString` | — | PostgreSQL connection string. The adapter owns and closes its pool. |
| `pool` | — | Bring-your-own `pg.Pool`; never closed by `PostgresPubSub.close()`. |
| `schema` | `mastra_pg_pubsub` | Schema for all tables. Must match `^[a-z_][a-z0-9_]*$` and cannot start with PostgreSQL's reserved `pg_` prefix. |
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
| `logger` | silent | Optional `debug`, `warn`, and `error` functions with structured context. |
| `tracer` | silent | Optional package-neutral tracing hooks for spans and events. |

### Lifecycle

Call `pubsub.wireMastraLifecycle(mastra)` immediately after constructing your
Mastra instance. The bridge migrates the SQL before `mastra.startWorkers()` runs,
closes the adapter after `mastra.shutdown()`, and cleans up if Mastra startup
fails after the PubSub has already started.

If shutdown fails because `flush()` timed out with locally unsettled deliveries,
the bridge leaves the adapter open instead of deleting private subscription rows,
so the pending delivery evidence remains available for retry or inspection.

Direct PubSub calls still migrate lazily, and you can still call
`await pubsub.start()`, `await pubsub.migrate()`, `await pubsub.flush()`, and
`await pubsub.close()` yourself when you are not letting Mastra own the lifecycle.

### Default schema upgrade note

New instances use the dedicated `mastra_pg_pubsub` schema by default and create
it automatically. PostgreSQL reserves schema names beginning with `pg_`, so the
literal `pg_pubsub` schema cannot be auto-created. Existing deployments that
already use the old `mastra_pubsub` schema can keep using those tables
explicitly:

```ts
const pubsub = new PostgresPubSub({
  connectionString: process.env.DATABASE_URL,
  schema: 'mastra_pubsub',
});
```

## Observability

`logger` and `tracer` are both optional and silent by default. They are called with
payload-safe context only: topics, event ids, event types, indexes, run ids,
subscription ids/kinds, attempts, counts, status, and durations. Event `data`,
connection strings, raw database rows, and arbitrary payload objects are not logged
or traced.

Logger and tracer failures are isolated from PubSub behavior. If an observability
sink throws, publishing, subscribing, delivery, ack/nack, replay, flush, listener
wakeups, and close continue normally. Error telemetry is sanitized to scalar
metadata such as `error.name`; raw thrown values and error messages are not passed
to observability sinks.

```ts
const pubsub = new PostgresPubSub({
  connectionString: process.env.DATABASE_URL,
  logger: {
    debug: (message, context) => console.debug(message, context),
    warn: (message, context) => console.warn(message, context),
    error: (message, context) => console.error(message, context),
  },
  tracer: {
    event: (name, attributes) => {
      console.debug('trace event', name, attributes);
    },
    startSpan: (name, attributes) => {
      const startedAt = Date.now();
      return {
        setAttribute: (key, value) => {
          console.debug('trace attr', name, key, value);
        },
        recordException: (error) => {
          console.error('trace error', name, error);
        },
        setStatus: (status) => {
          console.debug('trace status', name, status);
        },
        end: () => {
          console.debug('trace span end', name, Date.now() - startedAt, attributes);
        },
      };
    },
  },
});
```

Trace names use the `pg_pubsub.*` prefix, including spans such as
`pg_pubsub.lifecycle.start`, `pg_pubsub.lifecycle.mastra.start_workers`,
`pg_pubsub.lifecycle.mastra.shutdown`, `pg_pubsub.publish`,
`pg_pubsub.delivery`, `pg_pubsub.flush`, and listener or maintenance lifecycle
spans.

## Delivery guarantees

| Property | Guarantee |
| --- | --- |
| Delivery | At least once; `ack()` settles, missing ack redelivers after `ackDeadlineMs`. |
| Ordering | Per-topic `index` order for normal delivery; retries can interleave with newer events. |
| Groups | Each event is delivered to one member per group. |
| Fan-out | Each groupless subscriber receives every event published after it subscribes. |
| Replay | Historical events are ordered by per-topic `index` and available until retention trims them. |
| Idempotency | Event `id` is stable across redeliveries for consumer-side dedupe. |
| Lifecycle | `wireMastraLifecycle()` migrates before Mastra starts and closes after Mastra shutdown; `flush()` drains in-flight local work; `close()` is idempotent and cleans private subscriptions. |

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

The schema is created lazily under a Postgres advisory lock, explicitly with
`await pubsub.migrate()`, or during Mastra startup when
`wireMastraLifecycle()` is installed.

## Local development

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

`npm test` is key-free and uses the pinned Postgres service from `docker-compose.yml` on port `5544`.
`npm run test:cluster` runs the process-level cluster proof: multiple child Node
processes each create a real `Mastra` container with this adapter, then verify
fan-out, competing-consumer groups, and history through the shared Postgres schema.

### Real e2e tests

The e2e suite includes one real Mastra durable-agent stream backed by OpenAI and Postgres memory, plus no-OpenAI delivery semantics tests. The real agent test intentionally validates the durable-agent stream API and topic shape for the locked `@mastra/core` version; refresh it when upgrading Mastra.

```sh
OPENAI_API_KEY=... # or put it in .env
npm run db:up
npm run test:e2e
```

The script loads `.env` when present with Node's `--env-file-if-exists=.env`, so an exported `OPENAI_API_KEY` also works. Keep `.env` out of git.

## Package contents

`npm pack --dry-run` should include only the built `dist/` files plus package metadata, README, and license. Source, tests, local research notes, and `.env` are not published.
