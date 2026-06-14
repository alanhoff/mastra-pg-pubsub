# [FEATURE] Add optional PubSub lifecycle hooks for init and shutdown cleanup

## Problem Statement

Mastra lets applications provide a custom PubSub implementation with:

```ts
export const mastra = new Mastra({
  pubsub: new CustomPubSub(),
});
```

The current public PubSub contract is centered on event delivery: `publish`, `subscribe`, `unsubscribe`, and `flush`, plus delivery-mode and replay helpers. That is enough for in-process or simple transports, but it does not give custom PubSub adapters a first-class way to participate in the owning Mastra instance's lifecycle.

In current Mastra behavior, `stopWorkers()` flushes the configured PubSub, and `shutdown()` calls `stopWorkers()`, but a custom PubSub is not initialized before workers start and is not closed during shutdown through a documented base contract.

That leaves production adapters with external resources needing manual integration. Examples include adapters that own:

- database pools
- Redis or cloud PubSub clients
- listener sockets
- polling/reclaim/maintenance timers
- durable subscription rows
- schema migrations or broker setup

For example, a PostgreSQL-backed PubSub adapter needs to:

- create or migrate SQL schema before workers start subscribing or processing workflow events
- start listener/maintenance loops once
- drain local deliveries on shutdown
- close owned PostgreSQL pools and listener connections
- clean up private subscription rows

Today that requires adapter-specific user code such as:

```ts
const pubsub = new PostgresPubSub({ connectionString: process.env.DATABASE_URL });
const mastra = new Mastra({ pubsub });
pubsub.wireMastraLifecycle(mastra);
```

This works as a workaround, but it is easy to forget, harder to document consistently, and requires an external adapter to wrap Mastra lifecycle methods instead of using an official integration point.

## Proposed Solution

Add a backwards-compatible optional PubSub lifecycle contract and have `Mastra` call it.

Suggested public contract:

```ts
export interface PubSubLifecycle {
  init?(): Promise<void>;
  close?(): Promise<void>;
}
```

Suggested Mastra behavior:

1. Before `startWorkers()` initializes workers or subscribes event listeners, call:

   ```ts
   await pubsub.init?.();
   ```

2. During `shutdown()`, after `stopWorkers()` has unsubscribed listeners and flushed in-flight PubSub work, call:

   ```ts
   await pubsub.close?.();
   ```

3. If worker startup fails after PubSub initialization, attempt best-effort PubSub cleanup when safe.

4. Document the exact lifecycle order:

   ```text
   Mastra.startWorkers()
     -> pubsub.init?.()
     -> worker init/start
     -> push/user event subscriptions

   Mastra.shutdown()
     -> stopWorkers()
        -> worker stop
        -> event unsubscribe
        -> pubsub.flush()
     -> pubsub.close?.()
     -> storage/observability shutdown
   ```

5. Keep existing third-party PubSub implementations working unchanged. Adapters that do not expose `init()` or `close()` should continue to behave exactly as they do today.

6. Add tests for:

   - lifecycle hooks absent: no behavior change
   - `init()` called before worker initialization/subscription
   - `close()` called during `shutdown()` after `flush()`
   - `init()` failure behavior
   - `close()` failure behavior
   - idempotent shutdown behavior

An alternative implementation would add concrete no-op `init()` and `close()` methods to the `PubSub` base class instead of duck-typing optional methods. Either shape would work as long as it is backwards-compatible and documented.

If existing built-in adapters already use a different lifecycle name, such as `destroy()`, Mastra could either standardize on `close()` and update adapters, or temporarily support a compatibility alias while documenting `close()` as the preferred hook.

## Component

- Workflows
- Server
- Other: Core PubSub lifecycle

## Alternatives Considered

1. Manual adapter wiring, such as `pubsub.wireMastraLifecycle(mastra)`.

   This works, but it is a footgun. Users can forget it, and adapters need to wrap Mastra lifecycle methods from outside.

2. Lazy initialization on first `publish()` or `subscribe()`.

   This helps with schema migration and connection setup, but it does not provide deterministic startup ordering before workers begin and does not solve graceful shutdown.

3. Process signal handlers inside the PubSub adapter.

   This is incomplete because applications may call `mastra.shutdown()` without process exit, tests may create multiple Mastra instances in one process, and libraries should not globally own process signals by default.

4. Monkey-patching `Mastra.prototype`.

   This would be surprising, version-sensitive, and risky for other instrumentation. A documented lifecycle hook is cleaner.

5. Only documenting that users must call `pubsub.close()`.

   Documentation helps, but Mastra already owns `shutdown()`. When a PubSub is passed into `new Mastra({ pubsub })`, it is reasonable for users to expect Mastra to drain and close it during Mastra shutdown.

## Example Use Case

A developer builds a distributed Mastra app with a PostgreSQL-backed PubSub adapter:

```ts
const pubsub = new PostgresPubSub({
  connectionString: process.env.DATABASE_URL,
  schema: 'mastra_pg_pubsub',
});

export const mastra = new Mastra({
  pubsub,
  workflows,
  agents,
});
```

When the app starts workers, Mastra calls `pubsub.init()` before workers subscribe to workflow topics. The adapter creates/migrates its schema under a database lock and starts any required maintenance/listener loops.

When the app shuts down, the developer calls:

```ts
await mastra.shutdown();
```

Mastra stops workers, unsubscribes event listeners, flushes PubSub work, then calls `pubsub.close()` so the adapter can release database pools, close listener connections, stop timers, and clean up private subscription rows.

The developer no longer needs a separate adapter-specific call after constructing Mastra.

## Additional Context

Related docs and code areas:

- PubSub reference: https://mastra.ai/reference/pubsub/base
- Mastra `pubsub` configuration: https://mastra.ai/reference/configuration#pubsub
- Redis Streams PubSub docs already document implementation-specific `close()`: https://mastra.ai/reference/pubsub/redis-streams
- Adjacent PR that added `supportedModes` and worker routing: https://github.com/mastra-ai/mastra/pull/16309
- Adjacent PR that added PubSub reference docs: https://github.com/mastra-ai/mastra/pull/17491

I searched existing issues and PRs for the following terms and did not find a duplicate for this specific lifecycle hook request:

```text
pubsub lifecycle
pubsub shutdown close
"PubSub" "close()"
"PubSub" init close lifecycle
"PubSub" destroy shutdown
"pubsub" "flush" "shutdown"
```

This request is specifically about Mastra owning optional PubSub startup and shutdown hooks. It is not asking for a new transport implementation.

## Verification

- [x] I have searched the existing issues to make sure this is not a duplicate
- [x] I have provided sufficient context for the team to understand the request
