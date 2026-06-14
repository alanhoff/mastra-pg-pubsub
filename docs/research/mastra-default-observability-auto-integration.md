# Best-Practice Research: Auto-Wire Mastra Observability

Date: 2026-06-14

Question: how should `mastra-pg-pubsub` integrate with Mastra's default observability stack so users get logs and traces automatically when Mastra is configured, without manually passing `logger` and `tracer` adapters?

## Direct Recommendation

Auto-bind Mastra observability from `wireMastraLifecycle(mastra)` by default, while preserving the explicit `logger` and `tracer` options as overrides.

The best implementation shape is:

1. Keep constructor behavior dependency-free and silent when no Mastra instance is provided.
2. In `wireMastraLifecycle(mastra)`, before emitting the `pg_pubsub.lifecycle.mastra.wire` span, structurally inspect the host for Mastra's public observability/logging APIs.
3. If the user did not pass `logger`, adapt `mastra.getLogger()` into `PubSubLogger`. Prefer this over `mastra.loggerVNext` because Mastra's logger wrapper dual-writes to both the configured infrastructure logger and observability storage when observability is enabled.
4. If the user did not pass `tracer`, adapt `mastra.observability.getDefaultInstance()` into `PubSubTracer` only when a real default instance exists. If the entrypoint is missing, no-op, or has no default instance, leave tracing silent.
5. Do not add `@mastra/observability`, OpenTelemetry, Datadog, or any exporter package as a runtime dependency. Use structural checks against `@mastra/core` peer types and runtime methods.
6. Add an explicit opt-out if needed, for example `observability?: 'auto' | false`, defaulting to `'auto'`. Manual `logger` and `tracer` should always win.

This gives the good developer UX: users configure observability once in Mastra, pass this PubSub adapter to Mastra, call `wireMastraLifecycle(mastra)`, and the PubSub library automatically participates.

## Evidence Used

Official Mastra docs:

- [Observability overview](https://mastra.ai/docs/observability/overview): Mastra observability is configured once and covers traces, logs, and metrics. Traces are the foundation; logs are correlated to traces when emitted inside traced context.
- [Observability config](https://mastra.ai/docs/observability/config): the standard config installs `Observability`, `MastraStorageExporter`, `MastraPlatformExporter`, `SensitiveDataFilter`, and `logging`.
- [Logging](https://mastra.ai/docs/observability/logging): when observability is configured, logger calls are automatically forwarded to observability storage, and Mastra wraps the configured logger so calls write to both the original logger and the observability system.
- [Tracing overview](https://mastra.ai/docs/observability/tracing/overview): custom child spans use `tracingContext.currentSpan?.createChildSpan({ type: 'generic', ... })`; child spans inherit trace context and can record errors.
- [OpenTelemetry bridge](https://mastra.ai/docs/observability/integrations/bridges/otel): bridge users need spans to execute in context so auto-instrumented DB/HTTP operations nest correctly.
- [OpenTelemetry exporter](https://mastra.ai/docs/observability/integrations/exporters/otel): Mastra can export traces and logs as OTEL signals, so this library should avoid hard-coding exporter assumptions.

Current package/version evidence:

- Installed `@mastra/core` is `1.42.0`; `npm view @mastra/core version` also returned `1.42.0`.
- `npm view @mastra/observability version` returned `1.14.1`.
- This repo does not currently install `@mastra/observability`; that supports the structural/optional integration approach.

Repo-local evidence:

- `src/types.ts` defines `PubSubLogger`, `PubSubTracer`, and manual `logger`/`tracer` config. The current config loses whether those were explicitly supplied once `resolveConfig()` applies `{}` defaults.
- `src/observability.ts` already centralizes sink safety. Logger/tracer failures are swallowed, trace attributes are payload-safe, and spans emit start/error/end events.
- `src/postgres-pubsub.ts` currently resolves `logger` and `tracer` in the constructor only, stores `#logger`, and emits all PubSub lifecycle/delivery telemetry through that surface.
- `wireMastraLifecycle()` currently wraps `startWorkers()` and `shutdown()`, starts PubSub before Mastra workers, and closes PubSub after Mastra shutdown.
- Mastra's installed types expose `Config.observability?: ObservabilityEntrypoint`, `getLogger()`, `observability`, `loggerVNext`, and `metrics`.
- Mastra's installed runtime creates `NoOpObservability` when none is configured, wraps the configured logger with `DualLogger`, exposes `loggerVNext` from the default observability instance, and shuts observability down inside `Mastra.shutdown()`.

## Version / Date Context

Research was done against Mastra docs marked "Latest Version" on 2026-06-14, installed `@mastra/core@1.42.0`, and npm-current `@mastra/observability@1.14.1`.

The package peer range is `@mastra/core >=1.13.0-0 <2.0.0-0`, so implementation must tolerate older Mastra instances that do not have `loggerVNext`, `observability`, `getDefaultInstance()`, or span APIs. Detection should be structural and fail closed to the existing no-op behavior.

## Repo-Local Context

Current public UX requires manual observability wiring:

```ts
new PostgresPubSub({
  connectionString,
  logger: {
    debug: (message, data) => logger.debug(message, data),
    warn: (message, data) => logger.warn(message, data),
    error: (message, data) => logger.error(message, data),
  },
  tracer: {
    startSpan: (name, attributes) => tracer.startSpan(name, { attributes }),
    event: (name, attributes) => tracer.addEvent(name, attributes),
  },
})
```

That is too much ceremony for Mastra users because Mastra already has a configured logger and observability entrypoint on the app instance.

The likely implementation needs a small internal state change:

- Preserve whether `logger` and `tracer` were explicitly supplied, for example `loggerProvided` and `tracerProvided` in `ResolvedConfig`.
- Make the default logger/tracer objects mutable delegates. `#logger` already points at `#config.logger`; assigning methods onto the default object in `wireMastraLifecycle()` lets existing call sites keep working.
- Add a helper such as `bindMastraObservability(mastra)` that is idempotent per host and runs before lifecycle wiring telemetry is emitted.

## Recommended Adapter Design

### Logger Binding

Preferred source:

```ts
const logger = typeof mastra.getLogger === 'function' ? mastra.getLogger() : undefined;
```

Adapt it as:

```ts
{
  debug: (message, context) => logger.debug(message, context),
  warn: (message, context) => logger.warn(message, context),
  error: (message, context) => logger.error(message, context),
}
```

Why `getLogger()` first:

- Mastra docs recommend `mastra.getLogger()` for app/tool/workflow logging.
- Mastra runtime wraps the configured infrastructure logger in `DualLogger`.
- `DualLogger` forwards calls to `loggerVNext` when available, while preserving the user's configured logger transport.

Fallback:

- If `getLogger()` is absent but `mastra.loggerVNext` has `debug/warn/error`, adapt `loggerVNext`.
- This fallback writes to observability but may not preserve the infrastructure logger.

Do not bind auto logger when `config.logger` was explicitly supplied.

### Tracer Binding

Preferred source:

```ts
const observability = mastra.observability ?? mastra.getObservability?.();
const instance = observability?.getDefaultInstance?.();
```

Only bind if `instance?.startSpan` is a function. This automatically treats `NoOpObservability` as silent because its default instance is `undefined`.

Map `PubSubTracer.startSpan(name, attributes)` to a Mastra generic span:

```ts
const span = instance.startSpan({
  type: 'generic',
  name,
  attributes,
  metadata: {
    component: 'mastra-pg-pubsub',
  },
});
```

Map the returned Mastra span into `PubSubTraceSpan`:

- `setAttribute(name, value)` and `setAttributes(attributes)` -> `span.update({ attributes })`.
- `recordException(error)` -> `span.error({ error, endSpan: false })` if available.
- `setStatus({ code: 'error', message })` should record an error if no explicit error was recorded.
- `end()` -> `span.end({ attributes })` for ok spans, or `span.error({ error, endSpan: true })` for error spans.

Map `PubSubTracer.event(name, attributes)` conservatively:

- If a current/root PubSub span is active in the library, create a child event span if the Mastra span API supports it.
- Otherwise create a short generic span with `isEvent: true` when supported, or fall back to `logger.debug(name, attributes)`.

Do not bind auto tracer when `config.tracer` was explicitly supplied.

### Context Propagation

Mastra spans expose `executeInContext(fn)` and `executeInContextSync(fn)`. When a bridge such as the OpenTelemetry bridge is configured, executing PubSub database work inside span context lets auto-instrumented `pg` spans nest under the PubSub span.

The current `ActiveTraceSpan` interface has no `run()` method, so a full bridge-aware implementation needs either:

- extend the internal-only `ActiveTraceSpan` with `run<T>(fn: () => Promise<T>): Promise<T>` and use it around important async operations; or
- add an internal helper `withTraceSpan(tracer, name, attrs, fn)` that starts, records, ends, and context-runs the operation.

This can be done without changing the public `PubSubTracer` type because the Mastra-specific adapter can keep `executeInContext` inside the private `ActiveTraceSpan`.

### Lifecycle Ordering

Start path is straightforward:

1. `wireMastraLifecycle()` binds Mastra observability.
2. Wrapped `startWorkers()` starts a `pg_pubsub.lifecycle.mastra.start_workers` span.
3. PubSub `start()` runs first, which runs `migrate()` and starts maintenance.
4. Original Mastra `startWorkers()` runs.

Shutdown needs more care. Current code calls original `mastra.shutdown()` first, then `pubsub.close()`. Installed Mastra runtime shuts observability down inside `Mastra.shutdown()`, so close telemetry emitted after original shutdown may not reach Mastra observability.

Best lifecycle design for fully observable shutdown:

1. In the wrapper, start `pg_pubsub.lifecycle.mastra.shutdown`.
2. If `host.stopWorkers` exists, call it first. Mastra's `stopWorkers()` unsubscribes listeners and calls `pubsub.flush()`, so drain telemetry happens before observability shutdown.
3. Call `pubsub.close()` while observability is still alive.
4. Call the original `mastra.shutdown()`. Its repeated `stopWorkers()` should be idempotent, and `flush()` on an already closed adapter should resolve because subscriptions have been cleared.
5. If `host.stopWorkers` is absent, fall back to the current order and document that post-shutdown close telemetry may only reach the infrastructure logger.

This preserves graceful shutdown, makes the close span observable, and avoids closing PubSub while workers are still active.

## Boundaries / Non-goals

- Do not auto-create a Mastra `Observability` instance or exporter from this library. Mastra owns observability configuration.
- Do not import `@mastra/observability` at runtime.
- Do not choose a user's exporter, storage backend, OTEL bridge, or hosted platform destination.
- Do not log event payload data. The current payload-safe trace attribute policy should remain.
- Do not make observability sink failures affect PubSub behavior.

## Test Handoff

Add tests that prove the integration without requiring real exporters:

- Auto logger binding: fake Mastra host with `getLogger()` captures `debug/warn/error`; no manual logger passed; `wireMastraLifecycle()`, `startWorkers()`, `publish()`, and `shutdown()` emit PubSub logs.
- Auto tracer binding: fake `observability.getDefaultInstance().startSpan()` captures `pg_pubsub.lifecycle.mastra.wire`, `pg_pubsub.lifecycle.start`, `pg_pubsub.migrate`, `pg_pubsub.flush`, and `pg_pubsub.close`.
- No-op observability: fake `observability.getDefaultInstance()` returns `undefined`; behavior remains silent and PubSub methods still pass.
- Manual override wins: pass manual `logger`/`tracer` and fake Mastra observability; assert only manual sinks receive PubSub telemetry.
- Sink isolation: fake Mastra logger/span methods throw; PubSub publish/subscribe/flush behavior still passes.
- Lifecycle order: fake host with `stopWorkers()` and `shutdown()` records order; assert `stopWorkers -> pubsub.close -> originalShutdown`, and assert close span ends before fake observability shutdown.
- Older Mastra compatibility: fake host only implements `startWorkers` and `shutdown`; wiring remains idempotent and existing lifecycle tests still pass.
- Optional real integration test: if dev dependency `@mastra/observability` is added for tests only, configure `Observability` with an in-memory exporter fake and assert actual Mastra spans/logs are emitted. This is useful but not required for runtime correctness.

## Handoff

Implement this as a narrow observability adapter layer, probably in `src/observability.ts` or a new `src/mastra-observability.ts`.

Expected code changes:

- Extend `ResolvedConfig` to track explicit logger/tracer configuration.
- Add structural types for `MastraObservabilityHost`, `MastraLoggerLike`, `MastraObservabilityEntrypointLike`, `MastraObservabilityInstanceLike`, and `MastraSpanLike`.
- Add `bindMastraObservability(host)` from `wireMastraLifecycle()` before the first lifecycle span.
- Update shutdown wrapper ordering to preserve close telemetry before Mastra observability shutdown when `stopWorkers()` is available.
- Update README observability docs so the common path says "configure Mastra observability once; this adapter auto-detects it when wired to Mastra"; keep manual examples as advanced overrides.

The recommended stop condition is targeted tests plus `npm run typecheck`, `npm test`, `npm run test:cluster`, and at least one lifecycle test proving close spans are emitted before the fake Mastra observability shutdown.
