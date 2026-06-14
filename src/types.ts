import type { Pool } from 'pg';

/**
 * A minimal log function. Compatible with `console.log`-style signatures and
 * with structured loggers that accept a message plus arbitrary context.
 */
export type LogFn = (message: string, ...context: unknown[]) => void;

/**
 * Optional logger injected into {@link PostgresPubSub}. Every method is
 * optional; when the whole logger (or an individual method) is absent the
 * adapter stays silent. Expected races (lost claims, redelivery) log at
 * `debug`; recoverable problems at `warn`; unexpected failures at `error`.
 */
export interface PubSubLogger {
  /** Verbose, expected-path diagnostics (claim races, wakeups, polls). */
  debug?: LogFn;
  /** Recoverable anomalies (dropped events, exhausted retries, stale prune). */
  warn?: LogFn;
  /** Unexpected failures inside background loops. */
  error?: LogFn;
}

/** Scalar values that can be safely attached to trace spans and events. */
export type PubSubTraceAttributeValue = string | number | boolean | null;

/** Payload-safe trace attributes emitted by the adapter. */
export type PubSubTraceAttributes = Record<string, PubSubTraceAttributeValue>;

/** Minimal span status shape for package-neutral tracer adapters. */
export interface PubSubTraceStatus {
  code: 'ok' | 'error';
  message?: string;
}

/**
 * Minimal span contract used by {@link PubSubTracer}. This intentionally avoids
 * importing a telemetry SDK; callers can adapt it to OpenTelemetry or any other
 * tracing implementation.
 */
export interface PubSubTraceSpan {
  setAttribute?: (name: string, value: PubSubTraceAttributeValue) => void;
  setAttributes?: (attributes: PubSubTraceAttributes) => void;
  recordException?: (error: unknown) => void;
  setStatus?: (status: PubSubTraceStatus) => void;
  end?: () => void;
}

/**
 * Optional tracer injected into {@link PostgresPubSub}. The adapter emits
 * payload-safe spans and events and swallows tracer failures so instrumentation
 * cannot affect PubSub behavior.
 */
export interface PubSubTracer {
  startSpan?: (name: string, attributes: PubSubTraceAttributes) => PubSubTraceSpan | undefined;
  event?: (name: string, attributes: PubSubTraceAttributes) => void;
}

/**
 * Configuration for {@link PostgresPubSub}.
 *
 * Provide exactly one of {@link PostgresPubSubConfig.connectionString} or
 * {@link PostgresPubSubConfig.pool}. A pool supplied here is owned by the
 * caller and is never ended by {@link PostgresPubSub.close}.
 */
export interface PostgresPubSubConfig {
  /**
   * PostgreSQL connection string. The adapter creates and owns the pool, and
   * ends it on {@link PostgresPubSub.close}. Mutually exclusive with
   * {@link PostgresPubSubConfig.pool}.
   */
  connectionString?: string;
  /**
   * Bring-your-own `pg.Pool`. The adapter never ends it on
   * {@link PostgresPubSub.close}. Mutually exclusive with
   * {@link PostgresPubSubConfig.connectionString}.
   */
  pool?: Pool;
  /**
   * PostgreSQL schema that holds every table. Must match
   * `^[a-z_][a-z0-9_]*$`. Defaults to `mastra_pubsub`.
   */
  schema?: string;
  /**
   * Backstop polling interval in milliseconds for each consume loop. Also
   * bounds how quickly visibility-timeout redeliveries are noticed.
   * Defaults to `1000`.
   */
  pollIntervalMs?: number;
  /**
   * Visibility timeout in milliseconds. A claimed delivery becomes visible
   * again this long after it was claimed unless acked or nacked, providing
   * crash safety. Defaults to `30_000`.
   */
  ackDeadlineMs?: number;
  /**
   * Delay in milliseconds before a nacked delivery becomes visible again.
   * Defaults to `0` (immediate redelivery).
   */
  nackDelayMs?: number;
  /**
   * Maximum delivery attempts before an event is dropped (and optionally
   * dead-lettered). `Infinity` disables the cap. `0` is treated as `Infinity`
   * with a one-time warning. Defaults to `5`.
   */
  maxDeliveryAttempts?: number;
  /** Number of deliveries claimed per loop iteration. Defaults to `32`. */
  batchSize?: number;
  /**
   * Retention cap per topic. Maintenance trims a topic's events down to this
   * many, never deleting events that still have pending deliveries. `0` keeps
   * everything. Defaults to `10_000`.
   */
  maxEventsPerTopic?: number;
  /**
   * Maintenance interval in milliseconds (retention trim + stale private
   * subscription pruning). `0` disables maintenance. Defaults to `60_000`.
   */
  cleanupIntervalMs?: number;
  /**
   * Age in milliseconds after which a private subscription whose
   * `last_seen_at` heartbeat has not advanced is pruned. Defaults to
   * `300_000`.
   */
  staleSubscriptionMs?: number;
  /**
   * Enable `LISTEN/NOTIFY` wakeups for low latency. When `false`, the adapter
   * relies purely on {@link PostgresPubSubConfig.pollIntervalMs} polling.
   * Defaults to `true`.
   */
  listen?: boolean;
  /**
   * Copy events that exhaust their delivery attempts into a `dead_events`
   * table instead of discarding them silently. Defaults to `false`.
   */
  deadLetter?: boolean;
  /** Optional logger; silent when omitted. */
  logger?: PubSubLogger;
  /** Optional tracer; silent when omitted. */
  tracer?: PubSubTracer;
}

/**
 * Fully-resolved configuration with every default applied. Internal.
 */
export interface ResolvedConfig {
  schema: string;
  pollIntervalMs: number;
  ackDeadlineMs: number;
  nackDelayMs: number;
  maxDeliveryAttempts: number;
  batchSize: number;
  maxEventsPerTopic: number;
  cleanupIntervalMs: number;
  staleSubscriptionMs: number;
  listen: boolean;
  deadLetter: boolean;
  logger: PubSubLogger;
  tracer: PubSubTracer;
}
