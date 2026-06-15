import type { IMastraLogger } from '@mastra/core/logger';
import type { Pool } from 'pg';

/**
 * Delivery settlement policy for callbacks that return without calling
 * `ack()` or `nack()`.
 */
export type SettlementPolicy = 'mastra-compatible' | 'explicit' | 'callback-success';

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
   * `^[a-z_][a-z0-9_]*$`. Defaults to `pg_pubsub`; other custom schema names
   * that start with PostgreSQL's reserved `pg_` prefix are rejected.
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
  /**
   * Callback settlement policy. Defaults to `mastra-compatible`, which
   * auto-acks successful private/fan-out callbacks after they resolve while
   * keeping group subscribers explicit by default. Use `explicit` to preserve
   * strict ack/nack-only settlement everywhere, or `callback-success` to
   * auto-ack successful callbacks for both private and group subscriptions.
   */
  settlement?: SettlementPolicy;
  /**
   * Optional logger with the same shape accepted by `new Mastra({ logger })`.
   * When omitted, PubSub resolves the current Mastra span and uses its
   * observability logger. Pass `false` to force silence.
   */
  logger?: IMastraLogger | false;
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
  settlement: SettlementPolicy;
  logger: IMastraLogger | false | undefined;
}
