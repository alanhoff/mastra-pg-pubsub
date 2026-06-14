import { createHash, randomUUID } from 'node:crypto';
import type { Event, EventCallback, SubscribeOptions } from '@mastra/core/events';
import { PubSub } from '@mastra/core/events';
import pg from 'pg';
import { type CallbackRegistry, ConsumeLoop } from './consume-loop.ts';
import { NotifyListener } from './listener.ts';
import {
  type ActiveObservabilitySpan,
  logDebug,
  logError,
  logWarn,
  observeEvent,
  startObservabilitySpan,
  traceAttributes,
} from './observability.ts';
import { runMigration } from './schema.ts';
import { assertValidSchema, notifyChannel, quoteIdentifier } from './sql.ts';
import type { PostgresPubSubConfig, ResolvedConfig } from './types.ts';

const DEFAULT_SCHEMA = 'pg_pubsub';

interface Subscription {
  readonly id: string;
  readonly topic: string;
  readonly isGroup: boolean;
  readonly registry: CallbackRegistry;
  readonly loop: ConsumeLoop;
  started: boolean;
  unregisterWake: (() => void) | undefined;
}

/** Links a user callback to the subscription and the callback actually run. */
interface Registration {
  readonly sub: Subscription;
  readonly registered: EventCallback;
}

interface EventRow {
  id: string;
  topic: string;
  index: string;
  type: string;
  run_id: string;
  data: unknown;
  created_at: Date;
}

function mapKey(topic: string, subscriptionId: string): string {
  return JSON.stringify([topic, subscriptionId]);
}

function hashPart(value: string): string {
  return createHash('sha256').update(value).digest('base64url');
}

function groupSubscriptionId(topic: string, group: string): string {
  return `__group:${hashPart(topic)}:${hashPart(group)}`;
}

function integerOption(
  name: keyof PostgresPubSubConfig,
  value: number | undefined,
  defaultValue: number,
  minimum: number,
): number {
  const resolved = value ?? defaultValue;
  if (!Number.isSafeInteger(resolved) || resolved < minimum) {
    throw new Error(`${String(name)} must be a safe integer >= ${minimum}`);
  }
  return resolved;
}

function maxDeliveryAttemptsOption(
  value: PostgresPubSubConfig['maxDeliveryAttempts'],
  logger: PostgresPubSubConfig['logger'],
): number {
  const resolved = value ?? 5;
  if (resolved === 0) {
    logWarn(
      logger,
      'maxDeliveryAttempts=0 is treated as Infinity (unbounded redelivery)',
      traceAttributes({
        configuredMaxDeliveryAttempts: 0,
        resolvedMaxDeliveryAttempts: 'Infinity',
      }),
    );
    return Number.POSITIVE_INFINITY;
  }
  if (resolved === Number.POSITIVE_INFINITY) {
    return resolved;
  }
  if (!Number.isSafeInteger(resolved) || resolved < 1) {
    throw new Error('maxDeliveryAttempts must be a positive safe integer, Infinity, or 0');
  }
  return resolved;
}

function resolveConfig(config: PostgresPubSubConfig): ResolvedConfig {
  const logger = config.logger;
  const schema = config.schema ?? DEFAULT_SCHEMA;
  assertValidSchema(schema);

  return {
    schema,
    pollIntervalMs: integerOption('pollIntervalMs', config.pollIntervalMs, 1000, 1),
    ackDeadlineMs: integerOption('ackDeadlineMs', config.ackDeadlineMs, 30_000, 1),
    nackDelayMs: integerOption('nackDelayMs', config.nackDelayMs, 0, 0),
    maxDeliveryAttempts: maxDeliveryAttemptsOption(config.maxDeliveryAttempts, logger),
    batchSize: integerOption('batchSize', config.batchSize, 32, 1),
    maxEventsPerTopic: integerOption('maxEventsPerTopic', config.maxEventsPerTopic, 10_000, 0),
    cleanupIntervalMs: integerOption('cleanupIntervalMs', config.cleanupIntervalMs, 60_000, 0),
    staleSubscriptionMs: integerOption(
      'staleSubscriptionMs',
      config.staleSubscriptionMs,
      300_000,
      1,
    ),
    listen: config.listen ?? true,
    deadLetter: config.deadLetter ?? false,
    logger,
  };
}

/**
 * A PostgreSQL-backed {@link PubSub} implementation for Mastra.
 *
 * Provides at-least-once delivery with ack/nack and visibility timeouts,
 * competing-consumer groups and groupless fan-out, replay addressed by
 * per-topic `index`, optional dead-lettering, and low-latency `LISTEN/NOTIFY`
 * wakeups with polling as the correctness backstop. The only runtime
 * dependency is `pg`; `@mastra/core` is a peer dependency.
 */
export class PostgresPubSub extends PubSub {
  readonly #config: ResolvedConfig;
  readonly #pool: pg.Pool;
  readonly #ownsPool: boolean;
  readonly #logger: ResolvedConfig['logger'];
  readonly #instanceId: string;
  readonly #channel: string;

  readonly #subscriptions = new Map<string, Subscription>();
  readonly #subscriptionLocks = new Map<string, Promise<void>>();
  readonly #cbIndex = new WeakMap<EventCallback, Registration[]>();

  #listener: NotifyListener | undefined;
  #migrated: Promise<void> | undefined;
  #started: Promise<void> | undefined;
  #lifecycleLock: Promise<void> = Promise.resolve();
  #maintenanceTimer: NodeJS.Timeout | undefined;
  #closed = false;
  #pendingPublishes = new Set<Promise<void>>();

  /**
   * @param config - Adapter configuration. Provide exactly one of
   *   `connectionString` or `pool`.
   */
  constructor(config: PostgresPubSubConfig) {
    super();
    this.#config = resolveConfig(config);
    this.#logger = this.#config.logger;

    if (config.pool && config.connectionString) {
      throw new Error('Provide either connectionString or pool, not both');
    }
    if (config.pool) {
      this.#pool = config.pool;
      this.#ownsPool = false;
    } else if (config.connectionString) {
      this.#pool = new pg.Pool({ connectionString: config.connectionString });
      this.#ownsPool = true;
    } else {
      throw new Error('Either connectionString or pool is required');
    }

    this.#instanceId = randomUUID();
    this.#channel = notifyChannel(this.#config.schema);
    const context = traceAttributes({
      schema: this.#config.schema,
      listen: this.#config.listen,
      deadLetter: this.#config.deadLetter,
      ownsPool: this.#ownsPool,
      instanceId: this.#instanceId,
      channel: this.#channel,
    });
    logDebug(this.#logger, 'postgres pubsub initialized', context);
    observeEvent('pg_pubsub.instance.created', context);
  }

  #q(table: string): string {
    return `${quoteIdentifier(this.#config.schema)}.${quoteIdentifier(table)}`;
  }

  /**
   * Create the schema and tables explicitly. Idempotent and safe to call
   * concurrently across instances (serialized by an advisory lock). When not
   * called, migration happens lazily on first use.
   */
  async migrate(): Promise<void> {
    const span = startObservabilitySpan(
      'pg_pubsub.migrate',
      traceAttributes({
        schema: this.#config.schema,
        deadLetter: this.#config.deadLetter,
      }),
    );
    if (!this.#migrated) {
      logDebug(
        this.#logger,
        'migration started',
        traceAttributes({
          schema: this.#config.schema,
          deadLetter: this.#config.deadLetter,
        }),
      );
      const migration = runMigration(this.#pool, this.#config.schema, this.#config.deadLetter);
      this.#migrated = migration;
    } else {
      span.setAttribute('migration.cached', true);
    }
    const migration = this.#migrated;
    try {
      await migration;
      logDebug(
        this.#logger,
        'migration completed',
        traceAttributes({
          schema: this.#config.schema,
          deadLetter: this.#config.deadLetter,
        }),
      );
      span.end();
    } catch (error) {
      logError(
        this.#logger,
        'migration failed',
        traceAttributes({
          schema: this.#config.schema,
          deadLetter: this.#config.deadLetter,
        }),
        error,
      );
      if (this.#migrated === migration) {
        this.#migrated = undefined;
      }
      span.recordError(error);
      this.#migrated = undefined;
      span.end({ code: 'error', message: 'migration failed' });
      throw error;
    }
  }

  /**
   * Start the adapter lifecycle explicitly. This migrates the configured
   * schema and starts maintenance once. Direct PubSub methods call this lazily.
   */
  async start(): Promise<void> {
    const span = startObservabilitySpan(
      'pg_pubsub.lifecycle.start',
      traceAttributes({
        schema: this.#config.schema,
        cleanupIntervalMs: this.#config.cleanupIntervalMs,
        deadLetter: this.#config.deadLetter,
      }),
    );
    try {
      await this.#serializeLifecycle(async () => {
        if (this.#closed) {
          throw new Error('PostgresPubSub is closed');
        }
        const start = this.#getStartPromise(span);
        await this.#awaitStartPromise(start);
      });
      logDebug(
        this.#logger,
        'postgres pubsub started',
        traceAttributes({
          schema: this.#config.schema,
        }),
      );
      span.end();
    } catch (error) {
      const message =
        error instanceof Error && error.message === 'PostgresPubSub is closed'
          ? 'postgres pubsub start skipped for closed pubsub'
          : 'postgres pubsub start failed';
      logError(this.#logger, message, traceAttributes({ schema: this.#config.schema }), error);
      this.#started = undefined;
      span.recordError(error);
      span.end({ code: 'error', message: 'pubsub start failed' });
      throw error;
    }
  }

  /** Alias for hosts that look for a generic async init hook. */
  async init(): Promise<void> {
    await this.start();
  }

  #getStartPromise(span: ActiveObservabilitySpan): Promise<void> {
    if (this.#started) {
      span.setAttribute('lifecycle.start.cached', true);
      return this.#started;
    }
    logDebug(
      this.#logger,
      'postgres pubsub start started',
      traceAttributes({
        schema: this.#config.schema,
      }),
    );
    const start = this.#start();
    this.#started = start;
    return start;
  }

  async #awaitStartPromise(start: Promise<void>): Promise<void> {
    try {
      await start;
    } catch (error) {
      if (this.#started === start) {
        this.#started = undefined;
      }
      throw error;
    }
  }

  async #start(): Promise<void> {
    await this.migrate();
    this.#startMaintenance();
  }

  async #serializeLifecycle<T>(fn: () => Promise<T>): Promise<T> {
    const previous = this.#lifecycleLock;
    let release!: () => void;
    const next = new Promise<void>((resolve) => {
      release = resolve;
    });
    this.#lifecycleLock = previous.then(
      () => next,
      () => next,
    );
    await previous.catch(() => undefined);
    try {
      return await fn();
    } finally {
      release();
    }
  }

  async #ensureReady(): Promise<void> {
    await this.start();
  }

  /**
   * Publish an event to a topic. Assigns `id`, `createdAt`, and a per-topic
   * `index` inside a single transaction, fans out one delivery row per active
   * subscription, and emits a `NOTIFY` wakeup.
   *
   * @param topic - The topic to publish to.
   * @param event - Event without `id`/`createdAt` (assigned here).
   * @param _options - Accepted for interface compatibility; `localOnly` is
   *   ignored because delivery is always database-mediated.
   */
  override async publish(
    topic: string,
    event: Omit<Event, 'id' | 'createdAt'>,
    _options?: { localOnly?: boolean },
  ): Promise<void> {
    await this.#ensureReady();
    const task = this.#publish(topic, event);
    this.#pendingPublishes.add(task);
    try {
      await task;
    } finally {
      this.#pendingPublishes.delete(task);
    }
  }

  async #publish(topic: string, event: Omit<Event, 'id' | 'createdAt'>): Promise<void> {
    const id = randomUUID();
    const span = startObservabilitySpan(
      'pg_pubsub.publish',
      traceAttributes({
        topic,
        eventId: id,
        eventType: event.type,
        runId: event.runId,
      }),
    );
    logDebug(
      this.#logger,
      'publish started',
      traceAttributes({
        topic,
        eventId: id,
        eventType: event.type,
        runId: event.runId,
      }),
    );
    const client = await this.#pool.connect();
    let index: bigint | undefined;
    let deliveryCount = 0;
    try {
      await client.query('BEGIN');
      const indexResult = await client.query<{ next_index: string }>(
        `INSERT INTO ${this.#q('topics')} (topic, next_index)
         VALUES ($1, 1)
         ON CONFLICT (topic) DO UPDATE SET next_index = ${this.#q('topics')}.next_index + 1
         RETURNING next_index`,
        [topic],
      );
      index = BigInt(indexResult.rows[0]?.next_index ?? '1') - 1n;
      span.setAttribute('event.index', Number(index));

      const eventResult = await client.query<{ seq: string }>(
        `INSERT INTO ${this.#q('events')} (id, topic, index, type, run_id, data)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING seq`,
        [
          id,
          topic,
          index.toString(),
          event.type,
          event.runId,
          event.data === undefined ? null : JSON.stringify(event.data),
        ],
      );
      const seq = eventResult.rows[0]?.seq;

      const deliveryResult = await client.query(
        `INSERT INTO ${this.#q('deliveries')} (event_seq, subscription_id)
         SELECT $1, s.id FROM ${this.#q('subscriptions')} s WHERE s.topic = $2`,
        [seq, topic],
      );
      deliveryCount = deliveryResult.rowCount ?? 0;
      span.setAttribute('delivery.count', deliveryCount);

      await client.query(`SELECT pg_notify($1, $2)`, [this.#channel, topic]);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      logError(
        this.#logger,
        'publish failed',
        traceAttributes({
          topic,
          eventId: id,
          eventType: event.type,
          runId: event.runId,
          eventIndex: index === undefined ? undefined : Number(index),
        }),
        error,
      );
      span.recordError(error);
      span.end({ code: 'error', message: 'publish failed' });
      throw error;
    } finally {
      client.release();
    }

    logDebug(
      this.#logger,
      'published event',
      traceAttributes({
        topic,
        eventId: id,
        eventType: event.type,
        runId: event.runId,
        eventIndex: index === undefined ? undefined : Number(index),
        deliveryCount,
      }),
    );
    span.end();
    this.#wakeLocal(topic);
  }

  #wakeLocal(topic: string): void {
    let woken = 0;
    for (const sub of this.#subscriptions.values()) {
      if (sub.topic === topic) {
        sub.loop.wake();
        woken++;
      }
    }
    if (woken > 0) {
      const context = traceAttributes({
        topic,
        localSubscriptionCount: woken,
      });
      logDebug(this.#logger, 'woke local subscriptions', context);
      observeEvent('pg_pubsub.local_wake', context);
    }
  }

  /**
   * Subscribe to a topic. With `options.group`, members compete and each event
   * reaches exactly one member (round-robin across local callbacks). Without a
   * group, the subscription is private to this instance and receives every
   * event (fan-out).
   *
   * @param topic - The topic to subscribe to.
   * @param cb - Callback invoked per delivered event.
   * @param options - Subscribe options; `group` selects competing-consumer
   *   semantics. `batch` is ignored (no native batching).
   */
  override async subscribe(
    topic: string,
    cb: EventCallback,
    options?: SubscribeOptions,
  ): Promise<void> {
    await this.#subscribeInternal(topic, cb, cb, options?.group);
  }

  /**
   * Shared subscribe path. `userKey` is the callback the caller will later
   * pass to `unsubscribe`; `registered` is the callback actually invoked on
   * delivery (they differ only for the replay wrappers).
   */
  async #subscribeInternal(
    topic: string,
    userKey: EventCallback,
    registered: EventCallback,
    group: string | undefined,
    startImmediately = true,
  ): Promise<Subscription> {
    const subscriptionKind = group === undefined ? 'private' : 'group';
    const span = startObservabilitySpan(
      'pg_pubsub.subscribe',
      traceAttributes({
        topic,
        subscriptionKind,
        startImmediately,
      }),
    );
    let created = false;
    const subscriptionId = group
      ? groupSubscriptionId(topic, group)
      : `__private:${this.#instanceId}:${randomUUID()}`;
    const key = mapKey(topic, subscriptionId);
    try {
      await this.#ensureReady();
      return await this.#serializeSubscriptionSetup(key, async () => {
        let sub = this.#subscriptions.get(key);
        if (!sub) {
          const isGroup = group !== undefined;
          await this.#upsertSubscription(subscriptionId, topic, isGroup);
          const registry: CallbackRegistry = { callbacks: [], cursor: 0 };
          const loop = new ConsumeLoop(
            this.#pool,
            this.#config.schema,
            this.#config,
            topic,
            subscriptionId,
            isGroup,
            registry,
          );
          sub = {
            id: subscriptionId,
            topic,
            isGroup,
            registry,
            loop,
            started: false,
            unregisterWake: undefined,
          };
          this.#subscriptions.set(key, sub);
          created = true;
        }

        try {
          if (startImmediately) {
            await this.#startSubscription(sub);
          }
          this.#addCallbackRegistration(userKey, sub, registered);
          if (startImmediately) {
            sub.loop.wake();
          }
        } catch (error) {
          if (created) {
            await this.#rollbackCreatedSubscription(sub).catch((rollbackError) => {
              logWarn(
                this.#logger,
                'failed to roll back subscription after setup failure',
                traceAttributes({
                  topic,
                  subscriptionId,
                  subscriptionKind,
                }),
                rollbackError,
              );
            });
          }
          throw error;
        }
        const context = traceAttributes({
          topic,
          subscriptionId,
          subscriptionKind,
          callbackCount: sub.registry.callbacks.length,
          created,
          started: sub.started,
        });
        logDebug(this.#logger, 'subscription registered', context);
        span.setAttribute('subscription.id', subscriptionId);
        span.setAttribute('subscription.created', created);
        span.setAttribute('callback.count', sub.registry.callbacks.length);
        span.end();
        return sub;
      });
    } catch (error) {
      logError(
        this.#logger,
        'subscription registration failed',
        traceAttributes({
          topic,
          subscriptionId,
          subscriptionKind,
        }),
        error,
      );
      span.recordError(error);
      span.end({ code: 'error', message: 'subscription registration failed' });
      throw error;
    }
  }

  async #serializeSubscriptionSetup<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const previous = this.#subscriptionLocks.get(key) ?? Promise.resolve();
    let release!: () => void;
    const next = new Promise<void>((resolve) => {
      release = resolve;
    });
    const lock = previous.then(
      () => next,
      () => next,
    );
    this.#subscriptionLocks.set(key, lock);
    await previous.catch(() => undefined);
    try {
      return await fn();
    } finally {
      release();
      if (this.#subscriptionLocks.get(key) === lock) {
        this.#subscriptionLocks.delete(key);
      }
    }
  }

  #addCallbackRegistration(
    userKey: EventCallback,
    sub: Subscription,
    registered: EventCallback,
  ): void {
    sub.registry.callbacks.push(registered);
    let registrations = this.#cbIndex.get(userKey);
    if (!registrations) {
      registrations = [];
      this.#cbIndex.set(userKey, registrations);
    }
    registrations.push({ sub, registered });
  }

  async #rollbackCreatedSubscription(sub: Subscription): Promise<void> {
    this.#subscriptions.delete(mapKey(sub.topic, sub.id));
    sub.unregisterWake?.();
    await sub.loop.stop();
    if (!sub.isGroup) {
      await this.#pool.query(`DELETE FROM ${this.#q('subscriptions')} WHERE id = $1`, [sub.id]);
    }
  }

  async #startSubscription(sub: Subscription): Promise<void> {
    if (sub.started) {
      return;
    }
    if (this.#config.listen) {
      await this.#registerWake(sub);
    }
    sub.loop.start();
    sub.started = true;
    const context = traceAttributes({
      topic: sub.topic,
      subscriptionId: sub.id,
      subscriptionKind: sub.isGroup ? 'group' : 'private',
      listen: this.#config.listen,
    });
    logDebug(this.#logger, 'subscription loop started', context);
    observeEvent('pg_pubsub.subscription.started', context);
  }

  async #registerWake(sub: Subscription): Promise<void> {
    if (!this.#listener) {
      this.#listener = new NotifyListener(this.#pool, this.#config.schema, this.#logger);
    }
    sub.unregisterWake = await this.#listener.register(sub.topic, () => sub.loop.wake());
    const context = traceAttributes({
      topic: sub.topic,
      subscriptionId: sub.id,
      subscriptionKind: sub.isGroup ? 'group' : 'private',
    });
    logDebug(this.#logger, 'subscription wake registered', context);
    observeEvent('pg_pubsub.subscription.wake_registered', context);
  }

  async #upsertSubscription(id: string, topic: string, isGroup: boolean): Promise<void> {
    await this.#pool.query(
      `INSERT INTO ${this.#q('subscriptions')} (id, topic, is_group, last_seen_at)
       VALUES ($1, $2, $3, now())
       ON CONFLICT (id) DO UPDATE SET last_seen_at = now()`,
      [id, topic, isGroup],
    );
  }

  /**
   * Remove a callback from a topic. Tears down the underlying subscription
   * (and, for private subscriptions, deletes its database rows) once no local
   * callbacks remain.
   *
   * @param topic - The topic the callback was subscribed to.
   * @param cb - The callback to remove.
   */
  override async unsubscribe(topic: string, cb: EventCallback): Promise<void> {
    const span = startObservabilitySpan(
      'pg_pubsub.unsubscribe',
      traceAttributes({
        topic,
      }),
    );
    const registrations = this.#cbIndex.get(cb);
    if (!registrations) {
      logDebug(
        this.#logger,
        'unsubscribe skipped for unknown callback',
        traceAttributes({
          topic,
        }),
      );
      span.setAttribute('unsubscribe.matched', false);
      span.end();
      return;
    }
    const remaining: Registration[] = [];
    let removedCallbacks = 0;
    let tornDownSubscriptions = 0;
    for (const reg of registrations) {
      if (reg.sub.topic !== topic) {
        remaining.push(reg);
        continue;
      }
      const idx = reg.sub.registry.callbacks.indexOf(reg.registered);
      if (idx !== -1) {
        reg.sub.registry.callbacks.splice(idx, 1);
        removedCallbacks++;
      }
      if (reg.sub.registry.callbacks.length === 0) {
        await this.#teardownSubscription(reg.sub);
        tornDownSubscriptions++;
      }
    }
    if (remaining.length === 0) {
      this.#cbIndex.delete(cb);
    } else {
      this.#cbIndex.set(cb, remaining);
    }
    const context = traceAttributes({
      topic,
      removedCallbacks,
      tornDownSubscriptions,
      remainingRegistrations: remaining.length,
    });
    logDebug(this.#logger, 'unsubscribe completed', context);
    span.setAttribute('unsubscribe.matched', removedCallbacks > 0);
    span.setAttribute('callback.removed_count', removedCallbacks);
    span.setAttribute('subscription.torn_down_count', tornDownSubscriptions);
    if (tornDownSubscriptions > 0 && this.#subscriptions.size === 0) {
      await this.#stopIdleResources();
    }
    span.end();
  }

  async #teardownSubscription(sub: Subscription): Promise<void> {
    const key = mapKey(sub.topic, sub.id);
    this.#subscriptions.delete(key);
    sub.unregisterWake?.();
    await sub.loop.stop();
    const context = traceAttributes({
      topic: sub.topic,
      subscriptionId: sub.id,
      subscriptionKind: sub.isGroup ? 'group' : 'private',
    });
    logDebug(this.#logger, 'subscription torn down', context);
    observeEvent('pg_pubsub.subscription.torn_down', context);
    if (!sub.isGroup) {
      await this.#pool
        .query(`DELETE FROM ${this.#q('subscriptions')} WHERE id = $1`, [sub.id])
        .catch((error) =>
          logWarn(
            this.#logger,
            'failed to delete private subscription',
            traceAttributes({
              topic: sub.topic,
              subscriptionId: sub.id,
              subscriptionKind: 'private',
            }),
            error,
          ),
        );
    }
  }

  async #stopIdleResources(): Promise<void> {
    const span = startObservabilitySpan(
      'pg_pubsub.lifecycle.idle_stop',
      traceAttributes({
        schema: this.#config.schema,
        listen: this.#config.listen,
        cleanupIntervalMs: this.#config.cleanupIntervalMs,
        subscriptionCount: this.#subscriptions.size,
      }),
    );
    try {
      await this.#serializeLifecycle(async () => {
        if (this.#closed || this.#subscriptions.size > 0) {
          span.setAttribute('lifecycle.idle_stop.skipped', true);
          return;
        }

        if (this.#maintenanceTimer) {
          clearInterval(this.#maintenanceTimer);
          this.#maintenanceTimer = undefined;
        }

        if (this.#listener) {
          await this.#listener.close();
          this.#listener = undefined;
        }

        this.#started = undefined;
      });

      const context = traceAttributes({
        schema: this.#config.schema,
        listen: this.#config.listen,
        cleanupIntervalMs: this.#config.cleanupIntervalMs,
      });
      logDebug(this.#logger, 'postgres pubsub idle resources stopped', context);
      observeEvent('pg_pubsub.lifecycle.idle_stopped', context);
      span.end();
    } catch (error) {
      logWarn(
        this.#logger,
        'postgres pubsub idle resource stop failed',
        traceAttributes({
          schema: this.#config.schema,
        }),
        error,
      );
      span.recordError(error);
      span.end({ code: 'error', message: 'idle stop failed' });
    }
  }

  /**
   * Resolve once all in-flight publishes and local deliveries have settled.
   * Per-event callback errors are logged, never thrown. If locally-owned
   * deliveries remain unsettled after the bounded drain window, reject so
   * shutdown callers do not mistake a stuck subscriber for a clean drain.
   */
  override async flush(): Promise<void> {
    const span = startObservabilitySpan(
      'pg_pubsub.flush',
      traceAttributes({
        pendingPublishes: this.#pendingPublishes.size,
        subscriptionCount: this.#subscriptions.size,
      }),
    );
    try {
      await Promise.allSettled([...this.#pendingPublishes]);
      let pending = 0;
      for (let pass = 0; pass < 50; pass++) {
        await Promise.all([...this.#subscriptions.values()].map((s) => s.loop.drain()));
        pending = await this.#countPending();
        if (pending === 0) {
          const context = traceAttributes({
            subscriptionCount: this.#subscriptions.size,
            pendingDeliveries: pending,
            passCount: pass + 1,
          });
          logDebug(this.#logger, 'flush completed', context);
          span.setAttribute('pending.deliveries', pending);
          span.setAttribute('flush.pass_count', pass + 1);
          span.end();
          return;
        }
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      const error = new Error(
        `PostgresPubSub flush timed out with ${pending} unsettled deliveries`,
      );
      logWarn(
        this.#logger,
        'flush timed out',
        traceAttributes({
          subscriptionCount: this.#subscriptions.size,
          pendingDeliveries: pending,
        }),
        error,
      );
      span.recordError(error);
      span.end({ code: 'error', message: 'flush timed out' });
      throw error;
    } catch (error) {
      if (!(error instanceof Error && error.message.includes('flush timed out'))) {
        logError(
          this.#logger,
          'flush failed',
          traceAttributes({
            subscriptionCount: this.#subscriptions.size,
          }),
          error,
        );
        span.recordError(error);
        span.end({ code: 'error', message: 'flush failed' });
      }
      throw error;
    }
  }

  async #countPending(): Promise<number> {
    const ids = [...this.#subscriptions.values()].map((s) => s.id);
    if (ids.length === 0) {
      return 0;
    }
    const result = await this.#pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM ${this.#q('deliveries')}
       WHERE subscription_id = ANY($1::text[])`,
      [ids],
    );
    return Number(result.rows[0]?.count ?? '0');
  }

  /**
   * Fetch historical events for a topic with `index >= offset`, ordered by
   * `index`.
   *
   * @param topic - The topic to read history for.
   * @param offset - Inclusive starting `index`. Defaults to `0`.
   * @returns Events in ascending `index` order.
   */
  override async getHistory(topic: string, offset = 0): Promise<Event[]> {
    const span = startObservabilitySpan(
      'pg_pubsub.get_history',
      traceAttributes({
        topic,
        offset,
      }),
    );
    try {
      await this.#ensureReady();
      const result = await this.#pool.query<EventRow>(
        `SELECT id, topic, index, type, run_id, data, created_at
         FROM ${this.#q('events')}
         WHERE topic = $1 AND index >= $2
         ORDER BY index`,
        [topic, offset],
      );
      const events = result.rows.map(rowToEvent);
      const context = traceAttributes({
        topic,
        offset,
        historyCount: events.length,
      });
      logDebug(this.#logger, 'history fetched', context);
      span.setAttribute('history.count', events.length);
      span.end();
      return events;
    } catch (error) {
      logError(
        this.#logger,
        'history fetch failed',
        traceAttributes({
          topic,
          offset,
        }),
        error,
      );
      span.recordError(error);
      span.end({ code: 'error', message: 'history fetch failed' });
      throw error;
    }
  }

  /**
   * Subscribe and replay all history first, then continue live, deduping the
   * boundary by event `index` so no event is delivered twice.
   *
   * @param topic - The topic to subscribe to.
   * @param cb - Callback for replayed and live events.
   */
  override async subscribeWithReplay(topic: string, cb: EventCallback): Promise<void> {
    await this.subscribeFromOffset(topic, 0, cb);
  }

  /**
   * Subscribe and replay history starting at `offset`, then continue live,
   * deduping the boundary by event `index`.
   *
   * @param topic - The topic to subscribe to.
   * @param offset - Inclusive starting `index` for replay.
   * @param cb - Callback for replayed and live events.
   */
  override async subscribeFromOffset(
    topic: string,
    offset: number,
    cb: EventCallback,
  ): Promise<void> {
    const span = startObservabilitySpan(
      'pg_pubsub.subscribe_from_offset',
      traceAttributes({
        topic,
        offset,
      }),
    );
    const seen = new Set<number>();
    let live = false;
    const wrapped: EventCallback = (event, ack, nack) => {
      if (live && event.index !== undefined) {
        seen.add(event.index);
      }
      return cb(event, ack, nack);
    };

    // Create the live subscription row first so events published during replay
    // get delivery rows, but keep the consume loop paused until historical
    // callbacks run in order. Replayed deliveries are then settled for this
    // private subscription to avoid duplicate live delivery at the boundary.
    const sub = await this.#subscribeInternal(topic, cb, wrapped, undefined, false);
    span.setAttribute('subscription.id', sub.id);

    try {
      const replayedIndexes: number[] = [];
      let replayCallbackErrorCount = 0;
      const history = await this.getHistory(topic, offset);
      for (const event of history) {
        if (event.index !== undefined && seen.has(event.index)) {
          continue;
        }
        try {
          await cb(event);
          if (event.index !== undefined) {
            replayedIndexes.push(event.index);
          }
        } catch (error) {
          replayCallbackErrorCount++;
          logError(
            this.#logger,
            'replay callback threw',
            traceAttributes({
              topic,
              eventId: event.id,
              eventType: event.type,
              eventIndex: event.index,
              runId: event.runId,
              subscriptionId: sub.id,
            }),
            error,
          );
        }
      }
      await this.#ackReplayedDeliveries(topic, sub.id, replayedIndexes);

      live = true;
      await this.#startSubscription(sub);
      sub.loop.wake();
      const context = traceAttributes({
        topic,
        offset,
        subscriptionId: sub.id,
        replayedCount: replayedIndexes.length,
        replayCallbackErrorCount,
        replayCallbackStatus: replayCallbackErrorCount > 0 ? 'error' : 'ok',
      });
      logDebug(this.#logger, 'subscribe from offset completed', context);
      span.setAttribute('replayed.count', replayedIndexes.length);
      span.setAttribute('replay.callback_error_count', replayCallbackErrorCount);
      span.setAttribute('replay.callback_status', replayCallbackErrorCount > 0 ? 'error' : 'ok');
      span.end();
    } catch (error) {
      try {
        await this.unsubscribe(topic, cb);
      } catch (teardownError) {
        logWarn(
          this.#logger,
          'failed to clean up replay subscription after setup failure',
          traceAttributes({
            topic,
            offset,
            subscriptionId: sub.id,
          }),
          teardownError,
        );
      }
      logError(
        this.#logger,
        'subscribe from offset failed',
        traceAttributes({
          topic,
          offset,
          subscriptionId: sub.id,
        }),
        error,
      );
      span.recordError(error);
      span.end({ code: 'error', message: 'subscribe from offset failed' });
      throw error;
    }
  }

  async #ackReplayedDeliveries(
    topic: string,
    subscriptionId: string,
    indexes: number[],
  ): Promise<void> {
    if (indexes.length === 0) {
      return;
    }
    const result = await this.#pool.query(
      `DELETE FROM ${this.#q('deliveries')} d
       USING ${this.#q('events')} e
       WHERE d.event_seq = e.seq
         AND d.subscription_id = $1
         AND e.topic = $2
         AND e.index = ANY($3::bigint[])`,
      [subscriptionId, topic, indexes],
    );
    const context = traceAttributes({
      topic,
      subscriptionId,
      replayedCount: indexes.length,
      settledDeliveryCount: result.rowCount ?? 0,
    });
    logDebug(this.#logger, 'replayed deliveries settled', context);
    observeEvent('pg_pubsub.replay.settled', context);
  }

  #startMaintenance(): void {
    if (this.#maintenanceTimer || this.#config.cleanupIntervalMs <= 0 || this.#closed) {
      return;
    }
    this.#maintenanceTimer = setInterval(() => {
      this.#runMaintenance().catch(() => undefined);
    }, this.#config.cleanupIntervalMs);
    this.#maintenanceTimer.unref?.();
    const context = traceAttributes({
      cleanupIntervalMs: this.#config.cleanupIntervalMs,
      staleSubscriptionMs: this.#config.staleSubscriptionMs,
      maxEventsPerTopic: this.#config.maxEventsPerTopic,
    });
    logDebug(this.#logger, 'maintenance started', context);
    observeEvent('pg_pubsub.maintenance.started', context);
  }

  async #runMaintenance(): Promise<void> {
    const span = startObservabilitySpan(
      'pg_pubsub.maintenance.cycle',
      traceAttributes({
        schema: this.#config.schema,
      }),
    );
    try {
      const heartbeatCount = await this.#heartbeat();
      const prunedSubscriptions = await this.#pruneStaleSubscriptions();
      const trimmedEvents = await this.#trimRetention();
      const context = traceAttributes({
        schema: this.#config.schema,
        heartbeatCount,
        prunedSubscriptions,
        trimmedEvents,
      });
      logDebug(this.#logger, 'maintenance cycle completed', context);
      span.setAttribute('heartbeat.count', heartbeatCount);
      span.setAttribute('subscription.pruned_count', prunedSubscriptions);
      span.setAttribute('event.trimmed_count', trimmedEvents);
      span.end();
    } catch (error) {
      logWarn(
        this.#logger,
        'maintenance cycle failed',
        traceAttributes({
          schema: this.#config.schema,
        }),
        error,
      );
      span.recordError(error);
      span.end({ code: 'error', message: 'maintenance cycle failed' });
      throw error;
    }
  }

  async #heartbeat(): Promise<number> {
    const ids = [...this.#subscriptions.values()].filter((s) => !s.isGroup).map((s) => s.id);
    if (ids.length === 0) {
      return 0;
    }
    const result = await this.#pool.query(
      `UPDATE ${this.#q('subscriptions')} SET last_seen_at = now() WHERE id = ANY($1::text[])`,
      [ids],
    );
    return result.rowCount ?? 0;
  }

  async #pruneStaleSubscriptions(): Promise<number> {
    const result = await this.#pool.query(
      `DELETE FROM ${this.#q('subscriptions')}
       WHERE is_group = false
         AND id LIKE '__private:%'
         AND last_seen_at < now() - ($1::double precision * interval '1 millisecond')`,
      [this.#config.staleSubscriptionMs],
    );
    return result.rowCount ?? 0;
  }

  async #trimRetention(): Promise<number> {
    if (this.#config.maxEventsPerTopic <= 0) {
      return 0;
    }
    const result = await this.#pool.query(
      `WITH ranked AS (
         SELECT e.seq,
                row_number() OVER (PARTITION BY e.topic ORDER BY e.index DESC) AS rn
         FROM ${this.#q('events')} e
       )
       DELETE FROM ${this.#q('events')} e
       USING ranked
       WHERE e.seq = ranked.seq
         AND ranked.rn > $1
         AND NOT EXISTS (
           SELECT 1 FROM ${this.#q('deliveries')} d WHERE d.event_seq = e.seq
         )`,
      [this.#config.maxEventsPerTopic],
    );
    return result.rowCount ?? 0;
  }

  #clearMaintenanceTimer(): void {
    if (!this.#maintenanceTimer) {
      return;
    }
    clearInterval(this.#maintenanceTimer);
    this.#maintenanceTimer = undefined;
  }

  async #settlePendingPublishes(): Promise<void> {
    await Promise.allSettled([...this.#pendingPublishes]);
  }

  async #stopSubscriptionLoops(): Promise<void> {
    for (const sub of this.#subscriptions.values()) {
      sub.unregisterWake?.();
    }
    await Promise.all([...this.#subscriptions.values()].map((s) => s.loop.stop()));
  }

  async #closeListener(): Promise<void> {
    if (!this.#listener) {
      return;
    }
    await this.#listener.close();
    this.#listener = undefined;
  }

  async #clearSubscriptions(): Promise<string[]> {
    const privateIds = [...this.#subscriptions.values()].filter((s) => !s.isGroup).map((s) => s.id);
    this.#subscriptions.clear();
    if (privateIds.length > 0) {
      await this.#pool
        .query(`DELETE FROM ${this.#q('subscriptions')} WHERE id = ANY($1::text[])`, [privateIds])
        .catch((error) =>
          logWarn(
            this.#logger,
            'failed to delete private subscriptions',
            traceAttributes({
              privateSubscriptionCount: privateIds.length,
            }),
            error,
          ),
        );
    }
    return privateIds;
  }

  async #closeOwnedPool(): Promise<void> {
    if (this.#ownsPool) {
      await this.#pool.end();
    }
  }

  /**
   * Stop all loops, release the listener, delete this instance's private
   * subscriptions, and end the pool when the adapter created it. Idempotent.
   */
  async close(): Promise<void> {
    const span = startObservabilitySpan(
      'pg_pubsub.close',
      traceAttributes({
        schema: this.#config.schema,
        ownsPool: this.#ownsPool,
        subscriptionCount: this.#subscriptions.size,
      }),
    );
    if (this.#closed) {
      logDebug(
        this.#logger,
        'close skipped for already closed pubsub',
        traceAttributes({
          schema: this.#config.schema,
          ownsPool: this.#ownsPool,
        }),
      );
      span.setAttribute('close.already_closed', true);
      span.end();
      return;
    }
    try {
      this.#closed = true;

      this.#clearMaintenanceTimer();
      await this.#settlePendingPublishes();
      await this.#stopSubscriptionLoops();
      await this.#closeListener();

      const privateIds = await this.#clearSubscriptions();
      await this.#closeOwnedPool();
      const context = traceAttributes({
        schema: this.#config.schema,
        ownsPool: this.#ownsPool,
        privateSubscriptionCount: privateIds.length,
      });
      logDebug(this.#logger, 'postgres pubsub closed', context);
      span.setAttribute('private_subscription.count', privateIds.length);
      span.end();
    } catch (error) {
      logError(
        this.#logger,
        'close failed',
        traceAttributes({
          schema: this.#config.schema,
          ownsPool: this.#ownsPool,
        }),
        error,
      );
      span.recordError(error);
      span.end({ code: 'error', message: 'close failed' });
      throw error;
    }
  }
}

function rowToEvent(row: EventRow): Event {
  return {
    id: row.id,
    type: row.type,
    data: row.data,
    runId: row.run_id,
    createdAt: row.created_at,
    index: Number(row.index),
  };
}
