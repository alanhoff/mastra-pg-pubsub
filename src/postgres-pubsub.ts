import { randomUUID } from 'node:crypto';
import type { Event, EventCallback, SubscribeOptions } from '@mastra/core/events';
import { PubSub } from '@mastra/core/events';
import pg from 'pg';
import { type CallbackRegistry, ConsumeLoop } from './consume-loop.ts';
import { NotifyListener } from './listener.ts';
import { runMigration } from './schema.ts';
import { assertValidSchema, notifyChannel, quoteIdentifier } from './sql.ts';
import type { PostgresPubSubConfig, PubSubLogger, ResolvedConfig } from './types.ts';

const DEFAULT_SCHEMA = 'mastra_pubsub';

interface Subscription {
  readonly id: string;
  readonly topic: string;
  readonly isGroup: boolean;
  readonly registry: CallbackRegistry;
  readonly loop: ConsumeLoop;
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

function resolveConfig(config: PostgresPubSubConfig): ResolvedConfig {
  const logger = config.logger ?? {};
  const schema = config.schema ?? DEFAULT_SCHEMA;
  assertValidSchema(schema);

  let maxDeliveryAttempts = config.maxDeliveryAttempts ?? 5;
  if (maxDeliveryAttempts === 0) {
    logger.warn?.('maxDeliveryAttempts=0 is treated as Infinity (unbounded redelivery)');
    maxDeliveryAttempts = Number.POSITIVE_INFINITY;
  }

  return {
    schema,
    pollIntervalMs: config.pollIntervalMs ?? 1000,
    ackDeadlineMs: config.ackDeadlineMs ?? 30_000,
    nackDelayMs: config.nackDelayMs ?? 0,
    maxDeliveryAttempts,
    batchSize: config.batchSize ?? 32,
    maxEventsPerTopic: config.maxEventsPerTopic ?? 10_000,
    cleanupIntervalMs: config.cleanupIntervalMs ?? 60_000,
    staleSubscriptionMs: config.staleSubscriptionMs ?? 300_000,
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
  readonly #logger: PubSubLogger;
  readonly #instanceId: string;
  readonly #channel: string;

  readonly #subscriptions = new Map<string, Subscription>();
  readonly #cbIndex = new WeakMap<EventCallback, Registration[]>();

  #listener: NotifyListener | undefined;
  #migrated: Promise<void> | undefined;
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
    if (!this.#migrated) {
      this.#migrated = runMigration(this.#pool, this.#config.schema, this.#config.deadLetter);
    }
    await this.#migrated;
  }

  async #ensureReady(): Promise<void> {
    if (this.#closed) {
      throw new Error('PostgresPubSub is closed');
    }
    await this.migrate();
    this.#startMaintenance();
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
    const client = await this.#pool.connect();
    try {
      await client.query('BEGIN');
      const indexResult = await client.query<{ next_index: string }>(
        `INSERT INTO ${this.#q('topics')} (topic, next_index)
         VALUES ($1, 1)
         ON CONFLICT (topic) DO UPDATE SET next_index = ${this.#q('topics')}.next_index + 1
         RETURNING next_index`,
        [topic],
      );
      const index = BigInt(indexResult.rows[0]?.next_index ?? '1') - 1n;

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

      await client.query(
        `INSERT INTO ${this.#q('deliveries')} (event_seq, subscription_id)
         SELECT $1, s.id FROM ${this.#q('subscriptions')} s WHERE s.topic = $2`,
        [seq, topic],
      );

      await client.query(`SELECT pg_notify($1, $2)`, [this.#channel, topic]);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }

    this.#wakeLocal(topic);
  }

  #wakeLocal(topic: string): void {
    for (const sub of this.#subscriptions.values()) {
      if (sub.topic === topic) {
        sub.loop.wake();
      }
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
  ): Promise<void> {
    await this.#ensureReady();
    const subscriptionId = group ?? `__private:${this.#instanceId}:${randomUUID()}`;
    const key = `${topic} ${subscriptionId}`;

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
      sub = { id: subscriptionId, topic, isGroup, registry, loop, unregisterWake: undefined };
      this.#subscriptions.set(key, sub);
      loop.start();
      if (this.#config.listen) {
        await this.#registerWake(sub);
      }
    }

    sub.registry.callbacks.push(registered);
    let registrations = this.#cbIndex.get(userKey);
    if (!registrations) {
      registrations = [];
      this.#cbIndex.set(userKey, registrations);
    }
    registrations.push({ sub, registered });
    sub.loop.wake();
  }

  async #registerWake(sub: Subscription): Promise<void> {
    if (!this.#listener) {
      this.#listener = new NotifyListener(this.#pool, this.#config.schema, this.#logger);
    }
    sub.unregisterWake = await this.#listener.register(sub.topic, () => sub.loop.wake());
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
    const registrations = this.#cbIndex.get(cb);
    if (!registrations) {
      return;
    }
    const remaining: Registration[] = [];
    for (const reg of registrations) {
      if (reg.sub.topic !== topic) {
        remaining.push(reg);
        continue;
      }
      const idx = reg.sub.registry.callbacks.indexOf(reg.registered);
      if (idx !== -1) {
        reg.sub.registry.callbacks.splice(idx, 1);
      }
      if (reg.sub.registry.callbacks.length === 0) {
        await this.#teardownSubscription(reg.sub);
      }
    }
    if (remaining.length === 0) {
      this.#cbIndex.delete(cb);
    } else {
      this.#cbIndex.set(cb, remaining);
    }
  }

  async #teardownSubscription(sub: Subscription): Promise<void> {
    const key = `${sub.topic} ${sub.id}`;
    this.#subscriptions.delete(key);
    sub.unregisterWake?.();
    await sub.loop.stop();
    if (!sub.isGroup) {
      await this.#pool
        .query(`DELETE FROM ${this.#q('subscriptions')} WHERE id = $1`, [sub.id])
        .catch((error) => this.#logger.warn?.('failed to delete private subscription', error));
    }
  }

  /**
   * Resolve once all in-flight publishes and claimable deliveries have settled.
   * Per-event callback errors are logged, never thrown.
   */
  override async flush(): Promise<void> {
    await Promise.allSettled([...this.#pendingPublishes]);
    let pass = 0;
    while (pass < 50) {
      await Promise.all([...this.#subscriptions.values()].map((s) => s.loop.drain()));
      const pending = await this.#countPending();
      if (pending === 0) {
        return;
      }
      pass++;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }

  async #countPending(): Promise<number> {
    const ids = [...this.#subscriptions.values()].map((s) => s.id);
    if (ids.length === 0) {
      return 0;
    }
    const result = await this.#pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM ${this.#q('deliveries')}
       WHERE subscription_id = ANY($1::text[]) AND visible_at <= now()`,
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
    await this.#ensureReady();
    const result = await this.#pool.query<EventRow>(
      `SELECT id, topic, index, type, run_id, data, created_at
       FROM ${this.#q('events')}
       WHERE topic = $1 AND index >= $2
       ORDER BY index`,
      [topic, offset],
    );
    return result.rows.map(rowToEvent);
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
    const seen = new Set<number>();
    let live = false;
    const wrapped: EventCallback = (event, ack, nack) => {
      if (live && event.index !== undefined) {
        seen.add(event.index);
      }
      return cb(event, ack, nack);
    };

    // Register live first (keyed by the user's cb for unsubscribe), so no
    // event published during replay is missed.
    await this.#subscribeInternal(topic, cb, wrapped, undefined);
    live = true;

    const history = await this.getHistory(topic, offset);
    for (const event of history) {
      if (event.index !== undefined && seen.has(event.index)) {
        continue;
      }
      cb(event);
    }
  }

  #startMaintenance(): void {
    if (this.#maintenanceTimer || this.#config.cleanupIntervalMs <= 0 || this.#closed) {
      return;
    }
    this.#maintenanceTimer = setInterval(() => {
      this.#runMaintenance().catch((error) =>
        this.#logger.warn?.('maintenance cycle failed', error),
      );
    }, this.#config.cleanupIntervalMs);
    this.#maintenanceTimer.unref?.();
  }

  async #runMaintenance(): Promise<void> {
    await this.#heartbeat();
    await this.#pruneStaleSubscriptions();
    await this.#trimRetention();
  }

  async #heartbeat(): Promise<void> {
    const ids = [...this.#subscriptions.values()].filter((s) => !s.isGroup).map((s) => s.id);
    if (ids.length === 0) {
      return;
    }
    await this.#pool.query(
      `UPDATE ${this.#q('subscriptions')} SET last_seen_at = now() WHERE id = ANY($1::text[])`,
      [ids],
    );
  }

  async #pruneStaleSubscriptions(): Promise<void> {
    await this.#pool.query(
      `DELETE FROM ${this.#q('subscriptions')}
       WHERE is_group = false
         AND id LIKE '__private:%'
         AND last_seen_at < now() - ($1::double precision * interval '1 millisecond')`,
      [this.#config.staleSubscriptionMs],
    );
  }

  async #trimRetention(): Promise<void> {
    if (this.#config.maxEventsPerTopic <= 0) {
      return;
    }
    await this.#pool.query(
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
  }

  /**
   * Stop all loops, release the listener, delete this instance's private
   * subscriptions, and end the pool when the adapter created it. Idempotent.
   */
  async close(): Promise<void> {
    if (this.#closed) {
      return;
    }
    this.#closed = true;

    if (this.#maintenanceTimer) {
      clearInterval(this.#maintenanceTimer);
      this.#maintenanceTimer = undefined;
    }

    await Promise.allSettled([...this.#pendingPublishes]);

    for (const sub of this.#subscriptions.values()) {
      sub.unregisterWake?.();
    }
    await Promise.all([...this.#subscriptions.values()].map((s) => s.loop.stop()));

    if (this.#listener) {
      await this.#listener.close();
      this.#listener = undefined;
    }

    const privateIds = [...this.#subscriptions.values()].filter((s) => !s.isGroup).map((s) => s.id);
    this.#subscriptions.clear();
    if (privateIds.length > 0) {
      await this.#pool
        .query(`DELETE FROM ${this.#q('subscriptions')} WHERE id = ANY($1::text[])`, [privateIds])
        .catch((error) => this.#logger.warn?.('failed to delete private subscriptions', error));
    }

    if (this.#ownsPool) {
      await this.#pool.end();
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
