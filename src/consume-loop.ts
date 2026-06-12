import type { Event, EventCallback } from '@mastra/core/events';
import type { Pool } from 'pg';
import { quoteIdentifier } from './sql.ts';
import type { ResolvedConfig } from './types.ts';

interface ClaimedRow {
  seq: string;
  event_id: string;
  index: string;
  type: string;
  run_id: string;
  data: unknown;
  created_at: Date;
  delivery_attempt: number;
}

/**
 * Round-robin set of local callbacks bound to one (topic, subscription) pair.
 * Group subscriptions round-robin a claimed event across local members;
 * private (fan-out) subscriptions deliver every event to their single member.
 */
export interface CallbackRegistry {
  /** Ordered callbacks; group loops round-robin, private loops use index 0. */
  readonly callbacks: EventCallback[];
  /** Rotating cursor for round-robin group dispatch. */
  cursor: number;
}

/**
 * One consume loop per (topic, subscription id). Claims batches with
 * `FOR UPDATE SKIP LOCKED`, extends visibility, delivers sequentially, and
 * settles deliveries via ack/nack or visibility timeout. Woken by
 * `LISTEN/NOTIFY` and by a polling backstop that also reclaims expired
 * deliveries.
 */
export class ConsumeLoop {
  readonly #pool: Pool;
  readonly #schema: string;
  readonly #config: ResolvedConfig;
  readonly #topic: string;
  readonly #subscriptionId: string;
  readonly #isGroup: boolean;
  readonly #registry: CallbackRegistry;

  #stopped = false;
  #wakeRequested = false;
  #wake: (() => void) | undefined;
  #idle: Promise<void>;
  #resolveIdle: (() => void) | undefined;
  #inFlight = 0;
  #loopPromise: Promise<void> | undefined;

  /**
   * @param pool - Connection pool.
   * @param schema - Validated schema name.
   * @param config - Resolved configuration.
   * @param topic - Subscribed topic.
   * @param subscriptionId - Subscription row id (group name or private id).
   * @param isGroup - Whether this is a competing-consumer group.
   * @param registry - Shared local callback registry for this subscription.
   */
  constructor(
    pool: Pool,
    schema: string,
    config: ResolvedConfig,
    topic: string,
    subscriptionId: string,
    isGroup: boolean,
    registry: CallbackRegistry,
  ) {
    this.#pool = pool;
    this.#schema = schema;
    this.#config = config;
    this.#topic = topic;
    this.#subscriptionId = subscriptionId;
    this.#isGroup = isGroup;
    this.#registry = registry;
    this.#idle = Promise.resolve();
  }

  #q(table: string): string {
    return `${quoteIdentifier(this.#schema)}.${quoteIdentifier(table)}`;
  }

  /** Start the background loop. Safe to call once. */
  start(): void {
    if (this.#loopPromise) {
      return;
    }
    this.#loopPromise = this.#run();
  }

  /** Request an immediate poll (used by NOTIFY wakeups). */
  wake(): void {
    this.#wakeRequested = true;
    if (this.#wake) {
      const fn = this.#wake;
      this.#wake = undefined;
      fn();
    }
  }

  /**
   * Resolve once the loop is idle with no claimable work and no in-flight
   * deliveries — the basis for `flush()`.
   */
  async drain(): Promise<void> {
    this.wake();
    await this.#idle;
  }

  async #run(): Promise<void> {
    while (!this.#stopped) {
      this.#wakeRequested = false;
      let delivered = 0;
      try {
        delivered = await this.#tick();
      } catch (error) {
        if (!this.#stopped) {
          this.#config.logger.error?.('consume loop tick failed', error);
        }
      }
      if (this.#stopped) {
        break;
      }
      if (delivered > 0 || this.#wakeRequested) {
        continue;
      }
      this.#markIdle();
      await this.#sleep();
    }
    this.#markIdle();
  }

  #markIdle(): void {
    if (this.#inFlight === 0 && this.#resolveIdle) {
      const resolve = this.#resolveIdle;
      this.#resolveIdle = undefined;
      resolve();
    }
  }

  #armIdle(): void {
    if (!this.#resolveIdle) {
      this.#idle = new Promise<void>((resolve) => {
        this.#resolveIdle = resolve;
      });
    }
  }

  async #sleep(): Promise<void> {
    this.#armIdle();
    if (this.#wakeRequested || this.#stopped) {
      return;
    }
    await new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        this.#wake = undefined;
        resolve();
      }, this.#config.pollIntervalMs);
      this.#wake = () => {
        clearTimeout(timer);
        resolve();
      };
    });
  }

  async #tick(): Promise<number> {
    if (this.#registry.callbacks.length === 0) {
      return 0;
    }
    const rows = await this.#claim();
    if (rows.length === 0) {
      return 0;
    }
    this.#armIdle();
    for (const row of rows) {
      if (this.#stopped) {
        break;
      }
      await this.#deliver(row);
    }
    return rows.length;
  }

  async #claim(): Promise<ClaimedRow[]> {
    const visibility = this.#config.ackDeadlineMs;
    const sql = `
      WITH claimed AS (
        SELECT d.event_seq
        FROM ${this.#q('deliveries')} d
        WHERE d.subscription_id = $1 AND d.visible_at <= now()
        ORDER BY d.event_seq
        FOR UPDATE SKIP LOCKED
        LIMIT $2
      )
      UPDATE ${this.#q('deliveries')} d
      SET delivery_attempt = d.delivery_attempt + 1,
          visible_at = now() + ($3::double precision * interval '1 millisecond')
      FROM claimed, ${this.#q('events')} e
      WHERE d.subscription_id = $1
        AND d.event_seq = claimed.event_seq
        AND e.seq = d.event_seq
      RETURNING d.event_seq AS seq, e.id AS event_id, e.index, e.type,
                e.run_id, e.data, e.created_at, d.delivery_attempt`;
    const result = await this.#pool.query<ClaimedRow>(sql, [
      this.#subscriptionId,
      this.#config.batchSize,
      visibility,
    ]);
    return [...result.rows].sort((a, b) => Number(BigInt(a.seq) - BigInt(b.seq)));
  }

  async #deliver(row: ClaimedRow): Promise<void> {
    if (row.delivery_attempt > this.#config.maxDeliveryAttempts) {
      await this.#dropExhausted(row);
      return;
    }
    const callback = this.#nextCallback();
    if (!callback) {
      return;
    }
    const event: Event = {
      id: row.event_id,
      type: row.type,
      data: row.data,
      runId: row.run_id,
      createdAt: row.created_at,
      index: Number(row.index),
      deliveryAttempt: row.delivery_attempt,
    };

    this.#inFlight++;
    let settled = false;
    const ack = async (): Promise<void> => {
      if (settled) {
        return;
      }
      settled = true;
      await this.#ack(row.seq);
    };
    const nack = async (): Promise<void> => {
      if (settled) {
        return;
      }
      settled = true;
      await this.#nack(row.seq);
    };

    try {
      await callback(event, ack, nack);
    } catch (error) {
      this.#config.logger.error?.('subscriber callback threw', error);
    } finally {
      this.#inFlight--;
      this.#markIdle();
    }
  }

  #nextCallback(): EventCallback | undefined {
    const callbacks = this.#registry.callbacks;
    if (callbacks.length === 0) {
      return undefined;
    }
    if (!this.#isGroup) {
      return callbacks[0];
    }
    const index = this.#registry.cursor % callbacks.length;
    this.#registry.cursor = (this.#registry.cursor + 1) % callbacks.length;
    return callbacks[index];
  }

  async #ack(seq: string): Promise<void> {
    await this.#pool.query(
      `DELETE FROM ${this.#q('deliveries')} WHERE subscription_id = $1 AND event_seq = $2`,
      [this.#subscriptionId, seq],
    );
  }

  async #nack(seq: string): Promise<void> {
    await this.#pool.query(
      `UPDATE ${this.#q('deliveries')}
       SET visible_at = now() + ($3::double precision * interval '1 millisecond')
       WHERE subscription_id = $1 AND event_seq = $2`,
      [this.#subscriptionId, seq, this.#config.nackDelayMs],
    );
    this.wake();
  }

  async #dropExhausted(row: ClaimedRow): Promise<void> {
    this.#config.logger.warn?.(
      `dropping event ${row.event_id} on subscription ${this.#subscriptionId} after ${
        row.delivery_attempt - 1
      } attempts`,
    );
    if (this.#config.deadLetter) {
      await this.#pool.query(
        `INSERT INTO ${this.#q('dead_events')}
           (event_id, subscription_id, topic, index, type, run_id, data, created_at, delivery_attempt)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        [
          row.event_id,
          this.#subscriptionId,
          this.#topic,
          row.index,
          row.type,
          row.run_id,
          row.data === null || row.data === undefined ? null : JSON.stringify(row.data),
          row.created_at,
          row.delivery_attempt - 1,
        ],
      );
    }
    await this.#ack(row.seq);
  }

  /** Stop the loop and wait for it to settle. Idempotent. */
  async stop(): Promise<void> {
    this.#stopped = true;
    this.wake();
    if (this.#loopPromise) {
      // #run() swallows tick errors internally, so this never rejects.
      await this.#loopPromise;
    }
  }
}
