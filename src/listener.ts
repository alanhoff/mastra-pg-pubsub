import type { Pool, PoolClient } from 'pg';
import {
  logDebug,
  logWarn,
  observeEvent,
  startObservabilitySpan,
  traceAttributes,
} from './observability.ts';
import { notifyChannel, quoteIdentifier } from './sql.ts';
import type { ResolvedConfig } from './types.ts';

/**
 * Owns a single dedicated `LISTEN` connection and dispatches `NOTIFY` payloads
 * (topic names) to registered per-topic wakeup handlers. Reconnects on
 * connection loss so wakeups survive transient database blips; polling remains
 * the correctness backstop in the meantime.
 */
export class NotifyListener {
  readonly #pool: Pool;
  readonly #channel: string;
  readonly #logger: ResolvedConfig['logger'];
  readonly #handlers = new Map<string, Set<() => void>>();
  #client: PoolClient | undefined;
  #connecting: Promise<void> | undefined;
  #closed = false;

  /**
   * @param pool - Pool used to acquire the dedicated listen connection.
   * @param schema - Validated schema name; determines the channel.
   * @param logger - Logger for connection diagnostics.
   */
  constructor(pool: Pool, schema: string, logger: ResolvedConfig['logger']) {
    this.#pool = pool;
    this.#channel = notifyChannel(schema);
    this.#logger = logger;
  }

  /**
   * Register a wakeup handler for a topic, ensuring the listen connection is
   * established. Returns an unregister function.
   *
   * @param topic - The topic to wake on.
   * @param handler - Invoked when a `NOTIFY` for the topic arrives.
   * @returns A function that removes this handler.
   */
  async register(topic: string, handler: () => void): Promise<() => void> {
    let set = this.#handlers.get(topic);
    if (!set) {
      set = new Set();
      this.#handlers.set(topic, set);
    }
    set.add(handler);
    try {
      await this.#ensureConnected();
    } catch (error) {
      set.delete(handler);
      if (set.size === 0) {
        this.#handlers.delete(topic);
      }
      throw error;
    }
    const context = traceAttributes({
      channel: this.#channel,
      topic,
      topicHandlerCount: set.size,
      topicCount: this.#handlers.size,
    });
    logDebug(this.#logger, 'listen handler registered', context);
    observeEvent('pg_pubsub.listener.handler_registered', context);
    return () => {
      const current = this.#handlers.get(topic);
      if (current) {
        current.delete(handler);
        if (current.size === 0) {
          this.#handlers.delete(topic);
        }
        const unregisterContext = traceAttributes({
          channel: this.#channel,
          topic,
          topicHandlerCount: current.size,
          topicCount: this.#handlers.size,
        });
        logDebug(this.#logger, 'listen handler unregistered', unregisterContext);
        observeEvent('pg_pubsub.listener.handler_unregistered', unregisterContext);
      }
    };
  }

  async #ensureConnected(): Promise<void> {
    if (this.#closed || this.#client) {
      return;
    }
    if (!this.#connecting) {
      this.#connecting = this.#connect().finally(() => {
        this.#connecting = undefined;
      });
    }
    return this.#connecting;
  }

  async #connect(): Promise<void> {
    const span = startObservabilitySpan(
      'pg_pubsub.listener.connect',
      traceAttributes({
        channel: this.#channel,
        topicCount: this.#handlers.size,
      }),
    );
    const client = await this.#pool.connect();
    try {
      client.on('notification', (msg) => {
        if (msg.payload === undefined) {
          return;
        }
        const handlers = this.#handlers.get(msg.payload);
        const context = traceAttributes({
          channel: this.#channel,
          topic: msg.payload,
          handlerCount: handlers?.size ?? 0,
        });
        logDebug(this.#logger, 'notification received', context);
        observeEvent('pg_pubsub.listener.notification', context);
        if (handlers) {
          for (const handler of handlers) {
            handler();
          }
        }
      });
      client.on('error', (error) => {
        logDebug(
          this.#logger,
          'listen connection error',
          traceAttributes({
            channel: this.#channel,
            errorName: error.name,
          }),
        );
        observeEvent(
          'pg_pubsub.listener.error',
          traceAttributes({
            channel: this.#channel,
            errorName: error.name,
          }),
        );
        this.#handleDisconnect(client);
      });
      await client.query(`LISTEN ${quoteIdentifier(this.#channel)}`);
      if (this.#closed) {
        client.removeAllListeners('notification');
        client.release();
        span.setAttribute('listener.closed_before_ready', true);
        span.end();
        return;
      }
      this.#client = client;
      const context = traceAttributes({
        channel: this.#channel,
        topicCount: this.#handlers.size,
      });
      logDebug(this.#logger, 'listen connection established', context);
      span.end();
    } catch (error) {
      client.removeAllListeners('notification');
      client.removeAllListeners('error');
      client.release();
      span.recordError(error);
      span.end({ code: 'error', message: 'listen connection failed' });
      throw error;
    }
  }

  #handleDisconnect(client: PoolClient): void {
    if (this.#client !== client) {
      return;
    }
    this.#client = undefined;
    client.removeAllListeners('notification');
    client.release(true);
    const context = traceAttributes({
      channel: this.#channel,
      topicCount: this.#handlers.size,
    });
    logDebug(this.#logger, 'listen connection disconnected', context);
    observeEvent('pg_pubsub.listener.disconnected', context);
    if (this.#closed || this.#handlers.size === 0) {
      return;
    }
    this.#ensureConnected().catch((error) => {
      logWarn(
        this.#logger,
        'listen reconnect failed',
        traceAttributes({
          channel: this.#channel,
          topicCount: this.#handlers.size,
        }),
        error,
      );
    });
  }

  /**
   * Release the listen connection and stop dispatching. Idempotent.
   */
  async close(): Promise<void> {
    const span = startObservabilitySpan(
      'pg_pubsub.listener.close',
      traceAttributes({
        channel: this.#channel,
        topicCount: this.#handlers.size,
      }),
    );
    this.#closed = true;
    this.#handlers.clear();
    if (this.#connecting) {
      await this.#connecting.catch(() => undefined);
    }
    const client = this.#client;
    this.#client = undefined;
    if (client) {
      client.removeAllListeners('notification');
      client.removeAllListeners('error');
      try {
        await client.query(`UNLISTEN ${quoteIdentifier(this.#channel)}`);
      } catch (error) {
        logWarn(
          this.#logger,
          'listen unlisten failed during close',
          traceAttributes({ channel: this.#channel }),
          error,
        );
      }
      client.release();
    }
    logDebug(
      this.#logger,
      'listen connection closed',
      traceAttributes({
        channel: this.#channel,
      }),
    );
    span.end();
  }
}
