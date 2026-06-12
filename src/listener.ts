import type { Pool, PoolClient } from 'pg';
import { notifyChannel, quoteIdentifier } from './sql.ts';
import type { PubSubLogger } from './types.ts';

/**
 * Owns a single dedicated `LISTEN` connection and dispatches `NOTIFY` payloads
 * (topic names) to registered per-topic wakeup handlers. Reconnects on
 * connection loss so wakeups survive transient database blips; polling remains
 * the correctness backstop in the meantime.
 */
export class NotifyListener {
  readonly #pool: Pool;
  readonly #channel: string;
  readonly #logger: PubSubLogger;
  readonly #handlers = new Map<string, Set<() => void>>();
  #client: PoolClient | undefined;
  #connecting: Promise<void> | undefined;
  #closed = false;

  /**
   * @param pool - Pool used to acquire the dedicated listen connection.
   * @param schema - Validated schema name; determines the channel.
   * @param logger - Logger for connection diagnostics.
   */
  constructor(pool: Pool, schema: string, logger: PubSubLogger) {
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
    await this.#ensureConnected();
    return () => {
      const current = this.#handlers.get(topic);
      if (current) {
        current.delete(handler);
        if (current.size === 0) {
          this.#handlers.delete(topic);
        }
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
    const client = await this.#pool.connect();
    client.on('notification', (msg) => {
      if (msg.payload === undefined) {
        return;
      }
      const handlers = this.#handlers.get(msg.payload);
      if (handlers) {
        for (const handler of handlers) {
          handler();
        }
      }
    });
    client.on('error', (error) => {
      this.#logger.debug?.('listen connection error', error);
      this.#handleDisconnect(client);
    });
    await client.query(`LISTEN ${quoteIdentifier(this.#channel)}`);
    if (this.#closed) {
      client.removeAllListeners('notification');
      client.release();
      return;
    }
    this.#client = client;
  }

  #handleDisconnect(client: PoolClient): void {
    if (this.#client !== client) {
      return;
    }
    this.#client = undefined;
    client.removeAllListeners('notification');
    client.release(true);
    if (this.#closed || this.#handlers.size === 0) {
      return;
    }
    this.#ensureConnected().catch((error) => {
      this.#logger.warn?.('listen reconnect failed', error);
    });
  }

  /**
   * Release the listen connection and stop dispatching. Idempotent.
   */
  async close(): Promise<void> {
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
      } catch {
        // best effort; releasing the client drops the listen anyway
      }
      client.release();
    }
  }
}
