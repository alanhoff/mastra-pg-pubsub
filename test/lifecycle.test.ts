import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import type { EventCallback } from '@mastra/core/events';
import type { Pool, QueryResult, QueryResultRow } from 'pg';
import pg from 'pg';
import { PostgresPubSub } from '../src/index.ts';
import {
  DATABASE_URL,
  dropSchema,
  makePubSub,
  makeTestLogger,
  schemaExists,
  sleep,
  uniqueSchema,
  waitFor,
} from './helpers.ts';

interface FakeClient {
  query<T extends QueryResultRow = QueryResultRow>(sql: string): Promise<QueryResult<T>>;
  release(): void;
}

class RetryMigrationPool {
  connectCount = 0;
  readonly #failFirstConnect: boolean;

  constructor({ failFirstConnect }: { failFirstConnect: boolean }) {
    this.#failFirstConnect = failFirstConnect;
  }

  async connect(): Promise<FakeClient> {
    this.connectCount++;
    if (this.#failFirstConnect && this.connectCount === 1) {
      throw new Error('transient connect failure');
    }
    return {
      query: async <T extends QueryResultRow = QueryResultRow>(sql: string) => {
        const rows = sql.includes('to_regnamespace') ? [{ exists: true }] : [];
        return { rows } as unknown as QueryResult<T>;
      },
      release: () => undefined,
    };
  }

  async query<T extends QueryResultRow = QueryResultRow>(): Promise<QueryResult<T>> {
    return { rows: [] } as unknown as QueryResult<T>;
  }
}

class FailingListenPool {
  connectCount = 0;
  readonly queries: string[] = [];

  async connect(): Promise<FakeClient> {
    this.connectCount++;
    if (this.connectCount === 2) {
      throw new Error('listen connect failure');
    }
    return {
      query: async <T extends QueryResultRow = QueryResultRow>(sql: string) => {
        const rows = sql.includes('to_regnamespace') ? [{ exists: true }] : [];
        return { rows } as unknown as QueryResult<T>;
      },
      release: () => undefined,
    };
  }

  async query<T extends QueryResultRow = QueryResultRow>(sql: string): Promise<QueryResult<T>> {
    this.queries.push(sql);
    return { rows: [], rowCount: sql.startsWith('DELETE') ? 1 : 0 } as unknown as QueryResult<T>;
  }
}

const schema = uniqueSchema();
const pubsubs: Array<{ close(): Promise<void> }> = [];

interface FakeMigrationClient {
  query(
    sql: string,
    values?: unknown[],
  ): Promise<{ rows: Array<{ exists?: boolean }>; rowCount: number }>;
  release(): void;
}

function makeTransientMigrationPool(failures: number): pg.Pool & { connectCount: number } {
  let remainingFailures = failures;
  const pool = {
    connectCount: 0,
    async connect(): Promise<FakeMigrationClient> {
      this.connectCount++;
      if (remainingFailures > 0) {
        remainingFailures--;
        throw new Error('transient connect failure');
      }
      return {
        async query(sql: string) {
          if (sql.includes('to_regnamespace')) {
            return { rows: [{ exists: true }], rowCount: 1 };
          }
          return { rows: [], rowCount: 0 };
        },
        release() {
          // no-op fake client
        },
      };
    },
  };
  return pool as pg.Pool & { connectCount: number };
}

async function subscriptionRowCount(schemaName: string, topic: string): Promise<number> {
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  try {
    const result = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM "${schemaName}".subscriptions WHERE topic = $1`,
      [topic],
    );
    return Number(result.rows[0]?.count ?? '0');
  } finally {
    await pool.end();
  }
}

after(async () => {
  await Promise.all(pubsubs.map((ps) => ps.close()));
  await dropSchema(schema);
});

test('constructor is side-effect free and getHistory lazily migrates on first database use', async () => {
  const lazySchema = uniqueSchema();
  await dropSchema(lazySchema);
  const ps = makePubSub(lazySchema);
  pubsubs.push(ps);

  assert.equal(await schemaExists(lazySchema), false);

  const history = await ps.getHistory('topic-lazy');

  assert.deepEqual(history, []);
  assert.equal(await schemaExists(lazySchema), true);
});

test('publish lazily starts and persists without an explicit start call', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  await ps.publish('topic-lazy-publish', {
    type: 'created',
    data: { value: 1 },
    runId: 'run-lazy',
  });

  const history = await ps.getHistory('topic-lazy-publish');
  assert.equal(history.length, 1);
  assert.equal(history[0]?.type, 'created');
  assert.equal(history[0]?.runId, 'run-lazy');
});

test('init aliases start and prepares the schema explicitly', async () => {
  const initSchema = uniqueSchema();
  const ps = makePubSub(initSchema);
  pubsubs.push(ps);

  await ps.init();

  assert.equal(await schemaExists(initSchema), true);
});

test('removing the last subscriber stops idle resources and a later subscriber restarts them', async () => {
  const idleSchema = uniqueSchema();
  const debugMessages: string[] = [];
  const ps = makePubSub(idleSchema, {
    listen: true,
    cleanupIntervalMs: 10_000,
    logger: makeTestLogger({
      debug: (message: string) => {
        debugMessages.push(message);
      },
    }),
  });
  pubsubs.push(ps);

  const received: string[] = [];
  const cb: EventCallback = (event, ack) => {
    received.push(event.type);
    ack?.();
  };

  await ps.subscribe('topic-idle', cb);
  await waitFor(() => debugMessages.includes('listen connection established'));
  assert.equal(debugMessages.filter((msg) => msg === 'postgres pubsub start started').length, 1);

  await ps.unsubscribe('topic-idle', cb);

  await waitFor(() => debugMessages.includes('postgres pubsub idle resources stopped'));
  assert.ok(
    debugMessages.includes('listen connection closed'),
    'idle stop should release the listen connection',
  );

  await ps.subscribe('topic-idle', cb);
  await waitFor(
    () => debugMessages.filter((msg) => msg === 'postgres pubsub start started').length >= 2,
  );

  await ps.publish('topic-idle', { type: 'after-restart', data: null, runId: 'run-idle' });
  await waitFor(() => received.includes('after-restart'));
});

test('idle stop waits until every local subscriber has been removed', async () => {
  const idleSchema = uniqueSchema();
  const debugMessages: string[] = [];
  const ps = makePubSub(idleSchema, {
    listen: false,
    cleanupIntervalMs: 10_000,
    logger: makeTestLogger({
      debug: (message: string) => {
        debugMessages.push(message);
      },
    }),
  });
  pubsubs.push(ps);

  const received: string[] = [];
  const cbA: EventCallback = (_event, ack) => {
    ack?.();
  };
  const cbB: EventCallback = (event, ack) => {
    received.push(event.type);
    ack?.();
  };

  await ps.subscribe('topic-a', cbA);
  await ps.subscribe('topic-b', cbB);

  await ps.unsubscribe('topic-a', cbA);
  await sleep(100);

  assert.equal(
    debugMessages.includes('postgres pubsub idle resources stopped'),
    false,
    'one remaining subscription should keep lifecycle resources active',
  );

  await ps.publish('topic-b', { type: 'still-active', data: null, runId: 'run-active' });
  await waitFor(() => received.includes('still-active'));

  await ps.unsubscribe('topic-b', cbB);
  await waitFor(() => debugMessages.includes('postgres pubsub idle resources stopped'));
});

test('idle stop does not close a caller-owned pool', async () => {
  const poolSchema = uniqueSchema();
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  const ps = new PostgresPubSub({
    pool,
    schema: poolSchema,
    pollIntervalMs: 100,
    listen: false,
    cleanupIntervalMs: 10_000,
  });

  const cb: EventCallback = (_event, ack) => {
    ack?.();
  };

  try {
    await ps.subscribe('topic-pool', cb);
    await ps.unsubscribe('topic-pool', cb);

    const result = await pool.query<{ one: string }>('SELECT 1::text AS one');
    assert.equal(result.rows[0]?.one, '1');
  } finally {
    await ps.close();
    await pool.end();
    await dropSchema(poolSchema);
  }
});


test('start retries after a transient migration failure on the same instance', async () => {
  const retrySchema = uniqueSchema();
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  const ps = new PostgresPubSub({
    pool,
    schema: retrySchema,
    listen: false,
    cleanupIntervalMs: 0,
    pollIntervalMs: 25,
  });

  try {
    await pool.query(`CREATE SCHEMA "${retrySchema}"`);
    await pool.query(`CREATE VIEW "${retrySchema}".events AS SELECT 1 AS seq`);

    await assert.rejects(() => ps.start());

    await dropSchema(retrySchema);
    await assert.doesNotReject(() => ps.start());
    assert.equal(await schemaExists(retrySchema), true);
  } finally {
    await ps.close().catch(() => undefined);
    await pool.end();
    await dropSchema(retrySchema);
  }
});

test('subscribe rolls back local and private row state when listener setup fails', async () => {
  const rollbackSchema = uniqueSchema();
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  const warnMessages: string[] = [];
  const ps = new PostgresPubSub({
    pool,
    schema: rollbackSchema,
    listen: true,
    cleanupIntervalMs: 0,
    pollIntervalMs: 10_000,
  });
  pubsubs.push(ps);

  await ps.migrate();

  const originalConnect = pool.connect.bind(pool);
  let connectCalls = 0;
  pool.connect = (async (...args: []) => {
    connectCalls++;
    if (connectCalls === 2) {
      throw new Error('listen connect failed');
    }
    return originalConnect(...args);
  }) as typeof pool.connect;

  try {
    await assert.rejects(
      () =>
        ps.subscribe(topic, (_event, ack) => {
          ack?.();
        }),
      /listen connect failed/,
    );
  } finally {
    pool.connect = originalConnect as typeof pool.connect;
  }

  assert.equal(await subscriptionRowCount(rollbackSchema, topic), 0);

  await ps.subscribe(topic, (_event, ack) => {
    ack?.();
  });
  assert.equal(await subscriptionRowCount(rollbackSchema, topic), 1);

  await ps.close();
  await pool.end();
  await dropSchema(rollbackSchema);
});

test('concurrent same-group subscribes serialize to one local consume loop', async () => {
  const groupSchema = uniqueSchema();
  const debugMessages: string[] = [];
  const ps = makePubSub(groupSchema, {
    listen: false,
    pollIntervalMs: 10_000,
    logger: makeTestLogger({
      warn: (message: string) => {
        warnMessages.push(message);
      },
    }),
  });
  const originalConnect = pool.connect.bind(pool);
  const cb: EventCallback = (_event, ack) => ack?.();

  try {
    await ps.start();
    pool.connect = async () => {
      throw new Error('listen unavailable');
    };

    await assert.rejects(() => ps.subscribe('topic-rollback', cb), /listen unavailable/);

    const rowCount = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM "${rollbackSchema}".subscriptions WHERE topic = $1`,
      ['topic-rollback'],
    );
    assert.equal(rowCount.rows[0]?.count, '0');

    pool.connect = originalConnect;
    await ps.subscribe('topic-rollback', cb);
    await ps.publish('topic-rollback', { type: 'after-rollback', data: null, runId: 'run-rollback' });
    await ps.flush();
    assert.equal(
      warnMessages.some((message) => message.includes('failed to roll back subscription')),
      false,
      'rollback should complete without warning when the pool remains usable',
    );
  } finally {
    pool.connect = originalConnect;
    await ps.close().catch(() => undefined);
    await pool.end();
    await dropSchema(rollbackSchema);
  }
});
