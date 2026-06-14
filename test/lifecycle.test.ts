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

test('migrate clears a transient rejected migration promise and retries', async () => {
  const pool = new RetryMigrationPool({ failFirstConnect: true });
  const ps = new PostgresPubSub({
    pool: pool as unknown as Pool,
    schema: uniqueSchema(),
    cleanupIntervalMs: 0,
  });

  await assert.rejects(() => ps.migrate(), /transient connect failure/);
  await assert.doesNotReject(() => ps.migrate());
  assert.equal(pool.connectCount, 2, 'second migrate should acquire a fresh connection');
});

test('start clears a transient rejected startup promise and retries', async () => {
  const pool = new RetryMigrationPool({ failFirstConnect: true });
  const ps = new PostgresPubSub({
    pool: pool as unknown as Pool,
    schema: uniqueSchema(),
    cleanupIntervalMs: 0,
  });

  await assert.rejects(() => ps.start(), /transient connect failure/);
  await assert.doesNotReject(() => ps.start());
  assert.equal(pool.connectCount, 2, 'second start should rerun startup after failure');
});

test('failed listen setup rolls back a new private subscription row', async () => {
  const pool = new FailingListenPool();
  const ps = new PostgresPubSub({
    pool: pool as unknown as Pool,
    schema: uniqueSchema(),
    cleanupIntervalMs: 0,
    listen: true,
  });

  await assert.rejects(
    () => ps.subscribe('topic-listen-fails', () => undefined),
    /listen connect failure/,
  );

  assert.equal(
    pool.connectCount,
    2,
    'migration connect and failed listener connect should both run',
  );
  assert.ok(
    pool.queries.some((sql) => sql.startsWith('DELETE') && sql.includes('subscriptions')),
    'private subscription row should be deleted on setup rollback',
  );
});
