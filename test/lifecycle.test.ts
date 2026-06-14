import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import type { EventCallback } from '@mastra/core/events';
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
