import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import type { Event } from '@mastra/core/events';
import pg from 'pg';
import { PostgresPubSub } from '../src/index.ts';
import { DATABASE_URL, dropSchema, makePubSub, makeTestLogger, uniqueSchema, waitFor } from './helpers.ts';

const schema = uniqueSchema();
const pubsubs: Array<{ close(): Promise<void> }> = [];

after(async () => {
  await Promise.all(pubsubs.map((ps) => ps.close()));
  await dropSchema(schema);
});

test('two local callbacks on same group receive each event exactly once total (round-robin)', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  const cb1Events: number[] = [];
  const cb2Events: number[] = [];

  await ps.subscribe(
    'topic-group-rr',
    (event, ack) => {
      if (event.index !== undefined) cb1Events.push(event.index);
      ack?.();
    },
    { group: 'my-group' },
  );
  await ps.subscribe(
    'topic-group-rr',
    (event, ack) => {
      if (event.index !== undefined) cb2Events.push(event.index);
      ack?.();
    },
    { group: 'my-group' },
  );

  const N = 4;
  for (let i = 0; i < N; i++) {
    await ps.publish('topic-group-rr', { type: 'e', data: i, runId: `r${i}` });
  }
  await ps.flush();

  const total = cb1Events.length + cb2Events.length;
  assert.equal(total, N, `expected ${N} total deliveries, got ${total}`);

  // No event should appear in both
  const allEvents = [...cb1Events, ...cb2Events];
  const uniqueEvents = new Set(allEvents);
  assert.equal(uniqueEvents.size, N, 'each event index should appear exactly once');
});

test('two separate instances on same group: each event reaches exactly one instance', async () => {
  const psA = makePubSub(schema);
  const psB = makePubSub(schema);
  pubsubs.push(psA, psB);

  const aEvents: number[] = [];
  const bEvents: number[] = [];

  await psA.subscribe(
    'topic-group-cross',
    (event, ack) => {
      if (event.index !== undefined) aEvents.push(event.index);
      ack?.();
    },
    { group: 'shared-group' },
  );
  await psB.subscribe(
    'topic-group-cross',
    (event, ack) => {
      if (event.index !== undefined) bEvents.push(event.index);
      ack?.();
    },
    { group: 'shared-group' },
  );

  const N = 6;
  for (let i = 0; i < N; i++) {
    await psA.publish('topic-group-cross', { type: 'e', data: i, runId: `r${i}` });
  }

  await waitFor(() => aEvents.length + bEvents.length >= N, { timeoutMs: 5000 });

  const total = aEvents.length + bEvents.length;
  assert.equal(total, N, `expected ${N} total, got ${total}`);

  const allIndexes = [...aEvents, ...bEvents].sort((a, b) => a - b);
  assert.deepEqual(allIndexes, [0, 1, 2, 3, 4, 5]);
});

test('two private subscribes (no group) each get every event', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  const sub1Events: Event[] = [];
  const sub2Events: Event[] = [];

  const cb1 = (event: Event, ack?: () => void) => {
    sub1Events.push(event);
    ack?.();
  };
  const cb2 = (event: Event, ack?: () => void) => {
    sub2Events.push(event);
    ack?.();
  };

  // No group = private fan-out
  await ps.subscribe('topic-fanout', cb1);
  await ps.subscribe('topic-fanout', cb2);

  const N = 3;
  for (let i = 0; i < N; i++) {
    await ps.publish('topic-fanout', { type: 'e', data: i, runId: `r${i}` });
  }

  await waitFor(() => sub1Events.length >= N && sub2Events.length >= N, { timeoutMs: 5000 });

  assert.equal(sub1Events.length, N, `sub1 expected ${N}, got ${sub1Events.length}`);
  assert.equal(sub2Events.length, N, `sub2 expected ${N}, got ${sub2Events.length}`);

  // Each subscriber should see all indexes
  const idx1 = sub1Events.map((e) => e.index).sort((a, b) => (a ?? 0) - (b ?? 0));
  const idx2 = sub2Events.map((e) => e.index).sort((a, b) => (a ?? 0) - (b ?? 0));
  assert.deepEqual(idx1, [0, 1, 2]);
  assert.deepEqual(idx2, [0, 1, 2]);
});

test('same group name can subscribe to different topics independently', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  const topicA: number[] = [];
  const topicB: number[] = [];

  await ps.subscribe(
    'topic-shared-group-a',
    (event, ack) => {
      if (event.index !== undefined) topicA.push(event.index);
      ack?.();
    },
    { group: 'same-logical-group' },
  );
  await ps.subscribe(
    'topic-shared-group-b',
    (event, ack) => {
      if (event.index !== undefined) topicB.push(event.index);
      ack?.();
    },
    { group: 'same-logical-group' },
  );

  await ps.publish('topic-shared-group-a', { type: 'a', data: null, runId: 'ra' });
  await ps.publish('topic-shared-group-b', { type: 'b', data: null, runId: 'rb' });

  await waitFor(() => topicA.length === 1 && topicB.length === 1, { timeoutMs: 5_000 });

  assert.deepEqual(topicA, [0]);
  assert.deepEqual(topicB, [0]);
});

test('group subscription persists across instances: only one receives per event', async () => {
  const psA = makePubSub(schema);
  pubsubs.push(psA);

  const received: number[] = [];
  await psA.subscribe(
    'topic-group-persist',
    (event, ack) => {
      if (event.index !== undefined) received.push(event.index);
      ack?.();
    },
    { group: 'persist-group' },
  );

  await psA.publish('topic-group-persist', { type: 'e', data: 0, runId: 'r' });
  await psA.flush();

  assert.equal(received.length, 1);
  assert.equal(received[0], 0);
});


test('concurrent same-group subscribes create one local consume loop', async () => {
  const concurrentSchema = uniqueSchema();
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  const debugMessages: string[] = [];
  const ps = new PostgresPubSub({
    pool,
    schema: concurrentSchema,
    listen: false,
    cleanupIntervalMs: 0,
    pollIntervalMs: 25,
    logger: makeTestLogger({
      debug: (message: string) => {
        debugMessages.push(message);
      },
    }),
  });
  pubsubs.push(ps);

  try {
    await ps.start();
    const originalQuery = pool.query.bind(pool);
    let delayedInserts = 0;
    pool.query = (async (...args: Parameters<typeof pool.query>) => {
      const sql = String(args[0]);
      if (sql.includes('INSERT INTO') && sql.includes('."subscriptions"') && delayedInserts < 2) {
        delayedInserts++;
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      return await originalQuery(...args);
    }) as typeof pool.query;

    const cbA = (event: Event, ack?: () => void) => ack?.();
    const cbB = (event: Event, ack?: () => void) => ack?.();
    await Promise.all([
      ps.subscribe('topic-concurrent-group', cbA, { group: 'same-group' }),
      ps.subscribe('topic-concurrent-group', cbB, { group: 'same-group' }),
    ]);

    assert.equal(
      debugMessages.filter((message) => message === 'consume loop started').length,
      1,
      'same local group should share one consume loop even under concurrent subscribe calls',
    );
  } finally {
    await ps.close().catch(() => undefined);
    await pool.end();
    await dropSchema(concurrentSchema);
  }
});
