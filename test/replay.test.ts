import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import type { Event } from '@mastra/core/events';
import pg from 'pg';
import { PostgresPubSub } from '../src/index.ts';
import { DATABASE_URL, dropSchema, makePubSub, sleep, uniqueSchema, waitFor } from './helpers.ts';

const schema = uniqueSchema();
const pubsubs: Array<{ close(): Promise<void> }> = [];

after(async () => {
  await Promise.all(pubsubs.map((ps) => ps.close()));
  await dropSchema(schema);
});

test('getHistory with offset=0 returns all events', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  for (let i = 0; i < 4; i++) {
    await ps.publish('topic-hist-all', { type: `t${i}`, data: i, runId: 'r' });
  }

  const history = await ps.getHistory('topic-hist-all');
  assert.equal(history.length, 4);
  assert.equal(history[0]?.index, 0);
  assert.equal(history[3]?.index, 3);
});

test('getHistory with offset=N skips events with index < N', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  for (let i = 0; i < 5; i++) {
    await ps.publish('topic-hist-offset', { type: `t${i}`, data: i, runId: 'r' });
  }

  const history = await ps.getHistory('topic-hist-offset', 2);
  assert.equal(history.length, 3);
  assert.equal(history[0]?.index, 2);
  assert.equal(history[1]?.index, 3);
  assert.equal(history[2]?.index, 4);
});

test('getHistory returns empty array for topic with no events', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  const history = await ps.getHistory('topic-empty');
  assert.equal(history.length, 0);
});

test('subscribeWithReplay replays history then live, no duplicate at boundary', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  // Publish some events before subscribing
  await ps.publish('topic-replay', { type: 'pre1', data: 0, runId: 'r' });
  await ps.publish('topic-replay', { type: 'pre2', data: 1, runId: 'r' });

  const received: Event[] = [];
  await ps.subscribeWithReplay('topic-replay', (event, ack) => {
    received.push(event);
    ack?.();
  });

  // Publish more events after subscribing
  await ps.publish('topic-replay', { type: 'post1', data: 2, runId: 'r' });
  await ps.publish('topic-replay', { type: 'post2', data: 3, runId: 'r' });

  await waitFor(() => received.length >= 4, { timeoutMs: 5000 });
  await ps.flush();

  // All 4 events, each index seen exactly once
  const indexes = received.map((e) => e.index).filter((i): i is number => i !== undefined);
  const unique = new Set(indexes);
  assert.equal(unique.size, 4, `expected 4 unique indexes, got ${unique.size}: ${indexes}`);
  assert.ok(unique.has(0), 'index 0 should be present');
  assert.ok(unique.has(1), 'index 1 should be present');
  assert.ok(unique.has(2), 'index 2 should be present');
  assert.ok(unique.has(3), 'index 3 should be present');
});

test('subscribeFromOffset replays only from offset then live', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  // Publish 4 events
  for (let i = 0; i < 4; i++) {
    await ps.publish('topic-replay-offset', { type: `t${i}`, data: i, runId: 'r' });
  }

  const received: Event[] = [];
  await ps.subscribeFromOffset('topic-replay-offset', 2, (event, ack) => {
    received.push(event);
    ack?.();
  });

  // Publish one more after subscribing
  await ps.publish('topic-replay-offset', { type: 't4', data: 4, runId: 'r' });

  await waitFor(() => received.length >= 3, { timeoutMs: 5000 });
  await ps.flush();

  // Should have events at indexes 2, 3, 4 (not 0 or 1)
  const indexes = received.map((e) => e.index).filter((i): i is number => i !== undefined);
  assert.ok(!indexes.includes(0), 'index 0 should not be present');
  assert.ok(!indexes.includes(1), 'index 1 should not be present');
  assert.ok(indexes.includes(2), 'index 2 should be present');
  assert.ok(indexes.includes(3), 'index 3 should be present');
  assert.ok(indexes.includes(4), 'index 4 should be present');
});

test('subscribeWithReplay: no duplicate when event arrives during replay', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  // Publish 2 events
  await ps.publish('topic-replay-dedup', { type: 'e', data: 0, runId: 'r' });
  await ps.publish('topic-replay-dedup', { type: 'e', data: 1, runId: 'r' });

  const indexCounts = new Map<number, number>();
  await ps.subscribeWithReplay('topic-replay-dedup', (event, ack) => {
    const idx = event.index ?? -1;
    indexCounts.set(idx, (indexCounts.get(idx) ?? 0) + 1);
    ack?.();
  });

  // Publish one more to trigger live delivery
  await ps.publish('topic-replay-dedup', { type: 'e', data: 2, runId: 'r' });

  await waitFor(() => indexCounts.has(2) && (indexCounts.get(2) ?? 0) >= 1, { timeoutMs: 5000 });
  await ps.flush();

  // Each index should appear exactly once
  for (const [idx, count] of indexCounts) {
    assert.equal(count, 1, `index ${idx} appeared ${count} times, expected 1`);
  }
});

test('subscribeFromOffset preserves history order before events published during replay', async () => {
  const ps = makePubSub(schema, { pollIntervalMs: 25 });
  pubsubs.push(ps);

  for (let i = 0; i < 3; i++) {
    await ps.publish('topic-replay-race', { type: 'history', data: i, runId: `history-${i}` });
  }

  const received: number[] = [];
  let publishedDuringReplay = false;
  await ps.subscribeFromOffset('topic-replay-race', 0, async (event, ack) => {
    if (event.index !== undefined) received.push(event.index);
    if (event.index === 0 && !publishedDuringReplay) {
      publishedDuringReplay = true;
      await ps.publish('topic-replay-race', { type: 'live', data: 3, runId: 'live-3' });
    }
    ack?.();
  });

  await waitFor(() => received.includes(3), { timeoutMs: 5_000 });
  await ps.flush();

  assert.deepEqual(received, [0, 1, 2, 3]);
});

test('subscribeWithReplay live event-only callbacks auto-settle by default', async () => {
  const ps = makePubSub(schema, { ackDeadlineMs: 200, pollIntervalMs: 25 });
  pubsubs.push(ps);

  await ps.publish('topic-replay-event-only', { type: 'history', data: 0, runId: 'history' });

  const received: number[] = [];
  await ps.subscribeWithReplay('topic-replay-event-only', (event) => {
    if (event.index !== undefined) received.push(event.index);
  });

  await ps.publish('topic-replay-event-only', { type: 'live', data: 1, runId: 'live' });
  await waitFor(() => received.includes(1), { timeoutMs: 5000 });
  await ps.flush();
  await sleep(300);

  assert.deepEqual(received, [0, 1]);
});

test('CachingPubSub-shaped wrapper does not prevent default private auto-settlement', async () => {
  const ps = makePubSub(schema, { ackDeadlineMs: 200, pollIntervalMs: 25 });
  pubsubs.push(ps);

  let count = 0;
  const original = async (_event: Event, _ack?: () => Promise<void>) => {
    count++;
  };
  await ps.subscribe('topic-caching-wrapper-shape', (event, ack) => original(event, ack));

  await ps.publish('topic-caching-wrapper-shape', { type: 'live', data: null, runId: 'live' });
  await ps.flush();
  await sleep(300);

  assert.equal(count, 1);
});

test('subscribeFromOffset live event-only callbacks auto-settle by default', async () => {
  const ps = makePubSub(schema, { ackDeadlineMs: 200, pollIntervalMs: 25 });
  pubsubs.push(ps);

  await ps.publish('topic-offset-event-only', { type: 'history', data: 0, runId: 'history-0' });
  await ps.publish('topic-offset-event-only', { type: 'history', data: 1, runId: 'history-1' });

  const received: number[] = [];
  await ps.subscribeFromOffset('topic-offset-event-only', 1, (event) => {
    if (event.index !== undefined) received.push(event.index);
  });

  await ps.publish('topic-offset-event-only', { type: 'live', data: 2, runId: 'live-2' });
  await waitFor(() => received.includes(2), { timeoutMs: 5000 });
  await ps.flush();
  await sleep(300);

  assert.deepEqual(received, [1, 2]);
});

test('subscribeWithReplay cleans up paused subscription when setup fails', async () => {
  const cleanupSchema = uniqueSchema();
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  const ps = new PostgresPubSub({
    pool,
    schema: cleanupSchema,
    pollIntervalMs: 25,
    cleanupIntervalMs: 0,
  });
  pubsubs.push(ps);

  try {
    await ps.publish('topic-replay-cleanup', { type: 'history', data: 0, runId: 'r' });

    const originalQuery = pool.query.bind(pool) as typeof pool.query;
    let replayAckFailed = false;
    pool.query = ((queryText: unknown, values?: unknown) => {
      if (
        typeof queryText === 'string' &&
        queryText.includes('DELETE FROM') &&
        queryText.includes('deliveries') &&
        queryText.includes('USING')
      ) {
        replayAckFailed = true;
        return Promise.reject(new Error('forced replay ack failure'));
      }
      return originalQuery(queryText as never, values as never);
    }) as typeof pool.query;

    await assert.rejects(
      () =>
        ps.subscribeWithReplay('topic-replay-cleanup', (_event, ack) => {
          ack?.();
        }),
      /forced replay ack failure/,
    );
    assert.equal(replayAckFailed, true);

    pool.query = originalQuery;
    const subscriptionRows = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM "${cleanupSchema}".subscriptions`,
    );
    assert.equal(subscriptionRows.rows[0]?.count, '0');
  } finally {
    pool.query = pool.query.bind(pool) as typeof pool.query;
    await ps.close().catch(() => undefined);
    await pool.end().catch(() => undefined);
    await dropSchema(cleanupSchema);
  }
});
