import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import type { Event } from '@mastra/core/events';
import { dropSchema, makePubSub, uniqueSchema, waitFor } from './helpers.ts';

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
