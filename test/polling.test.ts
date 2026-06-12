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

test('listen:false with small pollIntervalMs still delivers published events', async () => {
  const ps = makePubSub(schema, {
    listen: false,
    pollIntervalMs: 100,
  });
  pubsubs.push(ps);

  const received: Event[] = [];
  await ps.subscribe('topic-poll', (event, ack) => {
    received.push(event);
    ack?.();
  });

  await ps.publish('topic-poll', { type: 'poll-test', data: { mode: 'polling' }, runId: 'r' });

  await waitFor(() => received.length >= 1, { timeoutMs: 3000 });

  assert.equal(received.length, 1);
  assert.equal(received[0]?.type, 'poll-test');
  assert.deepEqual(received[0]?.data, { mode: 'polling' });
});

test('listen:false delivers multiple events in order', async () => {
  const ps = makePubSub(schema, {
    listen: false,
    pollIntervalMs: 100,
  });
  pubsubs.push(ps);

  const received: number[] = [];
  await ps.subscribe('topic-poll-order', (event, ack) => {
    if (event.index !== undefined) received.push(event.index);
    ack?.();
  });

  const N = 5;
  for (let i = 0; i < N; i++) {
    await ps.publish('topic-poll-order', { type: 'e', data: i, runId: `r${i}` });
  }

  await waitFor(() => received.length >= N, { timeoutMs: 5000 });

  assert.equal(received.length, N);
  const sorted = [...received].sort((a, b) => a - b);
  assert.deepEqual(sorted, [0, 1, 2, 3, 4]);
});

test('listen:false cross-instance still delivers via polling', async () => {
  const psA = makePubSub(schema, {
    listen: false,
    pollIntervalMs: 150,
  });
  const psB = makePubSub(schema, {
    listen: false,
    pollIntervalMs: 150,
  });
  pubsubs.push(psA, psB);

  const received: Event[] = [];
  await psB.subscribe('topic-poll-cross', (event, ack) => {
    received.push(event);
    ack?.();
  });

  await psA.publish('topic-poll-cross', { type: 'cross-poll', data: null, runId: 'r' });

  await waitFor(() => received.length >= 1, { timeoutMs: 5000 });

  assert.equal(received.length, 1);
  assert.equal(received[0]?.type, 'cross-poll');
});
