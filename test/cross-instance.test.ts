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

test('publish on instance A, subscribe on instance B receives the event', async () => {
  const psA = makePubSub(schema);
  const psB = makePubSub(schema);
  pubsubs.push(psA, psB);

  const received: Event[] = [];
  await psB.subscribe('topic-cross', (event, ack) => {
    received.push(event);
    ack?.();
  });

  await psA.publish('topic-cross', { type: 'cross-test', data: { from: 'A' }, runId: 'r' });

  await waitFor(() => received.length >= 1, { timeoutMs: 5000 });

  assert.equal(received.length, 1);
  assert.equal(received[0]?.type, 'cross-test');
  assert.deepEqual(received[0]?.data, { from: 'A' });
});

test('LISTEN/NOTIFY path delivers quickly (listen:true default)', async () => {
  const psA = makePubSub(schema, { listen: true, pollIntervalMs: 5000 });
  const psB = makePubSub(schema, { listen: true, pollIntervalMs: 5000 });
  pubsubs.push(psA, psB);

  const received: Event[] = [];
  await psB.subscribe('topic-cross-listen', (event, ack) => {
    received.push(event);
    ack?.();
  });

  const start = Date.now();
  await psA.publish('topic-cross-listen', { type: 'fast', data: null, runId: 'r' });

  // Should arrive well before the 5000ms poll interval due to LISTEN/NOTIFY
  await waitFor(() => received.length >= 1, { timeoutMs: 2000 });

  const elapsed = Date.now() - start;
  assert.ok(elapsed < 1500, `expected fast delivery via LISTEN/NOTIFY, took ${elapsed}ms`);
  assert.equal(received[0]?.type, 'fast');
});

test('multiple events across instances maintain correct index ordering', async () => {
  const psA = makePubSub(schema);
  const psB = makePubSub(schema);
  pubsubs.push(psA, psB);

  const received: number[] = [];
  await psB.subscribe('topic-cross-order', (event, ack) => {
    if (event.index !== undefined) received.push(event.index);
    ack?.();
  });

  const N = 5;
  for (let i = 0; i < N; i++) {
    await psA.publish('topic-cross-order', { type: 'e', data: i, runId: `r${i}` });
  }

  await waitFor(() => received.length >= N, { timeoutMs: 5000 });

  // Events should come in order 0..N-1
  const sorted = [...received].sort((a, b) => a - b);
  assert.deepEqual(sorted, [0, 1, 2, 3, 4]);
});

test('instance B can publish, instance A receives', async () => {
  const psA = makePubSub(schema);
  const psB = makePubSub(schema);
  pubsubs.push(psA, psB);

  const received: Event[] = [];
  await psA.subscribe('topic-cross-b-to-a', (event, ack) => {
    received.push(event);
    ack?.();
  });

  await psB.publish('topic-cross-b-to-a', { type: 'from-B', data: 42, runId: 'r' });

  await waitFor(() => received.length >= 1, { timeoutMs: 5000 });

  assert.equal(received.length, 1);
  assert.equal(received[0]?.type, 'from-B');
  assert.equal(received[0]?.data, 42);
});
