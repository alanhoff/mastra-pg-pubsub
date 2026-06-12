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

test('publishes and receives a single event', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  const received: Event[] = [];
  await ps.subscribe('topic-single', (event, ack) => {
    received.push(event);
    ack?.();
  });

  await ps.publish('topic-single', { type: 'test', data: { hello: 'world' }, runId: 'r1' });
  await ps.flush();

  assert.equal(received.length, 1);
  const evt = received[0];
  assert.ok(evt);
  assert.equal(evt.type, 'test');
  assert.deepEqual(evt.data, { hello: 'world' });
  assert.equal(evt.runId, 'r1');
  assert.ok(evt.id);
  assert.ok(evt.createdAt instanceof Date);
  assert.equal(evt.index, 0);
  assert.equal(evt.deliveryAttempt, 1);
});

test('events are ordered by index (0-based)', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  const received: number[] = [];
  await ps.subscribe('topic-ordered', (event, ack) => {
    if (event.index !== undefined) received.push(event.index);
    ack?.();
  });

  for (let i = 0; i < 5; i++) {
    await ps.publish('topic-ordered', { type: 'e', data: i, runId: `r${i}` });
  }
  await ps.flush();

  assert.deepEqual(received, [0, 1, 2, 3, 4]);
});

test('data round-trips: object, array, null', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  const received: unknown[] = [];
  await ps.subscribe('topic-data', (event, ack) => {
    received.push(event.data);
    ack?.();
  });

  await ps.publish('topic-data', { type: 'obj', data: { a: 1, b: 'x' }, runId: 'r' });
  await ps.publish('topic-data', { type: 'arr', data: [1, 'two', true], runId: 'r' });
  await ps.publish('topic-data', { type: 'null', data: null, runId: 'r' });
  await ps.flush();

  assert.equal(received.length, 3);
  assert.deepEqual(received[0], { a: 1, b: 'x' });
  assert.deepEqual(received[1], [1, 'two', true]);
  assert.equal(received[2], null);
});

test('subscribe after publish does NOT receive prior events (but getHistory does)', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  await ps.publish('topic-late-sub', { type: 'prior', data: 42, runId: 'r' });

  const received: Event[] = [];
  await ps.subscribe('topic-late-sub', (event, ack) => {
    received.push(event);
    ack?.();
  });
  await ps.flush();

  assert.equal(received.length, 0, 'late subscriber should not receive prior events');

  const history = await ps.getHistory('topic-late-sub');
  assert.equal(history.length, 1);
  assert.equal(history[0]?.type, 'prior');
});

test('localOnly option is accepted and ignored', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  const received: Event[] = [];
  await ps.subscribe('topic-local-only', (event, ack) => {
    received.push(event);
    ack?.();
  });

  // Should not throw
  await ps.publish(
    'topic-local-only',
    { type: 'local', data: null, runId: 'r' },
    { localOnly: true },
  );
  await ps.flush();

  assert.equal(received.length, 1);
});

test('getHistory returns all events with default offset 0', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  for (let i = 0; i < 3; i++) {
    await ps.publish('topic-history', { type: `e${i}`, data: i, runId: 'r' });
  }

  const history = await ps.getHistory('topic-history');
  assert.equal(history.length, 3);
  assert.equal(history[0]?.index, 0);
  assert.equal(history[1]?.index, 1);
  assert.equal(history[2]?.index, 2);
});

test('multiple callbacks on same private subscription each receive every event', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  const cb1Events: number[] = [];
  const cb2Events: number[] = [];

  await ps.subscribe('topic-multi-cb', (event, ack) => {
    if (event.index !== undefined) cb1Events.push(event.index);
    ack?.();
  });
  await ps.subscribe('topic-multi-cb', (event, ack) => {
    if (event.index !== undefined) cb2Events.push(event.index);
    ack?.();
  });

  await ps.publish('topic-multi-cb', { type: 'e', data: null, runId: 'r' });
  await ps.flush();

  // Each private subscription gets its own copy; however, two separate subscribes
  // each create a new private subscription, so each cb sees every event
  await waitFor(() => cb1Events.length >= 1 && cb2Events.length >= 1);
  assert.equal(cb1Events[0], 0);
  assert.equal(cb2Events[0], 0);
});
