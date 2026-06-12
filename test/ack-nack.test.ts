import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import type { Event } from '@mastra/core/events';
import { dropSchema, makePubSub, sleep, uniqueSchema, waitFor } from './helpers.ts';

const schema = uniqueSchema();
const pubsubs: Array<{ close(): Promise<void> }> = [];

after(async () => {
  await Promise.all(pubsubs.map((ps) => ps.close()));
  await dropSchema(schema);
});

test('ack settles the event: no redelivery', async () => {
  const ps = makePubSub(schema, {
    ackDeadlineMs: 300,
    pollIntervalMs: 50,
  });
  pubsubs.push(ps);

  let count = 0;
  await ps.subscribe('topic-ack', (_, ack) => {
    count++;
    ack?.();
  });

  await ps.publish('topic-ack', { type: 'e', data: null, runId: 'r' });
  await ps.flush();

  // Wait longer than ackDeadlineMs to confirm no redelivery
  await sleep(500);
  assert.equal(count, 1, 'event should be delivered exactly once after ack');
});

test('nack requeues with deliveryAttempt incremented', async () => {
  const ps = makePubSub(schema, {
    ackDeadlineMs: 5000,
    nackDelayMs: 0,
    pollIntervalMs: 50,
  });
  pubsubs.push(ps);

  const attempts: number[] = [];
  await ps.subscribe('topic-nack', (event, ack, nack) => {
    const attempt = event.deliveryAttempt ?? 0;
    attempts.push(attempt);
    if (attempts.length < 2) {
      nack?.();
    } else {
      ack?.();
    }
  });

  await ps.publish('topic-nack', { type: 'e', data: null, runId: 'r' });
  await waitFor(() => attempts.length >= 2, { timeoutMs: 5000 });

  assert.equal(attempts[0], 1, 'first delivery attempt should be 1');
  assert.equal(attempts[1], 2, 'second delivery attempt should be 2');
});

test('nackDelayMs delays redelivery', async () => {
  const nackDelayMs = 300;
  const ps = makePubSub(schema, {
    ackDeadlineMs: 5000,
    nackDelayMs,
    pollIntervalMs: 50,
  });
  pubsubs.push(ps);

  const times: number[] = [];
  await ps.subscribe('topic-nack-delay', (_, ack, nack) => {
    times.push(Date.now());
    if (times.length < 2) {
      nack?.();
    } else {
      ack?.();
    }
  });

  await ps.publish('topic-nack-delay', { type: 'e', data: null, runId: 'r' });
  await waitFor(() => times.length >= 2, { timeoutMs: 5000 });

  const [first, second] = times;
  assert.ok(first !== undefined && second !== undefined);
  const delay = second - first;
  assert.ok(delay >= nackDelayMs * 0.8, `expected delay >= ${nackDelayMs * 0.8}ms, got ${delay}ms`);
});

test('no-ack, no-nack redelivers after ackDeadlineMs with incremented deliveryAttempt', async () => {
  const ackDeadlineMs = 300;
  const ps = makePubSub(schema, {
    ackDeadlineMs,
    pollIntervalMs: 50,
  });
  pubsubs.push(ps);

  const deliveries: Event[] = [];
  await ps.subscribe('topic-no-settle', (event, ack) => {
    deliveries.push(event);
    // Intentionally not calling ack or nack for first delivery
    if ((event.deliveryAttempt ?? 0) >= 2) {
      ack?.();
    }
  });

  await ps.publish('topic-no-settle', { type: 'e', data: null, runId: 'r' });
  await waitFor(() => deliveries.length >= 2, { timeoutMs: 5000 });

  assert.equal(deliveries[0]?.deliveryAttempt, 1, 'first delivery attempt should be 1');
  assert.equal(deliveries[1]?.deliveryAttempt, 2, 'second delivery attempt should be 2');
});

test('event id is the same across redeliveries', async () => {
  const ps = makePubSub(schema, {
    ackDeadlineMs: 300,
    pollIntervalMs: 50,
  });
  pubsubs.push(ps);

  const ids: string[] = [];
  await ps.subscribe('topic-stable-id', (event, ack) => {
    ids.push(event.id);
    if (ids.length >= 2) {
      ack?.();
    }
    // else no-op to trigger redelivery
  });

  await ps.publish('topic-stable-id', { type: 'e', data: null, runId: 'r' });
  await waitFor(() => ids.length >= 2, { timeoutMs: 5000 });

  assert.equal(ids[0], ids[1], 'event id should be stable across redeliveries');
});
