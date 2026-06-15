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

test('default private successful callback without ack auto-settles', async () => {
  const ps = makePubSub(schema, {
    ackDeadlineMs: 200,
    pollIntervalMs: 25,
  });
  pubsubs.push(ps);

  let count = 0;
  await ps.subscribe('topic-private-auto-ack', () => {
    count++;
  });

  await ps.publish('topic-private-auto-ack', { type: 'e', data: null, runId: 'r' });
  await ps.flush();
  await sleep(300);

  assert.equal(count, 1, 'successful private callback should auto-ack by default');
});

test('default group no-ack, no-nack redelivers after ackDeadlineMs', async () => {
  const ackDeadlineMs = 300;
  const ps = makePubSub(schema, {
    ackDeadlineMs,
    pollIntervalMs: 50,
  });
  pubsubs.push(ps);

  const deliveries: Event[] = [];
  await ps.subscribe(
    'topic-no-settle-group',
    (event, ack) => {
      deliveries.push(event);
      if ((event.deliveryAttempt ?? 0) >= 2) {
        ack?.();
      }
    },
    { group: 'explicit-workers' },
  );

  await ps.publish('topic-no-settle-group', { type: 'e', data: null, runId: 'r' });
  await waitFor(() => deliveries.length >= 2, { timeoutMs: 5000 });

  assert.equal(deliveries[0]?.deliveryAttempt, 1, 'first delivery attempt should be 1');
  assert.equal(deliveries[1]?.deliveryAttempt, 2, 'second delivery attempt should be 2');
});

test('explicit private no-ack, no-nack redelivers after ackDeadlineMs', async () => {
  const ps = makePubSub(schema, {
    ackDeadlineMs: 300,
    pollIntervalMs: 50,
    settlement: 'explicit',
  });
  pubsubs.push(ps);

  const attempts: number[] = [];
  await ps.subscribe('topic-explicit-private-no-settle', (event, ack) => {
    attempts.push(event.deliveryAttempt ?? 0);
    if ((event.deliveryAttempt ?? 0) >= 2) {
      ack?.();
    }
  });

  await ps.publish('topic-explicit-private-no-settle', { type: 'e', data: null, runId: 'r' });
  await waitFor(() => attempts.length >= 2, { timeoutMs: 5000 });

  assert.deepEqual(attempts, [1, 2]);
});

test('event id is the same across explicit private redeliveries', async () => {
  const ps = makePubSub(schema, {
    ackDeadlineMs: 300,
    pollIntervalMs: 50,
    settlement: 'explicit',
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

test('callback-success auto-settles successful group callbacks', async () => {
  const ps = makePubSub(schema, {
    ackDeadlineMs: 200,
    pollIntervalMs: 25,
    settlement: 'callback-success',
  });
  pubsubs.push(ps);

  let count = 0;
  await ps.subscribe(
    'topic-group-callback-success',
    () => {
      count++;
    },
    { group: 'callback-success-workers' },
  );

  await ps.publish('topic-group-callback-success', { type: 'e', data: null, runId: 'r' });
  await ps.flush();
  await sleep(300);

  assert.equal(count, 1, 'callback-success should auto-ack successful group callback');
});

test('default callback failure nacks, then successful retry auto-settles', async () => {
  const ps = makePubSub(schema, {
    ackDeadlineMs: 5000,
    nackDelayMs: 0,
    pollIntervalMs: 25,
  });
  pubsubs.push(ps);

  const attempts: number[] = [];
  await ps.subscribe('topic-failure-auto-nack', (event) => {
    attempts.push(event.deliveryAttempt ?? 0);
    if (attempts.length === 1) {
      throw new Error('first attempt fails');
    }
  });

  await ps.publish('topic-failure-auto-nack', { type: 'e', data: null, runId: 'r' });
  await waitFor(() => attempts.length >= 2, { timeoutMs: 5000 });
  await ps.flush();

  assert.deepEqual(attempts, [1, 2]);
});

test('explicit callback failure remains deadline-based', async () => {
  const ackDeadlineMs = 250;
  const ps = makePubSub(schema, {
    ackDeadlineMs,
    pollIntervalMs: 25,
    settlement: 'explicit',
  });
  pubsubs.push(ps);

  const times: number[] = [];
  await ps.subscribe('topic-explicit-failure-deadline', (_event, ack) => {
    times.push(Date.now());
    if (times.length === 1) {
      throw new Error('first attempt fails');
    }
    ack?.();
  });

  await ps.publish('topic-explicit-failure-deadline', { type: 'e', data: null, runId: 'r' });
  await waitFor(() => times.length >= 2, { timeoutMs: 5000 });

  const [first, second] = times;
  assert.ok(first !== undefined && second !== undefined);
  assert.ok(
    second - first >= ackDeadlineMs * 0.75,
    `expected deadline-based retry, got ${second - first}ms`,
  );
});

test('manual nack wins over default private success auto-ack', async () => {
  const ps = makePubSub(schema, {
    ackDeadlineMs: 5000,
    nackDelayMs: 0,
    pollIntervalMs: 25,
  });
  pubsubs.push(ps);

  const attempts: number[] = [];
  await ps.subscribe('topic-manual-nack-wins', async (event, _ack, nack) => {
    attempts.push(event.deliveryAttempt ?? 0);
    if (attempts.length === 1) {
      await nack?.();
    }
  });

  await ps.publish('topic-manual-nack-wins', { type: 'e', data: null, runId: 'r' });
  await waitFor(() => attempts.length >= 2, { timeoutMs: 5000 });
  await ps.flush();
  await sleep(100);

  assert.deepEqual(attempts, [1, 2]);
});
