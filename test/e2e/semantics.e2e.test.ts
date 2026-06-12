import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import type { Event } from '@mastra/core/events';
import { dropSchema, makePubSub, uniqueSchema, waitFor } from './helpers.ts';

const schema = uniqueSchema('e2e_semantics');
const pubsubs: Array<{ close(): Promise<void> }> = [];

after(async () => {
  await Promise.allSettled(pubsubs.map((ps) => ps.close()));
  await dropSchema(schema);
});

test('at-least-once redelivery preserves event identity for idempotent consumers', async () => {
  const ps = makePubSub(schema, {
    ackDeadlineMs: 150,
    pollIntervalMs: 25,
    maxDeliveryAttempts: Number.POSITIVE_INFINITY,
  });
  pubsubs.push(ps);

  const deliveries: Event[] = [];
  const processed = new Set<string>();
  let sideEffects = 0;

  await ps.subscribe('semantics-redelivery', (event, ack) => {
    deliveries.push(event);
    if (!processed.has(event.id)) {
      processed.add(event.id);
      sideEffects++;
    }
    if ((event.deliveryAttempt ?? 0) >= 2) {
      ack?.();
    }
  });

  await ps.publish('semantics-redelivery', {
    type: 'redeliver',
    data: { idempotencyKey: 'once' },
    runId: 'redelivery-run',
  });

  await waitFor(() => deliveries.length >= 2, { timeoutMs: 5_000, intervalMs: 25 });

  assert.equal(deliveries[0]?.deliveryAttempt, 1);
  assert.equal(deliveries[1]?.deliveryAttempt, 2);
  assert.equal(deliveries[0]?.id, deliveries[1]?.id, 'redelivery must keep the same event id');
  assert.equal(sideEffects, 1, 'idempotent consumer should apply side effect once');
});

test('competing consumers share a group across instances without duplicates', async () => {
  const psA = makePubSub(schema, { pollIntervalMs: 25 });
  const psB = makePubSub(schema, { pollIntervalMs: 25 });
  pubsubs.push(psA, psB);

  const aIndexes: number[] = [];
  const bIndexes: number[] = [];

  await psA.subscribe(
    'semantics-group',
    (event, ack) => {
      if (event.index !== undefined) aIndexes.push(event.index);
      ack?.();
    },
    { group: 'shared-workers' },
  );
  await psB.subscribe(
    'semantics-group',
    (event, ack) => {
      if (event.index !== undefined) bIndexes.push(event.index);
      ack?.();
    },
    { group: 'shared-workers' },
  );

  const count = 12;
  for (let i = 0; i < count; i++) {
    await psA.publish('semantics-group', { type: 'work', data: { i }, runId: `group-${i}` });
  }

  await waitFor(() => aIndexes.length + bIndexes.length >= count, { timeoutMs: 8_000 });

  const delivered = [...aIndexes, ...bIndexes].sort((a, b) => a - b);
  assert.deepEqual(
    delivered,
    Array.from({ length: count }, (_, i) => i),
  );
  assert.equal(new Set(delivered).size, count, 'each index should be delivered once total');
  assert.ok(
    aIndexes.length > 0 || bIndexes.length > 0,
    'at least one worker should receive events',
  );
});

test('private subscribers fan out every event across instances', async () => {
  const psA = makePubSub(schema, { pollIntervalMs: 25 });
  const psB = makePubSub(schema, { pollIntervalMs: 25 });
  pubsubs.push(psA, psB);

  const aIndexes: number[] = [];
  const bIndexes: number[] = [];

  await psA.subscribe('semantics-fanout', (event, ack) => {
    if (event.index !== undefined) aIndexes.push(event.index);
    ack?.();
  });
  await psB.subscribe('semantics-fanout', (event, ack) => {
    if (event.index !== undefined) bIndexes.push(event.index);
    ack?.();
  });

  const count = 5;
  for (let i = 0; i < count; i++) {
    await psA.publish('semantics-fanout', { type: 'broadcast', data: i, runId: `fanout-${i}` });
  }

  await waitFor(() => aIndexes.length >= count && bIndexes.length >= count, { timeoutMs: 5_000 });

  assert.deepEqual(
    aIndexes.sort((a, b) => a - b),
    [0, 1, 2, 3, 4],
  );
  assert.deepEqual(
    bIndexes.sort((a, b) => a - b),
    [0, 1, 2, 3, 4],
  );
});

test('history, offset replay, and live continuation are ordered without boundary duplicates', async () => {
  const ps = makePubSub(schema, { pollIntervalMs: 25 });
  pubsubs.push(ps);

  for (let i = 0; i < 5; i++) {
    await ps.publish('semantics-replay', { type: 'history', data: i, runId: `history-${i}` });
  }

  const history = await ps.getHistory('semantics-replay');
  assert.deepEqual(
    history.map((event) => event.index),
    [0, 1, 2, 3, 4],
  );

  const replayed: number[] = [];
  await ps.subscribeFromOffset('semantics-replay', 2, (event, ack) => {
    if (event.index !== undefined) replayed.push(event.index);
    ack?.();
  });

  await waitFor(() => replayed.length >= 3, { timeoutMs: 5_000 });
  await ps.publish('semantics-replay', { type: 'live', data: 5, runId: 'history-5' });
  await waitFor(() => replayed.includes(5), { timeoutMs: 5_000 });

  assert.deepEqual(replayed, [2, 3, 4, 5]);
});

test('flush drains in-flight events before shutdown', async () => {
  const ps = makePubSub(schema, { pollIntervalMs: 25 });
  pubsubs.push(ps);

  const received: number[] = [];
  await ps.subscribe('semantics-flush', (event, ack) => {
    if (typeof event.data === 'number') received.push(event.data);
    ack?.();
  });

  const count = 8;
  for (let i = 0; i < count; i++) {
    await ps.publish('semantics-flush', { type: 'flush', data: i, runId: `flush-${i}` });
  }

  await ps.flush();
  assert.deepEqual(
    received.sort((a, b) => a - b),
    Array.from({ length: count }, (_, i) => i),
  );
});

test('unacked group delivery is recovered by another instance after close', async () => {
  const psA = makePubSub(schema, {
    ackDeadlineMs: 150,
    pollIntervalMs: 25,
    maxDeliveryAttempts: Number.POSITIVE_INFINITY,
  });
  const psB = makePubSub(schema, {
    ackDeadlineMs: 150,
    pollIntervalMs: 25,
    maxDeliveryAttempts: Number.POSITIVE_INFINITY,
  });
  pubsubs.push(psB);

  const firstDeliveries: Event[] = [];
  await psA.subscribe(
    'semantics-recovery',
    (event) => {
      firstDeliveries.push(event);
    },
    { group: 'recovery-workers' },
  );

  await psA.publish('semantics-recovery', {
    type: 'recoverable',
    data: { owner: 'first' },
    runId: 'recovery-run',
  });
  await waitFor(() => firstDeliveries.length === 1, { timeoutMs: 5_000 });
  await psA.close();

  const recovered: Event[] = [];
  await psB.subscribe(
    'semantics-recovery',
    (event, ack) => {
      recovered.push(event);
      ack?.();
    },
    { group: 'recovery-workers' },
  );

  await waitFor(() => recovered.length === 1, { timeoutMs: 5_000 });

  assert.equal(recovered[0]?.id, firstDeliveries[0]?.id);
  assert.equal(recovered[0]?.deliveryAttempt, 2);
});
