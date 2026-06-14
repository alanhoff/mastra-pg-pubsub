import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import pg from 'pg';
import {
  DATABASE_URL,
  dropSchema,
  makePubSub,
  makeTestLogger,
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

test('maxDeliveryAttempts caps redeliveries: event is dropped after max attempts', async () => {
  const maxDeliveryAttempts = 2;
  const ps = makePubSub(schema, {
    maxDeliveryAttempts,
    ackDeadlineMs: 200,
    pollIntervalMs: 50,
  });
  pubsubs.push(ps);

  const attempts: number[] = [];
  await ps.subscribe('topic-max-attempts', (event) => {
    // Never ack — let it exhaust
    if (event.deliveryAttempt !== undefined) {
      attempts.push(event.deliveryAttempt);
    }
  });

  await ps.publish('topic-max-attempts', { type: 'e', data: null, runId: 'r' });

  // Wait until we see maxDeliveryAttempts deliveries, then a bit more to confirm no extras
  await waitFor(() => attempts.length >= maxDeliveryAttempts, { timeoutMs: 5000 });
  await sleep(600); // Let extra redeliveries occur if buggy

  assert.equal(
    attempts.length,
    maxDeliveryAttempts,
    `expected exactly ${maxDeliveryAttempts} deliveries, got ${attempts.length}`,
  );
  assert.equal(attempts[0], 1);
  assert.equal(attempts[1], 2);
});

test('with deadLetter:true, exhausted event lands in dead_events table', async () => {
  const maxDeliveryAttempts = 2;
  const ps = makePubSub(schema, {
    maxDeliveryAttempts,
    ackDeadlineMs: 200,
    pollIntervalMs: 50,
    deadLetter: true,
  });
  pubsubs.push(ps);

  // Need to migrate to create dead_events table
  await ps.migrate();

  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  try {
    const attempts: number[] = [];
    await ps.subscribe('topic-dead-letter', (event) => {
      if (event.deliveryAttempt !== undefined) {
        attempts.push(event.deliveryAttempt);
      }
      // Never ack
    });

    await ps.publish('topic-dead-letter', { type: 'dead', data: { x: 1 }, runId: 'r-dead' });

    // Wait for delivery exhaustion
    await waitFor(() => attempts.length >= maxDeliveryAttempts, { timeoutMs: 5000 });
    // Give time for the drop/dead-letter insert to complete
    await sleep(400);

    const result = await pool.query<{
      event_id: string;
      topic: string;
      type: string;
      delivery_attempt: number;
    }>(`SELECT event_id, topic, type, delivery_attempt FROM "${schema}".dead_events`);

    assert.equal(result.rows.length, 1, 'expected one dead_events row');
    const row = result.rows[0];
    assert.ok(row);
    assert.equal(row.topic, 'topic-dead-letter');
    assert.equal(row.type, 'dead');
    // delivery_attempt stored is maxDeliveryAttempts (last attempt before drop)
    assert.equal(row.delivery_attempt, maxDeliveryAttempts);
  } finally {
    await pool.end();
  }
});

test('maxDeliveryAttempts=0 logs a warn and behaves as unbounded', async () => {
  const warnings: string[] = [];
  const ps = makePubSub(schema, {
    maxDeliveryAttempts: 0,
    ackDeadlineMs: 100,
    pollIntervalMs: 50,
    logger: makeTestLogger({
      warn: (msg: string) => {
        warnings.push(msg);
      },
    }),
  });
  pubsubs.push(ps);

  const attempts: number[] = [];
  await ps.subscribe('topic-unbounded', (event) => {
    if (event.deliveryAttempt !== undefined) {
      attempts.push(event.deliveryAttempt);
    }
    // Never ack
  });

  await ps.publish('topic-unbounded', { type: 'e', data: null, runId: 'r' });

  // Wait for at least 4 redeliveries
  await waitFor(() => attempts.length >= 4, { timeoutMs: 8000 });

  // Warn should have been emitted
  assert.ok(
    warnings.some((w) => w.includes('maxDeliveryAttempts=0')),
    `expected warn about maxDeliveryAttempts=0, got: ${JSON.stringify(warnings)}`,
  );

  // Should have received at least 4 attempts without being dropped
  assert.ok(attempts.length >= 4, `expected >= 4 attempts, got ${attempts.length}`);
});
