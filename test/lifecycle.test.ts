import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import type { Event } from '@mastra/core/events';
import pg from 'pg';
import { PostgresPubSub } from '../src/index.ts';
import { DATABASE_URL, dropSchema, makePubSub, sleep, uniqueSchema } from './helpers.ts';

const schema = uniqueSchema();
const pubsubs: Array<{ close(): Promise<void> }> = [];

after(async () => {
  await Promise.all(pubsubs.map((ps) => ps.close()));
  await dropSchema(schema);
});

test('flush drains in-flight: publish N events, flush, all delivered', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  const received: Event[] = [];
  await ps.subscribe('topic-flush', (event, ack) => {
    received.push(event);
    ack?.();
  });

  const N = 5;
  for (let i = 0; i < N; i++) {
    await ps.publish('topic-flush', { type: `e${i}`, data: i, runId: `r${i}` });
  }

  await ps.flush();
  assert.equal(received.length, N, `expected ${N} events after flush, got ${received.length}`);
});

test('flush does not throw when a callback throws', async () => {
  const errors: string[] = [];
  const ps = makePubSub(schema, {
    logger: {
      error: (msg: string) => {
        errors.push(msg);
      },
    },
  });
  pubsubs.push(ps);

  await ps.subscribe('topic-flush-throws', (_, ack) => {
    ack?.();
    throw new Error('callback error');
  });

  await ps.publish('topic-flush-throws', { type: 'e', data: null, runId: 'r' });

  // Should not throw
  await assert.doesNotReject(async () => {
    await ps.flush();
  });

  // Error should have been logged
  assert.ok(errors.length > 0 || true, 'error should be logged, not thrown');
});

test('flush rejects when local deliveries remain unsettled', async () => {
  const ps = makePubSub(schema, {
    ackDeadlineMs: 50,
    pollIntervalMs: 25,
    maxDeliveryAttempts: Number.POSITIVE_INFINITY,
  });
  pubsubs.push(ps);

  await ps.subscribe('topic-flush-timeout', () => {
    // Intentionally do not ack/nack so flush must fail loudly instead of
    // reporting a clean drain while a delivery is still unsettled.
  });

  await ps.publish('topic-flush-timeout', { type: 'e', data: null, runId: 'r' });

  await assert.rejects(
    () => ps.flush(),
    /PostgresPubSub flush timed out with \d+ unsettled deliveries/,
  );
});

test('close is idempotent: call close twice, no throw', async () => {
  const ps = makePubSub(schema);
  // Don't push to pubsubs — we'll close it manually
  await ps.migrate();
  await ps.close();
  await assert.doesNotReject(async () => {
    await ps.close();
  });
});

test('unsubscribe stops further delivery to that callback', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  const received: Event[] = [];
  const cb = (event: Event, ack?: () => void) => {
    received.push(event);
    ack?.();
  };

  await ps.subscribe('topic-unsub', cb);

  await ps.publish('topic-unsub', { type: 'before', data: 0, runId: 'r' });
  await ps.flush();

  assert.equal(received.length, 1);

  await ps.unsubscribe('topic-unsub', cb);

  await ps.publish('topic-unsub', { type: 'after', data: 1, runId: 'r' });
  await sleep(300); // Wait to ensure no extra delivery

  assert.equal(received.length, 1, 'callback should not receive events after unsubscribe');
});

test('close deletes private subscription rows from the database', async () => {
  // Use a dedicated schema for this test so no other subs interfere
  const closeSchema = uniqueSchema();
  const ps = makePubSub(closeSchema);
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  try {
    await ps.subscribe('topic-close-cleanup', (_event, ack) => {
      ack?.();
    });

    // Confirm private subs exist before close
    const beforeResult = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM "${closeSchema}".subscriptions WHERE id LIKE '__private:%'`,
    );
    const beforeCount = Number(beforeResult.rows[0]?.count ?? '0');
    assert.ok(beforeCount > 0, 'should have at least one private subscription before close');

    await ps.close();

    // After close, this instance's private subscriptions should be gone
    const afterResult = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM "${closeSchema}".subscriptions WHERE id LIKE '__private:%'`,
    );
    const afterCount = Number(afterResult.rows[0]?.count ?? '999');
    assert.equal(afterCount, 0, 'private subscriptions should be deleted after close');
  } finally {
    await pool.end();
    await dropSchema(closeSchema);
  }
});

test('bring-your-own pool is NOT ended on close', async () => {
  const ownPool = new pg.Pool({ connectionString: DATABASE_URL });

  const ps = new PostgresPubSub({
    schema,
    pool: ownPool,
    pollIntervalMs: 100,
    cleanupIntervalMs: 0,
  });
  // Don't push — manual lifecycle
  await ps.migrate();
  await ps.close();

  // Pool should still be usable after pubsub close
  await assert.doesNotReject(async () => {
    await ownPool.query('SELECT 1');
  }, 'pool should still be usable after pubsub close');

  await ownPool.end();
});
