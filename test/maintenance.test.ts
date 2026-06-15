import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import pg from 'pg';
import { DATABASE_URL, dropSchema, makePubSub, sleep, uniqueSchema, waitFor } from './helpers.ts';

const schema = uniqueSchema();
const pubsubs: Array<{ close(): Promise<void> }> = [];

after(async () => {
  await Promise.all(pubsubs.map((ps) => ps.close()));
  await dropSchema(schema);
});

test('retention trim: events beyond maxEventsPerTopic are removed after ack', async () => {
  const maxEventsPerTopic = 3;
  const ps = makePubSub(schema, {
    maxEventsPerTopic,
    cleanupIntervalMs: 150,
    pollIntervalMs: 50,
  });
  pubsubs.push(ps);

  const received: number[] = [];
  await ps.subscribe('topic-retention', (event, ack) => {
    if (event.index !== undefined) received.push(event.index);
    ack?.();
  });

  // Publish more than maxEventsPerTopic
  const total = maxEventsPerTopic + 3; // 6 events
  for (let i = 0; i < total; i++) {
    await ps.publish('topic-retention', { type: 'e', data: i, runId: `r${i}` });
  }

  // Wait for all events to be delivered and acked
  await waitFor(() => received.length >= total, { timeoutMs: 5000 });
  await ps.flush();

  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  try {
    // Wait for maintenance to trim events
    await waitFor(
      async () => {
        const result = await pool.query<{ count: string }>(
          `SELECT count(*)::text AS count FROM "${schema}".events WHERE topic = 'topic-retention'`,
        );
        return Number(result.rows[0]?.count ?? '999') <= maxEventsPerTopic;
      },
      { timeoutMs: 3000, intervalMs: 100 },
    );

    const result = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM "${schema}".events WHERE topic = 'topic-retention'`,
    );
    const count = Number(result.rows[0]?.count ?? '999');
    assert.ok(count <= maxEventsPerTopic, `expected <= ${maxEventsPerTopic} events, got ${count}`);
  } finally {
    await pool.end();
  }
});

test('retention does NOT delete events with pending deliveries', async () => {
  const maxEventsPerTopic = 2;
  const ps = makePubSub(schema, {
    maxEventsPerTopic,
    cleanupIntervalMs: 150,
    pollIntervalMs: 50,
    ackDeadlineMs: 30000, // Long deadline so deliveries stay pending
    settlement: 'explicit',
  });
  pubsubs.push(ps);

  // Subscribe but never ack — keeps deliveries pending
  await ps.subscribe('topic-retention-protect', (_event) => {
    // intentionally no ack
  });

  const total = maxEventsPerTopic + 2; // 4 events
  for (let i = 0; i < total; i++) {
    await ps.publish('topic-retention-protect', { type: 'e', data: i, runId: `r${i}` });
  }

  // Wait for maintenance cycles to run
  await sleep(500);

  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  try {
    const result = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM "${schema}".events WHERE topic = 'topic-retention-protect'`,
    );
    const count = Number(result.rows[0]?.count ?? '0');
    assert.equal(
      count,
      total,
      `expected all ${total} events retained (pending deliveries protect them), got ${count}`,
    );
  } finally {
    await pool.end();
  }
});

test('stale private subscription pruning', async () => {
  const staleSchema = uniqueSchema();
  const ps = makePubSub(staleSchema, {
    staleSubscriptionMs: 200,
    cleanupIntervalMs: 150,
    pollIntervalMs: 50,
  });
  const pool = new pg.Pool({ connectionString: DATABASE_URL });

  try {
    // Ensure schema is migrated before inserting a manually stale row. This
    // test uses a dedicated schema so maintenance timers from other tests in
    // this file cannot prune the row before the precondition assertion.
    await ps.migrate();

    const staleId = '__private:dead:staleness-test-001';
    await pool.query(
      `INSERT INTO "${staleSchema}".subscriptions (id, topic, is_group, last_seen_at)
       VALUES ($1, 'topic-stale', false, now() - interval '1 hour')
       ON CONFLICT (id) DO UPDATE SET last_seen_at = now() - interval '1 hour'`,
      [staleId],
    );

    const beforeResult = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM "${staleSchema}".subscriptions WHERE id = $1`,
      [staleId],
    );
    assert.equal(
      beforeResult.rows[0]?.count,
      '1',
      'stale subscription should exist before pruning',
    );

    // Trigger a subscribe to kick off ensureReady and startMaintenance.
    await ps.subscribe('topic-stale-trigger', (_, ack) => ack?.());

    await waitFor(
      async () => {
        const result = await pool.query<{ count: string }>(
          `SELECT count(*)::text AS count FROM "${staleSchema}".subscriptions WHERE id = $1`,
          [staleId],
        );
        return result.rows[0]?.count === '0';
      },
      { timeoutMs: 3000, intervalMs: 100 },
    );

    const afterResult = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM "${staleSchema}".subscriptions WHERE id = $1`,
      [staleId],
    );
    assert.equal(afterResult.rows[0]?.count, '0', 'stale subscription should be pruned');
  } finally {
    await pool.end();
    await ps.close();
    await dropSchema(staleSchema);
  }
});
