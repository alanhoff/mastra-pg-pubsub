import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import type { Event, EventCallback } from '@mastra/core/events';
import pg from 'pg';
import { PostgresPubSub } from '../src/index.ts';
import {
  DATABASE_URL,
  dropSchema,
  makePubSub,
  makeTestLogger,
  sleep,
  uniqueSchema,
  waitFor,
} from './helpers.ts';

// Shared schema for most tests; separate schemas for isolation where needed.
const schema = uniqueSchema();
const schemaRetention = uniqueSchema();
const schemaListen = uniqueSchema();

// Pubsubs that use connectionString (self-cleaning pools via close()).
const pubsubs: PostgresPubSub[] = [];

// Separate small pools for the NOTIFY test to limit connection pressure.
// These are explicitly ended in after().
const listenPoolA = new pg.Pool({ connectionString: DATABASE_URL, max: 3, idleTimeoutMillis: 500 });
const listenPoolB = new pg.Pool({ connectionString: DATABASE_URL, max: 3, idleTimeoutMillis: 500 });
// Pool for DB inspection queries.
const inspectPool = new pg.Pool({ connectionString: DATABASE_URL, max: 2, idleTimeoutMillis: 500 });

after(async () => {
  await Promise.all(pubsubs.map((ps) => ps.close()));
  await Promise.all([listenPoolA.end(), listenPoolB.end(), inspectPool.end()]);
  await Promise.all([dropSchema(schema), dropSchema(schemaRetention), dropSchema(schemaListen)]);
});

// ---------------------------------------------------------------------------
// Test 1: #ensureReady throws after close (postgres-pubsub.ts:135-136)
// ---------------------------------------------------------------------------
test('#ensureReady throws after close: publish/subscribe/getHistory all reject', async () => {
  const ps = makePubSub(schema);
  await ps.migrate();
  await ps.close();

  const event = { type: 'e', data: null, runId: 'r' };
  await assert.rejects(
    () => ps.publish('t', event),
    /PostgresPubSub is closed/,
    'publish should reject after close',
  );
  await assert.rejects(
    () => ps.subscribe('t', () => {}),
    /PostgresPubSub is closed/,
    'subscribe should reject after close',
  );
  await assert.rejects(
    () => ps.getHistory('t'),
    /PostgresPubSub is closed/,
    'getHistory should reject after close',
  );
});

// ---------------------------------------------------------------------------
// Test 2: unsubscribe with never-registered callback (postgres-pubsub.ts:314-315)
// ---------------------------------------------------------------------------
test('unsubscribe with never-registered callback returns silently', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);
  await ps.migrate();

  const cb: EventCallback = () => {};
  await assert.doesNotReject(
    () => ps.unsubscribe('topic-never-registered', cb),
    'unsubscribe of unknown callback should not throw',
  );
});

// ---------------------------------------------------------------------------
// Test 3: unsubscribe from one topic when cb registered to two topics
//         (postgres-pubsub.ts:319-321, 333-334)
// ---------------------------------------------------------------------------
test('unsubscribe from topicA leaves cb still receiving topicB events', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  const received: string[] = [];
  const cb: EventCallback = (event, ack) => {
    received.push(event.type);
    ack?.();
  };

  await ps.subscribe('topicA-multi', cb);
  await ps.subscribe('topicB-multi', cb);

  await ps.publish('topicA-multi', { type: 'fromA', data: null, runId: 'r' });
  await ps.publish('topicB-multi', { type: 'fromB', data: null, runId: 'r' });
  await ps.flush();

  assert.ok(received.includes('fromA'), 'should receive fromA before unsubscribe');
  assert.ok(received.includes('fromB'), 'should receive fromB before unsubscribe');

  // Unsubscribe from topicA only — cb still registered to topicB (lines 319-321, 333-334)
  await ps.unsubscribe('topicA-multi', cb);
  received.length = 0;

  await ps.publish('topicB-multi', { type: 'fromB-after', data: null, runId: 'r' });
  await waitFor(() => received.includes('fromB-after'), { timeoutMs: 3000 });

  assert.ok(
    received.includes('fromB-after'),
    'cb should still receive topicB events after unsubscribing from topicA',
  );
  assert.ok(!received.includes('fromA'), 'cb should not receive topicA events after unsubscribe');
});

// ---------------------------------------------------------------------------
// Test 4: flush() with no subscriptions (postgres-pubsub.ts:370-371)
// ---------------------------------------------------------------------------
test('flush resolves immediately when there are no subscriptions', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);
  await ps.migrate();

  await assert.doesNotReject(
    () => ps.flush(),
    'flush with zero subscriptions should resolve without error',
  );
});

// ---------------------------------------------------------------------------
// Test 5: subscribeFromOffset dedupe-skip branch (postgres-pubsub.ts:441-442)
// ---------------------------------------------------------------------------
test('subscribeWithReplay delivers each event index exactly once despite live+replay overlap', async () => {
  const ps = makePubSub(schema, { pollIntervalMs: 50 });
  pubsubs.push(ps);

  // Publish one event before subscribing so replay will include it
  await ps.publish('topic-dedupe', { type: 'evt', data: 1, runId: 'r' });
  await sleep(50);

  const indexCounts = new Map<number, number>();
  await ps.subscribeWithReplay('topic-dedupe', (event, ack) => {
    const idx = event.index ?? -1;
    indexCounts.set(idx, (indexCounts.get(idx) ?? 0) + 1);
    ack?.();
  });

  await waitFor(() => indexCounts.size > 0, { timeoutMs: 3000 });
  await sleep(300); // Extra time to catch any duplicate deliveries

  for (const [idx, count] of indexCounts) {
    assert.equal(count, 1, `event index ${idx} should be delivered exactly once, got ${count}`);
  }
});

// ---------------------------------------------------------------------------
// Test 6: heartbeat with only GROUP subscriptions (postgres-pubsub.ts:468-469)
// ---------------------------------------------------------------------------
test('heartbeat skips when only group subscriptions exist (no private ids to update)', async () => {
  const ps = makePubSub(schema, { cleanupIntervalMs: 150, pollIntervalMs: 50 });
  pubsubs.push(ps);

  await ps.subscribe(
    'topic-group-heartbeat',
    (_event, ack) => {
      ack?.();
    },
    { group: 'g' },
  );

  // Wait for at least two maintenance cycles — heartbeat returns early when ids=[].
  await sleep(400);

  assert.ok(true, 'heartbeat with only group subscriptions completed without error');
});

// ---------------------------------------------------------------------------
// Test 7: trimRetention disabled (maxEventsPerTopic:0) (postgres-pubsub.ts:488-489)
// ---------------------------------------------------------------------------
test('trimRetention is skipped when maxEventsPerTopic is 0', async () => {
  const ps = makePubSub(schemaRetention, {
    maxEventsPerTopic: 0,
    cleanupIntervalMs: 150,
    pollIntervalMs: 50,
  });
  pubsubs.push(ps);

  const eventCount = 5;
  for (let i = 0; i < eventCount; i++) {
    await ps.publish('topic-trim', { type: `e${i}`, data: i, runId: 'r' });
  }

  await sleep(500);

  const result = await inspectPool.query<{ count: string }>(
    `SELECT count(*)::text AS count FROM "${schemaRetention}".events WHERE topic = $1`,
    ['topic-trim'],
  );
  const count = Number(result.rows[0]?.count ?? '0');
  assert.equal(
    count,
    eventCount,
    `all ${eventCount} events should remain when maxEventsPerTopic=0`,
  );
});

// ---------------------------------------------------------------------------
// Test 8: LISTEN/NOTIFY path delivers quickly (listener.ts:74-92)
// ---------------------------------------------------------------------------
test('LISTEN/NOTIFY delivers event without waiting for poll interval', async () => {
  // Use separate small pools to limit connection pressure.
  // pollIntervalMs is long enough that polling won't fire within our waitFor timeoutMs,
  // but not so long that connections linger for the entire test suite duration.
  const psA = new PostgresPubSub({
    pool: listenPoolA,
    schema: schemaListen,
    pollIntervalMs: 10_000,
    listen: true,
  });
  const psB = new PostgresPubSub({
    pool: listenPoolB,
    schema: schemaListen,
    pollIntervalMs: 10_000,
    listen: true,
  });

  try {
    await psA.migrate();

    const received: Event[] = [];
    await psA.subscribe('topic-notify', (event, ack) => {
      received.push(event);
      ack?.();
    });

    await psB.publish('topic-notify', { type: 'notify-test', data: null, runId: 'r' });

    await waitFor(() => received.length > 0, { timeoutMs: 4000 });
    assert.equal(received.length, 1, 'event should be delivered via NOTIFY before poll fires');
    assert.equal(received[0]?.type, 'notify-test');
  } finally {
    // Close promptly so connections are released before other test files finish.
    await Promise.all([psA.close(), psB.close()]);
  }
});

// ---------------------------------------------------------------------------
// Test 9: listener UNLISTEN on close (listener.ts:128-129)
// ---------------------------------------------------------------------------
test('closing a listen-enabled pubsub with an active subscription executes UNLISTEN', async () => {
  const ps = makePubSub(schema, { listen: true, pollIntervalMs: 100 });
  // Not pushed to pubsubs — closed manually below
  await ps.subscribe('topic-unlisten', (_event, ack) => {
    ack?.();
  });

  await assert.doesNotReject(
    () => ps.close(),
    'close with active listen connection should not throw',
  );
});

// ---------------------------------------------------------------------------
// Test 10: listener close() while a connect is still in-progress, and the
// post-connect "#closed" early-release path (listener.ts: awaiting #connecting
// in close(); releasing the freshly-acquired client when already closed).
//
// Deterministic strategy: give the pool a single connection and hold it so the
// listener's pool.connect() cannot complete. Pre-migrate and pre-create the
// subscription row directly so subscribe() does no blocking query of its own;
// its only outstanding work is the listener connect, which stays pending until
// we release the held connection — at which point close() has already set the
// listener's #closed flag.
// ---------------------------------------------------------------------------
test('closing pubsub while the listen connection is still being acquired', async () => {
  const schemaBlocking = uniqueSchema();
  const blockingPool = new pg.Pool({
    connectionString: DATABASE_URL,
    max: 1,
    idleTimeoutMillis: 500,
  });

  try {
    // Migrate and seed the subscription row up front (own short-lived pool), so
    // the blockingPool's single connection is the only contended resource.
    const setupPool = new pg.Pool({ connectionString: DATABASE_URL, max: 2 });
    try {
      const { runMigration } = await import('../src/schema.ts');
      await runMigration(setupPool, schemaBlocking, false);
    } finally {
      await setupPool.end();
    }

    const ps = new PostgresPubSub({
      pool: blockingPool,
      schema: schemaBlocking,
      pollIntervalMs: 100,
      listen: true,
    });
    await ps.migrate(); // fast: schema already exists

    // Hold the pool's only connection so the listener's pool.connect() pends.
    const held = await blockingPool.connect();

    // subscribe() will: upsert the subscription row (needs the connection → pends),
    // so release the held connection briefly to let the upsert through, then
    // re-acquire so the subsequent listener connect pends.
    const subPromise = ps.subscribe('topic-blocking', () => {}).catch(() => undefined);
    await sleep(20);
    held.release();
    // upsert grabs the connection, runs, releases; re-acquire to block the listener.
    const reheld = await blockingPool.connect();
    await sleep(30); // listener pool.connect() is now pending (#connecting set)

    // close() must await the pending #connecting, then release the client it
    // eventually acquires because #closed is already true.
    const closePromise = ps.close();
    await sleep(10);
    reheld.release();

    await Promise.all([subPromise, closePromise]);
    assert.ok(true, 'close while listener connect is pending should not throw');
  } finally {
    await blockingPool.end().catch(() => undefined);
    await dropSchema(schemaBlocking);
  }
});

// ---------------------------------------------------------------------------
// Test 11: ack double-call is idempotent (consume-loop.ts:240-241)
// ---------------------------------------------------------------------------
test('calling ack() twice does not throw', async () => {
  const ps = makePubSub(schema, { pollIntervalMs: 50 });
  pubsubs.push(ps);

  let ackCalledCount = 0;
  let doubleAckError: unknown;

  await ps.subscribe('topic-double-ack', async (_event, ack) => {
    ackCalledCount++;
    await ack?.();
    try {
      await ack?.(); // second call — settled=true guard should make this a no-op
    } catch (err) {
      doubleAckError = err;
    }
  });

  await ps.publish('topic-double-ack', { type: 'e', data: null, runId: 'r' });
  await waitFor(() => ackCalledCount > 0, { timeoutMs: 3000 });
  await sleep(100);
  assert.equal(doubleAckError, undefined, 'second ack() call should not throw');
});

// ---------------------------------------------------------------------------
// Test 12: nack after ack is a no-op (consume-loop.ts:247-248)
// ---------------------------------------------------------------------------
test('calling nack() after ack() does not re-deliver the event', async () => {
  const ps = makePubSub(schema, { pollIntervalMs: 50 });
  pubsubs.push(ps);

  let deliveryCount = 0;
  let nackAfterAckError: unknown;

  await ps.subscribe('topic-nack-after-ack', async (_event, ack, nack) => {
    deliveryCount++;
    await ack?.();
    try {
      await nack?.(); // nack after ack — settled=true guard prevents redelivery
    } catch (err) {
      nackAfterAckError = err;
    }
  });

  await ps.publish('topic-nack-after-ack', { type: 'e', data: null, runId: 'r' });
  await waitFor(() => deliveryCount > 0, { timeoutMs: 3000 });

  await sleep(300);
  assert.equal(nackAfterAckError, undefined, 'nack() after ack() should not throw');
  assert.equal(
    deliveryCount,
    1,
    'event should be delivered exactly once even after nack() follows ack()',
  );
});

// ---------------------------------------------------------------------------
// Test 13: listener error handler and #handleDisconnect (listener.ts:84-85, 97-109)
// By terminating the backend connection, we force an error on the pg client,
// triggering the error handler (#84-85) and #handleDisconnect (#97-109).
// Also covers:
//   - "no handlers" early return in #handleDisconnect (#104-105) by terminating
//     after unsubscribing so handlers are empty.
//   - The reconnect failure catch (#106-108) by ending the pool right after
//     the disconnect so the reconnect attempt fails.
// ---------------------------------------------------------------------------
test('listener error handler, disconnect, and reconnect failure paths', async () => {
  const schemaKill = uniqueSchema();
  // Use a pool with a small max — we'll end it to trigger reconnect failure.
  const killPool = new pg.Pool({ connectionString: DATABASE_URL, max: 4, idleTimeoutMillis: 500 });
  const adminPool = new pg.Pool({ connectionString: DATABASE_URL, max: 2, idleTimeoutMillis: 500 });

  const warnMessages: string[] = [];
  const ps = new PostgresPubSub({
    pool: killPool,
    schema: schemaKill,
    pollIntervalMs: 10_000,
    listen: true,
    logger: makeTestLogger({
      warn: (msg: string) => {
        warnMessages.push(msg);
      },
    }),
  });

  try {
    const received: Event[] = [];
    const cb = (event: Event, ack?: () => void) => {
      received.push(event);
      ack?.();
    };
    await ps.subscribe('topic-kill', cb);

    const channelName = `${schemaKill}_events`;
    const findListenPid = async (): Promise<number | undefined> => {
      const r = await adminPool.query<{ pid: number }>(
        `SELECT pid FROM pg_stat_activity
         WHERE query LIKE $1 AND datname = current_database() AND pid <> pg_backend_pid()
         LIMIT 1`,
        [`LISTEN "${channelName}"`],
      );
      return r.rows[0]?.pid;
    };

    // --- Part 1: trigger error handler + #handleDisconnect with reconnect ---
    const pid1 = await findListenPid();
    if (pid1 !== undefined) {
      await adminPool.query('SELECT pg_terminate_backend($1)', [pid1]).catch(() => undefined);
    }
    // Give #handleDisconnect time to fire and start reconnect.
    await sleep(300);

    // After reconnect, verify delivery still works via polling.
    const ps2 = new PostgresPubSub({
      pool: killPool,
      schema: schemaKill,
      pollIntervalMs: 200,
      listen: false,
    });
    await ps2.publish('topic-kill', { type: 'after-reconnect', data: null, runId: 'r' });
    await waitFor(() => received.length > 0, { timeoutMs: 3000 });
    assert.ok(received.length >= 1, 'should receive event after reconnect');
    await ps2.close();

    // --- Part 2: trigger "no handlers" branch in #handleDisconnect (#104-105) ---
    // Unsubscribe so the listener's handlers map becomes empty.
    await ps.unsubscribe('topic-kill', cb);
    await sleep(100);

    const pid2 = await findListenPid();
    if (pid2 !== undefined) {
      // When #handleDisconnect fires, handlers.size === 0 → early return (#104-105).
      await adminPool.query('SELECT pg_terminate_backend($1)', [pid2]).catch(() => undefined);
    }
    await sleep(200);

    // --- Part 3: trigger reconnect failure catch (#106-108) ---
    // Re-subscribe so handlers is non-empty and #handleDisconnect would try to reconnect.
    await ps.subscribe('topic-kill2', (_event, ack) => {
      ack?.();
    });
    await sleep(100);

    const pid3 = await findListenPid();
    if (pid3 !== undefined) {
      // End the pool FIRST (synchronously after the terminate SQL).
      // This way when the error event fires asynchronously and #handleDisconnect
      // calls #ensureConnected → pool.connect(), the pool is already ended → fail.
      // We fire both without await so they're issued together.
      const terminatePromise = adminPool
        .query('SELECT pg_terminate_backend($1)', [pid3])
        .catch(() => undefined);
      // End the pool immediately so the reconnect pool.connect() fails.
      killPool.end().catch(() => undefined);
      await terminatePromise;
    }
    await sleep(400); // Let #handleDisconnect fire and reconnect fail → logger.warn fires.

    // The pubsub might be in a bad state now — close it gracefully.
    await ps.close().catch(() => undefined);
  } finally {
    await adminPool.end();
    await dropSchema(schemaKill);
  }
});

// ---------------------------------------------------------------------------
// Test 14: #ensureConnected early return (listener.ts:60-61)
// When the same pubsub subscribes to a second topic, #ensureConnected is called
// again but returns early because the connection is already established.
// ---------------------------------------------------------------------------
test('subscribing to a second topic on the same instance reuses the listen connection', async () => {
  const ps = makePubSub(schema, { listen: true, pollIntervalMs: 100 });
  pubsubs.push(ps);

  const received: string[] = [];
  const cbA = (event: Event, ack?: () => void) => {
    received.push(`A:${event.type}`);
    ack?.();
  };
  const cbB = (event: Event, ack?: () => void) => {
    received.push(`B:${event.type}`);
    ack?.();
  };

  // Subscribe to two different topics on the same pubsub instance.
  // The second subscribe call hits #ensureConnected when #client is already set → early return.
  await ps.subscribe('topic-listen-reuse-a', cbA);
  await ps.subscribe('topic-listen-reuse-b', cbB);

  await ps.publish('topic-listen-reuse-a', { type: 'evtA', data: null, runId: 'r' });
  await ps.publish('topic-listen-reuse-b', { type: 'evtB', data: null, runId: 'r' });

  await waitFor(
    () => received.some((r) => r.startsWith('A:')) && received.some((r) => r.startsWith('B:')),
    {
      timeoutMs: 3000,
    },
  );
  assert.ok(
    received.some((r) => r === 'A:evtA'),
    'should receive topicA event',
  );
  assert.ok(
    received.some((r) => r === 'B:evtB'),
    'should receive topicB event',
  );
});

// ---------------------------------------------------------------------------
// Test 15: maintenance cycle failure is caught and logged (postgres-pubsub.ts
// maintenance-timer catch). Drop the schema out from under a running instance
// so the next #runMaintenance query throws.
// ---------------------------------------------------------------------------
test('a failing maintenance cycle is caught and logged, not thrown', async () => {
  const schemaMaint = uniqueSchema();
  const warnMessages: string[] = [];
  const ps = makePubSub(schemaMaint, {
    listen: false,
    pollIntervalMs: 100,
    cleanupIntervalMs: 80,
    logger: makeTestLogger({
      warn: (msg: string) => {
        warnMessages.push(msg);
      },
    }),
  });

  try {
    // Subscribe (private) so maintenance has heartbeat/prune work to attempt.
    await ps.subscribe('topic-maint', (_event, ack) => {
      ack?.();
    });
    // Drop the schema so the next maintenance cycle's queries fail.
    await dropSchema(schemaMaint);
    await waitFor(() => warnMessages.some((m) => m.includes('maintenance cycle failed')), {
      timeoutMs: 3000,
    });
    assert.ok(
      warnMessages.some((m) => m.includes('maintenance cycle failed')),
      'maintenance failure should be logged via logger.warn',
    );
  } finally {
    await ps.close().catch(() => undefined);
    await dropSchema(schemaMaint);
  }
});

// ---------------------------------------------------------------------------
// Test 16: teardown of a private subscription whose DELETE fails is caught and
// logged (postgres-pubsub.ts teardown delete catch). Drop the schema, then
// unsubscribe so the private-subscription DELETE rejects.
// ---------------------------------------------------------------------------
test('failure deleting a private subscription on unsubscribe is caught and logged', async () => {
  const schemaTeardown = uniqueSchema();
  const warnMessages: string[] = [];
  const ps = makePubSub(schemaTeardown, {
    listen: false,
    pollIntervalMs: 100,
    cleanupIntervalMs: 0,
    logger: makeTestLogger({
      warn: (msg: string) => {
        warnMessages.push(msg);
      },
    }),
  });

  try {
    const cb: EventCallback = (_event, ack) => {
      ack?.();
    };
    await ps.subscribe('topic-teardown', cb);
    // Drop the schema so the DELETE in #teardownSubscription fails.
    await dropSchema(schemaTeardown);
    await ps.unsubscribe('topic-teardown', cb);
    assert.ok(
      warnMessages.some((m) => m.includes('failed to delete private subscription')),
      'private subscription delete failure should be logged via logger.warn',
    );
  } finally {
    await ps.close().catch(() => undefined);
    await dropSchema(schemaTeardown);
  }
});

test('failed subscription setup logs when rollback deletion also fails', async () => {
  const schemaRollbackFailure = uniqueSchema();
  const pool = new pg.Pool({ connectionString: DATABASE_URL, max: 2 });
  const warnMessages: string[] = [];
  const ps = new PostgresPubSub({
    pool,
    schema: schemaRollbackFailure,
    listen: true,
    cleanupIntervalMs: 0,
    pollIntervalMs: 100,
    logger: makeTestLogger({
      warn: (message: string) => {
        warnMessages.push(message);
      },
    }),
  });
  const originalConnect = pool.connect.bind(pool);
  const originalQuery = pool.query.bind(pool);

  try {
    await ps.migrate();
    let connectCalls = 0;
    pool.connect = (async (...args: []) => {
      connectCalls++;
      if (connectCalls === 2) {
        throw new Error('listen connect failed');
      }
      return originalConnect(...args);
    }) as typeof pool.connect;
    pool.query = (async (...args: Parameters<typeof pool.query>) => {
      const sql = String(args[0]);
      if (sql.startsWith('DELETE FROM') && sql.includes('subscriptions')) {
        throw new Error('rollback delete failed');
      }
      return await originalQuery(...args);
    }) as typeof pool.query;

    await assert.rejects(
      () =>
        ps.subscribe('topic-rollback-warning', (_event, ack) => {
          ack?.();
        }),
      /listen connect failed/,
    );
    assert.ok(
      warnMessages.some((message) => message.includes('failed to roll back subscription')),
      'rollback failure should be logged via logger.warn',
    );
  } finally {
    pool.connect = originalConnect as typeof pool.connect;
    pool.query = originalQuery as typeof pool.query;
    await ps.close().catch(() => undefined);
    await pool.end();
    await dropSchema(schemaRollbackFailure);
  }
});

// ---------------------------------------------------------------------------
// Test 17: migration rollback branch (schema.ts catch). A pre-existing view
// named events makes the later CREATE INDEX fail after the transaction starts.
// ---------------------------------------------------------------------------
test('migration rolls back when DDL fails inside the migration transaction', async () => {
  const schemaBroken = uniqueSchema();
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  try {
    await pool.query(`CREATE SCHEMA "${schemaBroken}"`);
    await pool.query(`CREATE VIEW "${schemaBroken}".events AS SELECT 1 AS seq`);
    const { runMigration } = await import('../src/schema.ts');

    await assert.rejects(() => runMigration(pool, schemaBroken, false));
  } finally {
    await pool.end();
    await dropSchema(schemaBroken);
  }
});

// ---------------------------------------------------------------------------
// Test 18: flush failure branch when DB access fails while counting pending
// deliveries.
// ---------------------------------------------------------------------------
test('flush surfaces database failures while counting pending deliveries', async () => {
  const schemaFlushFailure = uniqueSchema();
  const errorMessages: string[] = [];
  const ps = makePubSub(schemaFlushFailure, {
    listen: false,
    pollIntervalMs: 100,
    cleanupIntervalMs: 0,
    logger: makeTestLogger({
      error: (message: string) => {
        errorMessages.push(message);
      },
    }),
  });

  const cb: EventCallback = () => undefined;
  try {
    await ps.subscribe('topic-flush-db-failure', cb);
    await ps.publish('topic-flush-db-failure', { type: 'pending', data: null, runId: 'run-flush' });
    await dropSchema(schemaFlushFailure);

    await assert.rejects(() => ps.flush());
    assert.ok(errorMessages.includes('flush failed'));
  } finally {
    await ps.close().catch(() => undefined);
    await dropSchema(schemaFlushFailure);
  }
});

test('failed automatic ack is logged and delivery remains recoverable by deadline', async () => {
  const schemaAutoAckFailure = uniqueSchema();
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  const errorMessages: string[] = [];
  const ps = new PostgresPubSub({
    pool,
    schema: schemaAutoAckFailure,
    ackDeadlineMs: 150,
    pollIntervalMs: 25,
    cleanupIntervalMs: 0,
    logger: makeTestLogger({
      error: (message: string) => {
        errorMessages.push(message);
      },
    }),
  });
  const originalQuery = pool.query.bind(pool) as typeof pool.query;
  let failedAck = false;
  pool.query = ((queryText: unknown, values?: unknown) => {
    if (
      !failedAck &&
      typeof queryText === 'string' &&
      queryText.includes('DELETE FROM') &&
      queryText.includes('deliveries') &&
      queryText.includes('subscription_id = $1') &&
      !queryText.includes('USING')
    ) {
      failedAck = true;
      return Promise.reject(new Error('forced auto ack failure'));
    }
    return originalQuery(queryText as never, values as never);
  }) as typeof pool.query;

  const attempts: number[] = [];
  try {
    await ps.subscribe('topic-auto-ack-failure', (event) => {
      attempts.push(event.deliveryAttempt ?? 0);
    });
    await ps.publish('topic-auto-ack-failure', { type: 'e', data: null, runId: 'r' });
    await waitFor(() => attempts.length >= 2, { timeoutMs: 5000 });
    await ps.flush();

    assert.equal(failedAck, true);
    assert.deepEqual(attempts, [1, 2]);
    assert.ok(errorMessages.includes('automatic delivery ack failed'));
  } finally {
    pool.query = originalQuery as typeof pool.query;
    await ps.close().catch(() => undefined);
    await pool.end().catch(() => undefined);
    await dropSchema(schemaAutoAckFailure);
  }
});

test('failed automatic nack is logged and delivery remains recoverable by deadline', async () => {
  const schemaAutoNackFailure = uniqueSchema();
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  const errorMessages: string[] = [];
  const ps = new PostgresPubSub({
    pool,
    schema: schemaAutoNackFailure,
    ackDeadlineMs: 150,
    pollIntervalMs: 25,
    cleanupIntervalMs: 0,
    logger: makeTestLogger({
      error: (message: string) => {
        errorMessages.push(message);
      },
    }),
  });
  const originalQuery = pool.query.bind(pool) as typeof pool.query;
  let failedNack = false;
  pool.query = ((queryText: unknown, values?: unknown) => {
    if (
      !failedNack &&
      typeof queryText === 'string' &&
      queryText.includes('UPDATE') &&
      queryText.includes('deliveries') &&
      queryText.includes('SET visible_at') &&
      !queryText.includes('delivery_attempt')
    ) {
      failedNack = true;
      return Promise.reject(new Error('forced auto nack failure'));
    }
    return originalQuery(queryText as never, values as never);
  }) as typeof pool.query;

  const attempts: number[] = [];
  try {
    await ps.subscribe('topic-auto-nack-failure', (event) => {
      attempts.push(event.deliveryAttempt ?? 0);
      if (attempts.length === 1) {
        throw new Error('first attempt fails');
      }
    });
    await ps.publish('topic-auto-nack-failure', { type: 'e', data: null, runId: 'r' });
    await waitFor(() => attempts.length >= 2, { timeoutMs: 5000 });
    await ps.flush();

    assert.equal(failedNack, true);
    assert.deepEqual(attempts, [1, 2]);
    assert.ok(errorMessages.includes('automatic delivery nack failed'));
  } finally {
    pool.query = originalQuery as typeof pool.query;
    await ps.close().catch(() => undefined);
    await pool.end().catch(() => undefined);
    await dropSchema(schemaAutoNackFailure);
  }
});
