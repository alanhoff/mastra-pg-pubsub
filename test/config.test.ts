import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import pg from 'pg';
import { PostgresPubSub } from '../src/index.ts';
import {
  DATABASE_URL,
  dropSchema,
  isDisposableTestDatabase,
  makePubSub,
  makeTestLogger,
  schemaExists,
  tableExists,
  uniqueSchema,
} from './helpers.ts';

const schema = uniqueSchema();
const pubsubs: Array<{ close(): Promise<void> }> = [];

after(async () => {
  await Promise.all(pubsubs.map((ps) => ps.close()));
  await dropSchema(schema);
});

test('invalid schema names throw from the constructor', () => {
  const invalidNames = ['Bad-Name', '1abc', 'has space', 'with-dash', 'pg_internal', ''];
  for (const name of invalidNames) {
    assert.throws(
      () =>
        new PostgresPubSub({
          connectionString: DATABASE_URL,
          schema: name,
        }),
      /Invalid schema name/,
      `expected throw for schema name: ${JSON.stringify(name)}`,
    );
  }
});

test('valid schema name is accepted without throwing', () => {
  const ps = new PostgresPubSub({
    connectionString: DATABASE_URL,
    schema: 'valid_schema_123',
  });
  pubsubs.push(ps);
  // Just assert it's created without error
  assert.ok(ps);
});

test('providing both connectionString and pool throws', () => {
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  assert.throws(
    () =>
      new PostgresPubSub({
        connectionString: DATABASE_URL,
        pool,
      }),
    /connectionString or pool, not both/,
  );
  pool.end().catch(() => undefined);
});

test('providing neither connectionString nor pool throws', () => {
  assert.throws(() => new PostgresPubSub({}), /connectionString or pool is required/);
});

test('bring-your-own pool is NOT ended on close', async () => {
  const ownPool = new pg.Pool({ connectionString: DATABASE_URL });

  const ps = new PostgresPubSub({
    pool: ownPool,
    schema,
    pollIntervalMs: 100,
    cleanupIntervalMs: 0,
  });
  // Don't push — manual lifecycle

  await ps.migrate();
  await ps.close();

  // Pool should still be usable after pubsub close
  await assert.doesNotReject(async () => {
    const result = await ownPool.query<{ one: string }>('SELECT 1::text AS one');
    assert.equal(result.rows[0]?.one, '1');
  }, 'pool should still be usable after pubsub close');

  await ownPool.end();
});

test('migrate() is idempotent: call twice without error', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  await ps.migrate();
  await assert.doesNotReject(async () => {
    await ps.migrate();
  });
});

test('concurrent migrate across two instances does not error', async () => {
  const ps1 = makePubSub(uniqueSchema());
  const ps2 = makePubSub(uniqueSchema());
  pubsubs.push(ps1, ps2);

  await assert.doesNotReject(async () => {
    await Promise.all([ps1.migrate(), ps2.migrate()]);
  });
});

test('schema defaults to pg_pubsub and auto-creates the schema when not provided', async (t) => {
  if (!isDisposableTestDatabase()) {
    t.skip('default-schema drop is only safe against the disposable local test database');
    return;
  }
  await dropSchema('pg_pubsub');
  const ps = new PostgresPubSub({
    connectionString: DATABASE_URL,
    cleanupIntervalMs: 0,
    pollIntervalMs: 100,
  });
  try {
    await ps.migrate();
    assert.equal(await schemaExists('pg_pubsub'), true);
    assert.equal(await tableExists('pg_pubsub', 'events'), true);
    assert.equal(await tableExists('pg_pubsub', 'subscriptions'), true);
  } finally {
    await ps.close();
    await dropSchema('pg_pubsub');
  }
});

test('pre-created default schema can migrate with an ordinary schema-scoped role', async (t) => {
  if (!isDisposableTestDatabase()) {
    t.skip('role management is only safe against the disposable local test database');
    return;
  }

  const role = uniqueSchema().replace(/^test_/, 'app_');
  const password = `pw_${role}`;
  const adminPool = new pg.Pool({ connectionString: DATABASE_URL });
  const lowPrivilegeUrl = new URL(DATABASE_URL);
  lowPrivilegeUrl.username = role;
  lowPrivilegeUrl.password = password;
  let ps: PostgresPubSub | undefined;

  try {
    await adminPool.query('DROP SCHEMA IF EXISTS "pg_pubsub" CASCADE');
    await adminPool.query(`DROP ROLE IF EXISTS "${role}"`);
    await adminPool.query(`CREATE ROLE "${role}" LOGIN PASSWORD '${password}'`);
    try {
      await adminPool.query('BEGIN');
      await adminPool.query('SET LOCAL allow_system_table_mods = on');
      await adminPool.query('CREATE SCHEMA "pg_pubsub"');
      await adminPool.query('COMMIT');
    } catch (error) {
      await adminPool.query('ROLLBACK').catch(() => undefined);
      throw error;
    }
    await adminPool.query(`GRANT USAGE, CREATE ON SCHEMA "pg_pubsub" TO "${role}"`);

    ps = new PostgresPubSub({
      connectionString: lowPrivilegeUrl.toString(),
      cleanupIntervalMs: 0,
      pollIntervalMs: 100,
    });

    await ps.migrate();
    assert.equal(await schemaExists('pg_pubsub'), true);
    assert.equal(await tableExists('pg_pubsub', 'events'), true);
  } finally {
    await ps?.close().catch(() => undefined);
    await adminPool.query('DROP SCHEMA IF EXISTS "pg_pubsub" CASCADE');
    await adminPool.query(`DROP ROLE IF EXISTS "${role}"`);
    await adminPool.end();
  }
});

test('custom schema is still created when schema is provided', async () => {
  const customSchema = uniqueSchema();
  const ps = new PostgresPubSub({
    connectionString: DATABASE_URL,
    schema: customSchema,
    cleanupIntervalMs: 0,
    pollIntervalMs: 100,
  });
  try {
    await ps.migrate();
    assert.equal(await schemaExists(customSchema), true);
    assert.equal(await tableExists(customSchema, 'events'), true);
  } finally {
    await ps.close();
    await dropSchema(customSchema);
  }
});

test('maxDeliveryAttempts=0 produces exactly one warn', () => {
  const warnings: string[] = [];
  const ps = new PostgresPubSub({
    connectionString: DATABASE_URL,
    schema,
    maxDeliveryAttempts: 0,
    cleanupIntervalMs: 0,
    pollIntervalMs: 100,
    logger: makeTestLogger({
      warn: (msg: string) => warnings.push(msg),
    }),
  });
  pubsubs.push(ps);

  assert.equal(warnings.length, 1, 'should warn exactly once');
  assert.ok(
    warnings[0]?.includes('maxDeliveryAttempts=0'),
    `warn message should mention maxDeliveryAttempts=0, got: ${warnings[0]}`,
  );
});

test('invalid numeric options throw before reaching timers or SQL', () => {
  const invalidCases: Array<[string, Partial<ConstructorParameters<typeof PostgresPubSub>[0]>]> = [
    ['pollIntervalMs', { pollIntervalMs: 0 }],
    ['ackDeadlineMs', { ackDeadlineMs: Number.NaN }],
    ['nackDelayMs', { nackDelayMs: -1 }],
    ['batchSize', { batchSize: 1.5 }],
    ['maxEventsPerTopic', { maxEventsPerTopic: Number.POSITIVE_INFINITY }],
    ['cleanupIntervalMs', { cleanupIntervalMs: -1 }],
    ['staleSubscriptionMs', { staleSubscriptionMs: 0 }],
    ['maxDeliveryAttempts', { maxDeliveryAttempts: -1 }],
  ];

  for (const [option, overrides] of invalidCases) {
    assert.throws(
      () =>
        new PostgresPubSub({
          connectionString: DATABASE_URL,
          schema,
          cleanupIntervalMs: 0,
          ...overrides,
        }),
      new RegExp(option),
      `${option} should be validated`,
    );
  }
});

test('valid schema names: lowercase, underscore, digits after first char', () => {
  const validNames = ['abc', 'a_b_c', 'test123', '_private', 'my_schema_1'];
  for (const name of validNames) {
    assert.doesNotThrow(
      () => {
        const ps = new PostgresPubSub({
          connectionString: DATABASE_URL,
          schema: name,
          cleanupIntervalMs: 0,
        });
        pubsubs.push(ps);
      },
      `expected no throw for schema name: ${JSON.stringify(name)}`,
    );
  }
});

test('invalid numeric options throw from the constructor before reaching timers or SQL', () => {
  const invalidCases: Array<
    [string, Partial<ConstructorParameters<typeof PostgresPubSub>[0]>, RegExp]
  > = [
    ['pollIntervalMs zero', { pollIntervalMs: 0 }, /pollIntervalMs/],
    ['pollIntervalMs NaN', { pollIntervalMs: Number.NaN }, /pollIntervalMs/],
    ['ackDeadlineMs negative', { ackDeadlineMs: -1 }, /ackDeadlineMs/],
    ['nackDelayMs negative', { nackDelayMs: -1 }, /nackDelayMs/],
    ['batchSize fractional', { batchSize: 1.5 }, /batchSize/],
    ['maxEventsPerTopic negative', { maxEventsPerTopic: -1 }, /maxEventsPerTopic/],
    [
      'cleanupIntervalMs infinite',
      { cleanupIntervalMs: Number.POSITIVE_INFINITY },
      /cleanupIntervalMs/,
    ],
    ['staleSubscriptionMs zero', { staleSubscriptionMs: 0 }, /staleSubscriptionMs/],
    ['maxDeliveryAttempts NaN', { maxDeliveryAttempts: Number.NaN }, /maxDeliveryAttempts/],
    ['maxDeliveryAttempts negative', { maxDeliveryAttempts: -1 }, /maxDeliveryAttempts/],
  ];

  for (const [name, overrides, pattern] of invalidCases) {
    assert.throws(
      () =>
        new PostgresPubSub({
          connectionString: DATABASE_URL,
          schema: uniqueSchema(),
          ...overrides,
        }),
      pattern,
      name,
    );
  }
});

test('numeric options accept documented zero and Infinity sentinels', () => {
  const ps = new PostgresPubSub({
    connectionString: DATABASE_URL,
    schema: uniqueSchema(),
    nackDelayMs: 0,
    maxDeliveryAttempts: Number.POSITIVE_INFINITY,
    maxEventsPerTopic: 0,
    cleanupIntervalMs: 0,
    pollIntervalMs: 1,
    ackDeadlineMs: 1,
    batchSize: 1,
    staleSubscriptionMs: 1,
  });
  pubsubs.push(ps);
  assert.ok(ps);
});

test('settlement option accepts documented policies and rejects unknown values', () => {
  for (const settlement of ['mastra-compatible', 'explicit', 'callback-success'] as const) {
    assert.doesNotThrow(() => {
      const ps = new PostgresPubSub({
        connectionString: DATABASE_URL,
        schema: uniqueSchema(),
        cleanupIntervalMs: 0,
        settlement,
      });
      pubsubs.push(ps);
    }, `expected ${settlement} to be accepted`);
  }

  assert.throws(
    () =>
      new PostgresPubSub({
        connectionString: DATABASE_URL,
        schema: uniqueSchema(),
        cleanupIntervalMs: 0,
        settlement: 'auto-magical' as never,
      }),
    /settlement/,
  );
});
