import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import pg from 'pg';
import { PostgresPubSub } from '../src/index.ts';
import {
  DATABASE_URL,
  dropSchema,
  isDisposableTestDatabase,
  makePubSub,
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
  const invalidNames = ['Bad-Name', '1abc', 'has space', 'with-dash', 'pg_pubsub', ''];
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

test('schema defaults to mastra_pg_pubsub and auto-creates the schema when not provided', async (t) => {
  if (!isDisposableTestDatabase()) {
    t.skip('default-schema drop is only safe against the disposable local test database');
    return;
  }
  await dropSchema('mastra_pg_pubsub');
  const ps = new PostgresPubSub({
    connectionString: DATABASE_URL,
    cleanupIntervalMs: 0,
    pollIntervalMs: 100,
  });
  try {
    await ps.migrate();
    assert.equal(await schemaExists('mastra_pg_pubsub'), true);
    assert.equal(await tableExists('mastra_pg_pubsub', 'events'), true);
    assert.equal(await tableExists('mastra_pg_pubsub', 'subscriptions'), true);
  } finally {
    await ps.close();
    await dropSchema('mastra_pg_pubsub');
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
    logger: {
      warn: (msg: string) => warnings.push(msg),
    },
  });
  pubsubs.push(ps);

  assert.equal(warnings.length, 1, 'should warn exactly once');
  assert.ok(
    warnings[0]?.includes('maxDeliveryAttempts=0'),
    `warn message should mention maxDeliveryAttempts=0, got: ${warnings[0]}`,
  );
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
