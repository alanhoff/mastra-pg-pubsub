import type { Pool, PoolClient } from 'pg';
import { advisoryLockKey, assertValidSchema, quoteIdentifier } from './sql.ts';

const DEFAULT_RESERVED_SCHEMA = 'pg_pubsub';

/**
 * Build the ordered list of DDL statements that create all tables. Idempotent:
 * every statement uses `IF NOT EXISTS`.
 *
 * @param schema - The validated, unquoted schema name.
 * @param deadLetter - Whether to include the optional `dead_events` table.
 * @returns Ordered DDL statements to execute within one transaction.
 */
export function buildDdl(schema: string, deadLetter: boolean): string[] {
  const s = quoteIdentifier(schema);
  const statements: string[] = [
    `CREATE TABLE IF NOT EXISTS ${s}.topics (
       topic TEXT PRIMARY KEY,
       next_index BIGINT NOT NULL DEFAULT 0
     )`,
    `CREATE TABLE IF NOT EXISTS ${s}.events (
       seq BIGSERIAL PRIMARY KEY,
       id UUID NOT NULL UNIQUE,
       topic TEXT NOT NULL,
       index BIGINT NOT NULL,
       type TEXT NOT NULL,
       run_id TEXT NOT NULL,
       data JSONB,
       created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
       UNIQUE (topic, index)
     )`,
    `CREATE INDEX IF NOT EXISTS events_topic_index_idx
       ON ${s}.events (topic, index)`,
    `CREATE TABLE IF NOT EXISTS ${s}.subscriptions (
       id TEXT PRIMARY KEY,
       topic TEXT NOT NULL,
       is_group BOOLEAN NOT NULL,
       last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
     )`,
    `CREATE INDEX IF NOT EXISTS subscriptions_topic_idx
       ON ${s}.subscriptions (topic)`,
    `CREATE TABLE IF NOT EXISTS ${s}.deliveries (
       event_seq BIGINT NOT NULL REFERENCES ${s}.events (seq) ON DELETE CASCADE,
       subscription_id TEXT NOT NULL REFERENCES ${s}.subscriptions (id) ON DELETE CASCADE,
       delivery_attempt INT NOT NULL DEFAULT 0,
       visible_at TIMESTAMPTZ NOT NULL DEFAULT now(),
       PRIMARY KEY (subscription_id, event_seq)
     )`,
    `CREATE INDEX IF NOT EXISTS deliveries_claim_idx
       ON ${s}.deliveries (subscription_id, visible_at, event_seq)`,
    `CREATE INDEX IF NOT EXISTS deliveries_event_idx
       ON ${s}.deliveries (event_seq)`,
  ];

  if (deadLetter) {
    statements.push(
      `CREATE TABLE IF NOT EXISTS ${s}.dead_events (
         seq BIGSERIAL PRIMARY KEY,
         event_id UUID NOT NULL,
         subscription_id TEXT NOT NULL,
         topic TEXT NOT NULL,
         index BIGINT NOT NULL,
         type TEXT NOT NULL,
         run_id TEXT NOT NULL,
         data JSONB,
         created_at TIMESTAMPTZ NOT NULL,
         delivery_attempt INT NOT NULL,
         dead_at TIMESTAMPTZ NOT NULL DEFAULT now()
       )`,
    );
  }

  return statements;
}

async function schemaExists(client: PoolClient, schema: string): Promise<boolean> {
  const result = await client.query<{ exists: boolean }>(
    'SELECT to_regnamespace($1) IS NOT NULL AS exists',
    [schema],
  );
  return result.rows[0]?.exists === true;
}

async function createSchema(client: PoolClient, schema: string): Promise<void> {
  if (schema === DEFAULT_RESERVED_SCHEMA) {
    try {
      await client.query('SET LOCAL allow_system_table_mods = on');
    } catch (cause) {
      throw new Error(
        'Unable to create default schema "pg_pubsub": PostgreSQL reserves the pg_ prefix, so auto-creation requires a migration role allowed to set allow_system_table_mods. Pre-create the schema with an administrator role or configure an ordinary custom schema name.',
        { cause },
      );
    }
  }

  await client.query(`CREATE SCHEMA ${quoteIdentifier(schema)}`);
}

/**
 * Create the schema and tables under a transaction-scoped advisory lock so
 * concurrent instances serialize their migrations. Idempotent and safe to call
 * repeatedly.
 *
 * @param pool - The connection pool to run the migration on.
 * @param schema - The validated schema name.
 * @param deadLetter - Whether to include the optional `dead_events` table.
 */
export async function runMigration(pool: Pool, schema: string, deadLetter: boolean): Promise<void> {
  assertValidSchema(schema);
  const key = advisoryLockKey(schema);
  const client: PoolClient = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock($1)', [key]);
    if (!(await schemaExists(client, schema))) {
      await createSchema(client, schema);
    }
    for (const statement of buildDdl(schema, deadLetter)) {
      await client.query(statement);
    }
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}
