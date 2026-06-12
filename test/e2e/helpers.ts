import { randomBytes } from 'node:crypto';
import pg from 'pg';
import type { PostgresPubSubConfig } from '../../src/index.ts';
import { PostgresPubSub } from '../../src/index.ts';
import { assertValidSchema, quoteIdentifier } from '../../src/sql.ts';

/**
 * Connection string for the e2e Postgres. Loaded from `.env` via
 * `node --env-file-if-exists=.env` (the `test:e2e` npm script). Falls back to the
 * docker-compose database used by the unit/integration suite.
 */
export const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5544/mastra_pubsub';

/** A unique, valid schema name so e2e files are parallel-safe. */
export function uniqueSchema(prefix = 'e2e'): string {
  return `${prefix}_${randomBytes(6).toString('hex')}`;
}

/** Drop a test schema (used in `after` hooks). */
export async function dropSchema(schema: string): Promise<void> {
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  try {
    await pool.query(`DROP SCHEMA IF EXISTS ${quoteIdentifier(assertValidSchema(schema))} CASCADE`);
  } finally {
    await pool.end();
  }
}

/** Construct a PostgresPubSub against the e2e DB with fast polling defaults. */
export function makePubSub(
  schema: string,
  overrides: Partial<PostgresPubSubConfig> = {},
): PostgresPubSub {
  return new PostgresPubSub({
    connectionString: DATABASE_URL,
    schema,
    pollIntervalMs: 100,
    cleanupIntervalMs: 0,
    ...overrides,
  });
}

/** Resolve after `ms` milliseconds. */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Poll `predicate` until it returns true or the timeout elapses.
 * Throws on timeout so tests fail loudly rather than hang.
 */
export async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  { timeoutMs = 10_000, intervalMs = 50 }: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return;
    }
    await sleep(intervalMs);
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}
