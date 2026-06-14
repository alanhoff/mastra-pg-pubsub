import { randomBytes } from 'node:crypto';
import type { IMastraLogger } from '@mastra/core/logger';
import pg from 'pg';
import type { PostgresPubSubConfig } from '../src/index.ts';
import { PostgresPubSub } from '../src/index.ts';

/** Connection string for the docker-compose Postgres, overridable via env. */
export const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5544/mastra_pubsub';

/** A unique, valid schema name so test files are parallel-safe. */
export function uniqueSchema(): string {
  return `test_${randomBytes(6).toString('hex')}`;
}

/** Drop a test schema (used in `after` hooks). */
export async function dropSchema(schema: string): Promise<void> {
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  try {
    await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  } finally {
    await pool.end();
  }
}

/** True only for the repo's disposable docker-compose test database. */
export function isDisposableTestDatabase(): boolean {
  try {
    const url = new URL(DATABASE_URL);
    return (
      (url.hostname === 'localhost' || url.hostname === '127.0.0.1') &&
      url.port === '5544' &&
      url.pathname === '/mastra_pubsub' &&
      url.username === 'postgres'
    );
  } catch {
    return false;
  }
}

/** Check whether a PostgreSQL schema exists. */
export async function schemaExists(schema: string): Promise<boolean> {
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  try {
    const result = await pool.query<{ exists: boolean }>(
      `SELECT EXISTS (
         SELECT 1 FROM information_schema.schemata WHERE schema_name = $1
       ) AS exists`,
      [schema],
    );
    return result.rows[0]?.exists ?? false;
  } finally {
    await pool.end();
  }
}

/** Check whether a table exists inside a PostgreSQL schema. */
export async function tableExists(schema: string, table: string): Promise<boolean> {
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  try {
    const result = await pool.query<{ exists: boolean }>(
      `SELECT EXISTS (
         SELECT 1
         FROM information_schema.tables
         WHERE table_schema = $1 AND table_name = $2
       ) AS exists`,
      [schema, table],
    );
    return result.rows[0]?.exists ?? false;
  } finally {
    await pool.end();
  }
}

/** Construct a PostgresPubSub against the test DB with sensible fast defaults. */
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

/** Construct a complete Mastra-shaped logger for focused assertions. */
export function makeTestLogger(overrides: Partial<IMastraLogger> = {}): IMastraLogger {
  return {
    debug: () => undefined,
    info: () => undefined,
    warn: () => undefined,
    error: () => undefined,
    trackException: () => undefined,
    getTransports: () => new Map(),
    listLogs: async () => ({ logs: [], total: 0, page: 1, perPage: 100, hasMore: false }),
    listLogsByRunId: async () => ({ logs: [], total: 0, page: 1, perPage: 100, hasMore: false }),
    ...overrides,
  };
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
  { timeoutMs = 5000, intervalMs = 25 }: { timeoutMs?: number; intervalMs?: number } = {},
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
