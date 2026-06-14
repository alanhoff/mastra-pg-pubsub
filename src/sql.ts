/**
 * Identifier-safety helpers. All values flow through parameterized queries;
 * only identifiers (schema, channel) are interpolated, and only after they
 * have been validated against a strict allowlist and double-quoted.
 */

const SCHEMA_PATTERN = /^[a-z_][a-z0-9_]*$/;

/**
 * Validate a PostgreSQL schema name against `^[a-z_][a-z0-9_]*$` and reject
 * PostgreSQL's reserved `pg_` prefix before a migration reaches the server,
 * except for this package's default `pg_pubsub` schema.
 *
 * @param schema - Candidate schema name.
 * @returns The same schema name when valid.
 * @throws {Error} When the name does not match the allowlist pattern.
 */
export function assertValidSchema(schema: string): string {
  if (!SCHEMA_PATTERN.test(schema)) {
    throw new Error(
      `Invalid schema name ${JSON.stringify(schema)}: must match ${SCHEMA_PATTERN.source}`,
    );
  }
  if (schema.startsWith('pg_') && schema !== 'pg_pubsub') {
    throw new Error(
      `Invalid schema name ${JSON.stringify(schema)}: PostgreSQL reserves the pg_ prefix for system schemas`,
    );
  }
  return schema;
}

/**
 * Quote a validated identifier for safe interpolation into SQL text.
 *
 * @param identifier - An identifier already proven safe by validation.
 * @returns The double-quoted identifier.
 */
export function quoteIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

/**
 * Derive the deterministic `LISTEN/NOTIFY` channel name for a schema. The
 * schema is pre-validated, so the result is always a legal identifier.
 *
 * @param schema - The validated schema name.
 * @returns The notify channel name.
 */
export function notifyChannel(schema: string): string {
  return `${schema}_events`;
}

/**
 * A stable 64-bit advisory lock key derived from a string, used to serialize
 * lazy migrations across instances. FNV-1a folded into the signed BIGINT range
 * accepted by `pg_advisory_lock`.
 *
 * @param input - Seed string (the schema name).
 * @returns A bigint in the signed 64-bit range.
 */
export function advisoryLockKey(input: string): bigint {
  let hash = 1469598103934665603n;
  const prime = 1099511628211n;
  const mask = (1n << 64n) - 1n;
  for (let i = 0; i < input.length; i++) {
    hash ^= BigInt(input.charCodeAt(i));
    hash = (hash * prime) & mask;
  }
  return hash >= 1n << 63n ? hash - (1n << 64n) : hash;
}
