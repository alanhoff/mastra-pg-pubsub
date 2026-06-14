# Changelog

All notable changes to this project will be documented here.

## 0.2.1 - 2026-06-14

### Changed

- Re-published the Mastra lifecycle release with explicit upgrade notes for the
  `mastra_pg_pubsub` default schema and the `schema: 'mastra_pubsub'` opt-in for
  existing deployments that need to keep using old tables.
- Documented that `wireMastraLifecycle()` intentionally wraps the current Mastra
  `startWorkers()` and `shutdown()` lifecycle methods, so projects should
  re-run the lifecycle and cluster tests when upgrading `@mastra/core`.

## 0.2.0 - 2026-06-14

### Added

- Added `wireMastraLifecycle(mastra)` to migrate before Mastra workers start,
  close gracefully after Mastra shutdown, and preserve unsettled delivery
  evidence when a shutdown drain times out.
- Added package-neutral logging and tracing hooks across lifecycle, delivery,
  listener, maintenance, replay, flush, and shutdown paths.
- Added clustered Mastra process tests that prove fan-out, consumer-group
  delivery, and history work across multiple app instances.

### Changed

- Changed the default schema to `mastra_pg_pubsub`, which is auto-created during
  migration. Existing installs can opt into `schema: 'mastra_pubsub'` to keep
  using their old tables.
- Rejected schemas that start with PostgreSQL's reserved `pg_` prefix, including
  the literal `pg_pubsub` name.

## 0.1.0 - 2026-06-12

### Added

- Initial `PostgresPubSub` Mastra adapter backed by PostgreSQL.
- At-least-once delivery with ack/nack, visibility timeouts, bounded redelivery, and optional dead-letter storage.
- Consumer-group delivery, groupless fan-out, replay from offset, and ordered history.
- Low-latency `LISTEN/NOTIFY` wakeups with polling as the correctness backstop.
- Strict TypeScript, ESM build, Biome linting, `node:test` integration tests, coverage thresholds, and Docker Compose Postgres.
- Real e2e tests for Mastra durable agent streaming, OpenAI-backed completion, PostgresStore memory persistence, and no-OpenAI delivery semantics.
