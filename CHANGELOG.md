# Changelog

All notable changes to this project will be documented here.

## 0.1.0 - 2026-06-12

### Added

- Initial `PostgresPubSub` Mastra adapter backed by PostgreSQL.
- At-least-once delivery with ack/nack, visibility timeouts, bounded redelivery, and optional dead-letter storage.
- Consumer-group delivery, groupless fan-out, replay from offset, and ordered history.
- Low-latency `LISTEN/NOTIFY` wakeups with polling as the correctness backstop.
- Strict TypeScript, ESM build, Biome linting, `node:test` integration tests, coverage thresholds, and Docker Compose Postgres.
- Real e2e tests for Mastra durable agent streaming, OpenAI-backed completion, PostgresStore memory persistence, and no-OpenAI delivery semantics.
