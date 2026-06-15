# Changelog

All notable changes to this project will be documented here.

## 0.4.0 - 2026-06-15

### Added

- Added the `settlement` option with `mastra-compatible`, `explicit`, and `callback-success` policies.

### Changed

- Changed the default callback settlement policy to Mastra-compatible behavior: successful private/fan-out callbacks auto-ack after the returned callback promise resolves, while consumer groups remain explicit by default.
- Changed unsettled callback failures under the default and `callback-success` policies to auto-nack instead of waiting for the visibility timeout. Use `settlement: 'explicit'` to keep strict ack/nack-only settlement everywhere.
- Documented non-zero `nackDelayMs` guidance for callbacks that can repeatedly fail on external dependencies.

## 0.3.0 - 2026-06-14

### Changed

- Rewrote lifecycle handling as a breaking change: the adapter now starts lazily on database use and stops idle listener/maintenance resources after the final local subscriber is removed.
- Changed the default schema to `pg_pubsub`; migration auto-creates it and still allows ordinary custom schema names.
- Changed `logger` to accept Mastra's logger shape. When omitted, PubSub resolves the current Mastra span and uses that span's observability logger; `logger: false` silences PubSub logs.
- Reworked observability around Mastra current-span context, generic child spans, event spans, and sanitized scalar attributes.

### Removed

- Removed obsolete lifecycle and explicit observability sink APIs from the public package surface.

## 0.2.1 - 2026-06-14

### Changed

- Improved lifecycle release notes and schema guidance.

## 0.2.0 - 2026-06-14

### Added

- Added an earlier lifecycle and observability integration pass across startup, delivery, listener, maintenance, replay, flush, and shutdown paths.
- Added clustered Mastra process tests that prove fan-out, consumer-group delivery, and history work across multiple app instances.

### Changed

- Moved the package away from the original schema default and documented explicit schema selection for existing deployments.

## 0.1.0 - 2026-06-12

### Added

- Initial `PostgresPubSub` Mastra adapter backed by PostgreSQL.
- At-least-once delivery with ack/nack, visibility timeouts, bounded redelivery, and optional dead-letter storage.
- Consumer-group delivery, groupless fan-out, replay from offset, and ordered history.
- Low-latency `LISTEN/NOTIFY` wakeups with polling as the correctness backstop.
- Strict TypeScript, ESM build, Biome linting, `node:test` integration tests, coverage thresholds, and Docker Compose Postgres.
- Real e2e tests for Mastra durable agent streaming, OpenAI-backed completion, PostgresStore memory persistence, and no-OpenAI delivery semantics.
