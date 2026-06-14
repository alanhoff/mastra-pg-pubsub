# Mastra PubSub Lifecycle Issue Research

Date: 2026-06-14

Status: research only. No upstream issue has been created.

## Executive Summary

Mastra currently accepts a custom `PubSub` through `new Mastra({ pubsub })`, but the public `PubSub` contract does not include lifecycle ownership methods such as `init()`, `start()`, `close()`, or `destroy()`. In installed `@mastra/core@1.42.0`, `Mastra.shutdown()` reaches `stopWorkers()`, and `stopWorkers()` calls `pubsub.flush()`, but Mastra does not close or destroy the custom PubSub instance.

That creates a gap for production PubSub adapters with external resources. A PostgreSQL-backed adapter may need to run migrations before workers subscribe, start maintenance/listener loops, and close owned pools or listener connections on shutdown. Without a Mastra-owned lifecycle hook, an adapter can lazily initialize on first `publish()` or `subscribe()`, but it cannot know when the owning Mastra instance is shutting down. The local workaround is `pubsub.wireMastraLifecycle(mastra)`, which wraps `startWorkers()` and `shutdown()`.

The best upstream request is a feature issue asking Mastra to add a backwards-compatible optional PubSub lifecycle contract, then call it from Mastra startup and shutdown.

## Repository Guidelines For Opening This Issue

Mastra's repository is `mastra-ai/mastra`: https://github.com/mastra-ai/mastra

Mastra has issues enabled and blank issues disabled. Issue creation should go through one of the configured templates.

Relevant repository files:

- `CONTRIBUTING.md`: https://github.com/mastra-ai/mastra/blob/main/CONTRIBUTING.md
- Feature request template: https://github.com/mastra-ai/mastra/blob/main/.github/ISSUE_TEMPLATE/feature_request.yml
- Issue template config: https://github.com/mastra-ai/mastra/blob/main/.github/ISSUE_TEMPLATE/config.yml
- Code of conduct: https://github.com/mastra-ai/mastra/blob/main/CODE_OF_CONDUCT.md

Mastra's contribution guide says feature changes should start as a feature request and wait for maintainer feedback before a PR. It also says PRs must link to the issue they address, so opening the issue first is the right path for this kind of API request.

The correct template is `Feature Request`, not `Bug Report`, `Documentation Improvement`, or `Integration Request`, because the request is for an API lifecycle extension in Mastra core. The template automatically applies the `enhancement` label.

Required feature-request fields:

- `Problem Statement`
- `Proposed Solution`
- `Component`
- `Example Use Case`
- Verification checkboxes:
  - searched existing issues
  - provided sufficient context

Optional fields that should still be filled:

- `Alternatives Considered`
- `Additional Context`

Suggested component selections:

- `Workflows`, because PubSub is used for workflow event processing and worker routing.
- `Server`, because shutdown behavior is application lifecycle behavior.
- `Other`, with `Core PubSub lifecycle` named in the body, because the template has no `Core` or `PubSub` option.

## Duplicate Search Evidence

The following `gh` searches were run against `mastra-ai/mastra` and did not find an open duplicate for "Mastra should call optional PubSub init/close hooks":

```bash
gh search issues 'pubsub lifecycle' -R mastra-ai/mastra --state open --include-prs
gh search issues 'pubsub shutdown close' -R mastra-ai/mastra --state open --include-prs
gh search issues '"PubSub" "close()"' -R mastra-ai/mastra --state open --include-prs
gh search issues '"PubSub" init close lifecycle' -R mastra-ai/mastra --state open --include-prs
gh search issues '"PubSub" destroy shutdown' -R mastra-ai/mastra --state open --include-prs
gh search issues '"pubsub" "flush" "shutdown"' -R mastra-ai/mastra --state open --include-prs
```

Closest adjacent items found:

- https://github.com/mastra-ai/mastra/pull/16309 - added standalone workers, `supportedModes`, push-capable PubSub, and Redis Streams. Adjacent, but not a generic PubSub lifecycle hook.
- https://github.com/mastra-ai/mastra/pull/17491 - added PubSub reference docs. It documents the current base contract and implementation-specific methods.
- https://github.com/mastra-ai/mastra/pull/11052 - older emitter-to-PubSub refactor. Adjacent historically, but not the requested lifecycle ownership behavior.
- https://github.com/mastra-ai/mastra/issues/16761 - an open feature request for agent run lifecycle storage. It is a useful style example, but unrelated to PubSub lifecycle ownership.

## Current Mastra Behavior

Official docs:

- The PubSub reference says custom backends implement `publish`, `subscribe`, `unsubscribe`, and `flush`. It also documents delivery modes and replay helpers, but no owner lifecycle hook: https://mastra.ai/reference/pubsub/base
- The configuration reference says `pubsub` is a `PubSub` option on `Mastra`, used internally for workflow event processing and component communication: https://mastra.ai/reference/configuration#pubsub
- The Redis Streams PubSub docs say its implementation has `close()` and that callers should call it during graceful shutdown: https://mastra.ai/reference/pubsub/redis-streams

Installed package evidence in this repository:

- `@mastra/core` version: `1.42.0`
- `node_modules/@mastra/core/dist/events/pubsub.d.ts` declares the base `PubSub` abstract methods. It includes `flush()` but not `init()`, `start()`, `close()`, or `destroy()`.
- `node_modules/@mastra/core/dist/mastra/index.d.ts` declares `pubsub?: PubSub`, `get pubsub(): PubSub`, `startWorkers()`, and `shutdown()`.
- `node_modules/@mastra/core/dist/chunk-TRXIXO5J.js` stores `config.pubsub` in a private `#pubsub` field, calls `#pubsub.flush()` in `stopWorkers()`, and does not call `#pubsub.close()` in `shutdown()`.

Local adapter evidence:

- `src/postgres-pubsub.ts` has `migrate()`, `start()`, `init()`, `flush()`, `close()`, and `wireMastraLifecycle(mastra)`.
- `#ensureReady()` lazily calls `start()` on first `publish()` or `subscribe()`, so direct PubSub use can auto-migrate.
- Lazy start does not solve shutdown. The adapter cannot close its owned PostgreSQL pool, listener connection, maintenance timer, or private subscription rows unless it receives an ownership signal from Mastra or the user calls `close()`.

## Problem Statement

Mastra has a clean way to provide a custom PubSub instance, but not a clean way for that PubSub to participate in Mastra's lifecycle.

This matters for custom transports that own resources:

- database pools
- Redis/GCP clients
- listener sockets
- polling or reclaim timers
- durable subscription rows
- maintenance loops
- schema migrations or broker setup

The current base contract lets Mastra safely drain in-flight work with `flush()`, but not initialize the backend before workers start or release backend resources after shutdown. The result is a manual lifecycle step in every custom adapter integration.

For this PostgreSQL adapter, the manual step is:

```ts
const pubsub = new PostgresPubSub({ connectionString: process.env.DATABASE_URL });
const mastra = new Mastra({ pubsub });
pubsub.wireMastraLifecycle(mastra);
```

That works, but it is not ideal for library ergonomics:

- Developers can forget the extra call.
- The custom adapter must monkey-patch a Mastra instance to achieve lifecycle ownership.
- The wrapper is version-sensitive because it wraps public methods rather than using an official hook.
- It is difficult for adapters to provide consistent observability around startup and shutdown when Mastra does not call them.

## Recommended Upstream Fix

Ask Mastra to add a backwards-compatible optional PubSub lifecycle contract and have `Mastra` call it.

Suggested API shape:

```ts
export interface PubSubLifecycle {
  init?(): Promise<void>;
  close?(): Promise<void>;
}
```

Alternative names to consider:

- `start()` instead of `init()` if Mastra wants symmetry with `startWorkers()`.
- `destroy()` as an alias if existing built-in adapters already use it.

Recommended behavior:

1. Before `startWorkers()` initializes workers or subscribes event listeners, Mastra should call `await pubsub.init?.()`.
2. During `shutdown()`, after `stopWorkers()` has flushed and unsubscribed event listeners, Mastra should call `await pubsub.close?.()`.
3. If `startWorkers()` fails after PubSub initialization, Mastra should attempt best-effort cleanup with `close?.()` when safe.
4. Hook calls should be idempotent from Mastra's perspective. Adapters should also document idempotency.
5. Errors should be logged through Mastra's logger and propagated in the same style as storage or observability shutdown failures.
6. The PubSub reference docs should document the optional lifecycle methods and the exact order relative to `startWorkers()`, `stopWorkers()`, `flush()`, and `shutdown()`.
7. Existing third-party PubSub implementations must keep working unchanged.

Implementation options for maintainers:

- Minimal duck-typing: leave `PubSub` abstract methods unchanged, and have Mastra call optional methods if present.
- Stronger base-class API: add concrete no-op `init()` and `close()` methods to `PubSub`, then override them in adapters. This is simpler for users to discover, but it expands the public base class.
- Transitional support: call `init()` and `close()` as the standard names, with temporary support for `start()` or `destroy()` only if existing built-ins need compatibility.

## Acceptance Criteria To Request

- A custom PubSub can migrate/connect before any worker subscribes or starts processing workflow events.
- `Mastra.shutdown()` drains PubSub work and then closes/destroys custom PubSub resources.
- Existing PubSub implementations that only implement the current contract continue to work.
- The lifecycle ordering is documented in the PubSub reference.
- Tests cover:
  - `pubsub.init()` called before worker initialization/subscription.
  - `pubsub.close()` called during `shutdown()` after `flush()`.
  - no failure when hooks are absent.
  - error behavior when `init()` or `close()` rejects.
  - idempotent `shutdown()` behavior.

## Runbook: Opening The Issue Later

Do not create the issue until the project owner decides to proceed.

When ready:

1. Re-run the duplicate search:

   ```bash
   gh search issues 'pubsub lifecycle' -R mastra-ai/mastra --state open --include-prs
   gh search issues 'pubsub shutdown close' -R mastra-ai/mastra --state open --include-prs
   gh search issues '"PubSub" init close lifecycle' -R mastra-ai/mastra --state open --include-prs
   gh search issues '"PubSub" "close()"' -R mastra-ai/mastra --state open --include-prs
   ```

2. Open the feature request form:

   ```bash
   gh issue create \
     -R mastra-ai/mastra \
     --title "[FEATURE] Add optional PubSub lifecycle hooks for init and shutdown cleanup" \
     --label enhancement \
     --body-file docs/research/mastra-pubsub-lifecycle-feature-issue-draft.md
   ```

   Note: the web issue form is safer if maintainers require structured form fields exactly as configured in `feature_request.yml`.

3. Use `docs/research/mastra-pubsub-lifecycle-feature-issue-draft.md` as the body.

4. After maintainers respond positively, prepare a PR against Mastra. The PR must link back to the issue, per `CONTRIBUTING.md`.

5. If maintainers prefer a different API shape, update this adapter to match the accepted upstream lifecycle contract and remove or de-emphasize `wireMastraLifecycle()`.

## Sources

- Mastra repository: https://github.com/mastra-ai/mastra
- Mastra contributing guide: https://github.com/mastra-ai/mastra/blob/main/CONTRIBUTING.md
- Mastra feature request template: https://github.com/mastra-ai/mastra/blob/main/.github/ISSUE_TEMPLATE/feature_request.yml
- Mastra issue template config: https://github.com/mastra-ai/mastra/blob/main/.github/ISSUE_TEMPLATE/config.yml
- Mastra code of conduct: https://github.com/mastra-ai/mastra/blob/main/CODE_OF_CONDUCT.md
- Mastra PubSub reference: https://mastra.ai/reference/pubsub/base
- Mastra configuration reference, `pubsub`: https://mastra.ai/reference/configuration#pubsub
- Mastra Redis Streams PubSub reference: https://mastra.ai/reference/pubsub/redis-streams
- Adjacent PR, standalone workers and push-capable PubSub: https://github.com/mastra-ai/mastra/pull/16309
- Adjacent PR, PubSub reference docs: https://github.com/mastra-ai/mastra/pull/17491
- Adjacent PR, emitter-to-PubSub refactor: https://github.com/mastra-ai/mastra/pull/11052
