# Aggressive Adversarial Review Report

## Review scope
- Target (relative or redacted): `<review_root>` whole repository, excluding generated/vendor/runtime directories except where explicitly listed as evidence.
- Source of truth: Git-tracked worktree on branch `main`, HEAD `41efd4c` (`Rewrite pubsub lifecycle and observability`), file-set hash `5f4dbcc094d30de5f8ad2d3fe0ab9955de47cdbd323d04f367bb910d40335728`.
- Review mode: `$adversarial-review`, agentic, aggressive, peer-embargoed independent lanes, synthesis by lead reviewer.
- Risk tolerance: High for false-positive discovery, conservative for accepted severity. No production mutations were made.
- Tools used (redacted/summarized): `git`, `rg`, `sed`, `find`, `ls`, `node`, `npm`, `npx`, skill helper scripts `detect-code-project` and `analyze-code-quality`, native subagents, targeted no-DB repro.
- Forbidden actions respected: no destructive git commands, no source edits, no dependency changes, no publish, no credentials printed, no writes outside the requested report path.
- File writes requested: yes.
- File write authorization: explicit_user_request.
- Authorized write scope: `docs/reviews/*`.
- Code project detected: yes.
- Review status: complete.

## Independence level achieved
- Level: 3 - parallel independent subagent review with lead synthesis.
- Reason: Six bounded reviewers ran under peer embargo with distinct prompts, prompt hashes, context hash, and evidence packet hash. Their outputs were collected before synthesis and deduplicated into accepted/dismissed findings.
- Contamination or limitations: The synthesis reviewer could inspect all reviewer outputs after completion. External source checks were not used in this pass; compatibility claims against older Mastra versions remain evidence-needed.

## Reviewer final status
| Reviewer | Required? | Final status | Attempts | Retries used | Max retries | Exhausted? | Final validation | Notes |
|---|---:|---|---:|---:|---:|---:|---|---|
| A-defects / Ampere | yes | completed | 1 | 0 | 5 | no | valid | Runtime defects, retry poisoning, subscribe atomicity, flush semantics. |
| B-requirements / Bacon | yes | completed | 1 | 0 | 5 | no | valid | Requirement drift, CI coverage, peer range. |
| C-edge-cases / Sagan | yes | completed | 1 | 0 | 5 | no | valid | Concurrency and invalid config edge cases. |
| D-security-tool-abuse / Poincare | yes | completed | 1 | 0 | 5 | no | valid | Privilege, env, helper SQL, telemetry surfaces. |
| F-traces-observability / Hubble | yes | completed | 1 | 0 | 5 | no | valid | Lifecycle, logging, and observability coverage. |
| CQ-code-quality / Nash | yes | completed | 1 | 0 | 5 | no | valid | Static analyzer triage and gates H-R. |

## Reviewer attempts
| Reviewer | Attempt ID | Required? | Status | Attempt count | Retry count | Prompt hash | Context hash | Evidence packet hash | Read-only | Retry causes | Terminal state | Report validation | Peer embargo | Contamination notes | Timeout | Failure reason |
|---|---|---:|---|---:|---:|---|---|---|---:|---|---|---|---|---|---|---|
| A-defects / Ampere | `019ec7f9-788b-7e01-a26c-6a1071d340b2` | yes | completed | 1 | 0 | `sha256:c83d14962d1456975ea04967c3b7dec2425db0af9689106226b4c05c496bb6d6` | `sha256:5674c26bfa6963505eb187ede5c6455c208f2714cdb6f720fa928e3f2d25f991` | `sha256:070a666a44c967f1ffe9e59194ca87516d30f4bb8a744653b0745a8712f00e29` | yes | none | completed | valid | honored | none observed | no | none |
| B-requirements / Bacon | `019ec7f9-7bb6-77e3-8d90-06d7e74a7f13` | yes | completed | 1 | 0 | `sha256:058692d5df65cee8a4b9ee628e788f403b37ac39d2be63db822f179723555d0a` | `sha256:5674c26bfa6963505eb187ede5c6455c208f2714cdb6f720fa928e3f2d25f991` | `sha256:070a666a44c967f1ffe9e59194ca87516d30f4bb8a744653b0745a8712f00e29` | yes | none | completed | valid | honored | none observed | no | none |
| C-edge-cases / Sagan | `019ec7f9-7e63-77b1-a73b-f73c896b17c4` | yes | completed | 1 | 0 | `sha256:b0530b861d881b32dd261c2382b8f91fb163847cdd44afe79f6ce95c5c63ca23` | `sha256:5674c26bfa6963505eb187ede5c6455c208f2714cdb6f720fa928e3f2d25f991` | `sha256:070a666a44c967f1ffe9e59194ca87516d30f4bb8a744653b0745a8712f00e29` | yes | none | completed | valid | honored | none observed | no | none |
| D-security-tool-abuse / Poincare | `019ec7f9-80f8-7692-8c1d-0223e49ddada` | yes | completed | 1 | 0 | `sha256:5913d5d2a1c6c5ab8e3f95b139b0d899a7e04d177434c08216e89d7091de3238` | `sha256:5674c26bfa6963505eb187ede5c6455c208f2714cdb6f720fa928e3f2d25f991` | `sha256:070a666a44c967f1ffe9e59194ca87516d30f4bb8a744653b0745a8712f00e29` | yes | none | completed | valid | honored | none observed | no | none |
| F-traces-observability / Hubble | `019ec7f9-8378-7aa2-bf6b-1cda5f9f4d5c` | yes | completed | 1 | 0 | `sha256:4b4793547dec234b6cbe9c32b690aa52cb07bbc16cb2db0d4de50bf322c13d2f` | `sha256:5674c26bfa6963505eb187ede5c6455c208f2714cdb6f720fa928e3f2d25f991` | `sha256:070a666a44c967f1ffe9e59194ca87516d30f4bb8a744653b0745a8712f00e29` | yes | none | completed | valid | honored | none observed | no | none |
| CQ-code-quality / Nash | `019ec7f9-8769-7551-a1c0-89b9ef13fa00` | yes | completed | 1 | 0 | `sha256:43dae173b4540472557d8f3ee43f8e62bdd8d29c38511e1abb659c34f9ebb298` | `sha256:5674c26bfa6963505eb187ede5c6455c208f2714cdb6f720fa928e3f2d25f991` | `sha256:070a666a44c967f1ffe9e59194ca87516d30f4bb8a744653b0745a8712f00e29` | yes | none | completed | valid | honored | none observed | no | none |

## Code detection evidence
| Evidence type | Value | Result |
|---|---|---|
| Detector command | `node <skill>/scripts/detect-code-project <review_root>` | `hasCode: true`, `detectionStatus: code_detected` |
| Files/directories visited | `filesVisited 125`, `directoriesVisited 28`, `entriesVisited 156`, `traversalUnits 184` | Complete for configured detector budget |
| Code/manifests | `codeFiles 28`, `manifests 4` | Code project confirmed |
| Skipped directories | `.git`, `dist`, `node_modules`, `.omx/logs` | Expected vendor/generated/runtime skips |
| Errors/truncation | `readErrors []`, `truncated false` | No detector evidence loss |

## Static quality analyzer evidence
- Command: `node <skill>/scripts/analyze-code-quality <review_root> --json`
- Status: completed; analyzer generated at `2026-06-14T21:08:55.380Z`.
- Gate scope: whole repo, excluding configured generated/vendor/runtime directories.
- Files analyzed: 32.
- Logical lines: 9339.
- Functions detected: 86.
- Coverage truncated: false.
- Traversal units: 184.
- Analysis bytes read: 330777.
- Analysis budget exceeded: false.
- Analysis budget errors: none.
- Repo-wide analysis skipped: false.
- Directories visited: 28.
- Directory entries visited: 156.
- Skipped directories: `.git`, `dist`, `node_modules`, `.omx/logs`.
- Skipped directory details: generated/vendor/runtime directories skipped by analyzer policy; `dist` is separately covered by `npm pack --dry-run` evidence.
- Skipped project evidence: none beyond expected skips.
- Skipped probe errors: none.
- Skipped probe truncations: none.
- Read errors: none.
- Largest files: `package-lock.json` 3860 lines, `src/postgres-pubsub.ts` 1153 lines, `test/coverage.test.ts` 465 lines, `src/consume-loop.ts` 390 lines, `test/observability.test.ts` 319 lines.
- Most complex functions: `test/fixtures/cluster-worker.ts:isCommand` cyclomatic 14/cognitive 24; `src/postgres-pubsub.ts:close` cyclomatic 9/cognitive 16/81 lines; `test/fixtures/cluster-worker.ts:handleCommand` cyclomatic 9/cognitive 21.
- Threshold overrides: none recorded.
- Eval fixture outcome: not applicable; repo source analysis completed successfully.

## Plugin-eval acceptance
- Explicit user authorization: no.
- Available: not invoked.
- Invoked: no.
- Read-only: not applicable.
- Score: not applicable.
- Grade: not applicable.
- Threshold: not applicable.
- Pass: not applicable.
- Unavailable reason: plugin eval was not explicitly requested or authorized for this review turn.
- Findings summary or evidence hash: not applicable.
- Loop count: 0.

## Code Quality Reviewer
| Gate | Verdict | Evidence | Blocking? | Notes |
|---|---|---|---:|---|
| H | pass | Installed TypeScript types and local package metadata inspected. | no | External docs were not required for local static gate. |
| I | pass | No source mutations during review. | no | Report-only write authorized. |
| J | not_applicable | No frontend/UI code. | no | Not a UI project. |
| K | pass | Analyzer catch warnings triaged as intentional best-effort cleanup paths or test behavior. | no | Some logging improvements still accepted separately. |
| L | pass | No new dependencies or dependency changes. | no | Not an implementation pass. |
| M | pass | Validation commands passed in current environment. | no | See red-team measurements. |
| N | pass | No broad speculative refactors accepted as immediate findings. | no | Findings target concrete failure modes. |
| O | fail | `src/postgres-pubsub.ts:1124 close()` has 81 logical lines, cyclomatic 9, cognitive 16, nesting 4. | yes | Normal-priority patch recommended. |
| P | pass | No large duplicated logic accepted by analyzer. | no | Duplication not a blocking concern. |
| Q | pass | Analyzer unused-export candidates were internal/false positives after triage. | no | No API cleanup finding accepted. |
| R | pass | Promise chain in lifecycle lock is intentional serialization, not an orphaned promise. | no | Retry poisoning is accepted separately as a behavior bug. |

## Attack paths attempted
| Area | Required attempts | Actual attempts | Result |
|---|---:|---:|---|
| Startup/migration failure recovery | 1 | 2 | Accepted high finding; targeted no-DB repro confirms cached rejected promise. |
| Subscribe/listener failure atomicity | 1 | 3 | Accepted high finding; mutation-before-await and missing rollback found. |
| Same-group concurrency | 1 | 2 | Accepted high finding; check-then-await-then-set race found. |
| Cluster and multi-instance proof | 1 | 2 | Existing cluster test passes, but CI does not run it. |
| Flush semantics under shared groups | 1 | 1 | Accepted medium finding; local flush can count remote group member backlog. |
| Invalid numeric configuration | 1 | 3 | Accepted medium finding; invalid values flow to timers and SQL limits. |
| Observability error isolation | 1 | 2 | Accepted medium finding; callback context wrapper may propagate instrumentation failures. |
| Security and tool abuse | 1 | 4 | No high-confidence SQL injection in production paths; low/medium hardening findings accepted. |
| Release/package drift | 1 | 3 | Pack succeeds; peer range/source-map/CI drift findings accepted. |
| Stale removed API references | 1 | 1 | No matches for purged lifecycle or old trace-option compatibility terms. |

## Red-team measurements
| Objective | Probe | Expected defense | Observed result | Outcome | Severity if failed | Residual risk |
|---|---|---|---|---|---|---|
| Prove migration retry safety | Fake pool fails first `connect()`/query and call `migrate()` twice | Second call should retry with a new pool connection | `{"startupPoisonReproduced":true,"connectCount":1}` | failed | High | Needs regression test with transient migration/start failure. |
| Prove current test suite health | `npm run typecheck && npm run lint && npm test` | All pass | Typecheck pass; lint pass; unit tests 82/82 pass | passed | High | Does not cover accepted failure modes. |
| Prove cluster test still passes locally | `npm run test:cluster` | Cluster test pass | 1/1 pass | passed | High | CI currently omits this command. |
| Prove coverage suite still passes | `npm run test:coverage` | Coverage pass | 82/82 pass; all files 95.98% lines, 91.96% branches, 95.65% functions | passed | Medium | Coverage does not guarantee startup retry/atomicity. |
| Prove e2e suite against Postgres | `DATABASE_URL=<redacted> npm run test:e2e` | E2E pass | 7/7 pass | passed | High | CI skips all e2e if OpenAI key is missing. |
| Prove package is publishable shape | `npm pack --dry-run` | Pack succeeds with expected files | `mastra-pg-pubsub@0.3.0`, 36 files, 36.8 kB | passed | Medium | Source maps point to excluded source. |
| Prove removed compatibility API purge | `rg` for removed lifecycle helper terms, old trace-option aliases, and the old default schema name | No hits in README/docs/package/src/test | No matches; command exit 1 due no hits | passed | Medium | Search terms may not cover every possible prose synonym. |
| Prove diff hygiene | `git diff --check` | No whitespace errors | Passed in combined command before no-hit `rg` exit | passed | Low | Re-run after this report write if needed. |

## Claim and source ledger
| Claim | Source URI | Publisher | Source date | Accessed date | Authority tier | Supports exact claim? | Stale-source decision |
|---|---|---|---|---|---|---|---|
| `migrate()` caches rejected promises and does not clear them | `src/postgres-pubsub.ts:166-198`; targeted fake-pool repro | Local repo and local command output | 2026-06-14 | 2026-06-14 | Primary | yes | Current worktree evidence |
| `start()` caches rejected promises and does not clear them | `src/postgres-pubsub.ts:218-263` | Local repo | 2026-06-14 | 2026-06-14 | Primary | yes | Current worktree evidence |
| `subscribe()` mutates callback/index state before listener setup succeeds | `src/postgres-pubsub.ts:463-562`; `src/listener.ts:48-53` | Local repo | 2026-06-14 | 2026-06-14 | Primary | yes | Current worktree evidence |
| Same-group subscribe has a check/await/set race | `src/postgres-pubsub.ts:485-489` | Local repo | 2026-06-14 | 2026-06-14 | Primary | yes | Current worktree evidence |
| Flush counts all delivery rows for local subscription IDs | `src/postgres-pubsub.ts:752-823`; `README.md:135`; `docs/design.md:75` | Local repo | 2026-06-14 | 2026-06-14 | Primary | yes | Current worktree evidence |
| Invalid numeric config can flow to timers and SQL limits | `src/postgres-pubsub.ts:77`; `src/consume-loop.ts:181,221,225`; `src/postgres-pubsub.ts:1025` | Local repo | 2026-06-14 | 2026-06-14 | Primary | yes | Current worktree evidence |
| CI omits cluster test and skips all e2e without OpenAI key | `.github/workflows/ci.yml:43-83` | Local repo | 2026-06-14 | 2026-06-14 | Primary | yes | Current worktree evidence |
| Current local validation passes | Command outputs listed in red-team measurements | Local tool output | 2026-06-14 | 2026-06-14 | Primary | yes | Current command evidence |
| Peer range may be wider than tested | `package.json:59`; installed dev dependency `@mastra/core ^1.42.0`; docs/e2e usage | Local repo | 2026-06-14 | 2026-06-14 | Primary for range, partial for incompatibility | partial | Evidence-needed for older Mastra versions |

## Agentic trace coverage
| Trace class | Status | Evidence | Residual risk |
|---|---|---|---|
| `model_call` | covered | Six subagent attempt IDs, prompt hashes, context hash, and evidence packet hash recorded. | Detailed model internals are unavailable. |
| `tool_call` | covered | Commands and helper scripts summarized in scope, measurements, and audit manifest. | Raw terminal transcripts are not embedded fully. |
| `tool_args` | covered_redacted | Safe command flags and redacted arguments are included; database URL and roots redacted. | Sensitive/private args intentionally omitted. |
| `tool_outputs` | covered_summary | Key outputs included: test pass counts, analyzer metrics, pack summary, repro JSON. | Full stdout/stderr not embedded. |
| `state` | covered | Branch, HEAD, file-set hash, clean initial status, active goal state verified. | Subsequent report write changes worktree by design. |
| `memory` | not_applicable | No durable memory store was consulted for factual claims. | None. |
| `handoff` | covered | Compacted handoff preserved reviewer IDs/findings/evidence and was reconciled with current goal snapshot. | Original full chat trace not embedded. |
| `approval` | covered | User explicitly requested `docs/reviews/*` report write; no other writes performed. | No external publish/commit requested for this review. |
| `retry` | covered | Reviewer attempts table records zero retries and no exhaustion. | No failed reviewer retry path exercised. |
| `exit` | covered | This report is the completion artifact; final goal completion follows artifact verification. | Findings remain unfixed by design. |

## Privacy-safe audit manifest
- Reviewed targets (relative or `<review_root>` redacted): `src/**`, `test/**`, `README.md`, `CHANGELOG.md`, `docs/**`, `.github/workflows/ci.yml`, `package.json`, `package-lock.json`, `tsconfig*.json`, `biome.json`, `docker-compose.yml`, `LICENSE`, `CONTRIBUTING.md`, `CLAUDE.md`.
- Tools/commands used (safe flags only; secrets and private args redacted): `git status --short --branch`, `git ls-files`, `sha256sum`, `rg`, `sed`, `find`, `ls`, `node --version`, `npm --version`, `npx tsc --version`, `npm run typecheck`, `npm run lint`, `npm test`, `npm run test:cluster`, `npm run test:coverage`, `DATABASE_URL=<redacted> npm run test:e2e`, `npm pack --dry-run`, `git diff --check`, skill helper scripts `detect-code-project` and `analyze-code-quality`, targeted fake-pool repro.
- Redactions: home directory, workspace root, skill installation root, database URL value, absolute tool paths, private terminal/context details.
- Unavailable evidence: external Mastra historical compatibility verification; full raw subagent transcripts; external plugin eval; exact Mastra `executeWithContext` failure contract across versions.
- Reviewer attempt IDs: `019ec7f9-788b-7e01-a26c-6a1071d340b2`, `019ec7f9-7bb6-77e3-8d90-06d7e74a7f13`, `019ec7f9-7e63-77b1-a73b-f73c896b17c4`, `019ec7f9-80f8-7692-8c1d-0223e49ddada`, `019ec7f9-8378-7aa2-bf6b-1cda5f9f4d5c`, `019ec7f9-8769-7551-a1c0-89b9ef13fa00`.
- Residual-risk categories: `unverified_claim`, `plugin_eval_unavailable`, `coverage_gap`, `other_with_reason`.

## Accepted findings

### Critical
No critical findings accepted.

### High

#### AR-HIGH-001 - Transient migration/start failure permanently poisons a `PostgresPubSub` instance
- Severity: High
- Confidence: High
- Bucket: high_priority_fix
- Gate ID: R
- Gate verdict: pass
- Location: `src/postgres-pubsub.ts:166-198`, `src/postgres-pubsub.ts:218-263`
- Evidence: `migrate()` stores `#migrated = runMigration(...)` and only logs/rethrows in `catch`. `start()` similarly stores `#started = this.#start()` and only logs/rethrows. A targeted fake-pool repro returned `{"startupPoisonReproduced":true,"connectCount":1}` after two `migrate()` calls, proving the second call reused the rejected promise instead of retrying.
- Impact: A transient database outage, failed migration, or pool acquisition failure can make a long-lived adapter instance unusable until the process constructs a fresh instance.
- Why existing handling is insufficient: Lazy startup reduces when startup occurs, but the cached promise still becomes a permanent failed state once rejected.
- Suggested fix: Cache the in-flight promise only while active, and clear `#migrated`/`#started` on failure if the stored promise is still the failing promise. Add regression tests for transient `migrate()` and lazy `start()` failure followed by success.
- Falsification condition: A test showing that the second call after a rejected migration/start promise performs a fresh connection and succeeds without reconstructing the adapter.
- Reviewer sources: A-defects / Ampere, lead repro.
- Source date / verified on: 2026-06-14.

#### AR-HIGH-002 - `subscribe()` is not atomic when listener setup fails
- Severity: High
- Confidence: High
- Bucket: high_priority_fix
- Gate ID: null
- Gate verdict: null
- Location: `src/postgres-pubsub.ts:513`, `src/postgres-pubsub.ts:555-562`, `src/listener.ts:48-53`
- Evidence: `subscribe()` mutates callback/index state before `#startSubscription()` completes. `#startSubscription()` starts the consume loop and sets `sub.started = true` before `#registerWake()`. `NotifyListener.register()` adds the handler to its map before `#ensureConnected()` succeeds. The catch path logs/rethrows but does not roll back the subscription object, callback, listener handler, started loop, or inserted private subscription row.
- Impact: A failed `LISTEN` setup can leave a hidden live loop or handler after `subscribe()` rejects, causing leaks, unexpected deliveries, confusing unsubscribe behavior, and stuck rows.
- Why existing handling is insufficient: The code assumes a failed subscribe has no effect, but state mutation occurs before the last failing await.
- Suggested fix: Make subscribe setup transactional at the object level: perform listener registration before marking started or registering callbacks, or add a structured rollback path that stops the loop, unregisters wake handlers, removes callbacks/index entries, and deletes private rows on failure.
- Falsification condition: A regression test where listener registration fails and subsequent diagnostics prove no callback, handler, loop, map entry, or private row remains.
- Reviewer sources: A-defects / Ampere, C-edge-cases / Sagan.
- Source date / verified on: 2026-06-14.

#### AR-HIGH-003 - Concurrent same-group `subscribe()` calls can create duplicate local subscription state
- Severity: High
- Confidence: Medium
- Bucket: high_priority_fix
- Gate ID: null
- Gate verdict: null
- Location: `src/postgres-pubsub.ts:485-489`
- Evidence: The code checks `this.#subscriptions.get(key)` before an awaited upsert. Two concurrent calls for the same channel/group can both observe no existing local subscription, both await database work, then each create a separate local subscription object and consume loop for the same logical key.
- Impact: Duplicate local loops can compete for the same subscription ID inside one process. Unsubscribing one returned handle can delete shared map/index state while the other still appears active, creating hidden delivery loss or duplicate-processing behavior.
- Why existing handling is insufficient: The lifecycle lock serializes start/stop, not per-subscription creation. The group key check is not protected across the awaited database operation.
- Suggested fix: Add a per-subscription-key creation lock or insert a pending entry before awaits, with rollback on failure. Add a concurrency regression that calls `Promise.all([subscribe(group), subscribe(group)])` and asserts a single local subscription/loop.
- Falsification condition: A stress test proves concurrent same-group subscribes always converge to one local subscription object and a coherent unsubscribe count.
- Reviewer sources: C-edge-cases / Sagan.
- Source date / verified on: 2026-06-14.

### Medium

#### AR-MED-001 - Numeric options accept invalid values that can break timers, SQL limits, and retry behavior
- Severity: Medium
- Confidence: High
- Bucket: normal_patch
- Gate ID: K
- Gate verdict: pass
- Location: `src/postgres-pubsub.ts:77`, `src/consume-loop.ts:181`, `src/consume-loop.ts:221`, `src/consume-loop.ts:225`, `src/postgres-pubsub.ts:1025`
- Evidence: Options such as poll interval, batch size, ack deadline, stale timeout, and maintenance interval are defaulted but not validated for finite positive ranges before reaching `setTimeout`, SQL `LIMIT`, visibility deadlines, or maintenance cadence.
- Impact: `0`, negative, `NaN`, `Infinity`, or non-integer values can cause tight loops, invalid SQL, ineffective redelivery timing, or silently disabled maintenance behavior.
- Why existing handling is insufficient: TypeScript types do not protect runtime JavaScript consumers or environment-derived config.
- Suggested fix: Add centralized runtime option normalization with finite/integer/minimum checks and targeted tests for rejected invalid values.
- Falsification condition: Runtime tests demonstrate invalid numeric inputs are rejected or normalized before reaching timers or SQL.
- Reviewer sources: A-defects / Ampere, B-requirements / Bacon, C-edge-cases / Sagan.
- Source date / verified on: 2026-06-14.

#### AR-MED-002 - `flush()` can count shared group backlog as local work
- Severity: Medium
- Confidence: High
- Bucket: decision_needed
- Gate ID: null
- Gate verdict: null
- Location: `src/postgres-pubsub.ts:752-823`, `README.md:135`, `docs/design.md:75`
- Evidence: `flush()` counts pending delivery rows for the current process's subscription IDs. Group subscription IDs are shared across processes, so a local process can see another group member's unacked delivery row as pending local work.
- Impact: `flush()` may time out in one process because another instance is still handling or has failed a shared group delivery, despite documentation presenting flush as local drain behavior.
- Why existing handling is insufficient: The schema does not distinguish local instance ownership in the counted rows, and docs do not warn about the shared-group interpretation.
- Suggested fix: Decide whether flush should be global-for-subscription or local-to-process. If local, track claim ownership/instance IDs and count only locally claimed work. If global, update docs and tests to state the semantics explicitly.
- Falsification condition: A multi-instance test proves `flush()` in one group member ignores another member's in-flight delivery when documented as local, or docs/tests are updated to assert global semantics.
- Reviewer sources: A-defects / Ampere.
- Source date / verified on: 2026-06-14.

#### AR-MED-003 - Observability context wrapper may let instrumentation failures affect PubSub delivery
- Severity: Medium
- Confidence: Medium
- Bucket: evidence_needed
- Gate ID: K
- Gate verdict: pass
- Location: `src/observability.ts:228`, `src/consume-loop.ts:321`
- Evidence: `SafeObservabilitySpan.run()` delegates to `executeWithContext` without the same `safeCall` isolation used by other observability operations. Delivery callbacks run through this wrapper.
- Impact: If Mastra's `executeWithContext` can throw before or after invoking the callback for reasons unrelated to the callback itself, observability failure could change delivery behavior.
- Why existing handling is insufficient: The surrounding observability design is defensive, but this path relies on an external contract not proven in this review.
- Suggested fix: Verify the `executeWithContext` contract for the supported Mastra range. If it can throw instrumentation failures, isolate those errors while still preserving user callback errors.
- Falsification condition: Official/local Mastra implementation evidence shows `executeWithContext` only propagates the callback's own error and cannot fail independently.
- Reviewer sources: F-traces-observability / Hubble.
- Source date / verified on: 2026-06-14.

#### AR-MED-004 - CI does not prove the advertised cluster and key-free e2e behavior
- Severity: Medium
- Confidence: High
- Bucket: normal_patch
- Gate ID: M
- Gate verdict: pass
- Location: `.github/workflows/ci.yml:43-83`, `README.md` cluster/e2e claims
- Evidence: The verify job runs typecheck, lint, unit tests, coverage, and pack dry-run, but not `npm run test:cluster`. The e2e job skips all e2e when `OPENAI_API_KEY` is missing even though the current e2e suite includes key-free PubSub semantics.
- Impact: A regression in the multi-process cluster proof or database-only e2e semantics can pass CI.
- Why existing handling is insufficient: Local tests passed during this review, but CI is the durable release gate.
- Suggested fix: Add `npm run test:cluster` to CI and split e2e into key-free database PubSub tests versus OpenAI-required Mastra integration tests.
- Falsification condition: CI configuration is changed so every PR/release runs cluster tests and all key-free e2e tests regardless of OpenAI credential availability.
- Reviewer sources: B-requirements / Bacon.
- Source date / verified on: 2026-06-14.

#### AR-MED-005 - Default auto-migration requires runtime DDL privileges unless deployments opt out
- Severity: Medium
- Confidence: Medium
- Bucket: decision_needed
- Gate ID: null
- Gate verdict: null
- Location: `src/schema.ts:84-97`, `README.md:120`
- Evidence: The default startup path auto-creates schema/tables/indexes. The configured default schema is `pg_pubsub`, and auto-migration requires privileges to create or alter that schema.
- Impact: Production applications may run with broader database privileges than desired if they use default auto-migration directly in app runtime roles.
- Why existing handling is insufficient: Auto-migration is a deliberate feature, but the privilege model is a security/deployment tradeoff that should be explicit and easy to disable or pre-run.
- Suggested fix: Document least-privilege deployment patterns, ensure `autoMigrate: false` plus exported SQL/preflight flow is clear, and consider a runtime warning/log when migration runs outside test/dev if environment signals are available.
- Falsification condition: Documentation and tests show a first-class least-privilege setup where migrations are pre-applied and runtime roles do not need DDL permissions.
- Reviewer sources: D-security-tool-abuse / Poincare.
- Source date / verified on: 2026-06-14.

#### AR-MED-006 - `close()` centralizes too much teardown responsibility
- Severity: Medium
- Confidence: High
- Bucket: normal_patch
- Gate ID: O
- Gate verdict: fail
- Location: `src/postgres-pubsub.ts:1124-1210`
- Evidence: Static analyzer reports `close()` at 81 logical lines, cyclomatic complexity 9, cognitive complexity 16, and nesting 4. It handles state flags, subscription loops, listener close, pool close, logging, span status, and error aggregation in one method.
- Impact: Teardown bugs are more likely because changes to one shutdown concern can affect unrelated shutdown concerns. This is especially risky now that shutdown is driven by last-subscriber removal rather than Mastra lifecycle hooks.
- Why existing handling is insufficient: Tests pass, but the method is a critical reliability boundary and already exceeds the review gate's complexity threshold.
- Suggested fix: Extract focused helpers for stopping subscriptions, closing listener/pool, and emitting shutdown observability while preserving existing ordering. Add regression tests around multi-error teardown and idempotent close.
- Falsification condition: Complexity is reduced below gate thresholds or justified by stronger teardown regression coverage and clear local structure.
- Reviewer sources: CQ-code-quality / Nash.
- Source date / verified on: 2026-06-14.

### Low

#### AR-LOW-001 - Cluster test worker inherits the full parent environment
- Severity: Low
- Confidence: High
- Bucket: normal_patch
- Gate ID: null
- Gate verdict: null
- Location: `test/cluster.test.ts:56-65`, `test/cluster.test.ts:81-83`, `test/cluster.test.ts:148-150`, `test/fixtures/cluster-worker.ts:59-64`
- Evidence: Cluster workers are forked with inherited environment plus overrides. The worker only needs a narrow set of test values.
- Impact: Test subprocesses may receive unrelated secrets or environment flags, making failures harder to reproduce and increasing accidental disclosure risk in future debug output.
- Why existing handling is insufficient: The current worker does not print env, but inherited environment is broader than necessary.
- Suggested fix: Pass a minimal explicit environment to forked workers.
- Falsification condition: Test worker launch code constructs a minimal env allowlist and tests still pass.
- Reviewer sources: D-security-tool-abuse / Poincare.
- Source date / verified on: 2026-06-14.

#### AR-LOW-002 - Test `dropSchema` helper interpolates identifiers without validation
- Severity: Low
- Confidence: High
- Bucket: normal_patch
- Gate ID: null
- Gate verdict: null
- Location: `test/helpers.ts:16-20`, compare `test/e2e/helpers.ts:21-24`
- Evidence: One test helper interpolates schema identifiers directly while the e2e helper has safer handling.
- Impact: This is test-only, but helper reuse with dynamic schema input could make local test databases vulnerable to accidental destructive SQL.
- Why existing handling is insufficient: Current call sites may use controlled names, but the helper itself is footgun-prone.
- Suggested fix: Reuse the safer identifier quoting/validation helper from e2e tests or centralize schema identifier handling.
- Falsification condition: The helper validates/quotes identifiers or is made private to constant-only call sites.
- Reviewer sources: D-security-tool-abuse / Poincare.
- Source date / verified on: 2026-06-14.

#### AR-LOW-003 - Logs omit sanitized `errorName` for listener errors
- Severity: Low
- Confidence: High
- Bucket: normal_patch
- Gate ID: null
- Gate verdict: null
- Location: `src/listener.ts:121-135`
- Evidence: Listener error span events include an error name, but the log entry only includes channel context.
- Impact: Operators have less useful log-only diagnostics for listener failures.
- Why existing handling is insufficient: Trace data is richer than logs, but logs are often the first and sometimes only operational signal.
- Suggested fix: Include sanitized `errorName` in listener error logs, matching the existing trace event sanitization.
- Falsification condition: Listener error logs include sanitized error class/name without leaking stack/message content.
- Reviewer sources: F-traces-observability / Hubble.
- Source date / verified on: 2026-06-14.

#### AR-LOW-004 - Replay callback failures are not summarized on the parent replay span
- Severity: Low
- Confidence: Medium
- Bucket: normal_patch
- Gate ID: null
- Gate verdict: null
- Location: `src/postgres-pubsub.ts:935`, `src/postgres-pubsub.ts:962-964`
- Evidence: Replay callback errors are logged, but the parent `subscribe_from_offset` span can still end without a callback error count/status summary.
- Impact: Trace readers may see a successful replay operation while individual replay callback failures are buried in events/logs.
- Why existing handling is insufficient: Event-level evidence exists, but parent span summaries are better for dashboards and alerts.
- Suggested fix: Track replay callback error count and attach it to the parent span/log summary.
- Falsification condition: Parent replay span includes error count/status whenever any replay callback fails.
- Reviewer sources: F-traces-observability / Hubble.
- Source date / verified on: 2026-06-14.

#### AR-LOW-005 - Published source maps reference source files that are excluded from the package
- Severity: Low
- Confidence: High
- Bucket: decision_needed
- Gate ID: null
- Gate verdict: null
- Location: `package.json` files config, `npm pack --dry-run` output, `dist/*.map`
- Evidence: `npm pack --dry-run` includes `dist/*.map` but not `src/*.ts`; the package documentation says source is not published.
- Impact: Consumers may get less useful debugging from source maps, and maps can reveal source path structure without the corresponding source payload.
- Why existing handling is insufficient: Pack succeeds, but the package contents policy and sourcemap behavior are not aligned.
- Suggested fix: Either publish source files intentionally with maps, or disable/exclude source maps from the package.
- Falsification condition: Package contents are adjusted so source maps resolve to packaged source or are not published.
- Reviewer sources: B-requirements / Bacon.
- Source date / verified on: 2026-06-14.

#### AR-LOW-006 - Telemetry allowlist still emits user-controlled scalar identifiers
- Severity: Low
- Confidence: Medium
- Bucket: decision_needed
- Gate ID: null
- Gate verdict: null
- Location: `src/postgres-pubsub.ts:325-342`, `src/consume-loop.ts:269-276`, `src/listener.ts:107-114`, `README.md:148`
- Evidence: Observability attributes avoid payloads and SQL, but channel/group/subscription-like identifiers can still be application/user controlled.
- Impact: Applications that encode tenant, user, or business identifiers in channels/groups can leak those identifiers into logs/traces/metrics.
- Why existing handling is insufficient: The README states payloads are never logged, but identifier sensitivity is deployment-specific and not fully addressed by payload redaction.
- Suggested fix: Document identifier sensitivity and consider optional hashing/redaction for channel/group/subscription attributes.
- Falsification condition: Docs and/or configuration make identifier exposure an explicit, controllable operator choice.
- Reviewer sources: D-security-tool-abuse / Poincare.
- Source date / verified on: 2026-06-14.

### Info

#### AR-INFO-001 - Current local validation is strong but does not cover the accepted high-risk paths
- Severity: Info
- Confidence: High
- Bucket: defer
- Gate ID: M
- Gate verdict: pass
- Location: local command evidence
- Evidence: Typecheck, lint, unit, coverage, cluster, e2e, and pack dry-run all passed. The accepted high findings were found through adversarial state/race inspection and a targeted repro outside the current test suite.
- Impact: The project is in a releasable-looking state by normal checks, but additional regression tests should be added before claiming these specific reliability bugs are fixed.
- Why existing handling is insufficient: General coverage percentages do not prove transient failure retry, atomic subscribe rollback, or same-group concurrency safety.
- Suggested fix: Add focused tests for each accepted high/medium behavior.
- Falsification condition: Targeted tests for the accepted findings fail before fixes and pass after fixes.
- Reviewer sources: lead synthesis.
- Source date / verified on: 2026-06-14.

## Decision-needed items
- Decide whether `flush()` is intended to be local-to-process or global-for-shared-subscription groups; code and docs should agree.
- Decide whether the package should publish `src` with source maps or omit maps from `npm pack`.
- Decide whether default auto-migration should remain the primary recommended production path or be documented as a dev/convenience path with least-privilege production guidance.
- Decide whether channel/group/subscription identifiers should be treated as safe operational metadata or optionally redacted/hashed.

## Evidence-needed items
- Verify the minimum supported `@mastra/core` peer version. The package declares `>=1.13.0-0 <2.0.0-0`, while development and tests use `^1.42.0`.
- Verify Mastra `executeWithContext` behavior for the supported peer range before accepting or dismissing AR-MED-003.
- Add or run a compatibility matrix against the oldest supported Mastra peer version before publishing claims that the full range is supported.

## Dismissed or downgraded material candidates
| Candidate | Original severity | Decision | Reason |
|---|---|---|---|
| `package-lock.json` large-file analyzer warning | Medium | dismissed | Generated lockfile; not a maintainability finding for source code. |
| Empty catch in `src/listener.ts` cleanup | Low/Medium | dismissed | Best-effort cleanup path; no evidence of swallowed actionable failure beyond accepted logging improvements. |
| Empty catch in `src/observability.ts` safe calls | Low/Medium | dismissed | Deliberate observability isolation; accepted issue is limited to `SafeObservabilitySpan.run()`. |
| Empty catch in cluster test cleanup | Low | dismissed | Test cleanup guard; no production impact. |
| Analyzer unused-export warnings in `src/observability.ts` | Low | dismissed | Internal module exports/false positives after triage. |
| Promise `.then()` lifecycle lock | Low | dismissed | Serialization pattern is intentional; accepted finding targets rejected-promise cache poisoning. |
| Cluster fixture `isCommand`/`handleCommand` complexity | Medium | downgraded | Test-only helper complexity; less urgent than production `close()` complexity. |
| Peer dependency range too broad | High | downgraded to evidence-needed | The declared/tested version mismatch is real, but actual incompatibility with older Mastra versions was not proven in this review. |
| Default migration privilege concern | Medium/High | accepted as Medium decision-needed | Auto-migration is a requested feature; the issue is deployment guidance/least privilege, not an unconditional vulnerability. |

## Residual risk and unreviewed areas
- No external vulnerability audit or npm advisory lookup was performed.
- `node_modules` and generated `dist` source were not line-reviewed; `dist` was covered only through package dry-run.
- The accepted concurrency findings were reasoned from code structure; only the startup poison finding had an executable repro in this review.
- Older supported Mastra versions were not installed or tested.
- Full raw subagent transcripts are not embedded in this report; the attempt IDs and hashes are retained for traceability.
- No source fixes were applied as part of this review; all accepted findings remain open.

## Review quality self-check
- Strongest evidence: AR-HIGH-001 has both source inspection and a targeted repro showing one connection attempt across two migration calls.
- Weakest accepted finding: AR-MED-003 depends on Mastra `executeWithContext` behavior not proven in this pass, so it is marked evidence-needed.
- Most important assumption: The reviewed worktree at HEAD `41efd4c` is the intended release candidate for this review.
- Most likely false positive: AR-MED-003, if `executeWithContext` is guaranteed to propagate only the callback's own error.
- Most likely missed class of issue: PostgreSQL transaction/isolation behavior under real high-concurrency load beyond the current cluster test.
- Whether another review pass is warranted: Yes, after fixes. The repair pass should include concurrency stress tests, transient DB failure tests, and CI workflow verification.

## Recommended next actions
1. Fix AR-HIGH-001 with rejected-promise cache clearing and transient failure regression tests.
2. Fix AR-HIGH-002 with atomic subscribe rollback tests covering failed listener registration.
3. Fix AR-HIGH-003 with a per-subscription-key creation lock and same-group concurrency tests.
4. Add runtime option validation for numeric config.
5. Update CI to run cluster tests and split key-free e2e coverage from OpenAI-keyed e2e coverage.
6. Resolve the `flush()` semantics decision and update code/docs/tests accordingly.
7. Verify the oldest supported Mastra peer version or narrow the peer range before publish.
8. Refactor `close()` into smaller teardown helpers after behavior is covered by regression tests.
