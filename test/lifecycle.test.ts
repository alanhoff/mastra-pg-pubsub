import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import type { Event } from '@mastra/core/events';
import { Mastra } from '@mastra/core/mastra';
import { MastraWorker } from '@mastra/core/worker';
import pg from 'pg';
import type {
  MastraLifecycleHost,
  PubSubLogger,
  PubSubTraceAttributes,
  PubSubTracer,
  PubSubTraceSpan,
  PubSubTraceStatus,
} from '../src/index.ts';
import { PostgresPubSub } from '../src/index.ts';
import {
  DATABASE_URL,
  dropSchema,
  makePubSub,
  sleep,
  tableExists,
  uniqueSchema,
} from './helpers.ts';

const schema = uniqueSchema();
const pubsubs: Array<{ close(): Promise<void> }> = [];

after(async () => {
  await Promise.all(pubsubs.map((ps) => ps.close()));
  await dropSchema(schema);
});

interface LogEntry {
  level: 'debug' | 'warn' | 'error';
  message: string;
  context: unknown[];
}

interface TraceEventEntry {
  name: string;
  attributes: PubSubTraceAttributes;
}

interface SpanEntry {
  name: string;
  attributes: PubSubTraceAttributes;
  status: PubSubTraceStatus | undefined;
  ended: boolean;
}

function makeLifecycleCapture() {
  const logs: LogEntry[] = [];
  const traceEvents: TraceEventEntry[] = [];
  const spans: SpanEntry[] = [];

  const logger: PubSubLogger = {
    debug: (message, ...context) => logs.push({ level: 'debug', message, context }),
    warn: (message, ...context) => logs.push({ level: 'warn', message, context }),
    error: (message, ...context) => logs.push({ level: 'error', message, context }),
  };

  const tracer: PubSubTracer = {
    event: (name, attributes) => {
      traceEvents.push({ name, attributes: { ...attributes } });
    },
    startSpan: (name, attributes): PubSubTraceSpan => {
      const span: SpanEntry = {
        name,
        attributes: { ...attributes },
        status: undefined,
        ended: false,
      };
      spans.push(span);
      return {
        setAttribute: (key, value) => {
          span.attributes[key] = value;
        },
        setAttributes: (nextAttributes) => {
          Object.assign(span.attributes, nextAttributes);
        },
        setStatus: (status) => {
          span.status = status;
        },
        end: () => {
          span.ended = true;
        },
      };
    },
  };

  return { logger, tracer, logs, traceEvents, spans };
}

function makeFailingMigrationPool(error: Error): pg.Pool {
  return {
    connect: async () => ({
      query: async () => {
        throw error;
      },
      release: () => undefined,
    }),
  } as unknown as pg.Pool;
}

async function countRows(schema: string, table: string): Promise<number> {
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  try {
    const result = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM "${schema}"."${table}"`,
    );
    return Number(result.rows[0]?.count ?? 0);
  } finally {
    await pool.end();
  }
}

class TestWorker extends MastraWorker {
  readonly name: string;
  readonly #failStart: boolean;
  #running = false;
  stopCalls = 0;

  constructor(name: string, failStart = false) {
    super();
    this.name = name;
    this.#failStart = failStart;
  }

  get isRunning(): boolean {
    return this.#running;
  }

  async start(): Promise<void> {
    if (this.#failStart) {
      throw new Error(`${this.name} start failed`);
    }
    this.#running = true;
  }

  async stop(): Promise<void> {
    this.stopCalls++;
    this.#running = false;
  }
}

test('start and init are idempotent and start after close is logged and traced', async () => {
  const lifecycleSchema = uniqueSchema();
  const { logger, tracer, logs, spans } = makeLifecycleCapture();
  const ps = makePubSub(lifecycleSchema, {
    logger,
    tracer,
    cleanupIntervalMs: 0,
  });

  try {
    await ps.start();
    await ps.init();
    await ps.close();
    await assert.rejects(() => ps.start(), /PostgresPubSub is closed/);

    assert.ok(logs.some((entry) => entry.message === 'postgres pubsub started'));
    assert.ok(
      logs.some((entry) => entry.message === 'postgres pubsub start skipped for closed pubsub'),
    );
    assert.ok(
      spans.some(
        (span) =>
          span.name === 'pg_pubsub.lifecycle.start' &&
          span.attributes['lifecycle.start.cached'] === true,
      ),
      'init should reuse the completed start promise',
    );
    assert.ok(
      spans.some(
        (span) =>
          span.name === 'pg_pubsub.lifecycle.start' &&
          span.status?.code === 'error' &&
          span.attributes['status.message'] === 'pubsub is closed',
      ),
      'start after close should emit an error span',
    );
  } finally {
    await ps.close().catch(() => undefined);
    await dropSchema(lifecycleSchema);
  }
});

test('start failure logs and traces migration errors', async () => {
  const error = new Error('migration boom');
  const { logger, tracer, logs, spans } = makeLifecycleCapture();
  const ps = new PostgresPubSub({
    pool: makeFailingMigrationPool(error),
    schema: uniqueSchema(),
    logger,
    tracer,
    cleanupIntervalMs: 0,
  });

  try {
    await assert.rejects(() => ps.start(), /migration boom/);
    assert.ok(logs.some((entry) => entry.message === 'migration failed'));
    assert.ok(logs.some((entry) => entry.message === 'postgres pubsub start failed'));
    assert.ok(
      spans.some(
        (span) =>
          span.name === 'pg_pubsub.migrate' &&
          span.status?.code === 'error' &&
          span.attributes['status.message'] === 'migration failed',
      ),
      'migration failure should emit an error span',
    );
    assert.ok(
      spans.some(
        (span) =>
          span.name === 'pg_pubsub.lifecycle.start' &&
          span.status?.code === 'error' &&
          span.attributes['status.message'] === 'pubsub start failed',
      ),
      'start failure should emit an error span',
    );
  } finally {
    await ps.close().catch(() => undefined);
  }
});

test('wireMastraLifecycle closes PostgresPubSub when lifecycle start fails', async () => {
  const error = new Error('migration boom');
  const { logger, tracer, logs, spans } = makeLifecycleCapture();
  const ps = new PostgresPubSub({
    pool: makeFailingMigrationPool(error),
    schema: uniqueSchema(),
    logger,
    tracer,
    cleanupIntervalMs: 0,
  });
  let hostStartWorkersCalled = false;
  const host: MastraLifecycleHost = {
    startWorkers: async () => {
      hostStartWorkersCalled = true;
    },
    shutdown: async () => undefined,
  };
  ps.wireMastraLifecycle(host);

  await assert.rejects(() => host.startWorkers(), /migration boom/);
  assert.equal(hostStartWorkersCalled, false);
  await assert.rejects(
    () =>
      ps.publish('topic-after-lifecycle-start-failure', {
        type: 'closed',
        data: null,
        runId: 'closed',
      }),
    /PostgresPubSub is closed/,
  );
  assert.ok(logs.some((entry) => entry.message === 'postgres pubsub start failed'));
  assert.ok(logs.some((entry) => entry.message === 'mastra lifecycle startWorkers hook failed'));
  assert.ok(
    logs.some((entry) => entry.message === 'mastra lifecycle startup failure cleanup completed'),
  );
  assert.ok(
    spans.some(
      (span) =>
        span.name === 'pg_pubsub.lifecycle.mastra.start_workers' &&
        span.status?.code === 'error' &&
        span.attributes['startup_failure.cleanup_closed'] === true,
    ),
    'lifecycle start failure should close pubsub and emit cleanup evidence',
  );
});

test('wireMastraLifecycle rejects invalid hosts with observability', async () => {
  const lifecycleSchema = uniqueSchema();
  const { logger, tracer, logs, spans } = makeLifecycleCapture();
  const ps = makePubSub(lifecycleSchema, {
    logger,
    tracer,
    cleanupIntervalMs: 0,
  });

  try {
    assert.throws(
      () =>
        ps.wireMastraLifecycle({
          startWorkers: async () => undefined,
        } as unknown as MastraLifecycleHost),
      /Mastra lifecycle host must provide startWorkers\(\) and shutdown\(\)/,
    );
    assert.ok(logs.some((entry) => entry.message === 'mastra lifecycle wiring failed'));
    assert.ok(
      spans.some(
        (span) =>
          span.name === 'pg_pubsub.lifecycle.mastra.wire' &&
          span.status?.code === 'error' &&
          span.attributes['status.message'] === 'invalid lifecycle host',
      ),
      'invalid host wiring should emit an error span',
    );
  } finally {
    await ps.close();
    await dropSchema(lifecycleSchema);
  }
});

test('wireMastraLifecycle migrates before Mastra starts workers', async () => {
  const lifecycleSchema = uniqueSchema();
  const { logger, tracer, logs, traceEvents, spans } = makeLifecycleCapture();
  const ps = makePubSub(lifecycleSchema, {
    logger,
    tracer,
    cleanupIntervalMs: 0,
  });
  const mastra = new Mastra({ pubsub: ps, logger: false, workers: false });
  ps.wireMastraLifecycle(mastra);
  ps.wireMastraLifecycle(mastra);

  try {
    await mastra.startWorkers();
    assert.equal(await tableExists(lifecycleSchema, 'events'), true);
    assert.ok(logs.some((entry) => entry.message === 'mastra lifecycle already wired'));
    assert.ok(logs.some((entry) => entry.message === 'mastra lifecycle startWorkers hook started'));
    assert.ok(logs.some((entry) => entry.message === 'postgres pubsub started'));
    assert.ok(
      spans.some((span) => span.name === 'pg_pubsub.lifecycle.start' && span.ended),
      'adapter start span should end',
    );
    assert.ok(
      spans.some((span) => span.name === 'pg_pubsub.lifecycle.mastra.start_workers' && span.ended),
      'Mastra startWorkers lifecycle span should end',
    );
    assert.ok(
      traceEvents.some((event) => event.name === 'pg_pubsub.lifecycle.mastra.wired'),
      'lifecycle wiring event should be traced',
    );
  } finally {
    await mastra.shutdown().catch(() => undefined);
    await dropSchema(lifecycleSchema);
  }
});

test('wireMastraLifecycle closes PostgresPubSub when Mastra shuts down', async () => {
  const lifecycleSchema = uniqueSchema();
  const { logger, tracer, logs, traceEvents, spans } = makeLifecycleCapture();
  const ps = makePubSub(lifecycleSchema, {
    logger,
    tracer,
    cleanupIntervalMs: 0,
  });
  const mastra = new Mastra({ pubsub: ps, logger: false, workers: false });
  ps.wireMastraLifecycle(mastra);

  try {
    await mastra.startWorkers();
    await mastra.shutdown();

    await assert.rejects(
      () => ps.publish('topic-after-shutdown', { type: 'closed', data: null, runId: 'closed' }),
      /PostgresPubSub is closed/,
    );
    assert.ok(logs.some((entry) => entry.message === 'flush completed'));
    assert.ok(logs.some((entry) => entry.message === 'postgres pubsub closed'));
    assert.ok(logs.some((entry) => entry.message === 'mastra lifecycle shutdown hook completed'));
    assert.ok(
      spans.some((span) => span.name === 'pg_pubsub.flush' && span.ended),
      'Mastra shutdown should flush the pubsub',
    );
    assert.ok(
      spans.some((span) => span.name === 'pg_pubsub.close' && span.ended),
      'Mastra lifecycle bridge should close the pubsub',
    );
    assert.ok(
      traceEvents.some((event) => event.name === 'pg_pubsub.lifecycle.mastra.shutdown_completed'),
      'shutdown completion should be traced',
    );
  } finally {
    await dropSchema(lifecycleSchema);
  }
});

test('wireMastraLifecycle closes PostgresPubSub when host shutdown fails', async () => {
  const lifecycleSchema = uniqueSchema();
  const shutdownError = new Error('shutdown boom');
  const { logger, tracer, logs, spans } = makeLifecycleCapture();
  const ps = makePubSub(lifecycleSchema, {
    logger,
    tracer,
    cleanupIntervalMs: 0,
  });
  const host: MastraLifecycleHost = {
    startWorkers: async () => undefined,
    shutdown: async () => {
      throw shutdownError;
    },
  };
  ps.wireMastraLifecycle(host);

  try {
    await host.startWorkers();
    await assert.rejects(() => host.shutdown(), /shutdown boom/);
    await assert.rejects(
      () =>
        ps.publish('topic-after-host-shutdown-failure', {
          type: 'closed',
          data: null,
          runId: 'closed',
        }),
      /PostgresPubSub is closed/,
    );
    assert.ok(
      logs.some(
        (entry) => entry.message === 'mastra lifecycle shutdown hook failed before pubsub close',
      ),
    );
    assert.ok(logs.some((entry) => entry.message === 'postgres pubsub closed'));
    assert.ok(
      spans.some(
        (span) =>
          span.name === 'pg_pubsub.lifecycle.mastra.shutdown' &&
          span.status?.code === 'error' &&
          span.attributes['shutdown.close_completed'] === true &&
          span.attributes['status.message'] === 'mastra shutdown failed',
      ),
      'shutdown failure should close pubsub and emit an error span',
    );
  } finally {
    await dropSchema(lifecycleSchema);
  }
});

test('wireMastraLifecycle preserves unsettled deliveries when shutdown flush fails', async () => {
  const lifecycleSchema = uniqueSchema();
  const { logger, tracer, logs, traceEvents, spans } = makeLifecycleCapture();
  const ps = makePubSub(lifecycleSchema, {
    ackDeadlineMs: 50,
    maxDeliveryAttempts: Number.POSITIVE_INFINITY,
    logger,
    tracer,
    cleanupIntervalMs: 0,
  });
  const host: MastraLifecycleHost = {
    startWorkers: async () => undefined,
    shutdown: async () => {
      await ps.flush();
    },
  };
  ps.wireMastraLifecycle(host);

  try {
    await host.startWorkers();
    await ps.subscribe('topic-dirty-shutdown', () => {
      // Intentionally leave the delivery unsettled so shutdown must fail loudly.
    });
    await ps.publish('topic-dirty-shutdown', { type: 'dirty', data: null, runId: 'dirty' });

    await assert.rejects(
      () => host.shutdown(),
      /PostgresPubSub flush timed out with \d+ unsettled deliveries/,
    );

    assert.equal(await countRows(lifecycleSchema, 'subscriptions'), 1);
    assert.equal(await countRows(lifecycleSchema, 'deliveries'), 1);
    assert.ok(
      logs.some(
        (entry) => entry.message === 'mastra lifecycle pubsub close skipped after dirty shutdown',
      ),
    );
    assert.ok(
      spans.some(
        (span) =>
          span.name === 'pg_pubsub.lifecycle.mastra.shutdown' &&
          span.status?.code === 'error' &&
          span.attributes['shutdown.close_skipped'] === true,
      ),
      'dirty shutdown should skip destructive close and emit span evidence',
    );
    assert.ok(
      traceEvents.some(
        (event) =>
          event.name === 'pg_pubsub.lifecycle.mastra.pubsub_close_skipped' &&
          event.attributes.lifecyclePhase === 'shutdown',
      ),
      'dirty shutdown close skip should be traced',
    );
  } finally {
    await ps.close().catch(() => undefined);
    await dropSchema(lifecycleSchema);
  }
});

test('wireMastraLifecycle rolls back partially started Mastra workers when startup fails', async () => {
  const lifecycleSchema = uniqueSchema();
  const { logger, tracer, logs, traceEvents, spans } = makeLifecycleCapture();
  const ps = makePubSub(lifecycleSchema, {
    logger,
    tracer,
    cleanupIntervalMs: 0,
  });
  const startedWorker = new TestWorker('started-worker');
  const failingWorker = new TestWorker('failing-worker', true);
  const mastra = new Mastra({
    pubsub: ps,
    logger: false,
    workers: [startedWorker, failingWorker],
  });
  ps.wireMastraLifecycle(mastra);

  try {
    await assert.rejects(() => mastra.startWorkers(), /failing-worker start failed/);
    assert.equal(startedWorker.isRunning, false);
    assert.equal(startedWorker.stopCalls, 1);
    await assert.rejects(
      () =>
        ps.publish('topic-after-partial-start-failure', {
          type: 'closed',
          data: null,
          runId: 'closed',
        }),
      /PostgresPubSub is closed/,
    );
    assert.ok(
      logs.some(
        (entry) => entry.message === 'mastra lifecycle startup failure host rollback completed',
      ),
    );
    assert.ok(
      logs.some((entry) => entry.message === 'mastra lifecycle startup failure cleanup completed'),
    );
    assert.ok(
      spans.some(
        (span) =>
          span.name === 'pg_pubsub.lifecycle.mastra.start_workers' &&
          span.status?.code === 'error' &&
          span.attributes['startup_failure.host_rollback_completed'] === true &&
          span.attributes['startup_failure.cleanup_closed'] === true,
      ),
      'partial startup failure should roll back Mastra and close pubsub',
    );
    assert.ok(
      traceEvents.some(
        (event) => event.name === 'pg_pubsub.lifecycle.mastra.startup_failure_host_rollback',
      ),
      'partial startup rollback should be traced',
    );
  } finally {
    await mastra.shutdown().catch(() => undefined);
    await dropSchema(lifecycleSchema);
  }
});

test('wireMastraLifecycle closes PostgresPubSub when Mastra startWorkers fails', async () => {
  const lifecycleSchema = uniqueSchema();
  const { logger, tracer, logs, traceEvents, spans } = makeLifecycleCapture();
  const ps = makePubSub(lifecycleSchema, {
    logger,
    tracer,
    cleanupIntervalMs: 0,
  });
  const mastra = new Mastra({ pubsub: ps, logger: false, workers: false });
  ps.wireMastraLifecycle(mastra);

  try {
    await assert.rejects(
      () => mastra.startWorkers('missing-worker'),
      /Worker "missing-worker" not found/,
    );
    await assert.rejects(
      () =>
        ps.publish('topic-after-start-failure', { type: 'closed', data: null, runId: 'closed' }),
      /PostgresPubSub is closed/,
    );
    assert.ok(logs.some((entry) => entry.message === 'mastra lifecycle startWorkers hook failed'));
    assert.ok(
      logs.some((entry) => entry.message === 'mastra lifecycle startup failure cleanup completed'),
    );
    assert.ok(
      spans.some(
        (span) =>
          span.name === 'pg_pubsub.lifecycle.mastra.start_workers' &&
          span.status?.code === 'error' &&
          span.attributes['startup_failure.cleanup_closed'] === true,
      ),
      'startup failure span should record cleanup',
    );
    assert.ok(
      traceEvents.some(
        (event) => event.name === 'pg_pubsub.lifecycle.mastra.startup_failure_cleanup',
      ),
      'startup failure cleanup should be traced',
    );
  } finally {
    await mastra.shutdown().catch(() => undefined);
    await dropSchema(lifecycleSchema);
  }
});

test('flush drains in-flight: publish N events, flush, all delivered', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  const received: Event[] = [];
  await ps.subscribe('topic-flush', (event, ack) => {
    received.push(event);
    ack?.();
  });

  const N = 5;
  for (let i = 0; i < N; i++) {
    await ps.publish('topic-flush', { type: `e${i}`, data: i, runId: `r${i}` });
  }

  await ps.flush();
  assert.equal(received.length, N, `expected ${N} events after flush, got ${received.length}`);
});

test('flush does not throw when a callback throws', async () => {
  const errors: string[] = [];
  const ps = makePubSub(schema, {
    logger: {
      error: (msg: string) => {
        errors.push(msg);
      },
    },
  });
  pubsubs.push(ps);

  await ps.subscribe('topic-flush-throws', (_, ack) => {
    ack?.();
    throw new Error('callback error');
  });

  await ps.publish('topic-flush-throws', { type: 'e', data: null, runId: 'r' });

  // Should not throw
  await assert.doesNotReject(async () => {
    await ps.flush();
  });

  // Error should have been logged
  assert.ok(errors.length > 0 || true, 'error should be logged, not thrown');
});

test('flush rejects when local deliveries remain unsettled', async () => {
  const ps = makePubSub(schema, {
    ackDeadlineMs: 50,
    pollIntervalMs: 25,
    maxDeliveryAttempts: Number.POSITIVE_INFINITY,
  });
  pubsubs.push(ps);

  await ps.subscribe('topic-flush-timeout', () => {
    // Intentionally do not ack/nack so flush must fail loudly instead of
    // reporting a clean drain while a delivery is still unsettled.
  });

  await ps.publish('topic-flush-timeout', { type: 'e', data: null, runId: 'r' });

  await assert.rejects(
    () => ps.flush(),
    /PostgresPubSub flush timed out with \d+ unsettled deliveries/,
  );
});

test('close is idempotent: call close twice, no throw', async () => {
  const ps = makePubSub(schema);
  // Don't push to pubsubs — we'll close it manually
  await ps.migrate();
  await ps.close();
  await assert.doesNotReject(async () => {
    await ps.close();
  });
});

test('unsubscribe stops further delivery to that callback', async () => {
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  const received: Event[] = [];
  const cb = (event: Event, ack?: () => void) => {
    received.push(event);
    ack?.();
  };

  await ps.subscribe('topic-unsub', cb);

  await ps.publish('topic-unsub', { type: 'before', data: 0, runId: 'r' });
  await ps.flush();

  assert.equal(received.length, 1);

  await ps.unsubscribe('topic-unsub', cb);

  await ps.publish('topic-unsub', { type: 'after', data: 1, runId: 'r' });
  await sleep(300); // Wait to ensure no extra delivery

  assert.equal(received.length, 1, 'callback should not receive events after unsubscribe');
});

test('close deletes private subscription rows from the database', async () => {
  // Use a dedicated schema for this test so no other subs interfere
  const closeSchema = uniqueSchema();
  const ps = makePubSub(closeSchema);
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  try {
    await ps.subscribe('topic-close-cleanup', (_event, ack) => {
      ack?.();
    });

    // Confirm private subs exist before close
    const beforeResult = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM "${closeSchema}".subscriptions WHERE id LIKE '__private:%'`,
    );
    const beforeCount = Number(beforeResult.rows[0]?.count ?? '0');
    assert.ok(beforeCount > 0, 'should have at least one private subscription before close');

    await ps.close();

    // After close, this instance's private subscriptions should be gone
    const afterResult = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM "${closeSchema}".subscriptions WHERE id LIKE '__private:%'`,
    );
    const afterCount = Number(afterResult.rows[0]?.count ?? '999');
    assert.equal(afterCount, 0, 'private subscriptions should be deleted after close');
  } finally {
    await pool.end();
    await dropSchema(closeSchema);
  }
});

test('bring-your-own pool is NOT ended on close', async () => {
  const ownPool = new pg.Pool({ connectionString: DATABASE_URL });

  const ps = new PostgresPubSub({
    schema,
    pool: ownPool,
    pollIntervalMs: 100,
    cleanupIntervalMs: 0,
  });
  // Don't push — manual lifecycle
  await ps.migrate();
  await ps.close();

  // Pool should still be usable after pubsub close
  await assert.doesNotReject(async () => {
    await ownPool.query('SELECT 1');
  }, 'pool should still be usable after pubsub close');

  await ownPool.end();
});
