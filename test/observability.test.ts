import assert from 'node:assert/strict';
import { after, afterEach, test } from 'node:test';
import {
  type AnySpan,
  SpanType,
  setCurrentSpanResolver,
  setExecuteWithContext,
} from '@mastra/core/observability';
import {
  logDebug,
  logError,
  observeEvent,
  startObservabilitySpan,
  traceAttributes,
} from '../src/observability.ts';
import { dropSchema, makePubSub, makeTestLogger, uniqueSchema, waitFor } from './helpers.ts';

interface LogEntry {
  level: 'debug' | 'warn' | 'error';
  message: string;
  args: unknown[];
}

interface EventRecord {
  name: string;
  attributes: Record<string, unknown>;
  output: unknown;
}

interface SpanRecord {
  name: string;
  attributes: Record<string, unknown>;
  events: EventRecord[];
  children: FakeSpan[];
  errors: unknown[];
  ended: boolean;
}

interface FakeSpan {
  span: AnySpan;
  record: SpanRecord;
}

const schema = uniqueSchema();
const pubsubs: Array<{ close(): Promise<void> }> = [];

afterEach(() => {
  setCurrentSpanResolver(undefined);
  setExecuteWithContext(async ({ fn }) => fn());
});

after(async () => {
  await Promise.all(pubsubs.map((ps) => ps.close()));
  await dropSchema(schema);
});

function makeCaptureLogger(logs: LogEntry[]) {
  return makeTestLogger({
    debug: (message: string, ...args: unknown[]) => {
      logs.push({ level: 'debug', message, args });
    },
    warn: (message: string, ...args: unknown[]) => {
      logs.push({ level: 'warn', message, args });
    },
    error: (message: string, ...args: unknown[]) => {
      logs.push({ level: 'error', message, args });
    },
  });
}

function makeFakeSpan(name: string, logs: LogEntry[] = []): FakeSpan {
  const record: SpanRecord = {
    name,
    attributes: {},
    events: [],
    children: [],
    errors: [],
    ended: false,
  };
  const logger = makeCaptureLogger(logs);
  const span = {
    id: `${name}-id`,
    traceId: `${name}-trace`,
    name,
    type: SpanType.GENERIC,
    startTime: new Date(),
    isEvent: false,
    isInternal: false,
    observabilityInstance: {
      getLogger: () => logger,
    },
    createChildSpan: (options: { name: string; attributes?: Record<string, unknown> }) => {
      const child = makeFakeSpan(options.name, logs);
      child.record.attributes = { ...(options.attributes ?? {}) };
      record.children.push(child);
      return child.span;
    },
    createEventSpan: (options: {
      name: string;
      attributes?: Record<string, unknown>;
      output?: unknown;
    }) => {
      record.events.push({
        name: options.name,
        attributes: { ...(options.attributes ?? {}) },
        output: options.output,
      });
      return makeFakeSpan(options.name, logs).span;
    },
    update: (options: { attributes?: Record<string, unknown> }) => {
      Object.assign(record.attributes, options.attributes ?? {});
    },
    error: (options: unknown) => {
      record.errors.push(options);
    },
    end: (options?: { attributes?: Record<string, unknown> }) => {
      record.ended = true;
      Object.assign(record.attributes, options?.attributes ?? {});
    },
    executeInContext: async <T>(fn: () => Promise<T>) => fn(),
    executeInContextSync: <T>(fn: () => T) => fn(),
    get isRootSpan() {
      return true;
    },
    get isValid() {
      return true;
    },
    getParentSpanId: () => undefined,
    findParent: () => undefined,
    exportSpan: () => undefined,
    get externalTraceId() {
      return `${name}-trace`;
    },
  } as unknown as AnySpan;

  return { span, record };
}

function telemetryContains(entries: unknown[], value: string): boolean {
  return JSON.stringify(entries).includes(value);
}

test('log helpers fall back to the current Mastra span logger when no logger is configured', () => {
  const logs: LogEntry[] = [];
  const root = makeFakeSpan('root', logs);
  setCurrentSpanResolver(() => root.span);

  logDebug(undefined, 'debug message', traceAttributes({ topic: 'topic-logs' }));
  logError(
    undefined,
    'error message',
    traceAttributes({ topic: 'topic-logs' }),
    new Error('secret'),
  );

  assert.deepEqual(
    logs.map((entry) => [entry.level, entry.message]),
    [
      ['debug', 'debug message'],
      ['error', 'error message'],
    ],
  );
  assert.equal(telemetryContains(logs, 'secret'), false);
  assert.equal(telemetryContains(logs, 'Error'), true);
});

test('logger false disables current-span fallback logging', () => {
  const logs: LogEntry[] = [];
  const root = makeFakeSpan('root', logs);
  setCurrentSpanResolver(() => root.span);

  logDebug(false, 'should not be logged', traceAttributes({ topic: 'topic-silent' }));

  assert.deepEqual(logs, []);
});

test('observability spans and events are created from the current Mastra span', async () => {
  const root = makeFakeSpan('root');
  let contextSpan: AnySpan | undefined;
  setCurrentSpanResolver(() => root.span);
  setExecuteWithContext(async ({ span, fn }) => {
    contextSpan = span;
    return fn();
  });

  const span = startObservabilitySpan(
    'pg_pubsub.test_operation',
    traceAttributes({
      topic: 'topic-span',
      omitted: undefined,
    }),
  );
  const result = await span.run(async () => 'done');
  span.setAttribute('extra', 'value');
  span.recordError(new Error('hidden-message'));
  span.end({ code: 'error', message: 'failed' });
  observeEvent('pg_pubsub.test_event', traceAttributes({ topic: 'topic-span' }));

  const child = root.record.children[0];
  assert.ok(child);
  assert.equal(child.record.name, 'pg_pubsub.test_operation');
  assert.equal(child.record.attributes.topic, 'topic-span');
  assert.equal(child.record.attributes.extra, 'value');
  assert.equal(child.record.attributes['error.name'], 'Error');
  assert.equal(child.record.attributes.status, 'error');
  assert.equal(child.record.attributes['status.message'], 'failed');
  assert.equal(child.record.ended, true);
  assert.equal(contextSpan, child.span);
  assert.equal(result, 'done');
  assert.equal(root.record.events[0]?.name, 'pg_pubsub.test_event');
});

test('observability helpers tolerate missing, throwing, and duplicate-end paths', async () => {
  const logs: LogEntry[] = [];
  const logger = makeCaptureLogger(logs);

  logDebug(logger, 'plain message');
  logError(logger, 'unknown error', undefined, 'string-failure');

  assert.equal(logs[0]?.message, 'plain message');
  assert.deepEqual(logs[1]?.args[0], { 'error.name': 'string' });

  const root = makeFakeSpan('root');
  const throwingSpan = {
    ...root.span,
    createChildSpan: () => {
      throw new Error('child failed');
    },
    createEventSpan: () => {
      throw new Error('event failed');
    },
    observabilityInstance: {
      getLogger: () => {
        throw new Error('logger failed');
      },
    },
  } as unknown as AnySpan;
  setCurrentSpanResolver(() => throwingSpan);

  assert.doesNotThrow(() => observeEvent('pg_pubsub.throwing_event'));
  assert.doesNotThrow(() => logDebug(undefined, 'ignored'));
  const noop = startObservabilitySpan('pg_pubsub.throwing_span');
  noop.setAttribute('ignored', undefined);
  noop.end();

  setCurrentSpanResolver(() => root.span);
  const span = startObservabilitySpan('pg_pubsub.double_end');
  span.setAttribute('ignored', undefined);
  span.end();
  span.end();

  const child = root.record.children.find(
    (candidate) => candidate.record.name === 'pg_pubsub.double_end',
  );
  assert.ok(child);
  assert.equal(child.record.ended, true);
  assert.equal(child.record.attributes.ignored, undefined);
});

test('PubSub methods use current-span logging and emit child spans without payload data', async () => {
  const logs: LogEntry[] = [];
  const root = makeFakeSpan('root', logs);
  setCurrentSpanResolver(() => root.span);
  const ps = makePubSub(schema);
  pubsubs.push(ps);

  await ps.publish('topic-observe', {
    type: 'published',
    data: { secret: 'do-not-log' },
    runId: 'run-observe',
  });
  const history = await ps.getHistory('topic-observe');

  assert.equal(history.length, 1);
  assert.ok(logs.some((entry) => entry.message === 'migration started'));
  assert.ok(logs.some((entry) => entry.message === 'published event'));
  assert.ok(root.record.children.some((child) => child.record.name === 'pg_pubsub.migrate'));
  assert.ok(root.record.children.some((child) => child.record.name === 'pg_pubsub.publish'));
  assert.ok(root.record.children.some((child) => child.record.name === 'pg_pubsub.get_history'));
  assert.equal(telemetryContains([logs, root.record], 'do-not-log'), false);
});

test('publish and replay failure telemetry stays sanitized under current-span observability', async () => {
  const logs: LogEntry[] = [];
  const root = makeFakeSpan('root', logs);
  setCurrentSpanResolver(() => root.span);
  const failureSchema = uniqueSchema();
  const ps = makePubSub(failureSchema, { listen: false, pollIntervalMs: 25 });
  pubsubs.push(ps);

  const circularPayload: Record<string, unknown> = { secret: 'payload-secret' };
  circularPayload.self = circularPayload;

  await assert.rejects(() =>
    ps.publish('topic-publish-failure', {
      type: 'bad-json',
      data: circularPayload,
      runId: 'run-publish-failure',
    }),
  );

  await ps.publish('topic-replay-failure', {
    type: 'history',
    data: { secret: 'replay-secret' },
    runId: 'run-replay-failure',
  });
  await ps.subscribeFromOffset('topic-replay-failure', 0, () => {
    throw new Error('callback-secret');
  });

  assert.ok(logs.some((entry) => entry.message === 'publish failed'));
  assert.ok(logs.some((entry) => entry.message === 'replay callback threw'));
  assert.ok(logs.some((entry) => entry.message === 'subscribe from offset completed'));
  assert.ok(
    root.record.children.some(
      (child) =>
        child.record.name === 'pg_pubsub.publish' &&
        child.record.attributes.status === 'error' &&
        child.record.ended,
    ),
  );
  assert.ok(
    root.record.children.some(
      (child) =>
        child.record.name === 'pg_pubsub.subscribe_from_offset' &&
        child.record.attributes['replay.callback_error_count'] === 1 &&
        child.record.attributes['replay.callback_status'] === 'error' &&
        child.record.ended,
    ),
  );
  assert.equal(telemetryContains([logs, root.record], 'payload-secret'), false);
  assert.equal(telemetryContains([logs, root.record], 'replay-secret'), false);
  assert.equal(telemetryContains([logs, root.record], 'callback-secret'), false);
});

test('delivery callbacks execute inside the delivery span context', async () => {
  const root = makeFakeSpan('root');
  let deliveryContextSeen = false;
  setCurrentSpanResolver(() => root.span);
  setExecuteWithContext(async ({ span, fn }) => {
    const child = root.record.children.find((candidate) => candidate.span === span);
    if (child?.record.name === 'pg_pubsub.delivery') {
      deliveryContextSeen = true;
    }
    return fn();
  });

  const ps = makePubSub(uniqueSchema(), { pollIntervalMs: 25 });
  pubsubs.push(ps);
  const received: string[] = [];
  await ps.subscribe('topic-delivery', (event, ack) => {
    received.push(event.type);
    ack?.();
  });

  await ps.publish('topic-delivery', { type: 'delivered', data: null, runId: 'run-delivery' });
  await waitFor(() => received.includes('delivered'));

  assert.equal(deliveryContextSeen, true);
  assert.ok(
    root.record.children.some(
      (child) =>
        child.record.name === 'pg_pubsub.delivery' &&
        child.record.attributes['delivery.settlement'] === 'ack' &&
        child.record.ended,
    ),
  );
});
