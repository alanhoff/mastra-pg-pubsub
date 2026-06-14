import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import { Mastra } from '@mastra/core/mastra';
import type {
  PubSubLogger,
  PubSubTraceAttributes,
  PubSubTracer,
  PubSubTraceSpan,
  PubSubTraceStatus,
} from '../src/index.ts';
import { logDebug, logWarn, startTraceSpan, traceAttributes } from '../src/observability.ts';
import { dropSchema, makePubSub, uniqueSchema, waitFor } from './helpers.ts';

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
  exceptions: string[];
  status: PubSubTraceStatus | undefined;
  ended: boolean;
}

function makeCapture() {
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
        exceptions: [],
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
        recordException: (error) => {
          span.exceptions.push(error instanceof Error ? error.message : String(error));
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

function telemetryIncludes(value: unknown, needle: string): boolean {
  return JSON.stringify(value, (_key, nextValue) => {
    if (nextValue instanceof Error) {
      return {
        name: nextValue.name,
        message: nextValue.message,
        stack: nextValue.stack,
      };
    }
    return nextValue;
  }).includes(needle);
}

const schema = uniqueSchema();

after(async () => {
  await dropSchema(schema);
});

test('observability helpers keep logger and tracer calls payload-safe and idempotent', () => {
  const calls: unknown[][] = [];
  const events: Array<{ name: string; attributes: PubSubTraceAttributes }> = [];
  const statuses: PubSubTraceStatus[] = [];
  const spanAttributes: PubSubTraceAttributes[] = [];
  let endCount = 0;

  const logger: PubSubLogger = {
    debug: (...args) => calls.push(args),
    warn: (...args) => calls.push(args),
  };
  const tracer: PubSubTracer = {
    event: (name, attributes) => events.push({ name, attributes: { ...attributes } }),
    startSpan: () => ({
      setAttribute: (key, value) => {
        spanAttributes.push({ [key]: value });
      },
      setAttributes: (attributes) => {
        spanAttributes.push({ ...attributes });
      },
      recordException: (error) => {
        spanAttributes.push({
          exception: error instanceof Error ? error.name : typeof error,
        });
      },
      setStatus: (status) => {
        statuses.push(status);
      },
      end: () => {
        endCount++;
      },
    }),
  };

  logDebug(logger, 'bare log call');
  logWarn(logger, 'error-only log call', undefined, new Error('plain failure'));

  const span = startTraceSpan(
    tracer,
    'pg_pubsub.test_helper',
    traceAttributes({
      topic: 'topic-observe',
      omitted: undefined,
    }),
  );
  span.setAttribute('ignored', undefined);
  span.recordError(new Error('typed failure'));
  span.recordError('string failure');
  span.end({ code: 'error', message: 'helper failed' });
  span.end();

  assert.deepEqual(calls[0], ['bare log call']);
  assert.deepEqual(calls[1], ['error-only log call', { 'error.name': 'Error' }]);
  assert.equal(telemetryIncludes(calls, 'plain failure'), false);
  assert.equal(events[0]?.name, 'pg_pubsub.test_helper.start');
  assert.equal(events.filter((event) => event.name === 'pg_pubsub.test_helper.end').length, 1);
  assert.equal(endCount, 1);
  assert.deepEqual(statuses, [{ code: 'error', message: 'helper failed' }]);
  assert.ok(
    spanAttributes.some((attributes) => attributes['error.name'] === 'Error'),
    'Error instances should be represented by safe scalar attributes',
  );
  assert.ok(
    spanAttributes.some((attributes) => attributes['error.name'] === 'string'),
    'non-Error exceptions should be represented by safe scalar attributes',
  );
  assert.equal(
    spanAttributes.some((attributes) => attributes.ignored !== undefined),
    false,
    'undefined attributes should be omitted',
  );
});

test('logger and tracer capture lifecycle without event payload data', async () => {
  const { logger, tracer, logs, traceEvents, spans } = makeCapture();
  const ps = makePubSub(schema, {
    logger,
    tracer,
    listen: false,
    pollIntervalMs: 50,
    cleanupIntervalMs: 0,
  });

  const secretPayload = {
    token: 'secret-value',
    nested: { password: 'do-not-log' },
  };

  try {
    await ps.subscribe('topic-observe', (event, ack) => {
      assert.deepEqual(event.data, secretPayload);
      ack?.();
    });
    await ps.publish('topic-observe', {
      type: 'observe',
      data: secretPayload,
      runId: 'run-observe',
    });
    await ps.flush();

    const history = await ps.getHistory('topic-observe');
    assert.equal(history.length, 1);
  } finally {
    await ps.close();
  }

  const messages = logs.map((entry) => entry.message);
  assert.ok(messages.includes('subscription registered'), 'subscription should be logged');
  assert.ok(messages.includes('published event'), 'publish should be logged');
  assert.ok(messages.includes('delivery acked'), 'ack should be logged');
  assert.ok(messages.includes('history fetched'), 'history fetch should be logged');
  assert.ok(messages.includes('postgres pubsub closed'), 'close should be logged');

  assert.ok(
    spans.some((span) => span.name === 'pg_pubsub.publish' && span.ended),
    'publish span should end',
  );
  assert.ok(
    spans.some(
      (span) =>
        span.name === 'pg_pubsub.delivery' && span.attributes['delivery.settlement'] === 'ack',
    ),
    'delivery span should record ack settlement',
  );
  assert.ok(
    traceEvents.some((event) => event.name === 'pg_pubsub.delivery.acked'),
    'delivery ack trace event should be emitted',
  );

  assert.ok(
    telemetryIncludes({ logs, traceEvents, spans }, 'topic-observe'),
    'topic should be observable',
  );
  assert.equal(
    telemetryIncludes({ logs, traceEvents, spans }, 'secret-value'),
    false,
    'payload token must not appear in logs or traces',
  );
  assert.equal(
    telemetryIncludes({ logs, traceEvents, spans }, 'do-not-log'),
    false,
    'nested payload value must not appear in logs or traces',
  );
});

test('publish failures are logged and traced without exposing payload data', async () => {
  const failureSchema = uniqueSchema();
  const { logger, tracer, logs, spans } = makeCapture();
  const ps = makePubSub(failureSchema, {
    logger,
    tracer,
    listen: false,
    cleanupIntervalMs: 0,
  });
  const circularPayload: Record<string, unknown> = {
    token: 'publish-failure-secret',
  };
  circularPayload.self = circularPayload;

  try {
    await assert.rejects(() =>
      ps.publish('topic-publish-failure', {
        type: 'observe',
        data: circularPayload,
        runId: 'run-publish-failure',
      }),
    );
  } finally {
    await ps.close();
    await dropSchema(failureSchema);
  }

  assert.ok(
    logs.some((entry) => entry.message === 'publish failed'),
    'publish failures should be logged',
  );
  assert.ok(
    spans.some(
      (span) => span.name === 'pg_pubsub.publish' && span.status?.code === 'error' && span.ended,
    ),
    'publish failures should end the publish span as an error',
  );
  assert.equal(
    telemetryIncludes({ logs, spans }, 'publish-failure-secret'),
    false,
    'failed publish telemetry must not include payload values',
  );
});

test('callback errors and group subscriptions use sanitized observability context', async () => {
  const callbackSchema = uniqueSchema();
  const { logger, tracer, logs, traceEvents, spans } = makeCapture();
  const ps = makePubSub(callbackSchema, {
    logger,
    tracer,
    listen: false,
    pollIntervalMs: 20,
    cleanupIntervalMs: 0,
  });

  try {
    let received = 0;
    await ps.subscribe(
      'topic-observe-callback-error',
      async (event, ack) => {
        received++;
        await ack?.();
        if (event.type === 'object-throw') {
          throw event.data;
        }
        throw new Error('callback-error-secret');
      },
      { group: 'raw-group-secret' },
    );
    await ps.publish('topic-observe-callback-error', {
      type: 'object-throw',
      data: { token: 'callback-object-secret' },
      runId: 'run-observe-callback-object',
    });
    await ps.publish('topic-observe-callback-error', {
      type: 'error-throw',
      data: { token: 'callback-payload-secret' },
      runId: 'run-observe-callback-error',
    });
    await ps.flush();

    assert.equal(received, 2);
  } finally {
    await ps.close();
    await dropSchema(callbackSchema);
  }

  const callbackErrorLogs = logs.filter((entry) => entry.message === 'subscriber callback threw');
  assert.equal(callbackErrorLogs.length, 2);
  assert.ok(
    callbackErrorLogs.every(
      (entry) =>
        entry.context.length === 1 &&
        typeof entry.context[0] === 'object' &&
        entry.context[0] !== null &&
        'error.name' in entry.context[0],
    ),
    'callback error logs should include only safe scalar error context',
  );
  assert.equal(telemetryIncludes({ logs, traceEvents, spans }, 'callback-error-secret'), false);
  assert.equal(telemetryIncludes({ logs, traceEvents, spans }, 'callback-object-secret'), false);
  assert.equal(telemetryIncludes({ logs, traceEvents, spans }, 'callback-payload-secret'), false);
  assert.equal(telemetryIncludes({ logs, traceEvents, spans }, 'raw-group-secret'), false);
});

test('nack, drop, and flush error paths emit payload-safe telemetry', async () => {
  const { logger, tracer, logs, traceEvents, spans } = makeCapture();
  const nackSchema = uniqueSchema();
  const nackPs = makePubSub(nackSchema, {
    logger,
    tracer,
    listen: false,
    maxDeliveryAttempts: 2,
    ackDeadlineMs: 40,
    nackDelayMs: 10,
    pollIntervalMs: 20,
    cleanupIntervalMs: 0,
  });

  try {
    let nackAttempts = 0;
    await nackPs.subscribe('topic-observe-nack', (_event, ack, nack) => {
      nackAttempts++;
      if (nackAttempts === 1) {
        nack?.();
      } else {
        ack?.();
      }
    });
    await nackPs.publish('topic-observe-nack', {
      type: 'observe',
      data: { token: 'nack-secret' },
      runId: 'run-observe-nack',
    });
    await nackPs.flush();
    assert.equal(nackAttempts, 2);
  } finally {
    await nackPs.close();
    await dropSchema(nackSchema);
  }

  const dropSchemaName = uniqueSchema();
  const dropPs = makePubSub(dropSchemaName, {
    logger,
    tracer,
    listen: false,
    maxDeliveryAttempts: 1,
    ackDeadlineMs: 40,
    pollIntervalMs: 20,
    cleanupIntervalMs: 0,
    deadLetter: true,
  });

  try {
    await dropPs.subscribe('topic-observe-drop', () => undefined);
    await dropPs.publish('topic-observe-drop', {
      type: 'observe',
      data: { token: 'drop-secret' },
      runId: 'run-observe-drop',
    });
    await waitFor(() => traceEvents.some((event) => event.name === 'pg_pubsub.delivery.dropped'), {
      timeoutMs: 3000,
    });
  } finally {
    await dropPs.close();
    await dropSchema(dropSchemaName);
  }

  const flushSchema = uniqueSchema();
  const flushPs = makePubSub(flushSchema, {
    logger,
    tracer,
    listen: false,
    maxDeliveryAttempts: Number.POSITIVE_INFINITY,
    ackDeadlineMs: 30_000,
    pollIntervalMs: 20,
    cleanupIntervalMs: 0,
  });

  try {
    await flushPs.subscribe('topic-observe-flush-timeout', () => undefined);
    await flushPs.publish('topic-observe-flush-timeout', {
      type: 'observe',
      data: { token: 'flush-secret' },
      runId: 'run-observe-flush',
    });
    await assert.rejects(() => flushPs.flush(), /flush timed out/);
  } finally {
    await flushPs.close();
    await dropSchema(flushSchema);
  }

  assert.ok(
    traceEvents.some((event) => event.name === 'pg_pubsub.delivery.nacked'),
    'nack trace event should be emitted',
  );
  assert.ok(
    traceEvents.some((event) => event.name === 'pg_pubsub.delivery.dropped'),
    'drop trace event should be emitted',
  );
  assert.ok(
    logs.some((entry) => entry.message === 'flush timed out'),
    'flush timeout should be logged',
  );
  assert.ok(
    spans.some((span) => span.name === 'pg_pubsub.flush' && span.status?.code === 'error'),
    'flush timeout should mark the flush span as an error',
  );
  assert.equal(telemetryIncludes({ logs, traceEvents, spans }, 'nack-secret'), false);
  assert.equal(telemetryIncludes({ logs, traceEvents, spans }, 'drop-secret'), false);
  assert.equal(telemetryIncludes({ logs, traceEvents, spans }, 'flush-secret'), false);
});

test('replay emits payload-safe history and settlement telemetry', async () => {
  const replaySchema = uniqueSchema();
  const { logger, tracer, logs, traceEvents, spans } = makeCapture();
  const ps = makePubSub(replaySchema, {
    logger,
    tracer,
    listen: false,
    pollIntervalMs: 20,
    cleanupIntervalMs: 0,
  });

  try {
    await ps.publish('topic-observe-replay', {
      type: 'observe',
      data: { token: 'replay-secret-a' },
      runId: 'run-observe-replay-a',
    });
    await ps.publish('topic-observe-replay', {
      type: 'observe',
      data: { token: 'replay-secret-b' },
      runId: 'run-observe-replay-b',
    });

    const replayedIndexes: Array<number | undefined> = [];
    await ps.subscribeFromOffset('topic-observe-replay', 0, (event) => {
      replayedIndexes.push(event.index);
    });

    assert.deepEqual(replayedIndexes, [0, 1]);
  } finally {
    await ps.close();
    await dropSchema(replaySchema);
  }

  assert.ok(
    spans.some(
      (span) =>
        span.name === 'pg_pubsub.get_history' &&
        span.attributes['history.count'] === 2 &&
        span.ended,
    ),
    'history span should record replay history count',
  );
  assert.ok(
    spans.some(
      (span) =>
        span.name === 'pg_pubsub.subscribe_from_offset' &&
        span.attributes['replayed.count'] === 2 &&
        span.ended,
    ),
    'subscribeFromOffset span should record replayed count',
  );
  assert.ok(
    traceEvents.some((event) => event.name === 'pg_pubsub.replay.settled'),
    'replayed deliveries should emit settlement telemetry',
  );
  assert.equal(telemetryIncludes({ logs, traceEvents, spans }, 'replay-secret-a'), false);
  assert.equal(telemetryIncludes({ logs, traceEvents, spans }, 'replay-secret-b'), false);
});

test('listener wakeup lifecycle emits payload-safe telemetry', async () => {
  const listenerSchema = uniqueSchema();
  const { logger, tracer, logs, traceEvents, spans } = makeCapture();
  const subscriber = makePubSub(listenerSchema, {
    logger,
    tracer,
    listen: true,
    pollIntervalMs: 5000,
    cleanupIntervalMs: 0,
  });
  const publisher = makePubSub(listenerSchema, {
    listen: false,
    cleanupIntervalMs: 0,
  });
  const callback = (_event: unknown, ack?: () => Promise<void>) => {
    received++;
    ack?.();
  };
  let received = 0;

  try {
    await subscriber.subscribe('topic-observe-listen', callback);
    await publisher.publish('topic-observe-listen', {
      type: 'observe',
      data: { token: 'listener-secret' },
      runId: 'run-observe-listener',
    });
    await waitFor(() => received === 1, { timeoutMs: 3000 });
    await subscriber.unsubscribe('topic-observe-listen', callback);
  } finally {
    await publisher.close();
    await subscriber.close();
    await dropSchema(listenerSchema);
  }

  assert.ok(
    spans.some((span) => span.name === 'pg_pubsub.listener.connect' && span.ended),
    'listener connect span should end',
  );
  assert.ok(
    spans.some((span) => span.name === 'pg_pubsub.listener.close' && span.ended),
    'listener close span should end',
  );
  assert.ok(
    traceEvents.some((event) => event.name === 'pg_pubsub.listener.handler_registered'),
    'listener handler registration should be traced',
  );
  assert.ok(
    traceEvents.some((event) => event.name === 'pg_pubsub.listener.notification'),
    'listener notification should be traced',
  );
  assert.ok(
    traceEvents.some((event) => event.name === 'pg_pubsub.listener.handler_unregistered'),
    'listener handler unregistration should be traced',
  );
  assert.equal(telemetryIncludes({ logs, traceEvents, spans }, 'listener-secret'), false);
});

test('maintenance lifecycle emits payload-safe telemetry', async () => {
  const maintenanceSchema = uniqueSchema();
  const { logger, tracer, logs, traceEvents, spans } = makeCapture();
  const ps = makePubSub(maintenanceSchema, {
    logger,
    tracer,
    listen: false,
    cleanupIntervalMs: 50,
    maxEventsPerTopic: 1,
  });

  try {
    await ps.publish('topic-observe-maintenance', {
      type: 'observe',
      data: { token: 'maintenance-secret-a' },
      runId: 'run-observe-maintenance-a',
    });
    await ps.publish('topic-observe-maintenance', {
      type: 'observe',
      data: { token: 'maintenance-secret-b' },
      runId: 'run-observe-maintenance-b',
    });
    await waitFor(
      () => traceEvents.some((event) => event.name === 'pg_pubsub.maintenance.started'),
      { timeoutMs: 1000 },
    );
    await waitFor(
      () => spans.some((span) => span.name === 'pg_pubsub.maintenance.cycle' && span.ended),
      { timeoutMs: 3000 },
    );
  } finally {
    await ps.close();
    await dropSchema(maintenanceSchema);
  }

  const maintenanceSpan = spans.find(
    (span) => span.name === 'pg_pubsub.maintenance.cycle' && span.ended,
  );
  assert.ok(maintenanceSpan, 'maintenance cycle span should end');
  assert.equal(typeof maintenanceSpan.attributes['heartbeat.count'], 'number');
  assert.equal(typeof maintenanceSpan.attributes['subscription.pruned_count'], 'number');
  assert.equal(typeof maintenanceSpan.attributes['event.trimmed_count'], 'number');
  assert.equal(telemetryIncludes({ logs, traceEvents, spans }, 'maintenance-secret-a'), false);
  assert.equal(telemetryIncludes({ logs, traceEvents, spans }, 'maintenance-secret-b'), false);
});

test('throwing logger and tracer sinks do not affect pubsub behavior', async () => {
  const throwingLogger: PubSubLogger = {
    debug: () => {
      throw new Error('debug sink failed');
    },
    warn: () => {
      throw new Error('warn sink failed');
    },
    error: () => {
      throw new Error('error sink failed');
    },
  };
  const throwingSpan: PubSubTraceSpan = {
    setAttribute: () => {
      throw new Error('setAttribute failed');
    },
    setAttributes: () => {
      throw new Error('setAttributes failed');
    },
    recordException: () => {
      throw new Error('recordException failed');
    },
    setStatus: () => {
      throw new Error('setStatus failed');
    },
    end: () => {
      throw new Error('end failed');
    },
  };
  const throwingTracer: PubSubTracer = {
    event: () => {
      throw new Error('trace event failed');
    },
    startSpan: () => throwingSpan,
  };

  const ps = makePubSub(schema, {
    logger: throwingLogger,
    tracer: throwingTracer,
    listen: false,
    maxDeliveryAttempts: 0,
    pollIntervalMs: 50,
    cleanupIntervalMs: 0,
  });

  try {
    let received = 0;
    await ps.subscribe('topic-throwing-observability', (_event, ack, nack) => {
      received++;
      if (received === 1) {
        nack?.();
      } else {
        ack?.();
      }
    });
    await ps.publish('topic-throwing-observability', {
      type: 'observe',
      data: { token: 'still-not-logged' },
      runId: 'run-throwing',
    });
    await ps.flush();
    assert.equal(received, 2);
  } finally {
    await ps.close();
  }

  const listenerSchema = uniqueSchema();
  const listenerSubscriber = makePubSub(listenerSchema, {
    logger: throwingLogger,
    tracer: throwingTracer,
    listen: true,
    pollIntervalMs: 5000,
    cleanupIntervalMs: 0,
  });
  const listenerPublisher = makePubSub(listenerSchema, {
    logger: throwingLogger,
    tracer: throwingTracer,
    listen: false,
    cleanupIntervalMs: 0,
  });

  try {
    let listenerReceived = 0;
    await listenerSubscriber.subscribe('topic-throwing-listener', (_event, ack) => {
      listenerReceived++;
      ack?.();
    });
    await listenerPublisher.publish('topic-throwing-listener', {
      type: 'observe',
      data: { token: 'throwing-listener-secret' },
      runId: 'run-throwing-listener',
    });
    await waitFor(() => listenerReceived === 1, { timeoutMs: 3000 });
  } finally {
    await listenerPublisher.close();
    await listenerSubscriber.close();
    await dropSchema(listenerSchema);
  }

  const startThrowSchema = uniqueSchema();
  const startThrowingTracer: PubSubTracer = {
    event: () => {
      throw new Error('trace event failed');
    },
    startSpan: () => {
      throw new Error('startSpan failed');
    },
  };
  const startThrowPs = makePubSub(startThrowSchema, {
    logger: throwingLogger,
    tracer: startThrowingTracer,
    cleanupIntervalMs: 0,
  });

  try {
    await assert.doesNotReject(() => startThrowPs.migrate());
  } finally {
    await startThrowPs.close();
    await dropSchema(startThrowSchema);
  }

  const lifecycleThrowSchema = uniqueSchema();
  const lifecycleThrowPs = makePubSub(lifecycleThrowSchema, {
    logger: throwingLogger,
    tracer: throwingTracer,
    cleanupIntervalMs: 0,
  });
  const mastra = new Mastra({ pubsub: lifecycleThrowPs, logger: false, workers: false });

  try {
    assert.doesNotThrow(() => lifecycleThrowPs.wireMastraLifecycle(mastra));
    await assert.doesNotReject(() => mastra.startWorkers());
    await assert.doesNotReject(() => mastra.shutdown());
  } finally {
    await dropSchema(lifecycleThrowSchema);
  }
});
