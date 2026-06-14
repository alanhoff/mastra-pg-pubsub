import type {
  PubSubLogger,
  PubSubTraceAttributes,
  PubSubTraceAttributeValue,
  PubSubTracer,
  PubSubTraceSpan,
  PubSubTraceStatus,
} from './types.ts';

type LogLevel = 'debug' | 'warn' | 'error';

type TraceAttributeSource = Record<string, PubSubTraceAttributeValue | undefined>;

export interface ActiveTraceSpan {
  setAttribute(name: string, value: PubSubTraceAttributeValue | undefined): void;
  recordError(error: unknown): void;
  end(status?: PubSubTraceStatus): void;
}

const noopSpan: ActiveTraceSpan = {
  setAttribute: () => undefined,
  recordError: () => undefined,
  end: () => undefined,
};

export function traceAttributes(source: TraceAttributeSource = {}): PubSubTraceAttributes {
  const attributes: PubSubTraceAttributes = {};
  for (const [key, value] of Object.entries(source)) {
    if (value !== undefined) {
      attributes[key] = value;
    }
  }
  return attributes;
}

export function logDebug(
  logger: PubSubLogger,
  message: string,
  context?: PubSubTraceAttributes,
): void {
  log(logger, 'debug', message, context);
}

export function logWarn(
  logger: PubSubLogger,
  message: string,
  context?: PubSubTraceAttributes,
  error?: unknown,
): void {
  log(logger, 'warn', message, context, error);
}

export function logError(
  logger: PubSubLogger,
  message: string,
  context?: PubSubTraceAttributes,
  error?: unknown,
): void {
  log(logger, 'error', message, context, error);
}

export function traceEvent(
  tracer: PubSubTracer,
  name: string,
  attributes: PubSubTraceAttributes = {},
): void {
  safeCall(() => tracer.event?.(name, attributes));
}

export function startTraceSpan(
  tracer: PubSubTracer,
  name: string,
  attributes: PubSubTraceAttributes = {},
): ActiveTraceSpan {
  if (!tracer.startSpan && !tracer.event) {
    return noopSpan;
  }
  return new SafeTraceSpan(tracer, name, attributes);
}

function log(
  logger: PubSubLogger,
  level: LogLevel,
  message: string,
  context?: PubSubTraceAttributes,
  error?: unknown,
): void {
  const fn = logger[level];
  if (!fn) {
    return;
  }
  safeCall(() => {
    const safeContext =
      error === undefined
        ? context
        : {
            ...(context ?? {}),
            ...errorAttributes(error),
          };
    if (safeContext && Object.keys(safeContext).length > 0) {
      fn(message, safeContext);
    } else if (error !== undefined) {
      fn(message, errorAttributes(error));
    } else {
      fn(message);
    }
  });
}

function safeCall(fn: () => void): void {
  try {
    fn();
  } catch {
    // Observability sinks must never change PubSub behavior.
  }
}

function errorAttributes(error: unknown): PubSubTraceAttributes {
  if (error instanceof Error) {
    return traceAttributes({
      'error.name': error.name,
    });
  }
  return traceAttributes({
    'error.name': typeof error,
  });
}

function safeTraceException(error: unknown): Error {
  const name = error instanceof Error ? error.name : typeof error;
  const safeError = new Error(name);
  safeError.name = name;
  return safeError;
}

class SafeTraceSpan implements ActiveTraceSpan {
  readonly #tracer: PubSubTracer;
  readonly #name: string;
  readonly #attributes: PubSubTraceAttributes;
  readonly #startedAt = Date.now();
  readonly #span: PubSubTraceSpan | undefined;
  #ended = false;

  constructor(tracer: PubSubTracer, name: string, attributes: PubSubTraceAttributes) {
    this.#tracer = tracer;
    this.#name = name;
    this.#attributes = { ...attributes };
    traceEvent(this.#tracer, `${this.#name}.start`, this.#attributes);
    let span: PubSubTraceSpan | undefined;
    safeCall(() => {
      span = this.#tracer.startSpan?.(this.#name, this.#attributes);
    });
    this.#span = span;
  }

  setAttribute(name: string, value: PubSubTraceAttributeValue | undefined): void {
    if (value === undefined) {
      return;
    }
    this.#attributes[name] = value;
    safeCall(() => {
      this.#span?.setAttribute?.(name, value);
    });
  }

  recordError(error: unknown): void {
    const attributes = errorAttributes(error);
    for (const [key, value] of Object.entries(attributes)) {
      this.setAttribute(key, value);
    }
    safeCall(() => {
      this.#span?.recordException?.(safeTraceException(error));
    });
    traceEvent(this.#tracer, `${this.#name}.error`, {
      ...this.#attributes,
      ...attributes,
    });
  }

  end(status: PubSubTraceStatus = { code: 'ok' }): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    this.setAttribute('status', status.code);
    if (status.message) {
      this.setAttribute('status.message', status.message);
    }
    this.setAttribute('duration.ms', Date.now() - this.#startedAt);
    safeCall(() => {
      this.#span?.setAttributes?.(this.#attributes);
      this.#span?.setStatus?.(status);
      this.#span?.end?.();
    });
    traceEvent(this.#tracer, `${this.#name}.end`, this.#attributes);
  }
}
