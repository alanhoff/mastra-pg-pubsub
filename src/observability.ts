import type { IMastraLogger } from '@mastra/core/logger';
import {
  type AnySpan,
  executeWithContext,
  resolveCurrentSpan,
  SpanType,
} from '@mastra/core/observability';

type LogLevel = 'debug' | 'warn' | 'error';

export type PubSubAttributeValue = string | number | boolean | null;

export type PubSubAttributes = Record<string, PubSubAttributeValue>;

type AttributeSource = Record<string, PubSubAttributeValue | undefined>;

type ConfiguredLogger = IMastraLogger | false | undefined;

export interface SpanStatus {
  code: 'ok' | 'error';
  message?: string;
}

export interface ActiveObservabilitySpan {
  setAttribute(name: string, value: PubSubAttributeValue | undefined): void;
  recordError(error: unknown): void;
  run<T>(fn: () => Promise<T>): Promise<T>;
  end(status?: SpanStatus): void;
}

const noopSpan: ActiveObservabilitySpan = {
  setAttribute: () => undefined,
  recordError: () => undefined,
  run: async (fn) => fn(),
  end: () => undefined,
};

export function traceAttributes(source: AttributeSource = {}): PubSubAttributes {
  const attributes: PubSubAttributes = {};
  for (const [key, value] of Object.entries(source)) {
    if (value !== undefined) {
      attributes[key] = value;
    }
  }
  return attributes;
}

export function logDebug(
  logger: ConfiguredLogger,
  message: string,
  context?: PubSubAttributes,
): void {
  log(logger, 'debug', message, context);
}

export function logWarn(
  logger: ConfiguredLogger,
  message: string,
  context?: PubSubAttributes,
  error?: unknown,
): void {
  log(logger, 'warn', message, context, error);
}

export function logError(
  logger: ConfiguredLogger,
  message: string,
  context?: PubSubAttributes,
  error?: unknown,
): void {
  log(logger, 'error', message, context, error);
}

export function observeEvent(name: string, attributes: PubSubAttributes = {}): void {
  const parent = currentSpan();
  if (!parent) {
    return;
  }
  safeCall(() => {
    parent.createEventSpan({
      type: SpanType.GENERIC,
      name,
      attributes,
      output: attributes,
    });
  });
}

export function startObservabilitySpan(
  name: string,
  attributes: PubSubAttributes = {},
): ActiveObservabilitySpan {
  const parent = currentSpan();
  if (!parent) {
    return noopSpan;
  }

  let child: AnySpan | undefined;
  safeCall(() => {
    child = parent.createChildSpan({
      type: SpanType.GENERIC,
      name,
      attributes,
    });
  });
  if (!child) {
    return noopSpan;
  }
  return new SafeObservabilitySpan(child, attributes);
}

function log(
  configuredLogger: ConfiguredLogger,
  level: LogLevel,
  message: string,
  context?: PubSubAttributes,
  error?: unknown,
): void {
  if (configuredLogger === false) {
    return;
  }

  const logger = configuredLogger ?? currentSpanLogger();
  if (!logger) {
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
      logger[level](message, safeContext);
    } else if (error !== undefined) {
      logger[level](message, errorAttributes(error));
    } else {
      logger[level](message);
    }
  });
}

function currentSpanLogger(): IMastraLogger | undefined {
  const span = currentSpan();
  if (!span) {
    return undefined;
  }

  let logger: IMastraLogger | undefined;
  safeCall(() => {
    logger = span.observabilityInstance?.getLogger?.();
  });
  return logger;
}

function currentSpan(): AnySpan | undefined {
  let span: AnySpan | undefined;
  safeCall(() => {
    span = resolveCurrentSpan();
  });
  return span;
}

function safeCall(fn: () => void): void {
  try {
    fn();
  } catch (error) {
    void error;
    // Observability must never change PubSub behavior.
  }
}

function errorAttributes(error: unknown): PubSubAttributes {
  if (error instanceof Error) {
    return traceAttributes({
      'error.name': error.name,
    });
  }
  return traceAttributes({
    'error.name': typeof error,
  });
}

function safeSpanError(error: unknown): Error {
  const name = error instanceof Error ? error.name : typeof error;
  const safeError = new Error(name);
  safeError.name = name;
  return safeError;
}

class SafeObservabilitySpan implements ActiveObservabilitySpan {
  readonly #span: AnySpan;
  readonly #attributes: PubSubAttributes;
  readonly #startedAt = Date.now();
  #ended = false;

  constructor(span: AnySpan, attributes: PubSubAttributes) {
    this.#span = span;
    this.#attributes = { ...attributes };
  }

  setAttribute(name: string, value: PubSubAttributeValue | undefined): void {
    if (value === undefined) {
      return;
    }
    this.#attributes[name] = value;
    safeCall(() => {
      this.#span.update({ attributes: { [name]: value } });
    });
  }

  recordError(error: unknown): void {
    const attributes = errorAttributes(error);
    for (const [key, value] of Object.entries(attributes)) {
      this.setAttribute(key, value);
    }
    safeCall(() => {
      this.#span.error({
        error: safeSpanError(error),
        attributes,
        endSpan: false,
      });
    });
  }

  async run<T>(fn: () => Promise<T>): Promise<T> {
    let callbackCompleted = false;
    let callbackError: unknown;
    let callbackResult: T | undefined;

    try {
      return await executeWithContext({
        span: this.#span,
        fn: async () => {
          try {
            callbackResult = await fn();
            callbackCompleted = true;
            return callbackResult;
          } catch (error) {
            callbackError = error;
            throw error;
          }
        },
      });
    } catch (error) {
      if (callbackError === error) {
        throw error;
      }
      if (callbackCompleted) {
        return callbackResult as T;
      }
      return fn();
    }
  }

  end(status: SpanStatus = { code: 'ok' }): void {
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
      this.#span.end({ attributes: this.#attributes });
    });
  }
}
