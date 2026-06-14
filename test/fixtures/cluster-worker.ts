import type { Event } from '@mastra/core/events';
import { Mastra } from '@mastra/core/mastra';
import { PostgresPubSub } from '../../src/index.ts';

const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5544/mastra_pubsub';

type Command =
  | {
      id: string;
      type: 'init';
      schema: string;
      workerId: string;
    }
  | {
      id: string;
      type: 'subscribe';
      topic: string;
      group?: string;
    }
  | {
      id: string;
      type: 'publish';
      topic: string;
      count: number;
      eventType: string;
    }
  | {
      id: string;
      type: 'history';
      topic: string;
    }
  | {
      id: string;
      type: 'close';
    };

interface DeliveryMessage {
  readonly type: 'delivery';
  readonly workerId: string;
  readonly topic: string;
  readonly index: number;
  readonly eventType: string;
  readonly runId: string;
}

let workerId = 'uninitialized';
let pubsub: PostgresPubSub | undefined;
let mastra: Mastra | undefined;

function send(message: unknown): void {
  process.send?.(message);
}

function sendReply(id: string, payload: Record<string, unknown> = {}): void {
  send({ type: 'reply', id, ok: true, ...payload });
}

function sendError(id: string, error: unknown): void {
  const reason =
    error instanceof Error
      ? { name: error.name, message: error.message, stack: error.stack }
      : { name: typeof error, message: String(error) };
  send({ type: 'reply', id, ok: false, error: reason });
}

function asObject(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null
    ? (value as Record<string, unknown>)
    : undefined;
}

function isCommand(value: unknown): value is Command {
  const message = asObject(value);
  if (!message || typeof message.id !== 'string' || typeof message.type !== 'string') {
    return false;
  }
  switch (message.type) {
    case 'init':
      return typeof message.schema === 'string' && typeof message.workerId === 'string';
    case 'subscribe':
      return (
        typeof message.topic === 'string' &&
        (message.group === undefined || typeof message.group === 'string')
      );
    case 'publish':
      return (
        typeof message.topic === 'string' &&
        typeof message.count === 'number' &&
        typeof message.eventType === 'string'
      );
    case 'history':
      return typeof message.topic === 'string';
    case 'close':
      return true;
    default:
      return false;
  }
}

function requireMastra(): Mastra {
  if (!mastra) {
    throw new Error('Worker has not been initialized');
  }
  return mastra;
}

function deliveryMessage(topic: string, event: Event): DeliveryMessage | undefined {
  if (typeof event.index !== 'number') {
    return undefined;
  }
  return {
    type: 'delivery',
    workerId,
    topic,
    index: event.index,
    eventType: event.type,
    runId: event.runId,
  };
}

async function handleCommand(command: Command): Promise<void> {
  switch (command.type) {
    case 'init': {
      workerId = command.workerId;
      pubsub = new PostgresPubSub({
        connectionString: DATABASE_URL,
        schema: command.schema,
        pollIntervalMs: 25,
        ackDeadlineMs: 500,
        cleanupIntervalMs: 0,
      });
      mastra = new Mastra({ pubsub, logger: false });
      pubsub.wireMastraLifecycle(mastra);
      await mastra.startWorkers();
      sendReply(command.id, {
        workerId,
        mastraPubSubConfigured: Boolean(requireMastra().pubsub),
      });
      return;
    }
    case 'subscribe': {
      await requireMastra().pubsub.subscribe(
        command.topic,
        (event, ack) => {
          const message = deliveryMessage(command.topic, event);
          if (message) {
            send(message);
          }
          ack?.();
        },
        command.group ? { group: command.group } : undefined,
      );
      sendReply(command.id, { topic: command.topic, group: command.group ?? null });
      return;
    }
    case 'publish': {
      for (let i = 0; i < command.count; i++) {
        await requireMastra().pubsub.publish(command.topic, {
          type: command.eventType,
          data: { index: i, workerId },
          runId: `${command.topic}-${workerId}-${i}`,
        });
      }
      await requireMastra().pubsub.flush();
      sendReply(command.id, { topic: command.topic, published: command.count });
      return;
    }
    case 'history': {
      const history = await requireMastra().pubsub.getHistory(command.topic);
      sendReply(command.id, {
        topic: command.topic,
        indexes: history.map((event) => event.index),
        eventTypes: history.map((event) => event.type),
        runIds: history.map((event) => event.runId),
      });
      return;
    }
    case 'close': {
      await requireMastra().shutdown();
      pubsub = undefined;
      mastra = undefined;
      sendReply(command.id);
      process.disconnect?.();
      setImmediate(() => process.exit(0));
    }
  }
}

process.on('message', (message: unknown) => {
  if (!isCommand(message)) {
    send({ type: 'worker-error', workerId, error: 'Invalid command' });
    return;
  }
  handleCommand(message).catch((error: unknown) => sendError(message.id, error));
});

send({ type: 'online' });
