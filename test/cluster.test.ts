import assert from 'node:assert/strict';
import { type ChildProcess, fork } from 'node:child_process';
import { after, test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { dropSchema, uniqueSchema, waitFor } from './helpers.ts';

interface ReplyMessage {
  readonly type: 'reply';
  readonly id: string;
  readonly ok: boolean;
  readonly error?: {
    readonly name: string;
    readonly message: string;
    readonly stack?: string;
  };
  readonly [key: string]: unknown;
}

interface DeliveryMessage {
  readonly type: 'delivery';
  readonly workerId: string;
  readonly topic: string;
  readonly index: number;
  readonly eventType: string;
  readonly runId: string;
}

interface PendingReply {
  readonly resolve: (value: ReplyMessage) => void;
  readonly reject: (reason: Error) => void;
  readonly timeout: NodeJS.Timeout;
}

const schema = uniqueSchema();
const workers: ClusterWorker[] = [];

function workerEnv(): NodeJS.ProcessEnv {
  return {
    DATABASE_URL:
      process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5544/mastra_pubsub',
    NODE_OPTIONS: process.env.NODE_OPTIONS,
    PATH: process.env.PATH,
    TMPDIR: process.env.TMPDIR,
    TEMP: process.env.TEMP,
    TMP: process.env.TMP,
  };
}

after(async () => {
  await Promise.allSettled(workers.map((worker) => worker.close()));
  await dropSchema(schema);
});

class ClusterWorker {
  readonly id: string;
  readonly deliveries: DeliveryMessage[] = [];
  readonly #child: ChildProcess;
  readonly #pending = new Map<string, PendingReply>();
  readonly #closed: Promise<void>;
  #sequence = 0;
  #stdout = '';
  #stderr = '';
  #closing = false;

  constructor(id: string) {
    this.id = id;
    const workerPath = fileURLToPath(new URL('./fixtures/cluster-worker.ts', import.meta.url));
    this.#child = fork(workerPath, [], {
      cwd: fileURLToPath(new URL('..', import.meta.url)),
      env: workerEnv(),
      execArgv: [],
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    });
    this.#child.stdout?.setEncoding('utf8');
    this.#child.stderr?.setEncoding('utf8');
    this.#child.stdout?.on('data', (chunk: string) => {
      this.#stdout += chunk;
    });
    this.#child.stderr?.on('data', (chunk: string) => {
      this.#stderr += chunk;
    });
    this.#child.on('message', (message: unknown) => this.#handleMessage(message));
    this.#child.on('error', (error) => this.#rejectPending(error));
    this.#closed = new Promise((resolve) => {
      this.#child.once('close', (code, signal) => {
        if (!this.#closing && code !== 0) {
          this.#rejectPending(
            new Error(
              `cluster worker ${this.id} exited with code ${code ?? 'null'} signal ${
                signal ?? 'null'
              }\nstdout:\n${this.#stdout}\nstderr:\n${this.#stderr}`,
            ),
          );
        }
        resolve();
      });
    });
  }

  async init(): Promise<void> {
    const reply = await this.#send({ type: 'init', schema, workerId: this.id });
    assert.equal(
      reply.mastraPubSubConfigured,
      true,
      `${this.id} should wire PubSub through Mastra`,
    );
  }

  async subscribe(topic: string, group?: string): Promise<void> {
    await this.#send(group ? { type: 'subscribe', topic, group } : { type: 'subscribe', topic });
  }

  async publish(topic: string, count: number, eventType: string): Promise<void> {
    const reply = await this.#send({ type: 'publish', topic, count, eventType }, 15_000);
    assert.equal(reply.published, count);
  }

  async history(topic: string): Promise<ReplyMessage> {
    return this.#send({ type: 'history', topic });
  }

  async close(): Promise<void> {
    if (this.#closing) {
      return this.#closed;
    }
    this.#closing = true;
    try {
      if (this.#child.connected) {
        await this.#send({ type: 'close' }, 5_000);
      }
    } catch {
      // Teardown should still kill the child and continue cleaning the schema.
    } finally {
      if (this.#child.exitCode === null && this.#child.signalCode === null) {
        this.#child.kill('SIGTERM');
      }
    }
    await this.#closed;
  }

  #deliveriesFor(topic: string): DeliveryMessage[] {
    return this.deliveries.filter((delivery) => delivery.topic === topic);
  }

  indexesFor(topic: string): number[] {
    return this.#deliveriesFor(topic).map((delivery) => delivery.index);
  }

  #send(command: Record<string, unknown>, timeoutMs = 10_000): Promise<ReplyMessage> {
    const id = `${this.id}-${++this.#sequence}`;
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.#pending.delete(id);
        reject(
          new Error(
            `cluster worker ${this.id} timed out waiting for ${String(
              command.type,
            )}\nstdout:\n${this.#stdout}\nstderr:\n${this.#stderr}`,
          ),
        );
      }, timeoutMs);
      this.#pending.set(id, { resolve, reject, timeout });
      this.#child.send?.({ ...command, id }, (error) => {
        if (!error) {
          return;
        }
        clearTimeout(timeout);
        this.#pending.delete(id);
        reject(error);
      });
    });
  }

  #handleMessage(message: unknown): void {
    if (isDeliveryMessage(message)) {
      this.deliveries.push(message);
      return;
    }
    if (!isReplyMessage(message)) {
      return;
    }
    const pending = this.#pending.get(message.id);
    if (!pending) {
      return;
    }
    clearTimeout(pending.timeout);
    this.#pending.delete(message.id);
    if (message.ok) {
      pending.resolve(message);
      return;
    }
    const details = message.error
      ? `${message.error.name}: ${message.error.message}\n${message.error.stack ?? ''}`
      : 'Unknown worker error';
    pending.reject(new Error(`cluster worker ${this.id} failed command ${message.id}: ${details}`));
  }

  #rejectPending(error: Error): void {
    for (const [id, pending] of this.#pending) {
      clearTimeout(pending.timeout);
      pending.reject(error);
      this.#pending.delete(id);
    }
  }
}

function isReplyMessage(value: unknown): value is ReplyMessage {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as { type?: unknown }).type === 'reply' &&
    typeof (value as { id?: unknown }).id === 'string' &&
    typeof (value as { ok?: unknown }).ok === 'boolean'
  );
}

function isDeliveryMessage(value: unknown): value is DeliveryMessage {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as { type?: unknown }).type === 'delivery' &&
    typeof (value as { workerId?: unknown }).workerId === 'string' &&
    typeof (value as { topic?: unknown }).topic === 'string' &&
    typeof (value as { index?: unknown }).index === 'number' &&
    typeof (value as { eventType?: unknown }).eventType === 'string' &&
    typeof (value as { runId?: unknown }).runId === 'string'
  );
}

async function startWorker(id: string): Promise<ClusterWorker> {
  const worker = new ClusterWorker(id);
  workers.push(worker);
  await worker.init();
  return worker;
}

function sortedIndexes(values: readonly number[]): number[] {
  return [...values].sort((a, b) => a - b);
}

function expectedIndexes(count: number): number[] {
  return Array.from({ length: count }, (_, index) => index);
}

test('clustered Mastra processes share fan-out, group delivery, and history', async () => {
  const subscriberA = await startWorker('cluster-a');
  const subscriberB = await startWorker('cluster-b');
  const publisher = await startWorker('cluster-publisher');

  const fanoutTopic = 'cluster-fanout';
  const fanoutCount = 4;
  await Promise.all([subscriberA.subscribe(fanoutTopic), subscriberB.subscribe(fanoutTopic)]);

  await publisher.publish(fanoutTopic, fanoutCount, 'cluster.broadcast');

  await waitFor(
    () =>
      subscriberA.indexesFor(fanoutTopic).length === fanoutCount &&
      subscriberB.indexesFor(fanoutTopic).length === fanoutCount,
    { timeoutMs: 10_000, intervalMs: 25 },
  );

  assert.deepEqual(
    sortedIndexes(subscriberA.indexesFor(fanoutTopic)),
    expectedIndexes(fanoutCount),
  );
  assert.deepEqual(
    sortedIndexes(subscriberB.indexesFor(fanoutTopic)),
    expectedIndexes(fanoutCount),
  );

  const groupTopic = 'cluster-group';
  const groupCount = 10;
  await Promise.all([
    subscriberA.subscribe(groupTopic, 'cluster-workers'),
    subscriberB.subscribe(groupTopic, 'cluster-workers'),
  ]);

  await publisher.publish(groupTopic, groupCount, 'cluster.work');

  await waitFor(
    () =>
      subscriberA.indexesFor(groupTopic).length + subscriberB.indexesFor(groupTopic).length >=
      groupCount,
    { timeoutMs: 10_000, intervalMs: 25 },
  );

  const groupIndexes = [
    ...subscriberA.indexesFor(groupTopic),
    ...subscriberB.indexesFor(groupTopic),
  ];
  assert.deepEqual(sortedIndexes(groupIndexes), expectedIndexes(groupCount));
  assert.equal(
    new Set(groupIndexes).size,
    groupCount,
    'group deliveries should not duplicate indexes',
  );

  const historyTopic = 'cluster-history';
  const historyCount = 3;
  await publisher.publish(historyTopic, historyCount, 'cluster.history');

  const history = await subscriberA.history(historyTopic);
  assert.deepEqual(history.indexes, expectedIndexes(historyCount));
  assert.deepEqual(
    history.eventTypes,
    Array.from({ length: historyCount }, () => 'cluster.history'),
  );
});
