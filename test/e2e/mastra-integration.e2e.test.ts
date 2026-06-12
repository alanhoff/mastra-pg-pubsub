import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { after, before, test } from 'node:test';
import { Agent } from '@mastra/core/agent';
import {
  AGENT_STREAM_TOPIC,
  AgentStreamEventTypes,
  createDurableAgent,
} from '@mastra/core/agent/durable';
import type { Event } from '@mastra/core/events';
import { Mastra } from '@mastra/core/mastra';
import { Memory } from '@mastra/memory';
import { PostgresStore } from '@mastra/pg';
import { DATABASE_URL, dropSchema, makePubSub, sleep, uniqueSchema, waitFor } from './helpers.ts';

const pubsubSchema = uniqueSchema('e2e_mastra_pubsub');
const storageSchema = uniqueSchema('e2e_mastra_store');
const pubsub = makePubSub(pubsubSchema, {
  ackDeadlineMs: 1_000,
  pollIntervalMs: 25,
});
const storage = new PostgresStore({
  id: 'pg-pubsub-e2e-storage',
  connectionString: DATABASE_URL,
  schemaName: storageSchema,
});
const memory = new Memory({ storage });
const baseAgent = new Agent({
  id: 'pg-pubsub-e2e-agent',
  name: 'Postgres PubSub E2E Agent',
  instructions: 'Reply with exactly the single token OK. Do not add punctuation.',
  model: 'openai/gpt-4o-mini',
  memory,
});
const durableAgent = createDurableAgent({
  agent: baseAgent,
  pubsub,
  cache: false,
  maxSteps: 1,
});
const mastra = new Mastra({
  storage,
  pubsub,
  agents: {
    durable: durableAgent,
    base: baseAgent,
  },
  logger: false,
});

before(async () => {
  assert.ok(
    process.env.OPENAI_API_KEY,
    'OPENAI_API_KEY must be set for the real Mastra/OpenAI e2e test',
  );
  await pubsub.migrate();
  await storage.init();
});

after(async () => {
  await Promise.allSettled([pubsub.close(), storage.close()]);
  await Promise.allSettled([dropSchema(pubsubSchema), dropSchema(storageSchema)]);
});

test('real Mastra durable agent streams through PostgresPubSub and persists memory', async () => {
  const runId = `run-${randomUUID()}`;
  const threadId = `thread-${randomUUID()}`;
  const resourceId = `resource-${randomUUID()}`;
  const topic = AGENT_STREAM_TOPIC(runId);
  const observedEvents: Event[] = [];

  const observeDurableEvent = (event: Event, ack?: () => void) => {
    observedEvents.push(event);
    ack?.();
  };

  await mastra.pubsub.subscribe(topic, observeDurableEvent);

  assert.equal(mastra.getAgent('durable').id, durableAgent.id);

  const result = await durableAgent.stream('Answer with exactly OK.', {
    runId,
    maxSteps: 1,
    memory: {
      thread: threadId,
      resource: resourceId,
    },
    modelSettings: {
      maxOutputTokens: 8,
      temperature: 0,
    },
  });

  const text = await result.output.text;
  assert.match(text.trim(), /^OK\.?$/i);

  await waitFor(
    () =>
      observedEvents.some((event) => event.type === AgentStreamEventTypes.CHUNK) &&
      observedEvents.some((event) => event.type === AgentStreamEventTypes.FINISH),
    { timeoutMs: 60_000, intervalMs: 100 },
  );
  result.cleanup();
  await pubsub.flush();

  const history = await pubsub.getHistory(topic);
  const eventTypes = new Set(history.map((event) => event.type));
  assert.ok(eventTypes.has(AgentStreamEventTypes.CHUNK), 'durable chunk should be persisted');
  assert.ok(eventTypes.has(AgentStreamEventTypes.FINISH), 'durable finish should be persisted');
  assert.ok(
    history.every((event) => event.runId === runId),
    'all durable events use the run id',
  );

  const thread = await memory.getThreadById({ threadId, resourceId });
  assert.ok(thread, 'Mastra memory should persist the conversation thread in Postgres');
  assert.equal(thread.id, threadId);

  const recalled = await memory.recall({ threadId, resourceId });
  assert.ok(recalled.total >= 2, 'Mastra memory should persist user and assistant messages');

  await mastra.pubsub.unsubscribe(topic, observeDurableEvent);
  await sleep(250);
});
