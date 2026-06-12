# Contributing

Thanks for helping improve `mastra-pg-pubsub`.

## Setup

```sh
npm install
npm run db:up
```

The default test database is `postgres://postgres:postgres@localhost:5544/mastra_pubsub`.

## Quality gates

Run these before opening a PR:

```sh
npm run typecheck
npm run lint
npm test
npm run test:coverage
npm run build
```

Run real e2e tests when you have an OpenAI key available. This suite validates the durable-agent stream API for the locked Mastra version, so update it alongside Mastra upgrades:

```sh
# .env may contain OPENAI_API_KEY; never commit it
npm run test:e2e
```

## Development notes

- Keep runtime dependencies minimal; do not add new dependencies without a clear reason.
- Preserve the Mastra `PubSub` contract and strict TypeScript settings.
- Prefer deterministic `waitFor` polling in tests over fixed sleeps.
- Never print or commit `.env`, API keys, or local `.research/` notes.
