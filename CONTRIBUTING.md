# Contributing

## Setup

```bash
npm install
cp .env.example .env   # fill in MONGODB_URI (same DB as the mqttcloud.ir Next.js app)
npm run dev             # nodemon, restarts on file changes
```

## Before opening a PR

- Run `npm start` locally and confirm the broker connects to MongoDB and binds
  its MQTT port(s) without errors.
- There's no automated test suite yet; CI (`.github/workflows/ci.yml`) only
  runs `node --check` against the files in `src/`, so make sure your change
  at least parses.
- If you touch `src/models.js`, keep it in sync by hand with the schemas in
  `mqttcloud.ir/src/models/Mqtt*.ts` — that repo is the source of truth, this
  one is a plain-JS mirror.

## Commits & PRs

- Keep commits scoped to one logical change.
- Describe *why* in the PR description, not just what changed — especially
  for anything touching auth, topic namespacing, or the Mongo schemas.
