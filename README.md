# mqttcloud broker service

[![CI](https://github.com/smnikzad91/broker-service/actions/workflows/ci.yml/badge.svg)](https://github.com/smnikzad91/broker-service/actions/workflows/ci.yml)

Standalone Aedes MQTT broker for mqttcloud.ir. It holds the long-lived TCP/TLS
connections an MQTT broker needs — something a Next.js API route can't do —
and talks to the **same MongoDB database** as the dashboard app
(`../mqttcloud.ir`), reading/writing the `MqttUser`, `MqttClient`,
`MqttActivity` and `MqttPayload` collections directly.

The dashboard app is the source of truth for those schemas
(`mqttcloud.ir/src/models/Mqtt*.ts`); `src/models.js` here is a plain-JS
mirror kept in sync by hand since this is a separate deployable with its own
`node_modules`, not a shared package.

## What it does

- Authenticates a connecting device against `MqttUser` (username + bcrypt
  password check, must be `isActive`) and requires its client id to match a
  pre-registered `MqttClient` under that credential set — devices and
  credentials are both created from the dashboard, not here.
- Enforces topic namespacing: a client authenticated as `alice` may only
  publish/subscribe under `alice/...`.
- Flips `MqttClient.isOnline`/`lastSeenAt` and appends `MqttActivity`
  connect/disconnect rows (auto-expire after 30 days).
- Logs published payloads to `MqttPayload` as a rolling debug aid
  (auto-expires after 7 days — not a message store).

Out of scope for this pass (see the project memory `broker-legacy-codebase`
for context): the GSM/SMS hardware bridge from the old `broker/backend`
codebase, wallet/plan-based connection limits, and real-time push of
connect/disconnect events to the dashboard UI (the dashboard currently polls
the REST endpoints instead).

## Running

```bash
npm install
cp .env.example .env   # fill in MONGODB_URI (same DB as the Next.js app)
npm start               # or `npm run dev` for nodemon
```

TLS only starts if both `TLS_CERT_PATH` and `TLS_KEY_PATH` point at readable
files; otherwise it logs a notice and serves plaintext-only on `MQTT_PORT`.

## Deploying alongside the dashboard

This process needs to stay running independently of `next start`. On a VPS,
run it as its own pm2/systemd service, e.g.:

```bash
pm2 start src/index.js --name mqttcloud-broker
```
