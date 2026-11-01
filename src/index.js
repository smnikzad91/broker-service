require("dotenv").config();

const fs       = require("fs");
const http     = require("http");
const net      = require("net");
const tls      = require("tls");
const mongoose = require("mongoose");
const bcrypt   = require("bcryptjs");
const Aedes    = require("aedes");
const { Server: SocketIOServer } = require("socket.io");
const websocketStream = require("websocket-stream");

const { MqttUser, MqttClient, MqttActivity, MqttPayload } = require("./models");
const { verifySocketToken } = require("./socketToken");

const MONGODB_URI  = process.env.MONGODB_URI;
const MQTT_PORT     = Number(process.env.MQTT_PORT ?? 1883);
const MQTT_TLS_PORT = Number(process.env.MQTT_TLS_PORT ?? 8883);
const MQTT_WS_PORT  = Number(process.env.MQTT_WS_PORT ?? 8884);
const TLS_CERT_PATH = process.env.TLS_CERT_PATH ?? "";
const TLS_KEY_PATH  = process.env.TLS_KEY_PATH ?? "";
const SOCKET_PORT          = Number(process.env.SOCKET_PORT ?? 4021);
const SOCKET_SHARED_SECRET = process.env.SOCKET_SHARED_SECRET ?? "";
const DASHBOARD_ORIGIN     = process.env.DASHBOARD_ORIGIN ?? "http://localhost:3000";

if (!MONGODB_URI) {
  console.error("MONGODB_URI is not set. Copy .env.example to .env and fill it in.");
  process.exit(1);
}
if (!SOCKET_SHARED_SECRET) {
  console.error("SOCKET_SHARED_SECRET is not set. Copy .env.example to .env and fill it in.");
  process.exit(1);
}

const aedes = new Aedes();

// ── Live push (Socket.IO) ────────────────────────────────────────────────
// Lets the dashboard show connect/disconnect events the moment they happen,
// instead of polling REST endpoints. A browser mints its own short-lived
// token at GET /api/user/mqtt/socket-token and joins room `user:{userId}`;
// see socketToken.js for the verification side of that handshake.
const socketHttpServer = http.createServer();
const io = new SocketIOServer(socketHttpServer, {
  cors: { origin: DASHBOARD_ORIGIN },
});

io.use((socket, next) => {
  const uid = verifySocketToken(socket.handshake.auth?.token, SOCKET_SHARED_SECRET);
  if (!uid) return next(new Error("unauthorized"));
  socket.data.userId = uid;
  next();
});

io.on("connection", (socket) => {
  socket.join(`user:${socket.data.userId}`);
});

// Counts clients already registered on this broker process for a given
// credential set, excluding `excludeClientId` — always the connecting
// client's own id, so a reconnect under the same clientId never counts
// against itself. Safe to call during authenticate(): aedes only adds a
// client to `aedes.clients` (and kicks any same-id predecessor) in
// registerClient(), which runs *after* authenticate() resolves — so a
// genuine reconnect's old session is still keyed under the same clientId
// at this point and gets excluded, never double-counted.
function countLiveConnections(mqttUserId, excludeClientId) {
  let count = 0;
  for (const id in aedes.clients) {
    if (id === excludeClientId) continue;
    const c = aedes.clients[id];
    if (c?.mqttUserId && String(c.mqttUserId) === String(mqttUserId)) count++;
  }
  return count;
}

// ── Auth ──────────────────────────────────────────────────────────────────
// A device may only connect if:
//   1. its username matches an active MqttUser credential set that isn't
//      planBlocked (over the account's plan, e.g. after a paid plan expired),
//   2. its client id (client.id) matches an MqttClient pre-registered under
//      that credential set from the dashboard, and
//   3. the credential set isn't already at its concurrent-connection limit
//      (MqttUser.maxConnection — set from the account's plan when the
//      credential was created; see assertCanCreateMqttUser on the
//      dashboard side for the matching registration-time cap).
// On success we stash the resolved ids on the `client` object — aedes gives
// no other reliable way to recover them later in authorize*/lifecycle hooks.
aedes.authenticate = async (client, username, password, callback) => {
  try {
    if (!username || !password) return callback(null, false);
    const uname = username.toString().toLowerCase();

    const mqttUser = await MqttUser.findOne({ username: uname });
    if (!mqttUser || !mqttUser.isActive || mqttUser.planBlocked) return callback(null, false);

    const match = await bcrypt.compare(password.toString(), mqttUser.password);
    if (!match) return callback(null, false);

    const mqttClient = await MqttClient.findOne({ mqttUserId: mqttUser._id, clientName: client.id });
    if (!mqttClient) return callback(null, false);

    const activeCount = countLiveConnections(mqttUser._id, client.id);
    if (activeCount >= mqttUser.maxConnection) {
      console.log(`[connection limit] ${uname}/${client.id} rejected — ${activeCount}/${mqttUser.maxConnection} already connected`);
      return callback(null, false);
    }

    client.username   = uname;
    client.userId     = mqttUser.userId;
    client.mqttUserId = mqttUser._id;
    client.connectedAt = Date.now();

    callback(null, true);
  } catch (err) {
    console.error("authenticate error:", err);
    callback(err, false);
  }
};

// Every topic a client publishes or subscribes to must start with its own
// username plus a slash, e.g. `alice/sensor1` — never the bare username
// itself (`alice`) and never another user's namespace (`aliceX/...`, which
// this prefix check (with the trailing "/") correctly tells apart from `alice/...`).
function isOwnNamespace(topic, username) {
  return topic.startsWith(`${username}/`);
}

aedes.authorizePublish = (client, packet, callback) => {
  if (!client) return callback(null); // broker-internal publish
  if (!isOwnNamespace(packet.topic, client.username)) return callback(new Error("unauthorized topic"));
  callback(null);
};

aedes.authorizeSubscribe = (client, subscription, callback) => {
  if (!isOwnNamespace(subscription.topic, client.username)) return callback(null, false);
  callback(null, subscription);
};

// ── Connection lifecycle ─────────────────────────────────────────────────
function pushActivity(userId, activity) {
  io.to(`user:${userId}`).emit("mqtt:activity", {
    id:         activity._id.toString(),
    clientName: activity.clientName,
    event:      activity.event,
    createdAt:  activity.createdAt,
  });
}

aedes.on("client", async (client) => {
  if (!client.mqttUserId) return; // internal/unauthenticated
  try {
    await MqttClient.updateOne(
      { mqttUserId: client.mqttUserId, clientName: client.id },
      { $set: { isOnline: true, lastSeenAt: new Date() } }
    );
    const activity = await MqttActivity.create({
      userId: client.userId, mqttUserId: client.mqttUserId, clientName: client.id, event: "connect",
    });
    pushActivity(client.userId, activity);
    console.log(`[connect] ${client.username}/${client.id}`);
  } catch (err) {
    console.error("client connect logging failed:", err);
  }
});

aedes.on("clientDisconnect", async (client) => {
  if (!client.mqttUserId) return;
  try {
    await MqttClient.updateOne(
      { mqttUserId: client.mqttUserId, clientName: client.id },
      { $set: { isOnline: false, lastSeenAt: new Date() } }
    );
    const activity = await MqttActivity.create({
      userId: client.userId, mqttUserId: client.mqttUserId, clientName: client.id, event: "disconnect",
    });
    pushActivity(client.userId, activity);
    console.log(`[disconnect] ${client.username}/${client.id}`);
  } catch (err) {
    console.error("client disconnect logging failed:", err);
  }
});

// Rolling debug log of published messages (see MqttPayload's TTL index — 7 days).
aedes.on("publish", async (packet, client) => {
  if (!client || !client.mqttUserId) return;
  if (packet.topic.startsWith("$SYS")) return;
  try {
    await MqttPayload.create({
      userId:     client.userId,
      mqttUserId: client.mqttUserId,
      clientName: client.id,
      topic:      packet.topic,
      payload:    packet.payload.toString().slice(0, 4000),
    });
  } catch (err) {
    console.error("payload logging failed:", err);
  }
});

aedes.on("clientError", (client, err) => {
  console.log(`[client error] ${client?.id ?? "?"}:`, err.message);
});

aedes.on("connectionError", (client, err) => {
  console.log(`[connection error] ${client?.id ?? "?"}:`, err.message);
});

// ── Live enforcement sweep ─────────────────────────────────────────────────
// authenticate() only runs at connect time, so a change made afterwards on
// the dashboard (a plan expiring to free, a credential suspended or deleted,
// a device deleted) would otherwise leave existing sessions connected
// indefinitely. Every ENFORCE_INTERVAL_MS this re-checks every live session
// against the same rules and closes the ones that no longer pass. When a
// credential set is over its maxConnection, the newest sessions go first.
const ENFORCE_INTERVAL_MS = Number(process.env.ENFORCE_INTERVAL_MS ?? 30_000);

async function enforceLimits() {
  const byCred = new Map();
  for (const id in aedes.clients) {
    const c = aedes.clients[id];
    if (!c?.mqttUserId) continue;
    const key = String(c.mqttUserId);
    if (!byCred.has(key)) byCred.set(key, []);
    byCred.get(key).push(c);
  }
  if (byCred.size === 0) return;

  const ids = [...byCred.keys()];
  const [creds, devices] = await Promise.all([
    MqttUser.find({ _id: { $in: ids } }).select("isActive planBlocked maxConnection").lean(),
    MqttClient.find({ mqttUserId: { $in: ids } }).select("mqttUserId clientName").lean(),
  ]);
  const credById = new Map(creds.map((c) => [String(c._id), c]));
  const registered = new Set(devices.map((d) => `${d.mqttUserId}:${d.clientName}`));

  for (const [key, clients] of byCred) {
    const cred = credById.get(key);
    const reason = !cred ? "credential deleted" : !cred.isActive ? "credential suspended" : cred.planBlocked ? "over plan limit" : null;
    const keep = [];
    for (const c of clients) {
      if (reason) kick(c, reason);
      else if (!registered.has(`${key}:${c.id}`)) kick(c, "device deleted");
      else keep.push(c);
    }
    if (reason) continue;
    keep.sort((a, b) => (a.connectedAt ?? 0) - (b.connectedAt ?? 0));
    for (const c of keep.slice(cred.maxConnection)) kick(c, `over ${cred.maxConnection} connections`);
  }
}

function kick(client, reason) {
  console.log(`[enforce] closing ${client.username}/${client.id} — ${reason}`);
  client.close();
}

function startEnforcement() {
  let running = false;
  setInterval(async () => {
    if (running) return;
    running = true;
    try { await enforceLimits(); }
    catch (err) { console.error("enforcement sweep failed:", err); }
    finally { running = false; }
  }, ENFORCE_INTERVAL_MS).unref();
}

// ── Servers ───────────────────────────────────────────────────────────────
async function main() {
  await mongoose.connect(MONGODB_URI);
  console.log("Connected to MongoDB");

  const server = net.createServer(aedes.handle);
  server.listen(MQTT_PORT, () => console.log(`MQTT broker listening on port ${MQTT_PORT}`));

  if (TLS_CERT_PATH && TLS_KEY_PATH && fs.existsSync(TLS_CERT_PATH) && fs.existsSync(TLS_KEY_PATH)) {
    const secureServer = tls.createServer(
      { key: fs.readFileSync(TLS_KEY_PATH), cert: fs.readFileSync(TLS_CERT_PATH) },
      aedes.handle
    );
    secureServer.listen(MQTT_TLS_PORT, () => console.log(`MQTT broker (TLS) listening on port ${MQTT_TLS_PORT}`));
  } else {
    console.log("TLS_CERT_PATH/TLS_KEY_PATH not set (or unreadable) — running plaintext only.");
  }

  // Browser clients (e.g. mqtt.js in the dashboard) can't open raw TCP
  // sockets — they need MQTT-over-WebSocket. This listens in plaintext;
  // nginx terminates TLS and proxies wss://mqtt.mqttcloud.ir/mqtt here.
  const wsHttpServer = http.createServer();
  websocketStream.createServer({ server: wsHttpServer }, aedes.handle);
  wsHttpServer.listen(MQTT_WS_PORT, () => console.log(`MQTT broker (WebSocket) listening on port ${MQTT_WS_PORT}`));

  startEnforcement();

  socketHttpServer.listen(SOCKET_PORT, () => console.log(`Socket.IO (live push) listening on port ${SOCKET_PORT}`));
}

main().catch((err) => {
  console.error("Failed to start broker service:", err);
  process.exit(1);
});

process.on("SIGINT", () => process.exit(0));
process.on("SIGTERM", () => process.exit(0));
