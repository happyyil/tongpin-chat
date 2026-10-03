import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer, WebSocket } from "ws";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const PUBLIC_DIR = path.join(ROOT, "public");
const DATA_DIR = path.join(ROOT, "data");
const STORE_FILE = path.join(DATA_DIR, "chat.json");

loadDotEnv(path.join(ROOT, ".env"));

const HOST = process.env.HOST || "127.0.0.1";
const PORT = Number(process.env.PORT || 8787);
const ROOM_PASSWORD = process.env.ROOM_PASSWORD || "";
const HISTORY_LIMIT = clampInt(process.env.HISTORY_LIMIT, 200, 20, 2000);
const MAX_TEXT = 2000;
const MAX_FRAME = 8 * 1024;
const RATE_WINDOW_MS = 10_000;
const RATE_MAX = 20;

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".png": "image/png",
  ".woff2": "font/woff2"
};

/** @type {Map<string, { id: string, nickname: string, room: string, joinedAt: number, ws: WebSocket, sendTimes: number[] }>} */
const clients = new Map();
/** @type {Map<string, Array<{ id: string, userId: string, nickname: string, text: string, ts: number }>>} */
const rooms = new Map();

loadStore();

const server = http.createServer((req, res) => {
  try {
    handleHttp(req, res);
  } catch (error) {
    console.error(error);
    res.writeHead(500, { "content-type": "text/plain; charset=utf-8" });
    res.end("Internal Server Error");
  }
});

const wss = new WebSocketServer({
  server,
  path: "/ws",
  maxPayload: MAX_FRAME
});

wss.on("connection", (ws, req) => {
  const userId = crypto.randomUUID();
  const client = {
    id: userId,
    nickname: "",
    room: "",
    joinedAt: 0,
    ws,
    sendTimes: []
  };
  clients.set(userId, client);

  ws.on("message", (raw, isBinary) => {
    if (isBinary) {
      sendError(ws, null, "invalid_frame", "只接受 JSON 文本帧。");
      return;
    }
    let envelope;
    try {
      envelope = JSON.parse(String(raw));
    } catch {
      sendError(ws, null, "invalid_json", "消息不是合法 JSON。");
      return;
    }
    handleEnvelope(client, envelope);
  });

  ws.on("close", () => {
    leaveRoom(client);
    clients.delete(userId);
  });

  ws.on("error", () => {
    ws.close();
  });

  const ip = req.socket.remoteAddress;
  console.log(`connected ${userId} from ${ip}`);
});

server.listen(PORT, HOST, () => {
  console.log(`同频 listening http://${HOST}:${PORT}`);
  console.log(`WebSocket path ws://${HOST}:${PORT}/ws`);
  if (ROOM_PASSWORD) console.log("ROOM_PASSWORD is enabled");
  else console.log("ROOM_PASSWORD is empty (LAN-friendly; set it for public hosts)");
});

function handleHttp(req, res) {
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

  if (url.pathname === "/health") {
    json(res, 200, {
      ok: true,
      clients: clients.size,
      rooms: [...rooms.keys()]
    });
    return;
  }

  if (url.pathname === "/config") {
    json(res, 200, {
      protocol: 1,
      wsPath: "/ws",
      requirePassword: Boolean(ROOM_PASSWORD),
      maxText: MAX_TEXT
    });
    return;
  }

  if (req.method !== "GET" && req.method !== "HEAD") {
    res.writeHead(405);
    res.end();
    return;
  }

  const relative = url.pathname === "/" ? "/index.html" : url.pathname;
  const filePath = safeJoin(PUBLIC_DIR, relative);
  if (!filePath) {
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
      res.end("Not Found");
      return;
    }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, { "content-type": MIME[ext] || "application/octet-stream" });
    res.end(req.method === "HEAD" ? undefined : data);
  });
}

function handleEnvelope(client, envelope) {
  if (!envelope || envelope.v !== 1 || typeof envelope.type !== "string") {
    sendError(client.ws, envelope?.id, "bad_envelope", "需要 { v: 1, type, id?, payload }。");
    return;
  }
  const id = typeof envelope.id === "string" ? envelope.id : null;
  const payload = envelope.payload && typeof envelope.payload === "object" ? envelope.payload : {};

  switch (envelope.type) {
    case "hello":
      handleHello(client, id, payload);
      break;
    case "send":
      handleSend(client, id, payload);
      break;
    case "ping":
      sendJson(client.ws, { v: 1, type: "pong", id, payload: { ts: Date.now() } });
      break;
    default:
      sendError(client.ws, id, "unknown_type", `未知消息类型：${envelope.type}`);
  }
}

function handleHello(client, id, payload) {
  const nickname = normalizeName(payload.nickname, 24);
  const room = normalizeRoom(payload.room);
  const password = typeof payload.password === "string" ? payload.password : "";

  if (!nickname) {
    sendError(client.ws, id, "bad_nickname", "请填写 1–24 个字符的昵称。");
    return;
  }
  if (!room) {
    sendError(client.ws, id, "bad_room", "房间名只能包含中文、字母、数字、连字符，长度 1–32。");
    return;
  }
  if (ROOM_PASSWORD && password !== ROOM_PASSWORD) {
    sendError(client.ws, id, "auth_failed", "房间口令不正确。");
    return;
  }

  if (client.room && client.room !== room) leaveRoom(client);

  client.nickname = nickname;
  client.room = room;
  client.joinedAt = Date.now();

  const history = getRoom(room);
  sendJson(client.ws, {
    v: 1,
    type: "hello_ok",
    id,
    payload: {
      userId: client.id,
      nickname,
      room,
      members: listMembers(room),
      history
    }
  });
  broadcastPresence(room);
}

function handleSend(client, id, payload) {
  if (!client.room) {
    sendError(client.ws, id, "not_joined", "尚未加入房间，无法发送。");
    return;
  }
  const text = typeof payload.text === "string" ? payload.text.trim() : "";
  if (!text) {
    sendError(client.ws, id, "empty", "消息不能为空。");
    return;
  }
  if (text.length > MAX_TEXT) {
    sendError(client.ws, id, "too_long", `消息不能超过 ${MAX_TEXT} 个字符。`);
    return;
  }
  if (!allowSend(client)) {
    sendError(client.ws, id, "rate_limited", "发送过于频繁，请稍后再试。");
    return;
  }

  const message = {
    id: crypto.randomUUID(),
    userId: client.id,
    nickname: client.nickname,
    text,
    ts: Date.now()
  };
  const history = getRoom(client.room);
  history.push(message);
  if (history.length > HISTORY_LIMIT) history.splice(0, history.length - HISTORY_LIMIT);
  persistSoon();

  sendJson(client.ws, {
    v: 1,
    type: "send_ok",
    id,
    payload: { messageId: message.id }
  });
  broadcast(client.room, {
    v: 1,
    type: "message",
    payload: message
  });
}

function leaveRoom(client) {
  const room = client.room;
  if (!room) return;
  client.room = "";
  broadcastPresence(room);
}

function getRoom(name) {
  if (!rooms.has(name)) rooms.set(name, []);
  return rooms.get(name);
}

function listMembers(room) {
  return [...clients.values()]
    .filter((c) => c.room === room && c.ws.readyState === WebSocket.OPEN)
    .map((c) => ({
      userId: c.id,
      nickname: c.nickname,
      joinedAt: c.joinedAt
    }));
}

function broadcastPresence(room) {
  broadcast(room, {
    v: 1,
    type: "presence",
    payload: { room, members: listMembers(room) }
  });
}

function broadcast(room, envelope) {
  const body = JSON.stringify(envelope);
  for (const client of clients.values()) {
    if (client.room === room && client.ws.readyState === WebSocket.OPEN) {
      client.ws.send(body);
    }
  }
}

function sendJson(ws, envelope) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(envelope));
}

function sendError(ws, id, code, message) {
  sendJson(ws, {
    v: 1,
    type: "error",
    id,
    payload: { code, message }
  });
}

function allowSend(client) {
  const now = Date.now();
  client.sendTimes = client.sendTimes.filter((t) => now - t < RATE_WINDOW_MS);
  if (client.sendTimes.length >= RATE_MAX) return false;
  client.sendTimes.push(now);
  return true;
}

function normalizeName(value, max) {
  if (typeof value !== "string") return "";
  const text = value.replace(/\s+/g, " ").trim();
  if (!text || text.length > max) return "";
  if (/[\u0000-\u001f]/.test(text)) return "";
  return text;
}

function normalizeRoom(value) {
  if (typeof value !== "string") return "";
  const text = value.trim();
  if (!/^[\u4e00-\u9fffA-Za-z0-9_-]{1,32}$/.test(text)) return "";
  return text;
}

function json(res, status, body) {
  res.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(body));
}

function safeJoin(root, relative) {
  const decoded = decodeURIComponent(relative);
  if (decoded.includes("\0") || path.isAbsolute(decoded)) return null;
  const resolved = path.resolve(root, "." + decoded);
  if (!resolved.startsWith(root)) return null;
  return resolved;
}

function clampInt(value, fallback, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(n)));
}

function loadStore() {
  try {
    const raw = fs.readFileSync(STORE_FILE, "utf8");
    const parsed = JSON.parse(raw);
    if (parsed && parsed.rooms && typeof parsed.rooms === "object") {
      for (const [name, messages] of Object.entries(parsed.rooms)) {
        if (!Array.isArray(messages)) continue;
        rooms.set(
          name,
          messages.filter((m) => m && typeof m.text === "string").slice(-HISTORY_LIMIT)
        );
      }
    }
  } catch (error) {
    if (error.code !== "ENOENT") console.error("failed to load store", error);
  }
}

let persistTimer = null;
function persistSoon() {
  clearTimeout(persistTimer);
  persistTimer = setTimeout(persistNow, 400);
}

function persistNow() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  const payload = { rooms: Object.fromEntries(rooms) };
  fs.writeFileSync(STORE_FILE, JSON.stringify(payload));
}

function loadDotEnv(file) {
  try {
    const text = fs.readFileSync(file, "utf8");
    for (const line of text.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const eq = trimmed.indexOf("=");
      if (eq < 1) continue;
      const key = trimmed.slice(0, eq).trim();
      let value = trimmed.slice(eq + 1).trim();
      if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
        value = value.slice(1, -1);
      }
      if (process.env[key] === undefined) process.env[key] = value;
    }
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
}
