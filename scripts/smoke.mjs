import { WebSocket } from "ws";

function once(ws, type) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timeout waiting for " + type)), 4000);
    const onMessage = (raw) => {
      const msg = JSON.parse(String(raw));
      if (msg.type === type || msg.type === "error") {
        clearTimeout(timer);
        ws.off("message", onMessage);
        if (msg.type === "error") reject(new Error(msg.payload.message));
        else resolve(msg);
      }
    };
    ws.on("message", onMessage);
  });
}

function open(name) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket("ws://127.0.0.1:8787/ws");
    ws.on("open", () => resolve(ws));
    ws.on("error", reject);
  });
}

const a = await open("a");
const b = await open("b");

a.send(JSON.stringify({ v: 1, type: "hello", id: "a1", payload: { nickname: "Alice", room: "大厅" } }));
b.send(JSON.stringify({ v: 1, type: "hello", id: "b1", payload: { nickname: "Bob", room: "大厅" } }));
await once(a, "hello_ok");
await once(b, "hello_ok");

const got = once(b, "message");
a.send(JSON.stringify({ v: 1, type: "send", id: "a2", payload: { text: "hello from alice" } }));
const message = await got;
if (message.payload.text !== "hello from alice") throw new Error("unexpected payload");
if (message.payload.nickname !== "Alice") throw new Error("unexpected nickname");

a.close();
b.close();
console.log("ok: two clients exchanged a room message");
