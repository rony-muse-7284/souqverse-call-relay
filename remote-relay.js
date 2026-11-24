// Remote voice-call relay for Telegram bot @souqverse_bot.
// Runs on a FREE HOST (Replit) — NOT on the sandbox VM.
//
// Why split: every tunnel (ngrok / Cloudflare / zrok) is blocked by the
// sandbox egress proxy on the VM, so the call page must live OUTSIDE the
// machine. This relay serves call.html and the WebSocket to the Mini App.
// The bot brain stays on the VM; a small LOCAL bridge (bridge.js, same dir
// layout) polls this relay's HTTP API and moves text into/out of the
// watcher dir (pending/ + call-replies/).
//
// Flow:
//   Mini App (browser) --WebSocket--> this relay --queue--> bridge --pending JSON--> replier
//   replier --call-replies/<call_id>.txt--> bridge --POST /api/bot-reply--> this relay --WebSocket--> Mini App
//
// This relay NEVER touches the bot token and NEVER talks to Telegram.
// It only queues TEXT.
//
// TODO (PRODUCTION, before real users): verify Telegram initData on every
// user_text message. The Mini App must send its initData string, and this
// relay must check HMAC-SHA256(initData, key = SHA256(bot token)) server-side
// and take chat_id/first_name from the VERIFIED data, never from the client
// payload. This POC accepts the client-sent chat_id as-is because there is
// no public URL / real traffic yet. Do NOT ship to real users without it.
//
// SECURITY NOTE (v1 limitation): the /api/* endpoints have NO auth. Anyone
// who knows this relay's public URL could queue fake user_text items or read
// queued texts. Acceptable for the first live test with Rony only; add a
// shared secret header (checked by bridge.js and remote-relay.js) before
// opening the call to real users.

'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const HOST = '0.0.0.0';
const PORT = Number(process.env.PORT || 8765);
const CALL_HTML = path.join(__dirname, 'call.html');
const STALE_HELD_MS = 10 * 60 * 1000; // drop held replies after 10 min

// call_id -> WebSocket (only while the Mini App page is connected)
const calls = new Map();
// call_id -> next message counter (n in "call-<call_id>-<n>")
const counters = new Map();
// Pending payloads queued for the bridge (drained by GET /api/pending-texts).
const queue = [];
// call_id -> { text, at } — bot replies that arrived while the caller was
// offline; delivered when that call sends its next user_text.
const heldReplies = new Map();

function log(msg) {
  console.log(new Date().toISOString() + ' ' + msg);
}

function nowIso() {
  // Same format as the watcher's now_iso(): YYYY-MM-DDTHH:MM:SSZ
  return new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
}

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(body);
}

// Read a JSON request body (small: a single bot reply). Rejects >64KB.
function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > 64 * 1024) { req.destroy(); reject(new Error('body too large')); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
      catch (e) { reject(new Error('bad JSON')); }
    });
    req.on('error', reject);
  });
}

// ---- HTTP server: call.html, health, bridge API --------------------------
const server = http.createServer(async (req, res) => {
  try {
    if (req.method === 'GET' && req.url === '/') {
      const html = fs.readFileSync(CALL_HTML, 'utf8');
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(html);
    } else if (req.method === 'GET' && req.url === '/health') {
      res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('ok');
    } else if (req.method === 'GET' && req.url === '/api/pending-texts') {
      // Drain the queue: items are returned exactly once. The bridge writes
      // them to pending/ immediately after this call, so the at-most-once
      // window is milliseconds.
      const items = queue.splice(0, queue.length);
      sendJson(res, 200, items);
    } else if (req.method === 'POST' && req.url === '/api/bot-reply') {
      let body;
      try { body = await readJsonBody(req); }
      catch (e) { sendJson(res, 400, { error: e.message }); return; }
      const callId = body && body.call_id;
      const text = body && body.text;
      if (typeof callId !== 'string' || !callId ||
          typeof text !== 'string' || !text.trim()) {
        sendJson(res, 400, { error: 'bad fields' });
        return;
      }
      const ws = calls.get(callId);
      if (ws && ws.readyState === WebSocket.OPEN) {
        try { ws.send(JSON.stringify({ type: 'bot_reply', text })); } catch (_) {}
        log('reply delivered call_id=' + callId + ' len=' + text.length);
        sendJson(res, 200, { status: 'delivered' });
      } else {
        heldReplies.set(callId, { text, at: Date.now() });
        log('reply held (caller offline) call_id=' + callId + ' len=' + text.length);
        sendJson(res, 200, { status: 'held' });
      }
    } else {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('not found');
    }
  } catch (e) {
    log('HTTP ERROR ' + req.method + ' ' + req.url + ': ' + e.message);
    try { res.writeHead(500); res.end('error'); } catch (_) {}
  }
});

// ---- WebSocket server: /ws -------------------------------------------------
const wss = new WebSocket.Server({ server, path: '/ws' });

wss.on('connection', (ws) => {
  let myCallId = null;
  log('WS connected');

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString('utf8')); } catch (e) {
      log('WS bad JSON, ignored');
      return;
    }
    if (!msg || msg.type !== 'user_text') return; // ignore unknown types

    // Validate: never queue a turn with bad data.
    const callId = msg.call_id;
    const chatId = msg.chat_id;
    const firstName = msg.first_name;
    const text = msg.text;
    if (typeof callId !== 'string' || !callId ||
        typeof chatId !== 'number' || !Number.isFinite(chatId) ||
        typeof firstName !== 'string' || !firstName ||
        typeof text !== 'string' || !text.trim()) {
      log('WS user_text rejected (bad fields) call_id=' + String(callId));
      try { ws.send(JSON.stringify({ type: 'error', error: 'bad fields' })); } catch (_) {}
      return;
    }

    myCallId = callId;
    calls.set(callId, ws);
    const n = (counters.get(callId) || 0) + 1;
    counters.set(callId, n);
    const messageId = 'call-' + callId + '-' + n;

    // EXACT pending payload format of the local relay (relay.js) — the
    // bridge writes this object verbatim into pending/<message_id>.json.
    const payload = {
      chat_id: chatId,
      user_id: chatId,
      first_name: firstName,
      username: (typeof msg.username === 'string' && msg.username) ? msg.username : null,
      text: text,
      message_id: messageId,   // string id: can NEVER collide with a real Telegram message id
      date: Math.floor(Date.now() / 1000),
      flagged_at: nowIso(),
      is_call: true,
      call_id: callId,
    };
    queue.push(payload);
    log('queued ' + messageId + ' chat=' + chatId + ' len=' + text.length + ' :: ' + text.slice(0, 50));
    try { ws.send(JSON.stringify({ type: 'ack', message_id: messageId })); } catch (_) {}

    // A reply may have been held while this caller was offline — deliver now.
    const held = heldReplies.get(callId);
    if (held) {
      heldReplies.delete(callId);
      try { ws.send(JSON.stringify({ type: 'bot_reply', text: held.text })); } catch (_) {}
      log('held reply delivered call_id=' + callId);
    }
  });

  ws.on('close', () => {
    if (myCallId && calls.get(myCallId) === ws) calls.delete(myCallId);
    log('WS closed call_id=' + (myCallId || '?'));
  });
  ws.on('error', (e) => log('WS error: ' + e.message));
});

// ---- Held-reply cleanup -----------------------------------------------------
setInterval(() => {
  const now = Date.now();
  for (const [callId, held] of heldReplies) {
    if (now - held.at > STALE_HELD_MS) {
      heldReplies.delete(callId);
      log('stale held reply dropped call_id=' + callId);
    }
  }
}, 60 * 1000);

// ---- Startup ----------------------------------------------------------------
process.on('uncaughtException', (e) => log('UNCAUGHT (kept alive): ' + (e && e.stack || e)));

server.listen(PORT, HOST, () => {
  log('remote relay up pid=' + process.pid + ' http://' + HOST + ':' + PORT + '/ ws /ws');
});
