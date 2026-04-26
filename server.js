// ============================================================
// QuestForge Multiplayer Server
// Pure Node.js WebSocket server — no Express needed
// ============================================================
//
// What it does:
//   - Accepts WebSocket connections on the configured port
//   - Groups players into "rooms" of up to 10 players (one big island instance)
//   - Broadcasts position/emote/dialog updates to everyone in the same room
//   - Cleans up disconnected players automatically
//
// Run with:
//   node server.js
// Or with PM2 / systemd in production.

const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS_PER_ROOM = 10;
const TICK_RATE_MS = 50;          // 20 ticks/sec broadcast
const HEARTBEAT_MS = 30000;       // ping every 30s to detect dead conns
const CLEAN_DEAD_AFTER_MS = 45000;

// ============ ROOM MANAGEMENT ============
const rooms = new Map();   // roomId -> { id, players: Map(playerId -> playerState) }

// Persistent counter for human-friendly room numbering (1, 2, 3...)
let roomCounter = 0;

function createRoom() {
  roomCounter++;
  const id = 'room-' + crypto.randomBytes(3).toString('hex');
  const room = { id, num: roomCounter, players: new Map(), createdAt: Date.now() };
  rooms.set(id, room);
  console.log(`[room] created ${id} (Room #${roomCounter})`);
  return room;
}

function findOrCreateRoom() {
  // Find first room with available space (in insertion order)
  for (const room of rooms.values()) {
    if (room.players.size < MAX_PLAYERS_PER_ROOM) return room;
  }
  // All rooms full — make new one
  return createRoom();
}

function removeEmptyRooms() {
  for (const [id, room] of rooms) {
    if (room.players.size === 0 && Date.now() - room.createdAt > 60000) {
      rooms.delete(id);
      console.log(`[room] removed empty ${id}`);
    }
  }
}
setInterval(removeEmptyRooms, 60000);

// ============ HTTP + WS SERVER ============
const server = http.createServer((req, res) => {
  // Tiny health endpoint
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      rooms: rooms.size,
      players: [...rooms.values()].reduce((sum, r) => sum + r.players.size, 0),
      uptime_s: Math.floor(process.uptime()),
      roomDetails: [...rooms.values()].map(r => ({
        num: r.num,
        id: r.id,
        players: r.players.size,
        capacity: MAX_PLAYERS_PER_ROOM,
      })),
    }, null, 2));
    return;
  }
  res.writeHead(404);
  res.end('Not found');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const playerId = crypto.randomBytes(6).toString('hex');
  const room = findOrCreateRoom();

  const player = {
    id: playerId,
    ws,
    name: 'Adventurer',
    char: 'warrior_blue',
    x: 22 * 32, y: 15 * 32,
    facing: 1, state: 'idle', frame: 0,
    emote: null,
    lastSeen: Date.now(),
    isAlive: true,
  };
  room.players.set(playerId, player);

  console.log(`[conn] ${playerId} joined ${room.id} (${room.players.size}/${MAX_PLAYERS_PER_ROOM})`);

  // Tell the new player their ID, room, and current other players
  send(ws, {
    type: 'welcome',
    you: playerId,
    room: room.id,
    roomNum: room.num,
    capacity: MAX_PLAYERS_PER_ROOM,
    others: [...room.players.values()]
      .filter(p => p.id !== playerId)
      .map(serializePlayer),
  });

  // Tell others a new player joined
  broadcast(room, { type: 'join', player: serializePlayer(player) }, playerId);

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); }
    catch { return; }
    player.lastSeen = Date.now();

    switch (msg.type) {
      case 'hello':
        // Player tells us their chosen name & character
        if (typeof msg.name === 'string' && msg.name.trim().length > 0) {
          player.name = msg.name.trim().substring(0, 16);
        }
        if (typeof msg.char === 'string') {
          player.char = msg.char.substring(0, 32);
        }
        broadcast(room, { type: 'update', player: serializePlayer(player) });
        break;

      case 'state':
        // Player position/state update — store, broadcast in next tick
        if (typeof msg.x === 'number' && typeof msg.y === 'number') {
          player.x = msg.x;
          player.y = msg.y;
        }
        if (typeof msg.facing === 'number') player.facing = msg.facing;
        if (typeof msg.state === 'string') player.state = msg.state;
        if (typeof msg.frame === 'number') player.frame = msg.frame;
        break;

      case 'emote':
        if (typeof msg.emote === 'string') {
          player.emote = msg.emote.substring(0, 4);
          // Broadcast emote immediately (it's an event, not a continuous state)
          broadcast(room, { type: 'emote', id: playerId, emote: player.emote });
          // Auto-clear after a moment
          setTimeout(() => { if (player.emote === msg.emote) player.emote = null; }, 3000);
        }
        break;

      case 'pong':
        // Heartbeat reply, do nothing
        break;
    }
  });

  ws.on('close', () => disconnectPlayer(room, playerId));
  ws.on('error', () => disconnectPlayer(room, playerId));
});

function disconnectPlayer(room, playerId) {
  if (!room.players.has(playerId)) return;
  room.players.delete(playerId);
  console.log(`[disc] ${playerId} left ${room.id} (${room.players.size}/${MAX_PLAYERS_PER_ROOM})`);
  broadcast(room, { type: 'leave', id: playerId });
}

function serializePlayer(p) {
  return {
    id: p.id, name: p.name, char: p.char,
    x: p.x, y: p.y, facing: p.facing, state: p.state, frame: p.frame,
    emote: p.emote,
  };
}

function send(ws, obj) {
  if (ws.readyState !== ws.OPEN) return;
  try { ws.send(JSON.stringify(obj)); } catch {}
}

function broadcast(room, obj, exceptPlayerId) {
  const payload = JSON.stringify(obj);
  for (const p of room.players.values()) {
    if (p.id === exceptPlayerId) continue;
    if (p.ws.readyState !== p.ws.OPEN) continue;
    try { p.ws.send(payload); } catch {}
  }
}

// ============ TICK BROADCAST ============
// Every TICK_RATE_MS we send a snapshot of all positions to each client.
// This keeps state synchronized without flooding the network on every keystroke.
setInterval(() => {
  for (const room of rooms.values()) {
    if (room.players.size <= 1) continue;
    const snapshot = {
      type: 'tick',
      players: [...room.players.values()].map(p => ({
        id: p.id, x: p.x, y: p.y, facing: p.facing, state: p.state, frame: p.frame,
      })),
    };
    const payload = JSON.stringify(snapshot);
    for (const p of room.players.values()) {
      if (p.ws.readyState !== p.ws.OPEN) continue;
      try { p.ws.send(payload); } catch {}
    }
  }
}, TICK_RATE_MS);

// ============ HEARTBEAT / DEAD CONN CLEANUP ============
setInterval(() => {
  const now = Date.now();
  for (const room of rooms.values()) {
    for (const [id, p] of room.players) {
      if (p.ws.readyState !== p.ws.OPEN) {
        disconnectPlayer(room, id);
        continue;
      }
      if (now - p.lastSeen > CLEAN_DEAD_AFTER_MS) {
        console.log(`[timeout] ${id} no activity, dropping`);
        try { p.ws.terminate(); } catch {}
        disconnectPlayer(room, id);
        continue;
      }
      send(p.ws, { type: 'ping', t: now });
    }
  }
}, HEARTBEAT_MS);

server.listen(PORT, () => {
  console.log(`QuestForge multiplayer server listening on :${PORT}`);
  console.log(`Max ${MAX_PLAYERS_PER_ROOM} players per room.`);
  console.log(`Health: GET http://localhost:${PORT}/health`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('Shutting down...');
  for (const room of rooms.values()) {
    for (const p of room.players.values()) {
      try { p.ws.close(1001, 'Server restarting'); } catch {}
    }
  }
  server.close(() => process.exit(0));
});
