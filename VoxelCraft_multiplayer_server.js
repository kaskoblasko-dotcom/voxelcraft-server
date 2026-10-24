const WebSocket = require('ws');
const PORT = Number(process.env.PORT || 3000);
const MAX_PLAYERS_PER_ROOM = 8;
const rooms = new Map();
let nextId = 1;
const wss = new WebSocket.Server({ port: PORT, maxPayload: 1024 * 1024 });
function send(ws, data) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data)); }
function cleanText(v, max) { return String(v || '').replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, max); }
function leave(ws) {
  if (!ws.room || !ws.playerId) return;
  const room = rooms.get(ws.room);
  if (room) { room.delete(ws.playerId); for (const peer of room.values()) send(peer.ws, { type: 'peer-left', id: ws.playerId }); if (!room.size) rooms.delete(ws.room); }
  ws.room = null; ws.playerId = null;
}
function broadcast(room, data, exceptId) { for (const peer of room.values()) if (peer.id !== exceptId) send(peer.ws, data); }
wss.on('connection', ws => {
  ws.room = null; ws.playerId = null;
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw.toString()); } catch { return; }
    if (!m || typeof m.type !== 'string') return;
    if (m.type === 'join') {
      if (ws.room) leave(ws);
      const roomName = cleanText(m.room, 40).toLowerCase().replace(/[^a-z0-9_.-]/g, '-');
      if (!roomName) return send(ws, { type: 'error', message: 'Enter a room code.' });
      let room = rooms.get(roomName); if (!room) { room = new Map(); rooms.set(roomName, room); }
      if (room.size >= MAX_PLAYERS_PER_ROOM) return send(ws, { type: 'error', message: 'Room is full (8 players maximum).' });
      const id = String(nextId++).padStart(10, '0');
      const peer = { id, ws, presence: { n: cleanText(m.name, 16) || 'Player', c: 0x2f6fdc } };
      ws.room = roomName; ws.playerId = id;
      const peers = [...room.values()].map(p => ({ id: p.id, presence: p.presence })); room.set(id, peer);
      send(ws, { type: 'joined', id, peers }); broadcast(room, { type: 'peer-joined', peer: { id, presence: peer.presence } }, id); return;
    }
    if (!ws.room || !ws.playerId) return send(ws, { type: 'error', message: 'Join a room first.' });
    const room = rooms.get(ws.room); if (!room) return; const self = room.get(ws.playerId); if (!self) return;
    if (m.type === 'presence' && m.presence && typeof m.presence === 'object') {
      const p = m.presence;
      self.presence = { n: cleanText(p.n || self.presence.n, 16), c: Number.isInteger(p.c) ? Math.max(0, Math.min(0xffffff, p.c)) : self.presence.c,
        x: Number.isFinite(p.x) ? p.x : self.presence.x, y: Number.isFinite(p.y) ? p.y : self.presence.y, z: Number.isFinite(p.z) ? p.z : self.presence.z,
        yaw: Number.isFinite(p.yaw) ? p.yaw : self.presence.yaw, d: p.d === 1 ? 1 : 0 };
      broadcast(room, { type: 'presence', id: self.id, presence: self.presence }); return;
    }
    if (m.type === 'emit') {
      const topic = cleanText(m.topic, 24);
      if (!['hello', 'world', 'chunk', 'blk', 'chat', 'pvp-hit'].includes(topic)) return;
      let data = m.data;
      if (topic === 'chat') { if (!data || typeof data.t !== 'string') return; data = { n: cleanText(data.n || self.presence.n, 16), t: cleanText(data.t, 200) }; if (!data.t) return; }
      if (topic === 'pvp-hit') {
        if (!data || typeof data.target !== 'string') return;
        const target = room.get(data.target); if (!target || target.id === self.id) return;
        const dmg = Number(data.dmg);
        data = { target: target.id, dmg: Number.isFinite(dmg) ? Math.max(1, Math.min(8, Math.round(dmg))) : 1, name: cleanText(self.presence.n, 16) };
        send(target.ws, { type: 'event', from: self.id, topic, data }); return;
      }
      const encoded = JSON.stringify(data); if (encoded.length > 900000) return send(ws, { type: 'error', message: 'Multiplayer message was too large.' });
      broadcast(room, { type: 'event', from: self.id, topic, data });
    }
  });
  ws.on('close', () => leave(ws)); ws.on('error', () => leave(ws));
});
console.log(`VoxelCraft multiplayer server listening on port ${PORT}`);
console.log('Keep this process running while players connect.');
