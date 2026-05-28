import { createServer } from "http";
import { Server } from "socket.io";
import mysql from "mysql2/promise";

const PORT = process.env.PORT || 5000;
const MONITOR_URL = process.env.LEGION_MONITOR_URL || 'https://legion-monitor.grudge.workers.dev';

// ── Legion Monitor: report errors + health to Cloudflare Worker ──────────────
function reportToLegion(message, stack = '', type = 'error') {
  const payload = JSON.stringify({
    site: 'grudge-pvp-server',
    type,
    message: String(message).slice(0, 1000),
    stack: String(stack).slice(0, 2000),
    severity: type === 'uncaughtException' ? 'critical' : 'error',
    ts: Date.now(),
  });
  fetch(`${MONITOR_URL}/error`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: payload,
  }).catch(() => {});
}

process.on('uncaughtException', (err) => {
  console.error('[pvp] UNCAUGHT EXCEPTION:', err);
  reportToLegion(err.message, err.stack, 'uncaughtException');
});

process.on('unhandledRejection', (reason) => {
  const msg = reason?.message || String(reason);
  console.error('[pvp] UNHANDLED REJECTION:', msg);
  reportToLegion(msg, reason?.stack, 'unhandledRejection');
});

const rooms = new Map();

// ── MySQL connection pool ─────────────────────────────────────────────────────

let db = null;

if (process.env.MYSQL_URL) {
  try {
    db = mysql.createPool(process.env.MYSQL_URL);
    console.log('[pvp] MySQL connection pool created');
  } catch (err) {
    console.error('[pvp] Failed to create MySQL pool:', err.message);
    reportToLegion(err.message, err.stack, 'error');
    db = null;
  }
} else {
  console.warn('[pvp] MYSQL_URL not set — running without database persistence');
}

// ── Database schema initialisation ───────────────────────────────────────────

async function initDb() {
  if (!db) return;
  try {
    await db.execute(`
      CREATE TABLE IF NOT EXISTS players (
        id         INT          NOT NULL AUTO_INCREMENT,
        socketId   VARCHAR(255) UNIQUE,
        username   VARCHAR(255),
        wins       INT          NOT NULL DEFAULT 0,
        losses     INT          NOT NULL DEFAULT 0,
        totalGames INT          NOT NULL DEFAULT 0,
        createdAt  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updatedAt  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
      )
    `);

    await db.execute(`
      CREATE TABLE IF NOT EXISTS games (
        id          INT          NOT NULL AUTO_INCREMENT,
        roomId      VARCHAR(10)  NOT NULL UNIQUE,
        p1PlayerId  INT,
        p2PlayerId  INT,
        p1Character VARCHAR(255),
        p2Character VARCHAR(255),
        winner      VARCHAR(2),
        gameMode    VARCHAR(50)  NOT NULL DEFAULT '1v1',
        startedAt   TIMESTAMP,
        endedAt     TIMESTAMP,
        createdAt   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        FOREIGN KEY (p1PlayerId) REFERENCES players(id),
        FOREIGN KEY (p2PlayerId) REFERENCES players(id)
      )
    `);

    await db.execute(`
      CREATE TABLE IF NOT EXISTS game_stats (
        id        INT          NOT NULL AUTO_INCREMENT,
        playerId  INT          NOT NULL,
        gameId    INT          NOT NULL,
        character VARCHAR(255) NOT NULL,
        result    VARCHAR(10)  NOT NULL,
        createdAt TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        FOREIGN KEY (playerId) REFERENCES players(id),
        FOREIGN KEY (gameId)   REFERENCES games(id)
      )
    `);

    console.log('[pvp] Database schema initialised');
  } catch (err) {
    console.error('[pvp] Failed to initialise database schema:', err.message);
    reportToLegion(err.message, err.stack, 'error');
  }
}

// ── DB helper: upsert / fetch player by socketId ───────────────────────────────

async function getOrCreatePlayer(socketId) {
  if (!db) return null;
  try {
    const [rows] = await db.execute(
      'SELECT id FROM players WHERE socketId = ?',
      [socketId]
    );
    if (rows.length > 0) return rows[0].id;

    const [result] = await db.execute(
      'INSERT INTO players (socketId, wins, losses, totalGames) VALUES (?, 0, 0, 0)',
      [socketId]
    );
    return result.insertId;
  } catch (err) {
    console.error('[pvp] getOrCreatePlayer error:', err.message);
    reportToLegion(err.message, err.stack, 'error');
    return null;
  }
}

// --- Helpers ---

function generateCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  for (let i = 0; i < 4; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return rooms.has(code) ? generateCode() : code;
}

/** Returns the public-facing shape of a room for lobby listings. */
function roomSummary(room) {
  return {
    roomId: room.id,
    gameMode: room.gameMode,
    maxPlayers: room.maxPlayers,
    playerCount: room.players.length,
    status: room.status,
    createdBy: room.createdBy,
    gameSettings: room.gameSettings,
    createdAt: room.createdAt,
  };
}

/** Removes a room and notifies all lobby clients. */
function removeRoom(roomId) {
  if (!rooms.has(roomId)) return;
  rooms.delete(roomId);
  io.emit("lobby:game-removed", { roomId });
  console.log(`[pvp] room ${roomId} removed`);
}

// --- HTTP server ---

const httpServer = createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost`);

  // ── Health check ───────────────────────────────────────────────────────────
  if (url.pathname === "/health") {
    const allRooms = [...rooms.values()];
    const waitingGames = allRooms.filter((r) => r.status === "waiting").length;
    const inProgressGames = allRooms.filter((r) => r.status === "in-progress").length;
    const totalPlayers = allRooms.reduce((sum, r) => sum + r.players.length, 0);

    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        status: "ok",
        uptime: process.uptime(),
        db: db ? "connected" : "unavailable",
        lobby: { waitingGames, inProgressGames, totalPlayers },
      })
    );
    return;
  }

  // ── GET /stats/:playerId ───────────────────────────────────────────────────
  const statsMatch = url.pathname.match(/^\\/stats\\/(\\d+)$/);
  if (statsMatch) {
    if (!db) {
      res.writeHead(503, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Database unavailable" }));
      return;
    }
    try {
      const [rows] = await db.execute(
        'SELECT id, socketId, username, wins, losses, totalGames, createdAt FROM players WHERE id = ?',
        [statsMatch[1]]
      );
      if (rows.length === 0) {
        res.writeHead(404, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Player not found" }));
        return;
      }
      const p = rows[0];
      const winRate = p.totalGames > 0 ? Math.round((p.wins / p.totalGames) * 100) : 0;
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ...p, winRate }));
    } catch (err) {
      console.error('[pvp] GET /stats error:', err.message);
      reportToLegion(err.message, err.stack, 'error');
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Internal server error" }));
    }
    return;
  }

  // ── GET /games/:roomId ─────────────────────────────────────────────────────
  const gamesMatch = url.pathname.match(/^\\/games\\/([A-Z0-9]+)$/i);
  if (gamesMatch) {
    if (!db) {
      res.writeHead(503, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Database unavailable" }));
      return;
    }
    try {
      const [rows] = await db.execute(
        `SELECT g.*,
                p1.socketId AS p1SocketId, p1.username AS p1Username,
                p2.socketId AS p2SocketId, p2.username AS p2Username
         FROM games g
         LEFT JOIN players p1 ON g.p1PlayerId = p1.id
         LEFT JOIN players p2 ON g.p2PlayerId = p2.id
         WHERE g.roomId = ?`,
        [gamesMatch[1].toUpperCase()]
      );
      if (rows.length === 0) {
        res.writeHead(404, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Game not found" }));
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(rows[0]));
    } catch (err) {
      console.error('[pvp] GET /games error:', err.message);
      reportToLegion(err.message, err.stack, 'error');
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Internal server error" }));
    }
    return;
  }

  // ── GET /leaderboard?limit=10 ──────────────────────────────────────────────
  if (url.pathname === "/leaderboard") {
    if (!db) {
      res.writeHead(503, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Database unavailable" }));
      return;
    }
    try {
      const limit = Math.min(parseInt(url.searchParams.get("limit") || "10", 10), 100);
      const [rows] = await db.execute(
        `SELECT id, socketId, username, wins, losses, totalGames,
                CASE WHEN totalGames > 0 THEN ROUND(wins / totalGames * 100) ELSE 0 END AS winRate
         FROM players
         ORDER BY wins DESC
         LIMIT ?`,
        [limit]
      );
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(rows));
    } catch (err) {
      console.error('[pvp] GET /leaderboard error:', err.message);
      reportToLegion(err.message, err.stack, 'error');
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Internal server error" }));
    }
    return;
  }

  // ── GET /player/:socketId ──────────────────────────────────────────────────
  const playerMatch = url.pathname.match(/^\\/player\\/(.+)$/);
  if (playerMatch) {
    if (!db) {
      res.writeHead(503, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Database unavailable" }));
      return;
    }
    try {
      const [rows] = await db.execute(
        `SELECT id, socketId, username, wins, losses, totalGames,
                CASE WHEN totalGames > 0 THEN ROUND(wins / totalGames * 100) ELSE 0 END AS winRate,
                createdAt
         FROM players WHERE socketId = ?`,
        [playerMatch[1]]
      );
      if (rows.length === 0) {
        res.writeHead(404, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Player not found" }));
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(rows[0]));
    } catch (err) {
      console.error('[pvp] GET /player error:', err.message);
      reportToLegion(err.message, err.stack, 'error');
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Internal server error" }));
    }
    return;
  }

  // ── Default: serve HTML dashboard ──────────────────────────────────────────
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(getHtmlDashboard());
});

function getHtmlDashboard() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Grudge PvP Server</title>
  <style>
    * {
      margin: 0;
      padding: 0;
      box-sizing: border-box;
    }

    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
      background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
      color: #e0e0e0;
      min-height: 100vh;
      padding: 20px;
    }

    .container {
      max-width: 1200px;
      margin: 0 auto;
    }

    header {
      text-align: center;
      margin-bottom: 40px;
      padding: 30px 0;
      border-bottom: 2px solid #e94560;
    }

    h1 {
      font-size: 3em;
      font-weight: 700;
      background: linear-gradient(135deg, #e94560 0%, #ff6b9d 100%);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
      margin-bottom: 10px;
      text-transform: uppercase;
      letter-spacing: 2px;
    }

    .subtitle {
      font-size: 1.1em;
      color: #b0b0b0;
      margin-bottom: 20px;
    }

    .stats-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
      gap: 20px;
      margin-bottom: 40px;
    }

    .stat-card {
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid rgba(233, 69, 96, 0.3);
      border-radius: 12px;
      padding: 25px;
      backdrop-filter: blur(10px);
      transition: all 0.3s ease;
    }

    .stat-card:hover {
      background: rgba(255, 255, 255, 0.08);
      border-color: #e94560;
      transform: translateY(-5px);
      box-shadow: 0 10px 30px rgba(233, 69, 96, 0.2);
    }

    .stat-label {
      font-size: 0.9em;
      color: #888;
      text-transform: uppercase;
      letter-spacing: 1px;
      margin-bottom: 10px;
    }

    .stat-value {
      font-size: 2.5em;
      font-weight: 700;
      color: #e94560;
    }

    .section {
      margin-bottom: 40px;
    }

    .section-title {
      font-size: 1.8em;
      font-weight: 600;
      margin-bottom: 20px;
      padding-bottom: 10px;
      border-bottom: 2px solid #e94560;
      color: #fff;
    }

    .button-group {
      display: flex;
      gap: 15px;
      flex-wrap: wrap;
      margin-bottom: 20px;
    }

    button {
      padding: 12px 24px;
      border: none;
      border-radius: 8px;
      font-size: 1em;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.3s ease;
      text-transform: uppercase;
      letter-spacing: 1px;
    }

    .btn-primary {
      background: linear-gradient(135deg, #e94560 0%, #ff6b9d 100%);
      color: white;
    }

    .btn-primary:hover {
      transform: translateY(-2px);
      box-shadow: 0 8px 20px rgba(233, 69, 96, 0.4);
    }

    .btn-secondary {
      background: rgba(255, 255, 255, 0.1);
      color: #e0e0e0;
      border: 1px solid rgba(233, 69, 96, 0.3);
    }

    .btn-secondary:hover {
      background: rgba(255, 255, 255, 0.15);
      border-color: #e94560;
    }

    .room-list {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
      gap: 20px;
    }

    .room-card {
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid rgba(233, 69, 96, 0.3);
      border-radius: 12px;
      padding: 20px;
      backdrop-filter: blur(10px);
      transition: all 0.3s ease;
    }

    .room-card:hover {
      background: rgba(255, 255, 255, 0.08);
      border-color: #e94560;
      transform: translateY(-5px);
      box-shadow: 0 10px 30px rgba(233, 69, 96, 0.2);
    }

    .room-id {
      font-size: 1.5em;
      font-weight: 700;
      color: #e94560;
      margin-bottom: 10px;
      font-family: 'Courier New', monospace;
    }

    .room-info {
      display: flex;
      justify-content: space-between;
      margin-bottom: 15px;
      font-size: 0.95em;
    }

    .room-status {
      display: inline-block;
      padding: 4px 12px;
      border-radius: 20px;
      font-size: 0.85em;
      font-weight: 600;
      text-transform: uppercase;
    }

    .status-waiting {
      background: rgba(76, 175, 80, 0.2);
      color: #4caf50;
    }

    .status-in-progress {
      background: rgba(255, 193, 7, 0.2);
      color: #ffc107;
    }

    .status-finished {
      background: rgba(244, 67, 54, 0.2);
      color: #f44336;
    }

    .loading {
      text-align: center;
      padding: 40px;
      color: #888;
    }

    .spinner {
      display: inline-block;
      width: 40px;
      height: 40px;
      border: 4px solid rgba(233, 69, 96, 0.2);
      border-top-color: #e94560;
      border-radius: 50%;
      animation: spin 1s linear infinite;
    }

    @keyframes spin {
      to { transform: rotate(360deg); }
    }

    .error {
      background: rgba(244, 67, 54, 0.1);
      border: 1px solid #f44336;
      color: #ff9999;
      padding: 15px;
      border-radius: 8px;
      margin-bottom: 20px;
    }

    .success {
      background: rgba(76, 175, 80, 0.1);
      border: 1px solid #4caf50;
      color: #99ff99;
      padding: 15px;
      border-radius: 8px;
      margin-bottom: 20px;
    }

    footer {
      text-align: center;
      padding: 30px 0;
      border-top: 1px solid rgba(233, 69, 96, 0.2);
      color: #666;
      margin-top: 60px;
    }

    .api-docs {
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid rgba(233, 69, 96, 0.3);
      border-radius: 12px;
      padding: 20px;
      margin-top: 20px;
    }

    .api-endpoint {
      background: rgba(0, 0, 0, 0.3);
      padding: 12px;
      border-radius: 6px;
      margin: 10px 0;
      font-family: 'Courier New', monospace;
      font-size: 0.9em;
      color: #4caf50;
    }

    @media (max-width: 768px) {
      h1 {
        font-size: 2em;
      }

      .stats-grid {
        grid-template-columns: 1fr;
      }

      .room-list {
        grid-template-columns: 1fr;
      }

      .button-group {
        flex-direction: column;
      }

      button {
        width: 100%;
      }
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <h1>⚔️ Grudge PvP</h1>
      <p class="subtitle">Real-time multiplayer battle arena</p>
    </header>

    <div class="stats-grid" id="stats">
      <div class="stat-card">
        <div class="stat-label">Active Games</div>
        <div class="stat-value" id="activeGames">-</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Waiting Rooms</div>
        <div class="stat-value" id="waitingRooms">-</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Total Players</div>
        <div class="stat-value" id="totalPlayers">-</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Server Status</div>
        <div class="stat-value" id="serverStatus" style="color: #4caf50;">✓ Online</div>
      </div>
    </div>

    <div class="section">
      <h2 class="section-title">Quick Actions</h2>
      <div class="button-group">
        <button class="btn-primary" onclick="refreshStats()">🔄 Refresh Stats</button>
        <button class="btn-secondary" onclick="viewLeaderboard()">🏆 View Leaderboard</button>
        <button class="btn-secondary" onclick="viewDocs()">📖 API Docs</button>
      </div>
    </div>

    <div class="section">
      <h2 class="section-title">API Documentation</h2>
      <div class="api-docs">
        <p style="margin-bottom: 15px;">The Grudge PvP Server provides WebSocket and REST APIs for game management.</p>
        
        <h3 style="margin-top: 20px; margin-bottom: 10px; color: #e94560;">REST Endpoints</h3>
        <div class="api-endpoint">GET /health</div>
        <p style="margin-bottom: 15px; color: #aaa;">Check server health and current game statistics</p>

        <div class="api-endpoint">GET /leaderboard?limit=10</div>
        <p style="margin-bottom: 15px; color: #aaa;">Fetch top players by wins (limit: 1-100)</p>

        <div class="api-endpoint">GET /stats/:playerId</div>
        <p style="margin-bottom: 15px; color: #aaa;">Get player statistics by ID</p>

        <div class="api-endpoint">GET /player/:socketId</div>
        <p style="margin-bottom: 15px; color: #aaa;">Get player info by socket ID</p>

        <div class="api-endpoint">GET /games/:roomId</div>
        <p style="margin-bottom: 15px; color: #aaa;">Get game details by room ID</p>

        <h3 style="margin-top: 20px; margin-bottom: 10px; color: #e94560;">WebSocket Events</h3>
        <p style="color: #aaa; margin-bottom: 10px;"><strong>Lobby Events:</strong></p>
        <div class="api-endpoint">lobby:list</div>
        <div class="api-endpoint">lobby:create-game</div>
        <div class="api-endpoint">lobby:join-game</div>

        <p style="color: #aaa; margin-bottom: 10px; margin-top: 15px;"><strong>Room Events:</strong></p>
        <div class="api-endpoint">room:pick</div>
        <div class="api-endpoint">room:ready</div>
        <div class="api-endpoint">fight:start</div>
        <div class="api-endpoint">fight:end</div>

        <p style="color: #aaa; margin-top: 15px; font-size: 0.9em;">
          WebSocket path: <code>/pvp</code> | CORS enabled for all origins
        </p>
      </div>
    </div>

    <footer>
      <p>Grudge PvP Server v1.0 | <a href="/health" style="color: #e94560; text-decoration: none;">Health Check</a></p>
    </footer>
  </div>

  <script>
    async function refreshStats() {
      try {
        const response = await fetch('/health');
        const data = await response.json();
        
        document.getElementById('activeGames').textContent = data.lobby.inProgressGames;
        document.getElementById('waitingRooms').textContent = data.lobby.waitingGames;
        document.getElementById('totalPlayers').textContent = data.lobby.totalPlayers;
        document.getElementById('serverStatus').textContent = data.db === 'connected' ? '✓ Online' : '⚠ Limited';
        document.getElementById('serverStatus').style.color = data.db === 'connected' ? '#4caf50' : '#ffc107';
      } catch (err) {
        console.error('Failed to fetch stats:', err);
        document.getElementById('serverStatus').textContent = '✗ Offline';
        document.getElementById('serverStatus').style.color = '#f44336';
      }
    }

    function viewLeaderboard() {
      alert('Leaderboard feature coming soon! Use GET /leaderboard API endpoint.');
    }

    function viewDocs() {
      alert('API documentation is displayed above. Use the REST endpoints or WebSocket connection to /pvp');
    }

    // Auto-refresh stats every 5 seconds
    refreshStats();
    setInterval(refreshStats, 5000);
  </script>
</body>
</html>`;
}

const io = new Server(httpServer, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  path: "/pvp",
});

// Clean stale rooms every minute (5-minute TTL for non-started rooms)
setInterval(() => {
  const now = Date.now();
  for (const [id, room] of rooms) {
    if (now - room.createdAt > 5 * 60 * 1000 && room.status === "waiting") {
      removeRoom(id);
    }
  }
}, 60000);

// --- Socket.io events ---

io.on("connection", async (socket) => {
  console.log(`[pvp] connected: ${socket.id}`);

  // ── Track player session in DB ─────────────────────────────────────────────
  const playerId = await getOrCreatePlayer(socket.id);
  socket.data.playerId = playerId;

  // ── Lobby ──────────────────────────────────────────────────────────────────

  /** Return all games currently accepting players. */
  socket.on("lobby:list", (cb) => {
    const waiting = [...rooms.values()]
      .filter((r) => r.status === "waiting")
      .map(roomSummary);
    if (typeof cb === "function") cb(waiting);
  });

  /** Create a new game and broadcast it to the lobby. */
  socket.on("lobby:create-game", async (options = {}, cb) => {
    const roomId = generateCode();
    const room = {
      id: roomId,
      players: [{ socketId: socket.id, characterId: null, ready: false, slot: "p1" }],
      gameMode: options.gameMode ?? "1v1",
      maxPlayers: options.maxPlayers ?? 2,
      gameSettings: options.gameSettings ?? {},
      status: "waiting",
      createdBy: socket.id,
      createdAt: Date.now(),
    };
    rooms.set(roomId, room);
    socket.join(roomId);
    console.log(`[pvp] room ${roomId} created by ${socket.id} (mode: ${room.gameMode})`);

    // Persist game record
    if (db) {
      try {
        const [result] = await db.execute(
          'INSERT INTO games (roomId, p1PlayerId, gameMode, startedAt) VALUES (?, ?, ?, NOW())',
          [roomId, socket.data.playerId ?? null, room.gameMode]
        );
        room.gameId = result.insertId;
      } catch (err) {
        console.error('[pvp] Failed to insert game record:', err.message);
        reportToLegion(err.message, err.stack, 'error');
      }
    }

    // Notify all lobby clients about the new game
    io.emit("lobby:game-updated", roomSummary(room));

    if (typeof cb === "function") {
      cb({ success: true, roomId, slot: "p1", room: roomSummary(room) });
    }
  });

  /** Join an existing game by roomId. */
  socket.on("lobby:join-game", (roomId, cb) => {
    const room = rooms.get(typeof roomId === "string" ? roomId.toUpperCase() : roomId);
    if (!room) return typeof cb === "function" && cb({ success: false, error: "Room not found" });
    if (room.status !== "waiting") return typeof cb === "function" && cb({ success: false, error: "Game is not open for joining" });
    if (room.players.length >= room.maxPlayers) return typeof cb === "function" && cb({ success: false, error: "Room is full" });

    room.players.push({ socketId: socket.id, characterId: null, ready: false, slot: "p2" });
    socket.join(room.id);
    console.log(`[pvp] ${socket.id} joined ${room.id} via lobby`);

    // Notify the creator that an opponent joined
    const p1 = room.players.find((p) => p.slot === "p1");
    if (p1) io.to(p1.socketId).emit("room:opponent-joined");

    // Broadcast updated game state to all lobby clients
    io.emit("lobby:game-updated", roomSummary(room));

    if (typeof cb === "function") {
      cb({ success: true, slot: "p2", roomId: room.id, room: roomSummary(room), opponentCharacter: p1?.characterId ?? null });
    }
  });

  // ── Legacy room events (kept for backwards compatibility) ──────────────────

  /** @deprecated Use lobby:create-game instead. */
  socket.on("room:create", (cb) => {
    const roomId = generateCode();
    const room = {
      id: roomId,
      players: [{ socketId: socket.id, characterId: null, ready: false, slot: "p1" }],
      gameMode: "1v1",
      maxPlayers: 2,
      gameSettings: {},
      status: "waiting",
      createdBy: socket.id,
      createdAt: Date.now(),
    };
    rooms.set(roomId, room);
    socket.join(roomId);
    console.log(`[pvp] room ${roomId} created (legacy)`);

    io.emit("lobby:game-updated", roomSummary(room));

    if (typeof cb === "function") cb({ roomId, slot: "p1" });
  });

  /** @deprecated Use lobby:join-game instead. */
  socket.on("room:join", (roomId, cb) => {
    const room = rooms.get(roomId.toUpperCase());
    if (!room) return typeof cb === "function" && cb({ success: false, error: "Room not found" });
    if (room.players.length >= room.maxPlayers) return typeof cb === "function" && cb({ success: false, error: "Room is full" });
    if (room.status !== "waiting") return typeof cb === "function" && cb({ success: false, error: "Already started" });

    room.players.push({ socketId: socket.id, characterId: null, ready: false, slot: "p2" });
    socket.join(roomId);
    console.log(`[pvp] ${socket.id} joined ${roomId} (legacy)`);

    const p1 = room.players.find((p) => p.slot === "p1");
    if (p1) io.to(p1.socketId).emit("room:opponent-joined");

    // Reflect the join in the lobby
    io.emit("lobby:game-updated", roomSummary(room));

    if (typeof cb === "function") {
      cb({ success: true, slot: "p2", opponentCharacter: p1?.characterId ?? null });
    }
  });

  // ── In-room events ─────────────────────────────────────────────────────────

  socket.on("room:pick", (data) => {
    const room = rooms.get(data.roomId);
    if (!room) return;
    const player = room.players.find((p) => p.socketId === socket.id);
    if (!player) return;
    player.characterId = data.characterId;
    const opponent = room.players.find((p) => p.socketId !== socket.id);
    if (opponent) io.to(opponent.socketId).emit("room:opponent-picked", { characterId: data.characterId });
  });

  socket.on("room:ready", async (data) => {
    const room = rooms.get(data.roomId);
    if (!room) return;
    const player = room.players.find((p) => p.socketId === socket.id);
    if (!player || !player.characterId) return;
    player.ready = true;

    if (room.players.length === 2 && room.players.every((p) => p.ready)) {
      room.status = "in-progress";
      const p1 = room.players.find((p) => p.slot === "p1");
      const p2 = room.players.find((p) => p.slot === "p2");
      console.log(`[pvp] room ${data.roomId} starting: ${p1.characterId} vs ${p2.characterId}`);

      // Update game record with p2 info and character selections
      if (db && room.gameId) {
        try {
          const p2PlayerId = room.players.find((p) => p.slot === "p2")
            ? await (async () => {
                const [rows] = await db.execute(
                  'SELECT id FROM players WHERE socketId = ?',
                  [p2.socketId]
                );
                return rows[0]?.id ?? null;
              })()
            : null;

          await db.execute(
            `UPDATE games
             SET p2PlayerId = ?, p1Character = ?, p2Character = ?, startedAt = NOW()
             WHERE id = ?`,
            [p2PlayerId, p1.characterId, p2.characterId, room.gameId]
          );
        } catch (err) {
          console.error('[pvp] Failed to update game record on start:', err.message);
          reportToLegion(err.message, err.stack, 'error');
        }
      }

      // Remove from lobby view
      io.emit("lobby:game-updated", roomSummary(room));

      io.to(data.roomId).emit("fight:start", {
        p1Character: p1.characterId,
        p2Character: p2.characterId,
      });
    }
  });

  /** Clients emit this when the fight concludes. data: { roomId, winner: 'p1'|'p2' } */
  socket.on("fight:end", async (data) => {
    const room = rooms.get(data.roomId);
    if (!room || !data.winner) return;

    const p1 = room.players.find((p) => p.slot === "p1");
    const p2 = room.players.find((p) => p.slot === "p2");
    console.log(`[pvp] room ${data.roomId} ended — winner: ${data.winner}`);

    if (db && room.gameId && p1 && p2) {
      try {
        // Resolve player IDs
        const [p1Rows] = await db.execute('SELECT id FROM players WHERE socketId = ?', [p1.socketId]);
        const [p2Rows] = await db.execute('SELECT id FROM players WHERE socketId = ?', [p2.socketId]);
        const p1DbId = p1Rows[0]?.id ?? null;
        const p2DbId = p2Rows[0]?.id ?? null;

        // Finalise game record
        await db.execute(
          'UPDATE games SET winner = ?, endedAt = NOW() WHERE id = ?',
          [data.winner, room.gameId]
        );

        // Insert game_stats for both players
        if (p1DbId) {
          await db.execute(
            'INSERT INTO game_stats (playerId, gameId, character, result) VALUES (?, ?, ?, ?)',
            [p1DbId, room.gameId, p1.characterId ?? '', data.winner === 'p1' ? 'win' : 'loss']
          );
        }
        if (p2DbId) {
          await db.execute(
            'INSERT INTO game_stats (playerId, gameId, character, result) VALUES (?, ?, ?, ?)',
            [p2DbId, room.gameId, p2.characterId ?? '', data.winner === 'p2' ? 'win' : 'loss']
          );
        }

        // Update player win/loss counts
        if (p1DbId) {
          const p1Win = data.winner === 'p1';
          await db.execute(
            `UPDATE players SET wins = wins + ?, losses = losses + ?, totalGames = totalGames + 1 WHERE id = ?`,
            [p1Win ? 1 : 0, p1Win ? 0 : 1, p1DbId]
          );
        }
        if (p2DbId) {
          const p2Win = data.winner === 'p2';
          await db.execute(
            `UPDATE players SET wins = wins + ?, losses = losses + ?, totalGames = totalGames + 1 WHERE id = ?`,
            [p2Win ? 1 : 0, p2Win ? 0 : 1, p2DbId]
          );
        }
      } catch (err) {
        console.error('[pvp] Failed to record fight:end:', err.message);
        reportToLegion(err.message, err.stack, 'error');
      }
    }
  });

  socket.on("input", (data) => socket.to(data.roomId).emit("input:remote", { frame: data.frame, keys: data.keys }));
  socket.on("action", (data) => socket.to(data.roomId).emit("action:remote", { action: data.action, params: data.params }));

  socket.on("disconnect", () => {
    console.log(`[pvp] disconnected: ${socket.id}`);
    for (const [roomId, room] of rooms) {
      const idx = room.players.findIndex((p) => p.socketId === socket.id);
      if (idx !== -1) {
        room.players.splice(idx, 1);
        io.to(roomId).emit("room:opponent-left");

        if (room.players.length === 0) {
          removeRoom(roomId);
        } else {
          // Room still has players — mark finished and update lobby
          room.status = "finished";
          io.emit("lobby:game-updated", roomSummary(room));
        }
      }
    }
  });
});

httpServer.listen(PORT, "0.0.0.0", async () => {
  console.log(`[pvp] Grudge PvP Server running on port ${PORT}`);
  await initDb();
  if (db) console.log('[pvp] Database ready');
});

