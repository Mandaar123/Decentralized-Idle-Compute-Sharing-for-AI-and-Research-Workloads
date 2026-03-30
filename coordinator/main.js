/**
 * MANDAAR — Express.js Backend (Brain of the system)
 * Run: node main.js
 * Or with auto-reload: npx nodemon main.js
 *
 * Required packages:
 *   npm install express better-sqlite3 jsonwebtoken bcryptjs cors uuid
 */

const express = require("express");
const Database = require("better-sqlite3");
const jwt = require("jsonwebtoken");
const bcrypt = require("bcryptjs");
const cors = require("cors");
const { v4: uuidv4 } = require("uuid");
const crypto = require("crypto");

const app = express();
const PORT = 8000;
const JWT_SECRET = "hackathon_secret_2025";
const DB_PATH = "compute.db";

// ─────────────────────────────────────────────
// MIDDLEWARE
// ─────────────────────────────────────────────

app.use(cors({ origin: "*", methods: "*", allowedHeaders: "*" }));
app.use(express.json());

// ─────────────────────────────────────────────
// DATABASE SETUP
// ─────────────────────────────────────────────

const db = new Database(DB_PATH);

// Enable WAL mode for better concurrent performance
db.pragma("journal_mode = WAL");

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT UNIQUE,
    password_hash TEXT,
    credits INTEGER DEFAULT 200,
    role TEXT DEFAULT 'submitter',
    created_at REAL
  );

  CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    username TEXT,
    cpu REAL DEFAULT 0,
    ram REAL DEFAULT 0,
    trust_score INTEGER DEFAULT 50,
    is_online INTEGER DEFAULT 0,
    current_task TEXT,
    last_seen REAL,
    total_completed INTEGER DEFAULT 0
  );

  CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    submitter_id TEXT,
    code TEXT,
    task_type TEXT DEFAULT 'python',
    status TEXT DEFAULT 'queued',
    worker_id TEXT,
    result TEXT,
    progress INTEGER DEFAULT 0,
    estimated_time INTEGER DEFAULT 60,
    created_at REAL,
    started_at REAL,
    completed_at REAL
  );

  CREATE TABLE IF NOT EXISTS credits_log (
    id TEXT PRIMARY KEY,
    user_id TEXT,
    amount INTEGER,
    reason TEXT,
    task_id TEXT,
    created_at REAL
  );
`);

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────

function hashPw(password) {
  return crypto.createHash("sha256").update(password).digest("hex");
}

function makeToken(userId) {
  return jwt.sign({ sub: userId }, JWT_SECRET, { expiresIn: "24h" });
}

function logCredit(userId, amount, reason, taskId = null) {
  db.prepare(
    "INSERT INTO credits_log VALUES (?,?,?,?,?,?)"
  ).run(uuidv4(), userId, amount, reason, taskId, Date.now() / 1000);

  db.prepare(
    "UPDATE users SET credits = credits + ? WHERE id = ?"
  ).run(amount, userId);
}

// ─────────────────────────────────────────────
// AUTH MIDDLEWARE
// ─────────────────────────────────────────────

function getCurrentUser(req, res, next) {
  try {
    const authHeader = req.headers["authorization"];
    if (!authHeader) return res.status(401).json({ detail: "Missing authorization header" });

    const token = authHeader.replace("Bearer ", "");
    const data = jwt.verify(token, JWT_SECRET);

    const user = db.prepare("SELECT * FROM users WHERE id = ?").get(data.sub);
    if (!user) return res.status(401).json({ detail: "User not found" });

    req.user = user;
    next();
  } catch {
    return res.status(401).json({ detail: "Invalid token" });
  }
}

// ─────────────────────────────────────────────
// AUTH ENDPOINTS
// ─────────────────────────────────────────────

// POST /auth/register
app.post("/auth/register", (req, res) => {
  const { username, password, role = "submitter" } = req.body;
  if (!username || !password)
    return res.status(400).json({ detail: "username and password required" });

  const userId = uuidv4();
  try {
    db.prepare(
      "INSERT INTO users VALUES (?,?,?,?,?,?)"
    ).run(userId, username, hashPw(password), 200, role, Date.now() / 1000);

    logCredit(userId, 0, "account_created");

    return res.json({
      token: makeToken(userId),
      user_id: userId,
      username,
      credits: 200,
    });
  } catch (e) {
    if (e.message.includes("UNIQUE")) {
      return res.status(400).json({ detail: "Username already exists" });
    }
    return res.status(500).json({ detail: e.message });
  }
});

// POST /auth/login
app.post("/auth/login", (req, res) => {
  const { username, password } = req.body;
  const user = db.prepare(
    "SELECT * FROM users WHERE username = ? AND password_hash = ?"
  ).get(username, hashPw(password));

  if (!user) return res.status(401).json({ detail: "Invalid credentials" });

  return res.json({
    token: makeToken(user.id),
    user_id: user.id,
    username: user.username,
    credits: user.credits,
  });
});

// ─────────────────────────────────────────────
// TASK ENDPOINTS
// ─────────────────────────────────────────────

// POST /tasks/submit
app.post("/tasks/submit", getCurrentUser, (req, res) => {
  const { code, task_type = "python" } = req.body;
  const user = req.user;

  if (user.credits < 50)
    return res.status(400).json({ detail: "Insufficient credits. Need 50 to submit a task." });

  const estimated = Math.min(30 + Math.floor(code.length / 10), 300);
  const taskId = uuidv4();

  db.prepare(
    "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
  ).run(taskId, user.id, code, task_type, "queued", null, null, 0, estimated, Date.now() / 1000, null, null);

  logCredit(user.id, -50, "task_submitted", taskId);

  return res.json({ task_id: taskId, status: "queued", estimated_time: estimated });
});

// GET /tasks/:task_id/status
app.get("/tasks/:task_id/status", (req, res) => {
  const task = db.prepare("SELECT * FROM tasks WHERE id = ?").get(req.params.task_id);
  if (!task) return res.status(404).json({ detail: "Task not found" });
  return res.json(task);
});

// GET /tasks/list/all
app.get("/tasks/list/all", getCurrentUser, (req, res) => {
  const tasks = db.prepare(
    "SELECT * FROM tasks WHERE submitter_id = ? ORDER BY created_at DESC"
  ).all(req.user.id);
  return res.json(tasks);
});

// GET /tasks/all/live
app.get("/tasks/all/live", (req, res) => {
  const tasks = db.prepare(
    "SELECT * FROM tasks ORDER BY created_at DESC LIMIT 50"
  ).all();
  return res.json(tasks);
});

// POST /tasks/result
app.post("/tasks/result", (req, res) => {
  const { worker_id, task_id, output, success } = req.body;
  const status = success ? "completed" : "failed";

  db.prepare(
    "UPDATE tasks SET status = ?, result = ?, progress = 100, completed_at = ? WHERE id = ?"
  ).run(status, output, Date.now() / 1000, task_id);

  db.prepare(
    "UPDATE workers SET current_task = NULL, is_online = 1 WHERE id = ?"
  ).run(worker_id);

  if (success) {
    db.prepare(
      "UPDATE workers SET trust_score = MIN(100, trust_score + 5), total_completed = total_completed + 1 WHERE id = ?"
    ).run(worker_id);

    const worker = db.prepare("SELECT * FROM workers WHERE id = ?").get(worker_id);
    if (worker) {
      const workerUser = db.prepare("SELECT * FROM users WHERE username = ?").get(worker.username);
      if (workerUser) {
        db.prepare("UPDATE users SET credits = credits + 10 WHERE id = ?").run(workerUser.id);
        db.prepare("INSERT INTO credits_log VALUES (?,?,?,?,?,?)").run(
          uuidv4(), workerUser.id, 10, "task_completed", task_id, Date.now() / 1000
        );
      }
    }
  } else {
    db.prepare(
      "UPDATE workers SET trust_score = MAX(0, trust_score - 10) WHERE id = ?"
    ).run(worker_id);
  }

  return res.json({ ok: true });
});

// POST /tasks/:task_id/progress
app.post("/tasks/:task_id/progress", (req, res) => {
  const { progress } = req.body;
  db.prepare("UPDATE tasks SET progress = ? WHERE id = ?").run(progress, req.params.task_id);
  return res.json({ ok: true });
});

// ─────────────────────────────────────────────
// WORKER ENDPOINTS
// ─────────────────────────────────────────────

// POST /workers/register
app.post("/workers/register", (req, res) => {
  const { username } = req.body;
  const workerId = uuidv4();

  db.prepare(
    "INSERT OR REPLACE INTO workers VALUES (?,?,?,?,?,?,?,?,?)"
  ).run(workerId, username, 0, 0, 50, 1, null, Date.now() / 1000, 0);

  return res.json({ worker_id: workerId, token: makeToken(workerId) });
});

// POST /workers/heartbeat
app.post("/workers/heartbeat", (req, res) => {
  const { worker_id, cpu, ram, current_task = null } = req.body;

  db.prepare(
    "UPDATE workers SET cpu = ?, ram = ?, is_online = 1, last_seen = ?, current_task = ? WHERE id = ?"
  ).run(cpu, ram, Date.now() / 1000, current_task, worker_id);

  return res.json({ ok: true });
});

// GET /workers/next-job/:worker_id
app.get("/workers/next-job/:worker_id", (req, res) => {
  const { worker_id } = req.params;
  const worker = db.prepare("SELECT * FROM workers WHERE id = ?").get(worker_id);

  if (!worker || worker.current_task) return res.json({ task_id: null });

  const task = db.prepare(
    "SELECT * FROM tasks WHERE worker_id = ? AND status = 'assigned'"
  ).get(worker_id);

  if (task) {
    db.prepare(
      "UPDATE tasks SET status = 'running', started_at = ? WHERE id = ?"
    ).run(Date.now() / 1000, task.id);

    db.prepare(
      "UPDATE workers SET current_task = ? WHERE id = ?"
    ).run(task.id, worker_id);

    return res.json(task);
  }

  return res.json({ task_id: null });
});

// GET /workers/list
app.get("/workers/list", (req, res) => {
  const workers = db.prepare(
    "SELECT * FROM workers ORDER BY trust_score DESC"
  ).all();
  return res.json(workers);
});

// ─────────────────────────────────────────────
// CREDITS ENDPOINTS
// ─────────────────────────────────────────────

// GET /credits/balance
app.get("/credits/balance", getCurrentUser, (req, res) => {
  const history = db.prepare(
    "SELECT * FROM credits_log WHERE user_id = ? ORDER BY created_at DESC LIMIT 20"
  ).all(req.user.id);

  return res.json({ credits: req.user.credits, history });
});

// ─────────────────────────────────────────────
// STATS ENDPOINT
// ─────────────────────────────────────────────

// GET /stats
app.get("/stats", (req, res) => {
  const total_tasks     = db.prepare("SELECT COUNT(*) as c FROM tasks").get().c;
  const completed_tasks = db.prepare("SELECT COUNT(*) as c FROM tasks WHERE status='completed'").get().c;
  const running_tasks   = db.prepare("SELECT COUNT(*) as c FROM tasks WHERE status='running'").get().c;
  const online_workers  = db.prepare("SELECT COUNT(*) as c FROM workers WHERE is_online=1").get().c;
  const total_users     = db.prepare("SELECT COUNT(*) as c FROM users").get().c;

  return res.json({ total_tasks, completed_tasks, running_tasks, online_workers, total_users });
});

// ─────────────────────────────────────────────
// ROOT
// ─────────────────────────────────────────────

app.get("/", (req, res) => {
  res.json({ status: "Distributed Compute Platform running", version: "1.0" });
});

// ─────────────────────────────────────────────
// BACKGROUND: SCHEDULER + HEARTBEAT CHECKER
// ─────────────────────────────────────────────

function schedulerLoop() {
  try {
    const queued = db.prepare(
      "SELECT * FROM tasks WHERE status = 'queued' ORDER BY created_at ASC"
    ).all();

    for (const task of queued) {
      const worker = db.prepare(`
        SELECT * FROM workers
        WHERE is_online = 1 AND current_task IS NULL AND trust_score >= 40
        ORDER BY cpu ASC, trust_score DESC
        LIMIT 1
      `).get();

      if (worker) {
        db.prepare(
          "UPDATE tasks SET status = 'assigned', worker_id = ? WHERE id = ?"
        ).run(worker.id, task.id);
      }
    }
  } catch (e) {
    console.error("Scheduler error:", e.message);
  }
}

function heartbeatChecker() {
  try {
    const staleTime = Date.now() / 1000 - 30;
    const staleWorkers = db.prepare(
      "SELECT * FROM workers WHERE last_seen < ? AND is_online = 1"
    ).all(staleTime);

    for (const worker of staleWorkers) {
      db.prepare("UPDATE workers SET is_online = 0 WHERE id = ?").run(worker.id);
      db.prepare(
        "UPDATE workers SET trust_score = MAX(0, trust_score - 15) WHERE id = ?"
      ).run(worker.id);
      db.prepare(`
        UPDATE tasks SET status = 'queued', worker_id = NULL, started_at = NULL
        WHERE worker_id = ? AND status IN ('running', 'assigned')
      `).run(worker.id);

      console.log(`Worker ${worker.id} went offline — tasks re-queued`);
    }
  } catch (e) {
    console.error("Heartbeat checker error:", e.message);
  }
}

// Run scheduler every 5s, heartbeat checker every 15s
setInterval(schedulerLoop, 5000);
setInterval(heartbeatChecker, 15000);

// ─────────────────────────────────────────────
// START SERVER
// ─────────────────────────────────────────────

app.listen(PORT, "0.0.0.0", () => {
  console.log(`✅ Distributed Compute Platform running on http://0.0.0.0:${PORT}`);
});
