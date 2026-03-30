/**
 * MANDAAR — Express.js Backend (Brain of the system)
 * Run: node main.js
 * Or with auto-reload: npx nodemon main.js
 *
 * Required packages:
 *   npm install express better-sqlite3 jsonwebtoken bcryptjs cors uuid
 *
 * NEW FEATURES:
 *   1. Failure Recovery       — stale worker detection re-queues tasks from last saved progress
 *   2. Credit Based System    — full credit economy with earn/spend tracking
 *   3. Network Visualization  — /network/snapshot returns live topology data
 *   4. Estimated Execution Time — per-task ETA based on code complexity + node capability
 *   5. Dynamic Task Assignment — picks best node by CPU load + trust score
 *   6. Real-Time Monitoring   — SSE /events stream for live dashboard updates
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
    total_completed INTEGER DEFAULT 0,
    avg_task_duration REAL DEFAULT 60
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
    actual_duration REAL,
    created_at REAL,
    started_at REAL,
    completed_at REAL,
    retry_count INTEGER DEFAULT 0,
    last_checkpoint INTEGER DEFAULT 0
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
  db.prepare("INSERT INTO credits_log VALUES (?,?,?,?,?,?)").run(
    uuidv4(), userId, amount, reason, taskId, Date.now() / 1000
  );
  db.prepare("UPDATE users SET credits = credits + ? WHERE id = ?").run(amount, userId);
}

// ─────────────────────────────────────────────
// FEATURE 4: ESTIMATED EXECUTION TIME
// Estimate based on code length, loops/complexity hints, and worker's avg duration
// ─────────────────────────────────────────────

function estimateExecutionTime(code, workerId = null) {
  let base = 10; // seconds

  // Code length factor
  base += Math.floor(code.length / 100) * 2;

  // Complexity hints in code
  const loopMatches = (code.match(/\bfor\b|\bwhile\b/g) || []).length;
  const importMatches = (code.match(/\bimport\b/g) || []).length;
  const mathMatches = (code.match(/numpy|scipy|pandas|sklearn|torch|tensorflow/g) || []).length;

  base += loopMatches * 3;
  base += importMatches * 2;
  base += mathMatches * 10;

  // Adjust by worker's historical avg if known
  if (workerId) {
    const worker = db.prepare("SELECT avg_task_duration FROM workers WHERE id = ?").get(workerId);
    if (worker && worker.avg_task_duration) {
      base = Math.round((base + worker.avg_task_duration) / 2);
    }
  }

  return Math.min(Math.max(base, 5), 600); // clamp 5s – 600s
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
// REAL-TIME SSE (FEATURE 6: Real-Time Monitoring)
// ─────────────────────────────────────────────

const sseClients = new Set();

function broadcastEvent(eventName, data) {
  const payload = `event: ${eventName}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const client of sseClients) {
    try { client.write(payload); } catch (_) { sseClients.delete(client); }
  }
}

// GET /events — SSE stream for live dashboard updates
app.get("/events", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  sseClients.add(res);

  // Send current snapshot immediately on connect
  const snapshot = buildNetworkSnapshot();
  res.write(`event: snapshot\ndata: ${JSON.stringify(snapshot)}\n\n`);

  req.on("close", () => sseClients.delete(res));
});

// ─────────────────────────────────────────────
// AUTH ENDPOINTS
// ─────────────────────────────────────────────

app.post("/auth/register", (req, res) => {
  const { username, password, role = "submitter" } = req.body;
  if (!username || !password)
    return res.status(400).json({ detail: "username and password required" });

  const userId = uuidv4();
  try {
    db.prepare("INSERT INTO users VALUES (?,?,?,?,?,?)").run(
      userId, username, hashPw(password), 200, role, Date.now() / 1000
    );
    logCredit(userId, 0, "account_created");
    return res.json({ token: makeToken(userId), user_id: userId, username, credits: 200 });
  } catch (e) {
    if (e.message.includes("UNIQUE"))
      return res.status(400).json({ detail: "Username already exists" });
    return res.status(500).json({ detail: e.message });
  }
});

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

  // FEATURE 4: Estimate time using smart estimator
  const estimated = estimateExecutionTime(code);
  const taskId = uuidv4();

  db.prepare(
    "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
  ).run(taskId, user.id, code, task_type, "queued", null, null, 0, estimated, null, Date.now() / 1000, null, null, 0, 0);

  // FEATURE 2: Deduct credits on submission
  logCredit(user.id, -50, "task_submitted", taskId);

  // FEATURE 6: Broadcast new task event
  broadcastEvent("task_queued", { task_id: taskId, estimated_time: estimated, submitter: user.username });

  return res.json({ task_id: taskId, status: "queued", estimated_time: estimated });
});

// GET /tasks/:task_id/status
app.get("/tasks/:task_id/status", (req, res) => {
  const task = db.prepare("SELECT * FROM tasks WHERE id = ?").get(req.params.task_id);
  if (!task) return res.status(404).json({ detail: "Task not found" });

  // FEATURE 4: Include live ETA based on progress
  let liveEta = null;
  if (task.status === "running" && task.started_at) {
    const elapsed = Date.now() / 1000 - task.started_at;
    const pct = task.progress || 1;
    liveEta = Math.max(0, Math.round((elapsed / pct) * (100 - pct)));
  }

  return res.json({ ...task, live_eta_seconds: liveEta });
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
  const now = Date.now() / 1000;

  const task = db.prepare("SELECT * FROM tasks WHERE id = ?").get(task_id);
  const actualDuration = task && task.started_at ? now - task.started_at : null;

  db.prepare(
    "UPDATE tasks SET status = ?, result = ?, progress = 100, completed_at = ?, actual_duration = ? WHERE id = ?"
  ).run(status, output, now, actualDuration, task_id);

  db.prepare("UPDATE workers SET current_task = NULL, is_online = 1 WHERE id = ?").run(worker_id);

  if (success) {
    // Trust score boost
    db.prepare(
      "UPDATE workers SET trust_score = MIN(100, trust_score + 5), total_completed = total_completed + 1 WHERE id = ?"
    ).run(worker_id);

    // Update worker's avg task duration for better future estimates (FEATURE 4)
    if (actualDuration) {
      db.prepare(`
        UPDATE workers SET avg_task_duration = (avg_task_duration * total_completed + ?) / (total_completed + 1)
        WHERE id = ?
      `).run(actualDuration, worker_id);
    }

    // FEATURE 2: Reward worker with credits
    const worker = db.prepare("SELECT * FROM workers WHERE id = ?").get(worker_id);
    if (worker) {
      const workerUser = db.prepare("SELECT * FROM users WHERE username = ?").get(worker.username);
      if (workerUser) {
        logCredit(workerUser.id, 10, "task_completed", task_id);
      }
    }
  } else {
    db.prepare(
      "UPDATE workers SET trust_score = MAX(0, trust_score - 10) WHERE id = ?"
    ).run(worker_id);
  }

  // FEATURE 6: Broadcast task completed/failed
  broadcastEvent("task_update", { task_id, status, worker_id, actual_duration: actualDuration });

  return res.json({ ok: true });
});

// POST /tasks/:task_id/progress  (also handles FEATURE 1 checkpoint saving)
app.post("/tasks/:task_id/progress", (req, res) => {
  const { progress } = req.query;
  const { checkpoint } = req.body || {};
  const p = parseInt(progress || req.body?.progress || 0);

  db.prepare(
    "UPDATE tasks SET progress = ?, last_checkpoint = ? WHERE id = ?"
  ).run(p, checkpoint ?? p, req.params.task_id);

  // FEATURE 6: Broadcast progress update
  broadcastEvent("task_progress", { task_id: req.params.task_id, progress: p });

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
    "INSERT OR REPLACE INTO workers VALUES (?,?,?,?,?,?,?,?,?,?)"
  ).run(workerId, username, 0, 0, 50, 1, null, Date.now() / 1000, 0, 60);

  // FEATURE 6: Broadcast new worker joined
  broadcastEvent("worker_joined", { worker_id: workerId, username });

  return res.json({ worker_id: workerId, token: makeToken(workerId) });
});

// POST /workers/heartbeat
app.post("/workers/heartbeat", (req, res) => {
  const { worker_id, cpu, ram, current_task = null } = req.body;

  db.prepare(
    "UPDATE workers SET cpu = ?, ram = ?, is_online = 1, last_seen = ?, current_task = ? WHERE id = ?"
  ).run(cpu, ram, Date.now() / 1000, current_task, worker_id);

  // FEATURE 6: Broadcast worker stats
  broadcastEvent("worker_heartbeat", { worker_id, cpu, ram, current_task });

  return res.json({ ok: true });
});

// GET /workers/next-job/:worker_id  (FEATURE 5: Dynamic Task Assignment)
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
    db.prepare("UPDATE workers SET current_task = ? WHERE id = ?").run(task.id, worker_id);

    // FEATURE 4: Return refined ETA for this specific worker
    const refinedEta = estimateExecutionTime(task.code, worker_id);

    broadcastEvent("task_started", { task_id: task.id, worker_id, estimated_time: refinedEta });

    return res.json({ ...task, estimated_time: refinedEta });
  }

  return res.json({ task_id: null });
});

// GET /workers/list
app.get("/workers/list", (req, res) => {
  const workers = db.prepare("SELECT * FROM workers ORDER BY trust_score DESC").all();
  return res.json(workers);
});

// ─────────────────────────────────────────────
// FEATURE 3: NETWORK VISUALIZATION
// ─────────────────────────────────────────────

function buildNetworkSnapshot() {
  const workers = db.prepare("SELECT * FROM workers ORDER BY trust_score DESC").all();
  const recentTasks = db.prepare(
    "SELECT id, status, worker_id, submitter_id, progress FROM tasks WHERE created_at > ? ORDER BY created_at DESC LIMIT 30"
  ).all(Date.now() / 1000 - 300); // last 5 minutes

  const nodes = workers.map((w) => ({
    id: w.id,
    label: w.username,
    status: w.is_online ? (w.current_task ? "busy" : "idle") : "offline",
    cpu: w.cpu,
    ram: w.ram,
    trust_score: w.trust_score,
    total_completed: w.total_completed,
  }));

  const edges = recentTasks
    .filter((t) => t.worker_id && t.submitter_id)
    .map((t) => ({
      from: t.submitter_id,
      to: t.worker_id,
      task_id: t.id,
      status: t.status,
      progress: t.progress,
    }));

  return { nodes, edges, timestamp: Date.now() };
}

// GET /network/snapshot — live topology for visualization
app.get("/network/snapshot", (req, res) => {
  return res.json(buildNetworkSnapshot());
});

// ─────────────────────────────────────────────
// CREDITS ENDPOINTS  (FEATURE 2: Credit Based System)
// ─────────────────────────────────────────────

app.get("/credits/balance", getCurrentUser, (req, res) => {
  // Re-fetch fresh credit count
  const user = db.prepare("SELECT * FROM users WHERE id = ?").get(req.user.id);
  const history = db.prepare(
    "SELECT * FROM credits_log WHERE user_id = ? ORDER BY created_at DESC LIMIT 20"
  ).all(req.user.id);

  return res.json({ credits: user.credits, history });
});

// POST /credits/award — manually award credits (admin/testing)
app.post("/credits/award", getCurrentUser, (req, res) => {
  const { target_username, amount, reason = "manual_award" } = req.body;
  const target = db.prepare("SELECT * FROM users WHERE username = ?").get(target_username);
  if (!target) return res.status(404).json({ detail: "User not found" });

  logCredit(target.id, amount, reason);
  return res.json({ ok: true, new_balance: target.credits + amount });
});

// GET /credits/leaderboard — top earners
app.get("/credits/leaderboard", (req, res) => {
  const rows = db.prepare(
    "SELECT username, credits FROM users ORDER BY credits DESC LIMIT 10"
  ).all();
  return res.json(rows);
});

// ─────────────────────────────────────────────
// STATS ENDPOINT
// ─────────────────────────────────────────────

app.get("/stats", (req, res) => {
  const total_tasks     = db.prepare("SELECT COUNT(*) as c FROM tasks").get().c;
  const completed_tasks = db.prepare("SELECT COUNT(*) as c FROM tasks WHERE status='completed'").get().c;
  const running_tasks   = db.prepare("SELECT COUNT(*) as c FROM tasks WHERE status='running'").get().c;
  const online_workers  = db.prepare("SELECT COUNT(*) as c FROM workers WHERE is_online=1").get().c;
  const total_users     = db.prepare("SELECT COUNT(*) as c FROM users").get().c;
  const queued_tasks    = db.prepare("SELECT COUNT(*) as c FROM tasks WHERE status='queued'").get().c;
  const failed_tasks    = db.prepare("SELECT COUNT(*) as c FROM tasks WHERE status='failed'").get().c;

  const avg_eta = db.prepare(
    "SELECT AVG(estimated_time) as a FROM tasks WHERE status='queued'"
  ).get().a;

  return res.json({
    total_tasks, completed_tasks, running_tasks,
    online_workers, total_users, queued_tasks, failed_tasks,
    avg_queue_eta_seconds: avg_eta ? Math.round(avg_eta) : null,
  });
});

app.get("/", (req, res) => {
  res.json({ status: "Distributed Compute Platform running", version: "2.0" });
});

// ─────────────────────────────────────────────
// BACKGROUND: SCHEDULER + HEARTBEAT CHECKER
// ─────────────────────────────────────────────

// FEATURE 5: Dynamic Task Assignment — picks best worker
function schedulerLoop() {
  try {
    const queued = db.prepare(
      "SELECT * FROM tasks WHERE status = 'queued' ORDER BY created_at ASC"
    ).all();

    for (const task of queued) {
      // Pick most suitable worker: online, free, trusted, lowest CPU
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

        broadcastEvent("task_assigned", { task_id: task.id, worker_id: worker.id, worker_name: worker.username });
        console.log(`[SCHEDULER] Task ${task.id} → Worker ${worker.username} (CPU:${worker.cpu}% Trust:${worker.trust_score})`);
      }
    }
  } catch (e) {
    console.error("Scheduler error:", e.message);
  }
}

// FEATURE 1: Failure Recovery — re-queues from last checkpoint on worker drop
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

      // Re-queue abandoned tasks; preserve last_checkpoint so workers can resume
      const abandoned = db.prepare(`
        SELECT * FROM tasks WHERE worker_id = ? AND status IN ('running', 'assigned')
      `).all(worker.id);

      for (const task of abandoned) {
        db.prepare(`
          UPDATE tasks SET status = 'queued', worker_id = NULL, started_at = NULL,
            retry_count = retry_count + 1
          WHERE id = ?
        `).run(task.id);

        console.log(`[RECOVERY] Task ${task.id} re-queued (checkpoint: ${task.last_checkpoint}%). Retry #${task.retry_count + 1}`);

        broadcastEvent("task_requeued", {
          task_id: task.id,
          reason: "worker_failure",
          worker_id: worker.id,
          resume_from: task.last_checkpoint,
          retry_count: task.retry_count + 1,
        });
      }

      broadcastEvent("worker_offline", { worker_id: worker.id, username: worker.username });
      console.log(`[HEARTBEAT] Worker ${worker.username} (${worker.id}) went offline — ${abandoned.length} task(s) re-queued`);
    }
  } catch (e) {
    console.error("Heartbeat checker error:", e.message);
  }
}

// Periodically broadcast stats for live dashboard (FEATURE 6)
function broadcastStats() {
  try {
    const total_tasks     = db.prepare("SELECT COUNT(*) as c FROM tasks").get().c;
    const completed_tasks = db.prepare("SELECT COUNT(*) as c FROM tasks WHERE status='completed'").get().c;
    const running_tasks   = db.prepare("SELECT COUNT(*) as c FROM tasks WHERE status='running'").get().c;
    const online_workers  = db.prepare("SELECT COUNT(*) as c FROM workers WHERE is_online=1").get().c;
    const queued_tasks    = db.prepare("SELECT COUNT(*) as c FROM tasks WHERE status='queued'").get().c;

    broadcastEvent("stats", { total_tasks, completed_tasks, running_tasks, online_workers, queued_tasks });
  } catch (_) {}
}

setInterval(schedulerLoop, 5000);
setInterval(heartbeatChecker, 15000);
setInterval(broadcastStats, 5000); // FEATURE 6: push stats every 5s

// ─────────────────────────────────────────────
// START SERVER
// ─────────────────────────────────────────────

app.listen(PORT, "0.0.0.0", () => {
  console.log(`✅ Distributed Compute Platform v2.0 running on http://0.0.0.0:${PORT}`);
  console.log("   Features: Failure Recovery | Credits | Network Viz | ETA | Dynamic Assignment | SSE Monitoring");
});