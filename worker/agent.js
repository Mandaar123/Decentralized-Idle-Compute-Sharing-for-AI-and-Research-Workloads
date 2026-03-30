/**
 * DHRUNIL — Worker Node Agent (JavaScript version)
 * Run: node agent.js
 * Make sure Docker Desktop is running before starting.
 * Set SERVER_URL env var to point to Mandaar's backend.
 *
 * Dependencies: npm install axios
 */

const axios = require("axios");
const { spawn } = require("child_process");
const fs = require("fs");
const path = require("path");
const os = require("os");

const SERVER = process.env.SERVER_URL || "http://localhost:8000";
const WORKER_NAME = process.env.WORKER_NAME || "dhrunil-node";

let WORKER_ID = null;
let TOKEN = null;

// ─────────────────────────────────────────────
// CPU / RAM (pure Node built-ins, no packages)
// ─────────────────────────────────────────────

function getCpuUsage() {
  return new Promise((resolve) => {
    const cpus1 = os.cpus();
    setTimeout(() => {
      const cpus2 = os.cpus();
      let totalIdle = 0, totalTick = 0;
      cpus1.forEach((cpu, i) => {
        const cpu2 = cpus2[i];
        for (const type in cpu2.times) {
          totalTick += cpu2.times[type] - cpu.times[type];
        }
        totalIdle += cpu2.times.idle - cpu.times.idle;
      });
      const usage = 100 - (totalIdle / totalTick) * 100;
      resolve(parseFloat(usage.toFixed(1)));
    }, 500);
  });
}

function getRamUsage() {
  const total = os.totalmem();
  const free = os.freemem();
  return parseFloat(((1 - free / total) * 100).toFixed(1));
}

// ─────────────────────────────────────────────
// REGISTRATION
// ─────────────────────────────────────────────

async function register() {
  console.log(`[WORKER] Registering as '${WORKER_NAME}' with server ${SERVER}...`);
  try {
    const res = await axios.post(
      `${SERVER}/workers/register`,
      { username: WORKER_NAME },
      { timeout: 10000 }
    );
    WORKER_ID = res.data.worker_id;
    TOKEN = res.data.token;
    console.log(`[WORKER] Registered! ID: ${WORKER_ID}`);
  } catch (err) {
    console.error(`[WORKER] Registration failed: ${err.message}`);
    console.log("[WORKER] Retrying in 5 seconds...");
    await sleep(5000);
    await register();
  }
}

// ─────────────────────────────────────────────
// HEARTBEAT
// ─────────────────────────────────────────────

async function sendHeartbeat(currentTask = null) {
  try {
    const cpu = await getCpuUsage();
    const ram = getRamUsage();
    await axios.post(
      `${SERVER}/workers/heartbeat`,
      { worker_id: WORKER_ID, cpu, ram, current_task: currentTask },
      { timeout: 5000 }
    );
  } catch (err) {
    console.error(`[HEARTBEAT] Failed: ${err.message}`);
  }
}

// ─────────────────────────────────────────────
// JOB POLLING
// ─────────────────────────────────────────────

async function pollForJob() {
  try {
    const res = await axios.get(`${SERVER}/workers/next-job/${WORKER_ID}`, { timeout: 5000 });
    if (res.data && res.data.task_id) return res.data;
  } catch (err) {
    console.error(`[POLL] Failed: ${err.message}`);
  }
  return null;
}

// ─────────────────────────────────────────────
// DOCKER EXECUTION
// ─────────────────────────────────────────────

async function runInDocker(taskId, code) {
  const taskDir = path.join(os.tmpdir(), `task_${taskId}`);
  fs.mkdirSync(taskDir, { recursive: true });
  fs.writeFileSync(path.join(taskDir, "job.py"), code, "utf8");

  console.log(`[DOCKER] Running task ${taskId}...`);

  try { await axios.post(`${SERVER}/tasks/${taskId}/progress`, null, { params: { progress: 10 }, timeout: 3000 }); } catch (_) {}

  let output = "", success = false;

  try {
    await new Promise((resolve, reject) => {
      const proc = spawn("docker", [
        "run", "--rm",
        "--memory=512m", "--cpus=1", "--network=none",
        "-v", `${taskDir}:/task`,
        "python:3.11-slim", "python", "/task/job.py",
      ], { stdio: ["ignore", "pipe", "pipe"] });

      let stdout = "", stderr = "";
      proc.stdout.on("data", (d) => { stdout += d.toString(); });
      proc.stderr.on("data", (d) => { stderr += d.toString(); });

      const timer = setTimeout(() => { proc.kill("SIGKILL"); reject(new Error("TIMEOUT")); }, 300000);

      proc.on("close", (code) => {
        clearTimeout(timer);
        if (code === 0) { output = stdout.trim() || "(no output)"; success = true; }
        else { output = `Error:\n${stderr.trim()}`; success = false; }
        resolve();
      });
      proc.on("error", (err) => { clearTimeout(timer); reject(err); });
    });
  } catch (err) {
    if (err.message === "TIMEOUT") output = "Task exceeded maximum execution time (5 minutes)";
    else if (err.code === "ENOENT") output = "Docker not found. Please install Docker Desktop.";
    else output = `Execution error: ${err.message}`;
    success = false;
  }

  try { await axios.post(`${SERVER}/tasks/${taskId}/progress`, null, { params: { progress: 90 }, timeout: 3000 }); } catch (_) {}
  try { fs.rmSync(taskDir, { recursive: true, force: true }); } catch (_) {}

  console.log(`[DOCKER] Task ${taskId} done. Success: ${success}`);
  console.log(`[DOCKER] Output preview: ${output.slice(0, 200)}`);
  return { output, success };
}

// ─────────────────────────────────────────────
// RESULT REPORTING
// ─────────────────────────────────────────────

async function reportResult(taskId, output, success) {
  try {
    await axios.post(
      `${SERVER}/tasks/result`,
      { worker_id: WORKER_ID, task_id: taskId, output, success },
      { timeout: 10000 }
    );
    console.log(`[RESULT] Reported for task ${taskId}`);
  } catch (err) {
    console.error(`[RESULT] Failed to report: ${err.message}`);
  }
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────

async function main() {
  await register();
  console.log(`[WORKER] Starting main loop. Polling ${SERVER} every 10s...`);
  console.log("[WORKER] Press Ctrl+C to stop.");

  let currentTaskId = null;

  while (true) {
    await sendHeartbeat(currentTaskId);

    if (!currentTaskId) {
      const job = await pollForJob();
      if (job && job.task_id) {
        currentTaskId = job.task_id;
        console.log(`\n[WORKER] Got task ${currentTaskId}!`);
        console.log(`[WORKER] Code:\n${job.code.slice(0, 300)}${job.code.length > 300 ? "..." : ""}\n`);
        const { output, success } = await runInDocker(currentTaskId, job.code);
        await reportResult(currentTaskId, output, success);
        currentTaskId = null;
      } else {
        const cpu = await getCpuUsage();
        console.log(`[WORKER] No tasks. Waiting... (CPU: ${cpu}%)`);
      }
    }

    await sleep(10000);
  }
}

process.on("SIGINT", () => { console.log("\n[WORKER] Shutting down..."); process.exit(0); });
main().catch((err) => { console.error("[FATAL]", err.message); process.exit(1); });
