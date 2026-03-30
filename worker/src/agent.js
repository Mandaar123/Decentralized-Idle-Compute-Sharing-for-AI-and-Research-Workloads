/**
 * DHRUNIL — Worker Node Agent (JavaScript version)
 * Run: node agent.js
 * Make sure Docker Desktop is running before starting.
 * Set SERVER_URL env var to point to Mandaar's backend.
 *
 * Dependencies: npm install axios node-os-utils
 */

const axios = require("axios");
const { execSync, spawn } = require("child_process");
const fs = require("fs");
const path = require("path");
const os = require("os");
const osUtils = require("node-os-utils");

// ─────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────

const SERVER = process.env.SERVER_URL || "http://localhost:8000";
const WORKER_NAME = process.env.WORKER_NAME || "dhrunil-node";

let WORKER_ID = null;
let TOKEN = null;

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
    const cpu = await osUtils.cpu.usage();
    const memInfo = await osUtils.mem.info();
    const ram = 100 - memInfo.freeMemPercentage;

    await axios.post(
      `${SERVER}/workers/heartbeat`,
      {
        worker_id: WORKER_ID,
        cpu: parseFloat(cpu.toFixed(1)),
        ram: parseFloat(ram.toFixed(1)),
        current_task: currentTask,
      },
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
    const res = await axios.get(`${SERVER}/workers/next-job/${WORKER_ID}`, {
      timeout: 5000,
    });
    if (res.data && res.data.task_id) {
      return res.data;
    }
  } catch (err) {
    console.error(`[POLL] Failed: ${err.message}`);
  }
  return null;
}

// ─────────────────────────────────────────────
// DOCKER EXECUTION
// ─────────────────────────────────────────────

/**
 * Write code to a temp file, run inside Docker, capture output.
 * Returns { output, success }
 */
async function runInDocker(taskId, code) {
  const taskDir = path.join(os.tmpdir(), `task_${taskId}`);
  fs.mkdirSync(taskDir, { recursive: true });
  const jobFile = path.join(taskDir, "job.py");
  fs.writeFileSync(jobFile, code, "utf8");

  console.log(`[DOCKER] Running task ${taskId}...`);

  // Send progress update: starting (10%)
  try {
    await axios.post(
      `${SERVER}/tasks/${taskId}/progress`,
      null,
      { params: { progress: 10 }, timeout: 3000 }
    );
  } catch (_) {}

  let output = "";
  let success = false;

  try {
    await new Promise((resolve, reject) => {
      const dockerProcess = spawn(
        "docker",
        [
          "run", "--rm",
          "--memory=512m",
          "--cpus=1",
          "--network=none",
          "-v", `${taskDir}:/task`,
          "python:3.11-slim",
          "python", "/task/job.py",
        ],
        { stdio: ["ignore", "pipe", "pipe"] }
      );

      let stdout = "";
      let stderr = "";

      dockerProcess.stdout.on("data", (data) => {
        stdout += data.toString();
      });

      dockerProcess.stderr.on("data", (data) => {
        stderr += data.toString();
      });

      // Timeout: 5 minutes
      const timer = setTimeout(() => {
        dockerProcess.kill("SIGKILL");
        reject(new Error("TIMEOUT"));
      }, 300000);

      dockerProcess.on("close", (code) => {
        clearTimeout(timer);
        if (code === 0) {
          output = stdout.trim() || "(no output)";
          success = true;
        } else {
          output = `Error:\n${stderr.trim()}`;
          success = false;
        }
        resolve();
      });

      dockerProcess.on("error", (err) => {
        clearTimeout(timer);
        reject(err);
      });
    });
  } catch (err) {
    if (err.message === "TIMEOUT") {
      output = "Task exceeded maximum execution time (5 minutes)";
    } else if (err.code === "ENOENT") {
      output = "Docker not found. Please install Docker Desktop.";
    } else {
      output = `Execution error: ${err.message}`;
    }
    success = false;
  }

  // Send progress update: 90%
  try {
    await axios.post(
      `${SERVER}/tasks/${taskId}/progress`,
      null,
      { params: { progress: 90 }, timeout: 3000 }
    );
  } catch (_) {}

  // Cleanup temp dir
  try {
    fs.rmSync(taskDir, { recursive: true, force: true });
  } catch (_) {}

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
      {
        worker_id: WORKER_ID,
        task_id: taskId,
        output: output,
        success: success,
      },
      { timeout: 10000 }
    );
    console.log(`[RESULT] Reported for task ${taskId}`);
  } catch (err) {
    console.error(`[RESULT] Failed to report: ${err.message}`);
  }
}

// ─────────────────────────────────────────────
// UTILITY
// ─────────────────────────────────────────────

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────

async function main() {
  await register();
  console.log(`[WORKER] Starting main loop. Polling ${SERVER} every 10s...`);
  console.log("[WORKER] Press Ctrl+C to stop.");

  let currentTaskId = null;

  while (true) {
    // Send heartbeat
    await sendHeartbeat(currentTaskId);

    // Poll for a job (only if not currently running one)
    if (!currentTaskId) {
      const job = await pollForJob();

      if (job && job.task_id) {
        const taskId = job.task_id;
        const code = job.code;
        currentTaskId = taskId;

        console.log(`\n[WORKER] Got task ${taskId}!`);
        console.log(
          `[WORKER] Code:\n${code.slice(0, 300)}${code.length > 300 ? "..." : ""}\n`
        );

        // Run it
        const { output, success } = await runInDocker(taskId, code);

        // Report back
        await reportResult(taskId, output, success);
        currentTaskId = null;
      } else {
        const cpuUsage = await osUtils.cpu.usage();
        console.log(`[WORKER] No tasks. Waiting... (CPU: ${cpuUsage.toFixed(1)}%)`);
      }
    }

    await sleep(10000);
  }
}

// Handle Ctrl+C gracefully
process.on("SIGINT", () => {
  console.log("\n[WORKER] Shutting down...");
  process.exit(0);
});

main().catch((err) => {
  console.error("[FATAL]", err.message);
  process.exit(1);
});