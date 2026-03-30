# ComputeGrid — Distributed Compute Platform
# GDG Hackathon @ VIT Vellore

## Team
- Mandaar   → backend/main.py      (FastAPI orchestrator)
- Dhrunil   → worker/agent.py      (Contributor agent)
- Ananya    → frontend/index.html  (React-style dashboard)
- Tushita   → docker/ + tasks/     (Docker + integration)

## Quick Start

### 1. Backend (Mandaar)
```bash
cd backend
pip install fastapi uvicorn pyjwt psutil
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 2. Worker Agent (Dhrunil) — on a SECOND machine
```bash
cd worker
pip install requests psutil
# Point to Mandaar's IP:
SERVER_URL=http://<MANDAAR_IP>:8000 WORKER_NAME=dhrunil-node python agent.py
```

### 3. Frontend (Ananya) — open in browser
```bash
# Just open frontend/index.html in a browser
# Or serve it:
cd frontend && python -m http.server 3000
# Visit http://localhost:3000
```

### 4. Docker (Tushita) — build the task image
```bash
cd docker
docker build -t compute-task .
```

## Features Implemented
1. Failure Recovery      — heartbeat checker re-queues dropped tasks
2. Credit System         — earn 10/job, spend 50 to submit
3. Network Visualization — live node graph with animated task flow
4. Estimated Exec Time   — shown before submission
5. Dynamic Task Assignment — scheduler picks best node by trust + load
6. Real-Time Monitoring  — 3s polling shows live status
7. User-Friendly UI      — dark terminal aesthetic, clear UX

## Demo Flow (for judges)
1. Open frontend → register as submitter
2. Start worker agent on second laptop
3. See node appear in Network tab
4. Submit MNIST task (tasks/mnist_demo.py)
5. Watch job go: queued → assigned → running → completed
6. Show credits transferred in Credits tab
7. Kill worker mid-job → watch re-queue (fault tolerance)

## If WiFi blocks cross-machine:
```bash
# On Mandaar's machine:
ngrok http 8000
# Use the ngrok URL as SERVER_URL for worker and frontend API const
```
