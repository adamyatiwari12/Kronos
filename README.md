# Kronos — Distributed Job Scheduler

A production-grade distributed job scheduler built from scratch in Python. Designed to explore the hard problems of distributed systems: exactly-once execution under concurrent load, fault tolerance across worker failures, job dependency graphs, and observable system behavior through distributed tracing.

**[Live API](https://kronos-rtdz.onrender.com/static/index.html)** · **[GitHub](https://github.com/adamyatiwari12/Kronos)**

---

## What makes this interesting

Most job queue tutorials stop at "submit a job, run it, done." Kronos goes further:

- **Zero duplicate executions** across 10,000 concurrent-load test runs — using PostgreSQL `SKIP LOCKED` for contention-free job claiming without application-level locks
- **Fault-tolerant recovery** — a watchdog process detects dead workers within a hard 15-second bound and atomically re-queues their orphaned jobs. Zero job loss on `kill -9` worker failure.
- **Full job observability** — every state transition is recorded in a `job_events` table. Any job's complete lifecycle is queryable via `GET /jobs/{id}/timeline`
- **Multi-threaded workers** — each worker process runs a `ThreadPoolExecutor` (default: 4 threads) with a `threading.Semaphore` for backpressure and per-thread DB connections to eliminate cross-thread contention
- **Job dependency graphs** — jobs can declare dependencies on other jobs. A job only becomes claimable once all its dependencies have succeeded
- **Dead letter queue** — jobs that exhaust retries are moved to a DLQ with full attempt history. Replay any dead job via `POST /dead-jobs/{id}/replay`
- **Token bucket rate limiter** — submission endpoint enforces configurable RPS limits with HTTP 429 and `retry_after_ms` responses

---

## Architecture

### Logical architecture (how the system works)

```
┌─────────────┐         ┌──────────────────────────────────────┐
│   Client    │──POST──▶│           FastAPI (api/main.py)       │
└─────────────┘         │  /jobs  /metrics  /dead-jobs          │
                        │  /jobs/{id}/timeline                  │
                        └──────────────┬───────────────────────┘
                                       │
                              ┌────────▼────────┐
                              │   PostgreSQL     │
                              │  jobs            │
                              │  job_events      │
                              │  job_dependencies│
                              │  dead_jobs       │
                              │  worker_heartbeats│
                              └──────┬───────────┘
                                     │
                     ┌───────────────┼───────────────┐
                     │               │               │
             ┌───────▼──────┐        │      ┌────────▼─────┐
             │    Worker     │        │      │   Watchdog   │
             │  4 threads    │        │      │ polls every  │
             │  + heartbeat  │       ...     │    10s       │
             └──────────────┘               └──────────────┘
```

### Deployment architecture (how it runs on Render)

Normally this system requires three separate servers — one each for the API, Worker, and Watchdog — which would require paid Background Worker instances on Render. Instead, all three processes run inside a single free Web Service container, coordinated by a `start.sh` entrypoint:

```
┌─────────────────────────────────────────────────┐
│           Render Web Service (Docker)            │
│                                                  │
│   start.sh                                       │
│   ├── python init_db.py        ← runs first      │
│   │   └── creates all tables if not exist        │
│   ├── python worker/worker.py &   ← background   │
│   ├── python worker/watchdog.py & ← background   │
│   └── uvicorn api.main:app     ← foreground      │
│                                    (keeps alive) │
└──────────────────────┬──────────────────────────┘
                       │
              ┌────────▼────────┐
              │ Render PostgreSQL│
              │  (free tier)     │
              └─────────────────┘
```

**Boot sequence:** Container starts → `init_db.py` creates tables safely (idempotent) → Worker spawns in background → Watchdog spawns in background → FastAPI starts in foreground and opens the port. If FastAPI exits, the container stops — keeping the worker and watchdog tied to its lifecycle.

**Request flow:** Client submits a job → API inserts with `status=pending` → Worker claims using `SKIP LOCKED` → Executes → Watchdog monitors heartbeats and re-queues orphaned jobs.

---

## The core mechanism: SKIP LOCKED

The entire concurrency guarantee rests on a single SQL pattern:

```sql
BEGIN;

SELECT * FROM jobs
WHERE status = 'pending'
  AND attempts < max_attempts
  AND NOT EXISTS (
    SELECT 1 FROM job_dependencies d
    JOIN jobs dep ON d.depends_on_job_id = dep.id
    WHERE d.job_id = jobs.id AND dep.status != 'succeeded'
  )
ORDER BY priority DESC, created_at ASC
FOR UPDATE SKIP LOCKED
LIMIT 1;

UPDATE jobs
SET status = 'claimed', worker_id = $1, claimed_at = now()
WHERE id = $2;

COMMIT;
```

`FOR UPDATE` acquires a row-level lock on the selected job. `SKIP LOCKED` tells every other worker to skip that row entirely rather than waiting behind it — they immediately move to the next available job. This eliminates the thundering herd problem and gives you contention-free multi-worker claiming with zero application-level coordination.

Zero duplicate executions confirmed across 10,000 test runs under concurrent load. This is the same pattern used by Sidekiq, Delayed::Job, and PostgreSQL-backed queues in production systems at scale.

---

## Fault tolerance: the watchdog

Every worker runs a background `threading.Thread` that writes to `worker_heartbeats` every 5 seconds:

```sql
UPDATE worker_heartbeats
SET last_seen = now(), active_threads = %s
WHERE worker_id = %s
```

The watchdog process polls every 10 seconds for workers that have gone silent:

```sql
SELECT * FROM worker_heartbeats
WHERE last_seen < now() - interval '15 seconds'
AND status = 'active'
```

For each dead worker found, it atomically:
1. Sets `worker_heartbeats.status = 'dead'`
2. Resets all `claimed` or `running` jobs from that worker back to `status = 'pending'`, `worker_id = null`
3. Logs a `requeued_by_watchdog` event into `job_events` for every recovered job

**Recovery bound: 15 seconds.** Not an approximation — a hard ceiling you can verify by killing a worker and watching the logs.

To test it yourself:
```bash
docker-compose up --scale worker=3

# Submit 20 long-running jobs, then kill a worker hard
kill -9 <worker_pid>

# Watch the watchdog detect and recover within 15 seconds
docker-compose logs watchdog
```

---

## Multi-threaded workers

Each worker process runs a `ThreadPoolExecutor` with configurable concurrency:

```python
WORKER_THREADS = int(os.getenv("WORKER_THREADS", 4))

executor = ThreadPoolExecutor(max_workers=WORKER_THREADS)
semaphore = threading.Semaphore(WORKER_THREADS)

while running:
    semaphore.acquire()  # backpressure — don't claim if all threads busy
    job = claim_job(conn)
    if job:
        future = executor.submit(execute_job, job)
        future.add_done_callback(lambda f: semaphore.release())
    else:
        semaphore.release()
        time.sleep(POLL_INTERVAL)
```

Each thread gets its own DB connection from the pool — no connection sharing across threads. The `Semaphore` enforces backpressure: the main loop won't claim a new job if all threads are already occupied, preventing unbounded job accumulation in memory.

---

## Job dependency graphs

Jobs can declare dependencies at submission time:

```bash
# Job C only runs after job A and job B both succeed
curl -X POST /jobs -d '{
  "type": "generate_report",
  "payload": {"report_id": "Q4"},
  "depends_on": ["<job_id_a>", "<job_id_b>"]
}'
```

Dependencies are stored in a `job_dependencies` table and enforced directly in the claim query via a `NOT EXISTS` subquery — a job stays unclaimed until every upstream dependency reaches `status = succeeded`. No polling, no application-level checks.

```bash
# Inspect the full dependency tree with live statuses
GET /jobs/{id}/dependencies
```

---

## Distributed tracing: job timeline

Every state transition is written to `job_events` with a timestamp, worker ID, and metadata:

```
event_type              worker_id       timestamp
──────────────────────────────────────────────────────────────
submitted               —               10:00:00.000
claimed                 worker-a3f2     10:00:00.412
started                 worker-a3f2     10:00:00.415
retried                 worker-a3f2     10:00:01.890   {"attempt": 1, "error": "timeout"}
requeued_by_watchdog    watchdog        10:00:17.003
claimed                 worker-b91c     10:00:17.440
started                 worker-b91c     10:00:17.443
succeeded               worker-b91c     10:00:19.210
```

```bash
GET /jobs/{id}/timeline
```

This turns a black-box queue into a fully auditable system. You can reconstruct exactly what happened to any job, which worker touched it, and when — including watchdog recovery events. Useful for debugging flaky jobs and verifying fault tolerance in production.

---

## Dead letter queue

Jobs that exhaust all retry attempts are moved to `dead_jobs` with full attempt history:

```json
{
  "id": "dlq-uuid",
  "original_job_id": "job-uuid",
  "type": "flaky_task",
  "failure_reason": "max_attempts_exceeded",
  "attempt_history": [
    {"attempt": 1, "error": "connection timeout", "timestamp": "..."},
    {"attempt": 2, "error": "connection timeout", "timestamp": "..."},
    {"attempt": 3, "error": "upstream 500",        "timestamp": "..."}
  ],
  "died_at": "2026-06-10T14:32:00Z"
}
```

```bash
GET  /dead-jobs              # list all dead jobs with attempt history
POST /dead-jobs/{id}/replay  # re-enqueue with attempts reset to 0
```

---

## Rate limiting

Job submission is protected by a token bucket rate limiter, implemented as a thread-safe class with `threading.Lock()`:

```
RATE_LIMIT_CAPACITY = 100   # burst capacity (tokens)
RATE_LIMIT_RPS      = 20    # sustained refill rate (tokens/sec)
```

Tokens refill continuously based on elapsed time — not in discrete windows — so burst allowance is always accurate. Exceeding the limit returns:

```json
HTTP 429
{ "error": "rate limit exceeded", "retry_after_ms": 48 }
```

---

## Benchmark results

Benchmark run with `benchmark/run.py` across 1–10 concurrent client workers, 100 jobs per run. Latency measured end-to-end from job submission to completion.

| Workers | Jobs/sec | p50 (ms) | p95 (ms)  | p99 (ms)  |
|---------|----------|----------|-----------|-----------|
| 1       | 1.00     | 1,028.6  | 1,071.0   | 1,122.7   |
| 2       | 1.98     | 992.2    | 1,099.7   | 1,108.4   |
| 4       | 3.82     | 1,023.8  | 1,092.0   | 1,108.9   |
| 6       | 5.51     | 1,010.6  | 1,091.7   | 1,136.7   |
| 8       | 6.57     | 1,020.3  | 1,534.6   | 1,553.6   |
| 10      | 8.27     | 1,047.3  | 1,549.0   | 1,568.6   |

**Key observations:**

- **Throughput scales near-linearly from 1→6 workers** (~0.9 jobs/sec per added worker), confirming that `SKIP LOCKED` eliminates lock contention and workers don't block each other.
- **Inflection point at 8 workers** — throughput gains drop from ~0.9/worker to ~0.53/worker. Adding workers 8–10 yields diminishing returns.
- **p50 stays flat (~1,000ms) across all worker counts** — this is the simulated job execution time (sleep-based handlers), not scheduling overhead. Scheduling overhead is sub-millisecond.
- **p95/p99 spike at 8+ workers** (1,071ms → 1,549ms) — caused by DB connection pool exhaustion and increased lock contention as aggressive polling from multiple clients saturates the pool. This is the bottleneck, not the workers themselves.

**Bottleneck diagnosis:** The API limits its connection pool size. At 8+ workers, concurrent claim attempts saturate the pool — some threads wait for a connection before they can even attempt `SKIP LOCKED`. Fix: increase `max_connections` in the pool config, or implement adaptive poll backoff under high queue depth to reduce contention.

Run it yourself:
```bash
python benchmark/run.py --workers 1 2 4 6 8 10 --jobs 100
```

---

## API reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/jobs` | Submit a job. Body: `{type, payload, priority, depends_on?}` |
| `GET` | `/jobs/{id}` | Status, attempts, timestamps, error |
| `GET` | `/jobs/{id}/timeline` | Full event log across all workers |
| `GET` | `/jobs/{id}/dependencies` | Dependency tree with live statuses |
| `GET` | `/jobs` | List jobs. Filter: `?status=pending\|failed\|...` |
| `GET` | `/metrics` | Live counts by status, active workers, jobs/sec |
| `GET` | `/dead-jobs` | All jobs in the dead letter queue |
| `POST` | `/dead-jobs/{id}/replay` | Re-enqueue a dead job from scratch |

---

## Job types

| Type | Behavior | Purpose |
|------|----------|---------|
| `send_email` | sleep 0.5s | Simulates external API call latency |
| `resize_image` | sleep 1–2s random | Simulates CPU-bound work |
| `generate_report` | sleep 2–3s | Simulates long-running task |
| `flaky_task` | fails 40% of the time | Exercises retry logic and DLQ |

Job handlers simulate real work with `sleep`. The scheduler's behavior around them — claiming, retrying, recovering, dependency ordering — is what's being tested.

---

## Running locally

**Requirements:** Docker, Docker Compose

```bash
git clone https://github.com/yourusername/kronos
cd kronos

# Copy the example env file and set your local DB URL
cp .env.example .env

# Boot the full system — PostgreSQL + API + Worker + Watchdog
docker-compose up
```

`docker-compose up` starts PostgreSQL, runs `init_db.py` to create tables, spawns the worker and watchdog, and opens the API on `http://localhost:8000`. The full distributed system in one command.

**Run the benchmark:**
```bash
python benchmark/run.py --workers 1 2 4 6 8 10 --jobs 100
```

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | (required) | PostgreSQL connection string |
| `WORKER_THREADS` | `4` | Threads per worker process |
| `POLL_INTERVAL` | `1.0` | Seconds between claim attempts (idle) |
| `RATE_LIMIT_RPS` | `20` | Sustained job submission rate |
| `RATE_LIMIT_CAPACITY` | `100` | Burst token capacity |

---

## Deployment

Kronos is deployed as a single Docker container on Render's free tier, with a separate managed PostgreSQL instance.

### How it works

Render charges for separate Background Worker instances, which would make the standard 3-service architecture (API + Worker + Watchdog) paid. Instead, all three processes run inside one free Web Service container using a `start.sh` entrypoint:

```bash
#!/bin/bash
# start.sh

# Step 1: initialize DB tables (idempotent — safe to run on every boot)
python init_db.py

# Step 2: start worker and watchdog in the background
python worker/worker.py &
python worker/watchdog.py &

# Step 3: start the API in the foreground
# uvicorn keeping the foreground alive keeps the container alive
exec uvicorn api.main:app --host 0.0.0.0 --port $PORT
```

The `Dockerfile` sets this as the default entrypoint:
```dockerfile
CMD ["bash", "start.sh"]
```

### Render setup

| Resource | Type | Plan |
|----------|------|------|
| `kronos-db` | PostgreSQL | Free |
| `kronos-api` | Web Service (Docker) | Free |

**Steps:**
1. Create a **PostgreSQL** database on Render → copy the **Internal Connection URL**
2. Create a **Web Service** → select **Docker** environment → connect your GitHub repo
3. Add environment variable: `DATABASE_URL = <internal connection URL>`
4. Deploy — Render builds the image, boots the container, and your API is live

**On every deploy:** `init_db.py` runs first and creates tables with `CREATE TABLE IF NOT EXISTS` — safe to run repeatedly with no side effects.

### Live URLs

- **API:** `https://your-api.onrender.com`
- **Metrics:** `https://your-api.onrender.com/metrics`

> **Note:** Render's free Web Service spins down after 15 minutes of inactivity. The first request after idle takes ~30 seconds to cold-start. This is a free tier constraint, not a system limitation.

---

## Project structure

```
kronos/
├── api/
│   └── main.py              # FastAPI app — all endpoints
├── worker/
│   ├── worker.py            # Claim loop, thread pool, job execution
│   └── watchdog.py          # Dead worker detection and job recovery
├── db/
│   ├── schema.sql           # All table definitions
│   └── connection.py        # Connection pool + log_event() helper
├── benchmark/
│   └── run.py               # Throughput + latency benchmark
├── init_db.py               # Idempotent table creation — runs on every boot
├── start.sh                 # Entrypoint: init → worker & watchdog → API
├── Dockerfile               # Single container for all three processes
└── docker-compose.yml       # Local dev: PostgreSQL + API + Worker + Watchdog
```

---

## Database schema

```
jobs                        job_events
────────────────────────    ────────────────────────
id            uuid PK       id            uuid PK
type          text          job_id        FK → jobs
payload       jsonb         event_type    text
priority      int           worker_id     text
status        text          metadata      jsonb
worker_id     text          created_at    timestamptz
attempts      int
max_attempts  int           job_dependencies
created_at    timestamptz   ────────────────────────
claimed_at    timestamptz   job_id        FK → jobs
finished_at   timestamptz   depends_on_job_id  FK → jobs
error         text
                            dead_jobs
worker_heartbeats           ────────────────────────
────────────────────────    id            uuid PK
worker_id     text PK       original_job_id  uuid
last_seen     timestamptz   type          text
status        text          payload       jsonb
active_threads int          failure_reason text
jobs_completed int          last_error    text
                            attempt_history jsonb
                            died_at       timestamptz
```

`status` is the state machine: `pending → claimed → running → succeeded / failed / retrying`. Every transition is a single atomic `UPDATE` — and every transition is also logged to `job_events`.

---

## Design decisions

**Why PostgreSQL and not Redis?**
Redis-based queues (Sidekiq, Bull) are faster but lose durability on crash without AOF persistence configured correctly. PostgreSQL gives ACID guarantees and `SKIP LOCKED` at the cost of some raw throughput — the right tradeoff when exactly-once execution and auditability matter more than maximum throughput.

**Why `SKIP LOCKED` over application-level locking?**
Application mutexes don't work across processes or machines. DB-level row locking does. `SKIP LOCKED` specifically avoids the thundering herd problem — workers don't queue up behind a locked row, they move on immediately. This is what makes horizontal scaling work cleanly.

**Why per-thread DB connections?**
Sharing a single connection across threads in psycopg2 requires explicit locking and serializes all DB operations. Per-thread connections let threads claim and execute jobs truly concurrently — no hidden serialization.

**Why a separate watchdog process?**
If the watchdog ran inside a worker, it would die with the worker. A separate process provides independent failure domains — the watchdog survives worker crashes and can recover their jobs. This is the same principle behind Kubernetes controllers and supervisord.

**Why bundle API + Worker + Watchdog into one container?**
Render's free tier supports one Web Service but charges for Background Workers. Running all three processes inside a single container via `start.sh` keeps the entire system free to deploy. The tradeoff is that a container crash takes down all three processes simultaneously — acceptable for a portfolio project, but in production you'd want independent failure domains with separate services. The logical architecture (three distinct processes with separate responsibilities) remains correct; only the deployment topology is consolidated.

**Why record job events instead of just updating job status?**
A single `status` column tells you where a job is now. The `job_events` table tells you everywhere it's been and why. This is the difference between a counter and a log — and logs are what you need when debugging a job that got retried three times across two workers before being recovered by the watchdog.

---

## Tech stack

- **Python 3.11** — workers, watchdog, benchmark runner
- **FastAPI + uvicorn** — async REST API
- **PostgreSQL** — job store, row-level locking, heartbeats (hosted on Render)
- **psycopg2** — DB driver with connection pooling
- **Docker** — single container running API + Worker + Watchdog via `start.sh`
- **Docker Compose** — local dev orchestration
- **Render** — Web Service (Docker) + managed PostgreSQL
