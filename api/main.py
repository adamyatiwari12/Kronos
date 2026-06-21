from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
import sys
import os
import time
import threading
from fastapi.responses import JSONResponse

# Add the parent directory to sys.path to be able to import db
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connection import get_db_connection, log_event

app = FastAPI(title="Job Scheduler API")

class TokenBucket:
    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.tokens = capacity
        self.refill_rate = refill_rate
        self.last_refill = time.time()
        self.lock = threading.Lock()

    def consume(self, n: int = 1):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_refill
            
            # Refill tokens
            tokens_to_add = elapsed * self.refill_rate
            self.tokens = min(self.capacity, self.tokens + tokens_to_add)
            self.last_refill = now
            
            if self.tokens >= n:
                self.tokens -= n
                return True, 0
            else:
                # Calculate time until enough tokens
                tokens_needed = n - self.tokens
                wait_time_sec = tokens_needed / self.refill_rate
                return False, int(wait_time_sec * 1000)

RATE_LIMIT_CAPACITY = int(os.environ.get("RATE_LIMIT_CAPACITY", "100"))
RATE_LIMIT_RPS = float(os.environ.get("RATE_LIMIT_RPS", "20"))
job_rate_limiter = TokenBucket(capacity=RATE_LIMIT_CAPACITY, refill_rate=RATE_LIMIT_RPS)

# Mount static directory for the dashboard UI
import os as _os
if not _os.path.exists("static"):
    _os.makedirs("static")
app.mount("/static", StaticFiles(directory="static", html=True), name="static")

class JobSubmitRequest(BaseModel):
    type: str
    payload: Optional[Dict[str, Any]] = None
    priority: int = 5
    max_attempts: int = 3

@app.post("/jobs", status_code=201)
def submit_job(job_req: JobSubmitRequest):
    """Submit a new job."""
    allowed, retry_after_ms = job_rate_limiter.consume(1)
    if not allowed:
        return JSONResponse(
            status_code=429,
            content={"error": "rate limit exceeded", "retry_after_ms": retry_after_ms}
        )
        
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO jobs (type, payload, priority, max_attempts)
                VALUES (%s, %s, %s, %s)
                RETURNING id, type, priority, status, created_at;
                """,
                (job_req.type, psycopg2.extras.Json(job_req.payload) if job_req.payload else None, job_req.priority, job_req.max_attempts)
            )
            new_job = cur.fetchone()
            log_event(conn, new_job['id'], 'submitted')
            conn.commit()
    return new_job

@app.get("/jobs/{job_id}")
def get_job_status(job_id: int):
    """Get the status of a specific job."""
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, type, payload, priority, status, worker_id, attempts, created_at, claimed_at, finished_at, error
                FROM jobs
                WHERE id = %s;
                """,
                (job_id,)
            )
            job = cur.fetchone()
            
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    return job

@app.get("/jobs/{job_id}/timeline")
def get_job_timeline(job_id: int):
    """Get the timeline of events for a specific job."""
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT event_type, worker_id, metadata, created_at as timestamp
                FROM job_events
                WHERE job_id = %s
                ORDER BY created_at ASC;
                """,
                (job_id,)
            )
            events = cur.fetchall()
            
    return events

@app.get("/jobs")
def list_jobs(status: Optional[str] = None):
    """List jobs with optional filter by status."""
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if status:
                cur.execute(
                    "SELECT id, type, priority, status, created_at FROM jobs WHERE status = %s ORDER BY created_at DESC LIMIT 100;",
                    (status,)
                )
            else:
                cur.execute(
                    "SELECT id, type, priority, status, created_at FROM jobs ORDER BY created_at DESC LIMIT 100;"
                )
            jobs = cur.fetchall()
    return jobs

@app.get("/dead-jobs")
def list_dead_jobs():
    """List all dead jobs."""
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM dead_jobs ORDER BY died_at DESC LIMIT 100;")
            jobs = cur.fetchall()
    return jobs

@app.post("/dead-jobs/{dead_job_id}/replay", status_code=201)
def replay_dead_job(dead_job_id: int):
    """Replay a dead job by inserting it back into the jobs table."""
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. Fetch the dead job
            cur.execute("SELECT original_job_id, type, payload FROM dead_jobs WHERE id = %s;", (dead_job_id,))
            dead_job = cur.fetchone()
            
            if not dead_job:
                raise HTTPException(status_code=404, detail="Dead job not found")
                
            # 2. Insert into jobs table
            cur.execute(
                """
                INSERT INTO jobs (type, payload, priority, max_attempts)
                VALUES (%s, %s, %s, %s)
                RETURNING id, type, priority, status, created_at;
                """,
                (dead_job['type'], psycopg2.extras.Json(dead_job['payload']) if dead_job['payload'] else None, 5, 3)
            )
            new_job = cur.fetchone()
            
            # 3. Log event
            log_event(conn, new_job['id'], 'submitted')
            
            # 4. Delete from dead_jobs
            cur.execute("DELETE FROM dead_jobs WHERE id = %s;", (dead_job_id,))
            
            conn.commit()
            
    return new_job

@app.get("/metrics")
def get_metrics():
    """Return counts grouped by status, avg latency, and worker count."""
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Counts grouped by status
            cur.execute("SELECT status, count(*) FROM jobs GROUP BY status;")
            status_counts = {row['status']: row['count'] for row in cur.fetchall()}
            
            # Avg latency of last 100 jobs (only considering succeeded ones for valid latency)
            cur.execute(
                """
                SELECT avg(EXTRACT(EPOCH FROM (finished_at - created_at))) as avg_latency
                FROM (
                    SELECT created_at, finished_at
                    FROM jobs
                    WHERE status = 'succeeded' AND finished_at IS NOT NULL
                    ORDER BY finished_at DESC
                    LIMIT 100
                ) as last_jobs;
                """
            )
            latency_row = cur.fetchone()
            avg_latency = float(latency_row['avg_latency']) if latency_row and latency_row['avg_latency'] else 0.0
            
            # Worker count
            cur.execute("SELECT count(*) FROM worker_heartbeats WHERE status = 'active' AND last_seen >= NOW() - INTERVAL '30 seconds';")
            worker_count = cur.fetchone()['count']
            
    return {
        "status_counts": status_counts,
        "avg_latency_seconds": avg_latency,
        "active_workers": worker_count
    }
