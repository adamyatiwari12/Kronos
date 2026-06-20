from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
import sys
import os

# Add the parent directory to sys.path to be able to import db
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connection import get_db_connection

app = FastAPI(title="Job Scheduler API")

class JobSubmitRequest(BaseModel):
    type: str
    payload: Optional[Dict[str, Any]] = None
    priority: int = 5
    max_attempts: int = 3

@app.post("/jobs", status_code=201)
def submit_job(job_req: JobSubmitRequest):
    """Submit a new job."""
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

