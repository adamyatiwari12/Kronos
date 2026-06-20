import os
import sys
import time
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connection import get_db_connection

WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"

def heartbeat(conn):
    """Register or update worker heartbeat."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO worker_heartbeats (worker_id, last_seen, status, jobs_completed)
            VALUES (%s, NOW(), 'active', 0)
            ON CONFLICT (worker_id) 
            DO UPDATE SET last_seen = NOW(), status = 'active';
            """,
            (WORKER_ID,)
        )
        conn.commit()

def claim_job(conn):
    """Claim the highest priority pending job."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Use SKIP LOCKED for concurrent worker safety
        cur.execute(
            """
            UPDATE jobs
            SET status = 'claimed', claimed_at = NOW(), worker_id = %s
            WHERE id = (
                SELECT id FROM jobs
                WHERE status IN ('pending', 'retrying')
                ORDER BY priority DESC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, type, payload, attempts, max_attempts;
            """,
            (WORKER_ID,)
        )
        job = cur.fetchone()
        conn.commit()
    return job

def run_job(conn, job):
    """Execute the job and update its status."""
    job_id = job['id']
    attempts = job['attempts'] + 1
    
    with conn.cursor() as cur:
        cur.execute("UPDATE jobs SET status = 'running', attempts = %s WHERE id = %s", (attempts, job_id))
        conn.commit()
        
    try:
        print(f"[{WORKER_ID}] Executing job {job_id} of type {job['type']}")
        time.sleep(1)
        
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET status = 'succeeded', finished_at = NOW() WHERE id = %s", 
                (job_id,)
            )
            
            cur.execute(
                "UPDATE worker_heartbeats SET jobs_completed = jobs_completed + 1 WHERE worker_id = %s",
                (WORKER_ID,)
            )
            conn.commit()
        print(f"[{WORKER_ID}] Job {job_id} succeeded")
            
    except Exception as e:
        error_msg = str(e)
        status = 'failed' if attempts >= job['max_attempts'] else 'retrying'
        
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET status = %s, finished_at = NOW(), error = %s WHERE id = %s",
                (status, error_msg, job_id)
            )
            conn.commit()
        print(f"[{WORKER_ID}] Job {job_id} {status}: {error_msg}")

def main_loop():
    print(f"Starting worker {WORKER_ID}")
    
    with get_db_connection() as conn:
        while True:
            try:
                heartbeat(conn)
                job = claim_job(conn)
                
                if job:
                    run_job(conn, job)
                else:
                    time.sleep(2)
            except psycopg2.Error as e:
                print(f"Database error: {e}")
                time.sleep(5)
                conn.rollback()

if __name__ == "__main__":
    main_loop()
