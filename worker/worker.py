import os
import sys
import time
import uuid
import psycopg2
import random
import threading
import signal
from psycopg2.extras import RealDictCursor

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connection import get_db_connection

WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"
shutdown_event = threading.Event()

def signal_handler(signum, frame):
    print(f"\n[{WORKER_ID}] Received shutdown signal. Finishing current job and exiting...")
    shutdown_event.set()

def heartbeat_thread_func():
    """Separate thread to update heartbeat every 5 seconds."""
    while not shutdown_event.is_set():
        try:
            with get_db_connection() as conn:
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
        except Exception as e:
            print(f"Heartbeat error: {e}")
        
        # Sleep for 5 seconds, checking shutdown_event
        for _ in range(50):
            if shutdown_event.is_set():
                break
            time.sleep(0.1)

def claim_job(conn):
    """Claim the highest priority pending job."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT * FROM jobs
            WHERE status = 'pending'
            AND attempts < max_attempts
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1;
            """
        )
        job = cur.fetchone()
        
        if job:
            cur.execute(
                """
                UPDATE jobs
                SET status = 'claimed', worker_id = %s, claimed_at = NOW()
                WHERE id = %s;
                """,
                (WORKER_ID, job['id'])
            )
            conn.commit()
        else:
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
        job_type = job['type']
        print(f"[{WORKER_ID}] Executing job {job_id} of type {job_type}")
        
        if job_type == 'send_email':
            time.sleep(0.5)
            print(f"[{WORKER_ID}] Payload: {job.get('payload')}")
        elif job_type == 'resize_image':
            time.sleep(random.uniform(1.0, 2.0))
        elif job_type == 'generate_report':
            time.sleep(random.uniform(2.0, 3.0))
        elif job_type == 'flaky_task':
            if random.random() < 0.4:
                raise Exception("Random failure in flaky_task")
            time.sleep(0.5)
        else:
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
        if attempts < job['max_attempts']:
            print(f"[{WORKER_ID}] Job {job_id} failed: {error_msg}. Retrying...")
            with conn.cursor() as cur:
                cur.execute("UPDATE jobs SET status = 'retrying', error = %s WHERE id = %s", (error_msg, job_id))
                conn.commit()
                
            backoff = 2 ** attempts
            print(f"[{WORKER_ID}] Waiting {backoff} seconds before re-queuing...")
            time.sleep(backoff)
            
            with conn.cursor() as cur:
                cur.execute("UPDATE jobs SET status = 'pending' WHERE id = %s", (job_id,))
                conn.commit()
        else:
            print(f"[{WORKER_ID}] Job {job_id} failed: {error_msg}. Max attempts reached.")
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE jobs SET status = 'failed', finished_at = NOW(), error = %s WHERE id = %s",
                    (error_msg, job_id)
                )
                conn.commit()

def set_worker_dead():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE worker_heartbeats SET status = 'dead' WHERE worker_id = %s", (WORKER_ID,))
                conn.commit()
    except Exception as e:
        print(f"Error setting worker dead: {e}")

def main_loop():
    print(f"Starting worker {WORKER_ID}")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    hb_thread = threading.Thread(target=heartbeat_thread_func, daemon=True)
    hb_thread.start()
    
    with get_db_connection() as conn:
        while not shutdown_event.is_set():
            try:
                job = claim_job(conn)
                
                if job:
                    run_job(conn, job)
                else:
                    time.sleep(1)
            except psycopg2.Error as e:
                print(f"Database error: {e}")
                time.sleep(5)
                conn.rollback()
                
    print(f"[{WORKER_ID}] Exiting cleanly. Setting status to dead.")
    set_worker_dead()

if __name__ == "__main__":
    main_loop()
