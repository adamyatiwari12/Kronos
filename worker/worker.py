import os
import sys
import time
import uuid
import psycopg2
import random
import threading
import signal
import concurrent.futures
import datetime
from psycopg2.extras import RealDictCursor, Json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connection import get_db_connection, log_event

WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"
WORKER_THREADS = int(os.environ.get("WORKER_THREADS", "4"))

shutdown_event = threading.Event()
job_semaphore = threading.Semaphore(WORKER_THREADS)
active_threads_count = 0
active_threads_lock = threading.Lock()

def signal_handler(signum, frame):
    print(f"\n[{WORKER_ID}] Received shutdown signal. Finishing current job and exiting...")
    shutdown_event.set()

def heartbeat_thread_func():
    """Separate thread to update heartbeat every 5 seconds."""
    while not shutdown_event.is_set():
        with active_threads_lock:
            current_active = active_threads_count
            
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO worker_heartbeats (worker_id, last_seen, status, jobs_completed, active_threads)
                        VALUES (%s, NOW(), 'active', 0, %s)
                        ON CONFLICT (worker_id) 
                        DO UPDATE SET last_seen = NOW(), status = 'active', active_threads = %s;
                        """,
                        (WORKER_ID, current_active, current_active)
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
            log_event(conn, job['id'], 'claimed', worker_id=WORKER_ID)
        else:
            conn.commit()
            
    return job

def execute_job(job):
    """Execute the job and update its status."""
    job_id = job['id']
    attempts = job['attempts'] + 1
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE jobs SET status = 'running', attempts = %s WHERE id = %s", (attempts, job_id))
                conn.commit()
            log_event(conn, job_id, 'started', worker_id=WORKER_ID, metadata={'attempt': attempts})
    except Exception as e:
        print(f"Database error setting job {job_id} to running: {e}")
        return
        
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
        
        with get_db_connection() as conn:
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
            log_event(conn, job_id, 'succeeded', worker_id=WORKER_ID)
        print(f"[{WORKER_ID}] Job {job_id} succeeded")
            
    except Exception as e:
        error_msg = str(e)
        if attempts < job['max_attempts']:
            print(f"[{WORKER_ID}] Job {job_id} failed: {error_msg}. Retrying...")
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE jobs SET status = 'retrying', error = %s WHERE id = %s", (error_msg, job_id))
                        conn.commit()
                    log_event(conn, job_id, 'retried', worker_id=WORKER_ID, metadata={'error': error_msg, 'attempt': attempts})
                    
                    backoff = 2 ** attempts
                    print(f"[{WORKER_ID}] Waiting {backoff} seconds before re-queuing...")
                    time.sleep(backoff)
                
                    with conn.cursor() as cur:
                        cur.execute("UPDATE jobs SET status = 'pending' WHERE id = %s", (job_id,))
                        conn.commit()
            except Exception as db_e:
                print(f"Database error retrying job {job_id}: {db_e}")
        else:
            print(f"[{WORKER_ID}] Job {job_id} failed: {error_msg}. Max attempts reached.")
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        # Fetch attempt history
                        cur.execute(
                            """
                            SELECT metadata->>'attempt' as attempt_num, metadata->>'error' as error, created_at as timestamp 
                            FROM job_events 
                            WHERE job_id = %s AND event_type IN ('failed', 'retried') 
                            ORDER BY created_at ASC
                            """, 
                            (job_id,)
                        )
                        history_rows = cur.fetchall()
                        attempt_history = []
                        for row in history_rows:
                            attempt_history.append({
                                "attempt_num": int(row[0]) if row[0] else None,
                                "error": row[1],
                                "timestamp": str(row[2])
                            })
                        
                        attempt_history.append({
                            "attempt_num": attempts,
                            "error": error_msg,
                            "timestamp": datetime.datetime.now().isoformat()
                        })
                        
                        # INSERT into dead_jobs
                        cur.execute(
                            """
                            INSERT INTO dead_jobs (original_job_id, type, payload, failure_reason, last_error, attempt_history)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (job_id, job['type'], Json(job['payload']) if job.get('payload') else None, "Max attempts reached", error_msg, Json(attempt_history))
                        )
                        
                        cur.execute(
                            "UPDATE jobs SET status = 'failed', finished_at = NOW(), error = %s WHERE id = %s",
                            (error_msg, job_id)
                        )
                        conn.commit()
                    log_event(conn, job_id, 'failed', worker_id=WORKER_ID, metadata={'error': error_msg, 'attempt': attempts})
            except Exception as db_e:
                print(f"Database error failing job {job_id}: {db_e}")

def on_job_done(future):
    global active_threads_count
    with active_threads_lock:
        active_threads_count -= 1
    job_semaphore.release()

def set_worker_dead():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE worker_heartbeats SET status = 'dead' WHERE worker_id = %s", (WORKER_ID,))
                conn.commit()
    except Exception as e:
        print(f"Error setting worker dead: {e}")

def main_loop():
    print(f"Starting worker {WORKER_ID} with {WORKER_THREADS} threads")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    hb_thread = threading.Thread(target=heartbeat_thread_func, daemon=True)
    hb_thread.start()
    
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_THREADS)
    
    with get_db_connection() as conn:
        while not shutdown_event.is_set():
            # Wait for a free slot in the thread pool before claiming a job
            if not job_semaphore.acquire(timeout=1.0):
                continue
                
            try:
                job = claim_job(conn)
                
                if job:
                    global active_threads_count
                    with active_threads_lock:
                        active_threads_count += 1
                        
                    try:
                        future = executor.submit(execute_job, job)
                        future.add_done_callback(on_job_done)
                    except Exception as e:
                        with active_threads_lock:
                            active_threads_count -= 1
                        job_semaphore.release()
                        print(f"Executor submit failed: {e}")
                else:
                    job_semaphore.release()
                    time.sleep(1)
            except psycopg2.Error as e:
                print(f"Database error in main_loop: {e}")
                job_semaphore.release()
                time.sleep(5)
                conn.rollback()
                
    print(f"[{WORKER_ID}] Exiting cleanly. Waiting for remaining jobs to finish...")
    executor.shutdown(wait=True)
    set_worker_dead()

if __name__ == "__main__":
    main_loop()
