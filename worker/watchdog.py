import os
import sys
import time
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connection import get_db_connection, log_event

HEARTBEAT_TIMEOUT_SECONDS = 15
RETENTION_DAYS_SUCCESS = int(os.environ.get("RETENTION_DAYS_SUCCESS", "7"))
RETENTION_DAYS_DEAD = int(os.environ.get("RETENTION_DAYS_DEAD", "30"))
def check_dead_workers(conn):
    with conn.cursor() as cur:
        # Find dead workers
        cur.execute(
            """
            SELECT worker_id FROM worker_heartbeats 
            WHERE last_seen < NOW() - INTERVAL '%s seconds' AND status = 'active';
            """,
            (HEARTBEAT_TIMEOUT_SECONDS,)
        )
        dead_workers = cur.fetchall()
        
        for row in dead_workers:
            worker_id = row[0]
            
            # Set their heartbeat status to dead
            cur.execute(
                "UPDATE worker_heartbeats SET status = 'dead' WHERE worker_id = %s;",
                (worker_id,)
            )
            
            # Requeue jobs
            cur.execute(
                """
                UPDATE jobs
                SET status = 'pending', worker_id = NULL
                WHERE status IN ('claimed', 'running')
                AND worker_id = %s
                RETURNING id;
                """,
                (worker_id,)
            )
            requeued_jobs = cur.fetchall()
            requeued_count = len(requeued_jobs)
            
            for job_row in requeued_jobs:
                log_event(conn, job_row[0], 'requeued_by_watchdog', metadata={'old_worker_id': worker_id})
            
            # Log the recovery event
            print(f"Worker {worker_id} timed out. Re-queued {requeued_count} jobs.")
            
        conn.commit()

def cleanup_old_data(conn):
    with conn.cursor() as cur:
        # Delete old successful jobs
        cur.execute(
            """
            DELETE FROM jobs 
            WHERE status = 'succeeded' 
            AND finished_at < NOW() - INTERVAL '%s days';
            """,
            (RETENTION_DAYS_SUCCESS,)
        )
        succeeded_deleted = cur.rowcount
        
        # Delete old dead jobs
        cur.execute(
            """
            DELETE FROM dead_jobs 
            WHERE died_at < NOW() - INTERVAL '%s days';
            """,
            (RETENTION_DAYS_DEAD,)
        )
        dead_deleted = cur.rowcount
        
        if succeeded_deleted > 0 or dead_deleted > 0:
            print(f"Cleanup: Deleted {succeeded_deleted} old successful jobs and {dead_deleted} old dead jobs.")
            
        conn.commit()

def run_watchdog():
    print("Starting watchdog...")
    last_cleanup = 0
    with get_db_connection() as conn:
        while True:
            try:
                check_dead_workers(conn)
                
                # Run database cleanup once every hour (3600 seconds)
                now = time.time()
                if now - last_cleanup > 3600:
                    cleanup_old_data(conn)
                    last_cleanup = now
                    
            except Exception as e:
                print(f"Watchdog error: {e}")
                conn.rollback()
            time.sleep(10)

if __name__ == "__main__":
    run_watchdog()
