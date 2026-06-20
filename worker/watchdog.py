import os
import sys
import time
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connection import get_db_connection

HEARTBEAT_TIMEOUT_SECONDS = 15

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
                AND worker_id = %s;
                """,
                (worker_id,)
            )
            requeued_count = cur.rowcount
            
            # Log the recovery event as specified
            print(f"Worker {worker_id} timed out. Re-queued {requeued_count} jobs.")
            
        conn.commit()

def run_watchdog():
    print("Starting watchdog...")
    with get_db_connection() as conn:
        while True:
            try:
                check_dead_workers(conn)
            except Exception as e:
                print(f"Watchdog error: {e}")
                conn.rollback()
            time.sleep(10)

if __name__ == "__main__":
    run_watchdog()
