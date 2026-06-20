import os
import sys
import time
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connection import get_db_connection

HEARTBEAT_TIMEOUT_SECONDS = 30

def check_dead_workers(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE worker_heartbeats
            SET status = 'dead'
            WHERE status = 'active' 
            AND last_seen < NOW() - INTERVAL '%s seconds'
            RETURNING worker_id;
            """,
            (HEARTBEAT_TIMEOUT_SECONDS,)
        )
        dead_workers = cur.fetchall()
        
        if dead_workers:
            worker_ids = tuple([w[0] for w in dead_workers])
            print(f"Detected dead workers: {worker_ids}")
            
            cur.execute(
                """
                UPDATE jobs
                SET status = 'pending', worker_id = NULL
                WHERE status IN ('claimed', 'running')
                AND worker_id IN %s;
                """,
                (worker_ids,)
            )
            requeued_count = cur.rowcount
            print(f"Requeued {requeued_count} jobs from dead workers.")
            
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
