import os
from psycopg2 import pool
from contextlib import contextmanager
from dotenv import load_dotenv
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")

db_pool = pool.ThreadedConnectionPool(
    minconn=1,
    maxconn=20,
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    port=DB_PORT,
)

@contextmanager
def get_db_connection():
    """Yields a database connection from the pool."""
    conn = db_pool.getconn()
    try:
        yield conn
    finally:
        db_pool.putconn(conn)
