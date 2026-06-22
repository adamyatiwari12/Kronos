import os
import psycopg2
import sys

def main():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL is not set.")
        sys.exit(1)

    print("Connecting to database to initialize schema...")
    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        
        schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "db", "schema.sql")
        
        with open(schema_path, "r") as f:
            schema_sql = f.read()
            
        cur.execute(schema_sql)
        conn.commit()
        print("Schema initialized successfully.")
        
    except Exception as e:
        print(f"Error initializing schema: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals() and conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    main()
