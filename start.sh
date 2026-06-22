#!/bin/bash

# Initialize the database schema
echo "Initializing database..."
python init_db.py

# Start the worker in the background
echo "Starting worker..."
python worker/worker.py &

# Start the watchdog in the background
echo "Starting watchdog..."
python worker/watchdog.py &

# Start the API (this runs in the foreground)
echo "Starting API..."
exec uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-8000}
