CREATE TYPE job_status AS ENUM ('pending', 'claimed', 'running', 'succeeded', 'failed', 'retrying');
CREATE TYPE worker_status AS ENUM ('active', 'dead');

CREATE TABLE IF NOT EXISTS jobs (
    id SERIAL PRIMARY KEY,
    type TEXT NOT NULL,
    payload JSONB,
    priority INT DEFAULT 5,
    status job_status DEFAULT 'pending',
    worker_id TEXT,
    attempts INT DEFAULT 0,
    max_attempts INT DEFAULT 3,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    claimed_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error TEXT
);

CREATE TABLE IF NOT EXISTS worker_heartbeats (
    worker_id TEXT PRIMARY KEY,
    last_seen TIMESTAMPTZ DEFAULT NOW(),
    status worker_status DEFAULT 'active',
    jobs_completed INT DEFAULT 0,
    active_threads INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS job_events (
    id SERIAL PRIMARY KEY,
    job_id INT REFERENCES jobs(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    worker_id TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_events_job_id ON job_events(job_id);
