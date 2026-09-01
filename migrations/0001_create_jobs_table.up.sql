CREATE TABLE IF NOT EXISTS jobs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payload          JSONB NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','dispatched','running','succeeded','failed','dead')),
    priority         SMALLINT NOT NULL DEFAULT 0,
    run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts         INT NOT NULL DEFAULT 0,
    max_attempts     INT NOT NULL DEFAULT 5,
    idempotency_key  TEXT UNIQUE,
    last_error       TEXT,
    result           JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Speeds up the scheduler's pending-job scan (status + run_at + priority)
CREATE INDEX IF NOT EXISTS idx_jobs_pending_scan
    ON jobs (status, run_at, priority DESC)
    WHERE status = 'pending';
