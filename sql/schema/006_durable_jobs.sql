BEGIN;

CREATE TABLE IF NOT EXISTS xetra_loader.loader_jobs (
    job_id UUID PRIMARY KEY,
    job_type TEXT NOT NULL CHECK (job_type = 'weekly_sync'),
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    result JSONB,
    error_code TEXT,
    created_at_utc TIMESTAMPTZ(6) NOT NULL DEFAULT clock_timestamp(),
    started_at_utc TIMESTAMPTZ(6),
    finished_at_utc TIMESTAMPTZ(6),
    CHECK (
        (status IN ('queued', 'running') AND finished_at_utc IS NULL)
        OR (status IN ('succeeded', 'failed') AND finished_at_utc IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS loader_jobs_one_active_weekly_sync_idx
    ON xetra_loader.loader_jobs (job_type)
    WHERE status IN ('queued', 'running');

CREATE INDEX IF NOT EXISTS loader_jobs_queue_order_idx
    ON xetra_loader.loader_jobs (created_at_utc, job_id)
    WHERE status = 'queued';

CREATE OR REPLACE FUNCTION xetra_loader.notify_loader_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('xetra_loader_jobs', NEW.job_id::text);
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS loader_jobs_notify_insert ON xetra_loader.loader_jobs;
CREATE TRIGGER loader_jobs_notify_insert
    AFTER INSERT ON xetra_loader.loader_jobs
    FOR EACH ROW EXECUTE FUNCTION xetra_loader.notify_loader_job();

REVOKE ALL ON TABLE xetra_loader.loader_jobs FROM PUBLIC, portfell_app;
GRANT SELECT, INSERT, UPDATE ON TABLE xetra_loader.loader_jobs TO "xetra-data-loader";
REVOKE ALL ON FUNCTION xetra_loader.notify_loader_job() FROM PUBLIC;

COMMENT ON TABLE xetra_loader.loader_jobs IS
    'Durable API job queue; xdl-api consumes inserts through PostgreSQL LISTEN/NOTIFY.';

COMMIT;
