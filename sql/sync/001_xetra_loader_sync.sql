BEGIN;
SET LOCAL TIME ZONE 'UTC';

CREATE SCHEMA IF NOT EXISTS xetra_loader_sync;

CREATE TABLE IF NOT EXISTS xetra_loader_sync.sync_state (
    dataset TEXT PRIMARY KEY,
    semantic_fingerprint CHAR(64) NOT NULL,
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    synced_at_utc TIMESTAMPTZ(6) NOT NULL,
    CHECK (semantic_fingerprint ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS xetra_loader_sync.loader_runs (
    run_id TEXT PRIMARY KEY,
    dataset TEXT NOT NULL,
    semantic_fingerprint CHAR(64) NOT NULL,
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    inserted_count BIGINT NOT NULL CHECK (inserted_count >= 0),
    updated_count BIGINT NOT NULL CHECK (updated_count >= 0),
    deleted_count BIGINT NOT NULL CHECK (deleted_count >= 0),
    retracted_count BIGINT NOT NULL CHECK (retracted_count >= 0),
    started_at_utc TIMESTAMPTZ(6) NOT NULL,
    finished_at_utc TIMESTAMPTZ(6) NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('applied', 'noop')),
    CHECK (semantic_fingerprint ~ '^[0-9a-f]{64}$'),
    CHECK (finished_at_utc >= started_at_utc)
);

CREATE TABLE IF NOT EXISTS xetra_loader_sync.row_hashes (
    dataset TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    row_sha256 CHAR(64) NOT NULL,
    PRIMARY KEY (dataset, entity_key),
    CHECK (btrim(dataset) <> ''),
    CHECK (btrim(entity_key) <> ''),
    CHECK (row_sha256 ~ '^[0-9a-f]{64}$')
);

REVOKE ALL ON SCHEMA xetra_loader_sync FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA xetra_loader_sync FROM PUBLIC;
GRANT USAGE ON SCHEMA xetra_loader_sync TO "xetra-data-loader";
REVOKE CREATE ON SCHEMA xetra_loader_sync FROM "xetra-data-loader";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA xetra_loader_sync
    TO "xetra-data-loader";
REVOKE ALL ON SCHEMA xetra_loader_sync FROM portfell_app;
REVOKE ALL ON ALL TABLES IN SCHEMA xetra_loader_sync FROM portfell_app;

COMMIT;
