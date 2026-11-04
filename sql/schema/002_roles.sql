BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'xetra_writer') THEN
        CREATE ROLE xetra_writer NOLOGIN;
    END IF;
END
$$;

DO $$
BEGIN
    CREATE ROLE portfell_app NOLOGIN;
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$$;

REVOKE ALL ON SCHEMA xetra_loader FROM PUBLIC;
GRANT USAGE ON SCHEMA xetra_loader TO xetra_writer, portfell_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA xetra_loader TO xetra_writer;
REVOKE CREATE ON SCHEMA xetra_loader FROM xetra_writer, portfell_app;

GRANT SELECT ON ALL TABLES IN SCHEMA xetra_loader TO portfell_app;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
    ON ALL TABLES IN SCHEMA xetra_loader FROM portfell_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA xetra_loader
    GRANT SELECT ON TABLES TO portfell_app;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'xetra_loader_sync') THEN
        REVOKE ALL ON SCHEMA xetra_loader_sync FROM portfell_app;
        REVOKE ALL ON ALL TABLES IN SCHEMA xetra_loader_sync FROM portfell_app;
        GRANT USAGE ON SCHEMA xetra_loader_sync TO xetra_writer;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA xetra_loader_sync
            TO xetra_writer;
    END IF;
END
$$;

COMMIT;
