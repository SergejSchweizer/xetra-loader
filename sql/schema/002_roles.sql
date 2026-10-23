BEGIN;

DO $$
BEGIN
    CREATE ROLE "xetra-data-loader" NOLOGIN;
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$$;

DO $$
BEGIN
    CREATE ROLE portfell_app NOLOGIN;
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$$;

DO $$
BEGIN
    CREATE ROLE xetra_data_loader_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$$;

GRANT "xetra-data-loader" TO xetra_data_loader_writer;
DO $$
BEGIN
    -- Parameter-level SET grants are available on PostgreSQL 15+. On older
    -- supported servers, temp_file_limit remains governed by its role/database
    -- setting and this newer privilege statement must not block provisioning.
    IF current_setting('server_version_num')::integer >= 150000 THEN
        EXECUTE 'GRANT SET ON PARAMETER temp_file_limit TO "xetra-data-loader"';
    END IF;
END
$$;

REVOKE ALL ON SCHEMA xetra_loader FROM PUBLIC;
GRANT USAGE ON SCHEMA xetra_loader TO "xetra-data-loader", portfell_app;
REVOKE CREATE ON SCHEMA xetra_loader FROM "xetra-data-loader", portfell_app;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA xetra_loader
    TO "xetra-data-loader";
GRANT SELECT ON ALL TABLES IN SCHEMA xetra_loader TO portfell_app;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
    ON ALL TABLES IN SCHEMA xetra_loader FROM portfell_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA xetra_loader
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "xetra-data-loader";
ALTER DEFAULT PRIVILEGES IN SCHEMA xetra_loader
    GRANT SELECT ON TABLES TO portfell_app;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'xetra_loader_sync') THEN
        GRANT USAGE ON SCHEMA xetra_loader_sync TO "xetra-data-loader";
        REVOKE CREATE ON SCHEMA xetra_loader_sync FROM "xetra-data-loader";
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA xetra_loader_sync
            TO "xetra-data-loader";
        REVOKE ALL ON SCHEMA xetra_loader_sync FROM portfell_app;
        REVOKE ALL ON ALL TABLES IN SCHEMA xetra_loader_sync FROM portfell_app;
    END IF;
END
$$;

COMMIT;
