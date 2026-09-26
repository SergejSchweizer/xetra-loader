BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'xetra_loader') THEN
        RAISE EXCEPTION
            'required external role "xetra_loader" is missing; provision it before applying this migration';
    END IF;
END
$$;

REVOKE CREATE ON SCHEMA xetra_loader FROM xetra_loader;
REVOKE ALL PRIVILEGES ON TABLE xetra_loader.xetra_features FROM xetra_loader;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
    ON TABLE xetra_loader.xetra_features FROM xetra_loader;
REVOKE ALL PRIVILEGES ON FUNCTION xetra_loader.refresh_xetra_features() FROM xetra_loader;
REVOKE ALL PRIVILEGES ON SCHEMA xetra_loader_sync FROM xetra_loader;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA xetra_loader_sync FROM xetra_loader;

GRANT USAGE ON SCHEMA xetra_loader TO xetra_loader;
GRANT SELECT ON TABLE xetra_loader.xetra_features TO xetra_loader;

COMMIT;
