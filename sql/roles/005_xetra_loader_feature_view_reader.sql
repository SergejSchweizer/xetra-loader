BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'xetra_reader') THEN
        RAISE EXCEPTION 'required login role xetra_reader is missing';
    END IF;
END
$$;

DO $$
BEGIN
    GRANT USAGE ON SCHEMA xetra_loader TO xetra_reader;
    GRANT SELECT ON TABLE xetra_loader.xetra_features TO xetra_reader;
END
$$;

COMMIT;
