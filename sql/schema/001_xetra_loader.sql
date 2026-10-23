BEGIN;

CREATE SCHEMA IF NOT EXISTS xetra_loader;

CREATE OR REPLACE FUNCTION xetra_loader.next_business_day_utc(source_date DATE)
RETURNS TIMESTAMPTZ
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
STRICT
AS $$
    SELECT (
        source_date + CASE EXTRACT(ISODOW FROM source_date)::integer
            WHEN 5 THEN 3
            WHEN 6 THEN 2
            ELSE 1
        END
    )::timestamp AT TIME ZONE 'UTC'
$$;

CREATE TABLE IF NOT EXISTS xetra_loader.listings (
    isin TEXT NOT NULL,
    exchange TEXT NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    instrument_type TEXT,
    currency TEXT,
    country TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    fetched_at_utc TIMESTAMPTZ(6) NOT NULL,
    published_at_utc TIMESTAMPTZ(6) NOT NULL,
    available_at_utc TIMESTAMPTZ(6) GENERATED ALWAYS AS (
        xetra_loader.next_business_day_utc((published_at_utc AT TIME ZONE 'UTC')::date)
    ) STORED,
    PRIMARY KEY (isin, exchange, code),
    CHECK (btrim(isin) <> ''),
    CHECK (btrim(exchange) <> ''),
    CHECK (btrim(code) <> '')
);

CREATE TABLE IF NOT EXISTS xetra_loader.eod_quotes (
    isin TEXT NOT NULL,
    exchange TEXT NOT NULL,
    code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    timestamp_eod TIMESTAMPTZ(6) NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    adjusted_close NUMERIC,
    volume BIGINT,
    fetched_at_utc TIMESTAMPTZ(6) NOT NULL,
    published_at_utc TIMESTAMPTZ(6) NOT NULL,
    available_at_utc TIMESTAMPTZ(6) GENERATED ALWAYS AS (
        xetra_loader.next_business_day_utc(trade_date)
    ) STORED,
    PRIMARY KEY (isin, exchange, code, trade_date),
    FOREIGN KEY (isin, exchange, code)
        REFERENCES xetra_loader.listings (isin, exchange, code),
    CHECK (timestamp_eod = (trade_date::timestamp AT TIME ZONE 'UTC')),
    CHECK (volume IS NULL OR volume >= 0)
);

CREATE TABLE IF NOT EXISTS xetra_loader.dividends (
    isin TEXT NOT NULL,
    exchange TEXT NOT NULL,
    code TEXT NOT NULL,
    event_key CHAR(64) NOT NULL,
    event_date DATE NOT NULL,
    declaration_date DATE,
    record_date DATE,
    payment_date DATE,
    value NUMERIC NOT NULL,
    currency TEXT,
    period TEXT,
    fetched_at_utc TIMESTAMPTZ(6) NOT NULL,
    published_at_utc TIMESTAMPTZ(6) NOT NULL,
    available_at_utc TIMESTAMPTZ(6) GENERATED ALWAYS AS (
        xetra_loader.next_business_day_utc(event_date)
    ) STORED,
    PRIMARY KEY (isin, exchange, code, event_key),
    FOREIGN KEY (isin, exchange, code)
        REFERENCES xetra_loader.listings (isin, exchange, code),
    CHECK (event_key ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS xetra_loader.splits (
    isin TEXT NOT NULL,
    exchange TEXT NOT NULL,
    code TEXT NOT NULL,
    event_key CHAR(64) NOT NULL,
    event_date DATE NOT NULL,
    split_ratio TEXT NOT NULL,
    split_factor NUMERIC,
    fetched_at_utc TIMESTAMPTZ(6) NOT NULL,
    published_at_utc TIMESTAMPTZ(6) NOT NULL,
    available_at_utc TIMESTAMPTZ(6) GENERATED ALWAYS AS (
        xetra_loader.next_business_day_utc(event_date)
    ) STORED,
    PRIMARY KEY (isin, exchange, code, event_key),
    FOREIGN KEY (isin, exchange, code)
        REFERENCES xetra_loader.listings (isin, exchange, code),
    CHECK (event_key ~ '^[0-9a-f]{64}$'),
    CHECK (btrim(split_ratio) <> '')
);

-- Upgrade tables created by earlier schema versions as well as keeping fresh
-- installations on the same generated-column contract. Adding a stored
-- generated column computes the value for every existing row.
ALTER TABLE xetra_loader.listings
    ADD COLUMN IF NOT EXISTS available_at_utc TIMESTAMPTZ(6) GENERATED ALWAYS AS (
        xetra_loader.next_business_day_utc((published_at_utc AT TIME ZONE 'UTC')::date)
    ) STORED;
ALTER TABLE xetra_loader.eod_quotes
    ADD COLUMN IF NOT EXISTS available_at_utc TIMESTAMPTZ(6) GENERATED ALWAYS AS (
        xetra_loader.next_business_day_utc(trade_date)
    ) STORED;
ALTER TABLE xetra_loader.eod_quotes
    ALTER COLUMN close DROP NOT NULL;
ALTER TABLE xetra_loader.dividends
    ADD COLUMN IF NOT EXISTS available_at_utc TIMESTAMPTZ(6) GENERATED ALWAYS AS (
        xetra_loader.next_business_day_utc(event_date)
    ) STORED;
ALTER TABLE xetra_loader.splits
    ADD COLUMN IF NOT EXISTS available_at_utc TIMESTAMPTZ(6) GENERATED ALWAYS AS (
        xetra_loader.next_business_day_utc(event_date)
    ) STORED;

COMMIT;
