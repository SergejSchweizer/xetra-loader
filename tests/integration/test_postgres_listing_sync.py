import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.gold.listings import build_listing_gold
from xetra_loader.sync import connect_postgres
from xetra_loader.sync.listings import prune_stale_listings, sync_listings

DSN = os.getenv("XDL_TEST_POSTGRES_DSN")
pytestmark = pytest.mark.integration


def _apply_sql(path: str) -> None:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    subprocess.run(
        ["psql", DSN, "-X", "-v", "ON_ERROR_STOP=1"],
        check=True,
        text=True,
        input=Path(path).read_text(encoding="utf-8"),
        capture_output=True,
    )


def test_listing_sync_initial_replay_and_one_update() -> None:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    _apply_sql("sql/schema/001_xetra_loader.sql")
    _apply_sql("sql/schema/002_roles.sql")
    _apply_sql("sql/sync/001_xetra_loader_sync.sql")
    connection = connect_postgres(DSN)
    try:
        with connection.transaction():
            connection.execute("TRUNCATE xetra_loader.listings CASCADE")
            connection.execute(
                "DELETE FROM xetra_loader_sync.loader_runs WHERE dataset = 'listings'"
            )
            connection.execute(
                "DELETE FROM xetra_loader_sync.sync_state WHERE dataset = 'listings'"
            )
            connection.execute(
                "DELETE FROM xetra_loader_sync.row_hashes WHERE dataset = 'listings'"
            )

        published = datetime(2026, 8, 22, 20, 0, tzinfo=UTC)
        first_gold = build_listing_gold([ListingRecord("DE0000000001", "XETRA", "AAA", name="A")])
        first = sync_listings(
            connection,
            first_gold,
            run_id="listing-first",
            published_at_utc=published,
        )
        assert first.counters.inserted == 1
        replay = sync_listings(
            connection,
            first_gold,
            run_id="listing-replay",
            published_at_utc=published,
        )
        assert replay.status == "noop"
        assert replay.counters.total_mutations == 0

        changed_gold = build_listing_gold(
            [
                ListingRecord(
                    "DE0000000001",
                    "XETRA",
                    "AAA",
                    name="Changed",
                    is_active=False,
                )
            ]
        )
        changed = sync_listings(
            connection,
            changed_gold,
            run_id="listing-change",
            published_at_utc=published,
        )
        assert changed.counters.updated == 1
        assert connection.execute(
            "SELECT name, is_active FROM xetra_loader.listings WHERE code = 'AAA'"
        ).fetchone() == ("Changed", False)
    finally:
        connection.close()


def test_listing_sync_removes_rows_outside_authoritative_gold_snapshot() -> None:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    _apply_sql("sql/schema/001_xetra_loader.sql")
    _apply_sql("sql/schema/002_roles.sql")
    _apply_sql("sql/sync/001_xetra_loader_sync.sql")
    connection = connect_postgres(DSN)
    try:
        with connection.transaction():
            connection.execute("TRUNCATE xetra_loader.listings CASCADE")
            connection.execute(
                "DELETE FROM xetra_loader_sync.loader_runs WHERE dataset = 'listings'"
            )
            connection.execute(
                "DELETE FROM xetra_loader_sync.sync_state WHERE dataset = 'listings'"
            )
            connection.execute(
                "DELETE FROM xetra_loader_sync.row_hashes WHERE dataset = 'listings'"
            )

        published = datetime(2026, 8, 22, 20, 0, tzinfo=UTC)
        initial_gold = build_listing_gold([ListingRecord("DE0000000001", "XETRA", "AAA", name="A")])
        sync_listings(connection, initial_gold, published_at_utc=published)

        with connection.transaction():
            connection.execute(
                "INSERT INTO xetra_loader.listings "
                "(isin, exchange, code, name, fetched_at_utc, published_at_utc) "
                "VALUES ('DE0000000002', 'XETRA', 'BBB', 'stale', %s, %s)",
                (published, published),
            )

        repaired = sync_listings(connection, initial_gold, published_at_utc=published)
        deleted = prune_stale_listings(connection, initial_gold)

        assert repaired.status == "applied"
        assert deleted == 1
        assert connection.execute("SELECT count(*) FROM xetra_loader.listings").fetchone() == (1,)
    finally:
        connection.close()


def test_listing_lifecycle_migration_backfills_existing_rows() -> None:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    _apply_sql("sql/schema/001_xetra_loader.sql")
    connection = connect_postgres(DSN)
    try:
        with connection.transaction():
            connection.execute("TRUNCATE xetra_loader.listings CASCADE")
            connection.execute("ALTER TABLE xetra_loader.listings DROP COLUMN is_active")
            connection.execute(
                "INSERT INTO xetra_loader.listings "
                "(isin, exchange, code, fetched_at_utc, published_at_utc) "
                "VALUES ('DE0000000001', 'XETRA', 'AAA', now(), now())"
            )

        _apply_sql("sql/schema/003_listing_lifecycle.sql")
        assert connection.execute(
            "SELECT is_active FROM xetra_loader.listings WHERE code = 'AAA'"
        ).fetchone() == (True,)
        assert connection.execute(
            "SELECT is_nullable, column_default FROM information_schema.columns "
            "WHERE table_schema = 'xetra_loader' AND table_name = 'listings' "
            "AND column_name = 'is_active'"
        ).fetchone() == ("NO", "true")
    finally:
        connection.close()
