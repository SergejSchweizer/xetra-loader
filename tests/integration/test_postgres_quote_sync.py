import os
import subprocess
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.gold.quotes import build_quote_gold
from xetra_loader.sync import connect_postgres
from xetra_loader.sync.quotes import sync_quotes

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


def _quote(day: int, close: str) -> QuoteRecord:
    return QuoteRecord(
        isin="DE0000000001",
        exchange="XETRA",
        code="AAA",
        trade_date=date(2026, 8, day),
        open=Decimal("9"),
        high=Decimal("11"),
        low=Decimal("8"),
        close=Decimal(close),
        adjusted_close=Decimal(close),
        volume=100,
    )


def test_quote_sync_initial_replay_correction_and_new_date() -> None:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    _apply_sql("sql/schema/001_xetra_loader.sql")
    _apply_sql("sql/schema/002_roles.sql")
    _apply_sql("sql/sync/001_xetra_loader_sync.sql")
    _apply_sql("sql/schema/004_xetra_features.sql")
    connection = connect_postgres(DSN)
    try:
        with connection.transaction():
            connection.execute("TRUNCATE xetra_loader.listings CASCADE")
            connection.execute(
                "DELETE FROM xetra_loader_sync.loader_runs WHERE dataset = 'eod_quotes'"
            )
            connection.execute(
                "DELETE FROM xetra_loader_sync.sync_state WHERE dataset = 'eod_quotes'"
            )
            connection.execute(
                "DELETE FROM xetra_loader_sync.row_hashes WHERE dataset = 'eod_quotes'"
            )
            connection.execute(
                "INSERT INTO xetra_loader.listings "
                "(isin, exchange, code, fetched_at_utc, published_at_utc) "
                "VALUES ('DE0000000001', 'XETRA', 'AAA', now(), now())"
            )

        published = datetime(2026, 8, 22, 20, 0, tzinfo=UTC)
        initial_gold = build_quote_gold([_quote(21, "10")])
        first = sync_quotes(
            connection,
            initial_gold,
            run_id="quote-first",
            published_at_utc=published,
        )
        assert first.counters.inserted == 1
        assert connection.execute(
            "SELECT count(*) FROM xetra_loader_sync.row_hashes "
            "WHERE dataset = 'eod_quotes'"
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT adjusted_close_level, volume_level "
            "FROM xetra_loader.xetra_features"
        ).fetchone() == (Decimal("10"), 100)
        replay = sync_quotes(
            connection,
            initial_gold,
            run_id="quote-replay",
            published_at_utc=published,
        )
        assert replay.counters.total_mutations == 0
        assert connection.execute(
            "SELECT count(*) FROM xetra_loader.xetra_features"
        ).fetchone() == (1,)

        correction = sync_quotes(
            connection,
            build_quote_gold([_quote(21, "10.5")]),
            run_id="quote-correction",
            published_at_utc=published,
        )
        assert correction.counters.updated == 1
        assert connection.execute(
            "SELECT adjusted_close_level FROM xetra_loader.xetra_features"
        ).fetchone() == (Decimal("10.5"),)

        extended = sync_quotes(
            connection,
            build_quote_gold([_quote(21, "10.5"), _quote(22, "11")]),
            run_id="quote-new-date",
            published_at_utc=published,
        )
        assert extended.counters.inserted == 1
        assert extended.counters.updated == 0
        quote_count = connection.execute("SELECT count(*) FROM xetra_loader.eod_quotes").fetchone()
        assert quote_count == (2,)

        removed = sync_quotes(
            connection,
            build_quote_gold([_quote(22, "11")]),
            run_id="quote-delete",
            published_at_utc=published,
        )
        assert removed.counters.deleted == 1
        assert connection.execute("SELECT count(*) FROM xetra_loader.eod_quotes").fetchone() == (1,)
        assert connection.execute(
            "SELECT count(*) FROM xetra_loader.xetra_features"
        ).fetchone() == (1,)
    finally:
        connection.close()
