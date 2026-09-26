import os
import subprocess
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from xetra_loader.contracts.corporate_actions import SplitEvent, retract_split
from xetra_loader.gold.splits import build_split_gold
from xetra_loader.sync import connect_postgres
from xetra_loader.sync.splits import sync_splits

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


def _event(ratio: str) -> SplitEvent:
    numerator, denominator = ratio.split(":", 1)
    return SplitEvent(
        isin="DE0000000001",
        exchange="XETRA",
        code="AAA",
        event_date=date(2026, 8, 20),
        split_ratio=ratio,
        split_factor=Decimal(numerator) / Decimal(denominator),
    )


def test_split_sync_initial_replay_correction_and_retraction() -> None:
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
                "DELETE FROM xetra_loader_sync.loader_runs WHERE dataset = 'splits'"
            )
            connection.execute(
                "DELETE FROM xetra_loader_sync.sync_state WHERE dataset = 'splits'"
            )
            connection.execute(
                "DELETE FROM xetra_loader_sync.row_hashes WHERE dataset = 'splits'"
            )
            connection.execute(
                "INSERT INTO xetra_loader.listings "
                "(isin, exchange, code, fetched_at_utc, published_at_utc) "
                "VALUES ('DE0000000001', 'XETRA', 'AAA', now(), now())"
            )

        published = datetime(2026, 8, 22, 20, 0, tzinfo=UTC)
        old = _event("2:1")
        first_gold = build_split_gold([old])
        first = sync_splits(
            connection,
            first_gold,
            run_id="split-first",
            published_at_utc=published,
        )
        assert first.counters.inserted == 1
        replay = sync_splits(
            connection,
            first_gold,
            run_id="split-replay",
            published_at_utc=published,
        )
        assert replay.counters.total_mutations == 0

        new = _event("3:1")
        correction = sync_splits(
            connection,
            build_split_gold([new, retract_split(old)]),
            run_id="split-correction",
            published_at_utc=published,
        )
        assert correction.counters.inserted == 1
        assert correction.counters.retracted == 1
        assert connection.execute("SELECT count(*) FROM xetra_loader.splits").fetchone() == (1,)

        removed = sync_splits(
            connection,
            build_split_gold([retract_split(new)]),
            run_id="split-retraction",
            published_at_utc=published,
        )
        assert removed.counters.retracted == 1
        assert connection.execute("SELECT count(*) FROM xetra_loader.splits").fetchone() == (0,)
    finally:
        connection.close()
