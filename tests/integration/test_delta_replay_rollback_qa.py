import json
import os
import subprocess
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from psycopg import Connection
from psycopg.errors import LockNotAvailable, QueryCanceled

from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.gold.quotes import build_quote_gold
from xetra_loader.sync import connect_postgres, digest_map, row_digests
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


def _quote(
    day: int,
    close: str,
    *,
    volume: int = 100,
    open_value: str = "9",
    high: str = "11",
    low: str = "8",
) -> QuoteRecord:
    return QuoteRecord(
        isin="DE0000000001",
        exchange="XETRA",
        code="AAA",
        trade_date=date(2026, 8, day),
        open=Decimal(open_value),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        adjusted_close=Decimal(close),
        volume=volume,
    )


def _gold(*records: QuoteRecord):
    return build_quote_gold(records)


def _row_hashes(connection: Connection[Any]) -> dict[str, str]:
    rows = connection.execute(
        "SELECT entity_key, row_sha256 FROM xetra_loader_sync.row_hashes "
        "WHERE dataset = 'eod_quotes'"
    ).fetchall()
    return {str(key): str(value) for key, value in rows}


def _view_marker(connection: Connection[Any]) -> int:
    row = connection.execute(
        "SELECT relfilenode FROM pg_class "
        "WHERE oid = 'xetra_loader.xetra_features'::regclass"
    ).fetchone()
    assert row is not None
    return int(row[0])


def _feature_projection(connection: Connection[Any]) -> tuple[object, ...]:
    row = connection.execute(
        "SELECT adjusted_close_log_return_1obs, high_low_range_1obs, "
        "intraday_return_1obs, overnight_gap_1obs "
        "FROM xetra_loader.xetra_features WHERE code = 'AAA' "
        "AND trade_date = '2026-08-21'"
    ).fetchone()
    assert row is not None
    return tuple(row)


def _write_report(path: Path, checks: dict[str, bool], counters: dict[str, int]) -> None:
    report: dict[str, Any] = {
        "work_order": "xdl-pr065-qa-xetra-delta-replay-and-rollback",
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "counters": counters,
        "secrets_included": False,
    }
    path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def test_delta_replay_and_rollback_converge_without_stale_state(tmp_path: Path) -> None:
    if DSN is None:
        pytest.skip("XDL_TEST_POSTGRES_DSN is not configured")
    _apply_sql("sql/schema/001_xetra_loader.sql")
    _apply_sql("sql/schema/002_roles.sql")
    _apply_sql("sql/sync/001_xetra_loader_sync.sql")
    _apply_sql("sql/schema/004_xetra_features.sql")
    connection = connect_postgres(DSN)
    blocker: Connection[Any] | None = None
    counters = {"inserted": 0, "updated": 0, "deleted": 0, "unchanged": 0, "refreshes": 0}
    try:
        with connection.transaction():
            connection.execute("TRUNCATE xetra_loader.listings CASCADE")
            connection.execute(
                "INSERT INTO xetra_loader.listings "
                "(isin, exchange, code, fetched_at_utc, published_at_utc) "
                "VALUES ('DE0000000001', 'XETRA', 'AAA', now(), now())"
            )

        published = datetime(2026, 8, 22, 20, 0, tzinfo=UTC)
        first_gold = _gold(_quote(21, "10"), _quote(22, "11"))
        first = sync_quotes(
            connection, first_gold, run_id="delta-first", published_at_utc=published
        )
        counters["inserted"] += first.counters.inserted
        counters["refreshes"] += 1
        initial_hashes = _row_hashes(connection)
        assert initial_hashes == digest_map(row_digests("eod_quotes", first_gold.semantic_rows()))

        before_replay = _view_marker(connection)
        replay = sync_quotes(
            connection, first_gold, run_id="delta-replay", published_at_utc=published
        )
        counters["unchanged"] += int(replay.counters.total_mutations == 0)
        assert replay.status == "noop"
        assert _view_marker(connection) == before_replay

        adjusted_gold = _gold(_quote(21, "10.5"), _quote(22, "11"))
        before_adjusted = _view_marker(connection)
        adjusted = sync_quotes(
            connection, adjusted_gold, run_id="delta-adjusted", published_at_utc=published
        )
        counters["updated"] += adjusted.counters.updated
        counters["refreshes"] += 1
        assert adjusted.counters.updated == 1
        assert _view_marker(connection) != before_adjusted
        assert len(set(initial_hashes) ^ set(_row_hashes(connection))) == 0
        assert len(
            [key for key in initial_hashes if initial_hashes[key] != _row_hashes(connection)[key]]
        ) == 1

        volume_gold = _gold(_quote(21, "10.5", volume=250), _quote(22, "11"))
        before_volume = _view_marker(connection)
        volume = sync_quotes(
            connection, volume_gold, run_id="delta-volume", published_at_utc=published
        )
        counters["updated"] += volume.counters.updated
        counters["refreshes"] += 1
        assert volume.counters.updated == 1
        assert _view_marker(connection) != before_volume

        baseline_projection = _feature_projection(connection)
        raw_variants = (
            _quote(21, "10.5", volume=250, open_value="9.5"),
            _quote(21, "10.5", volume=250, open_value="9.5", high="11.5"),
            _quote(21, "10.5", volume=250, open_value="9.5", high="11.5", low="7.5"),
            _quote(21, "10.7", volume=250, open_value="9.5", high="11.5", low="7.5"),
        )
        current = volume_gold
        raw_feature_changes = 0
        for index, variant in enumerate(raw_variants):
            candidate = _gold(variant, _quote(22, "11"))
            before = _view_marker(connection)
            outcome = sync_quotes(
                connection,
                candidate,
                run_id=f"delta-raw-{index}",
                published_at_utc=published,
            )
            counters["updated"] += outcome.counters.updated
            counters["refreshes"] += 1
            assert outcome.counters.updated == 1
            assert _view_marker(connection) != before
            raw_feature_changes += int(_feature_projection(connection) != baseline_projection)
            current = candidate
        assert raw_feature_changes == len(raw_variants)

        metadata_before = _view_marker(connection)
        metadata_replay = sync_quotes(
            connection,
            current,
            run_id="delta-metadata-only",
            published_at_utc=published + timedelta(hours=1),
        )
        counters["unchanged"] += int(metadata_replay.counters.total_mutations == 0)
        assert metadata_replay.status == "noop"
        assert _view_marker(connection) == metadata_before

        removed = _gold(raw_variants[-1])
        before_delete = _view_marker(connection)
        deletion = sync_quotes(
            connection, removed, run_id="delta-delete", published_at_utc=published
        )
        counters["deleted"] += deletion.counters.deleted
        counters["refreshes"] += 1
        assert deletion.counters.deleted == 1
        assert _view_marker(connection) != before_delete
        assert connection.execute("SELECT count(*) FROM xetra_loader.eod_quotes").fetchone() == (1,)

        failed_target = _gold(raw_variants[-1], _quote(23, "12", high="13"))
        prior_hashes = _row_hashes(connection)
        prior_marker = _view_marker(connection)
        blocker = connect_postgres(DSN)
        blocker.execute("SELECT count(*) FROM xetra_loader.xetra_features")
        connection.execute("SET statement_timeout = '2s'")
        connection.commit()
        with pytest.raises((LockNotAvailable, QueryCanceled)):
            sync_quotes(
                connection, failed_target, run_id="delta-rollback", published_at_utc=published
            )
        connection.execute("SET statement_timeout = '0'")
        connection.commit()
        assert connection.execute("SELECT count(*) FROM xetra_loader.eod_quotes").fetchone() == (1,)
        assert _row_hashes(connection) == prior_hashes
        assert _view_marker(connection) == prior_marker
        blocker.rollback()
        blocker.close()
        blocker = None

        recovered = sync_quotes(
            connection, failed_target, run_id="delta-recover", published_at_utc=published
        )
        counters["inserted"] += recovered.counters.inserted
        counters["refreshes"] += 1
        assert recovered.counters.inserted == 1
        assert connection.execute("SELECT count(*) FROM xetra_loader.eod_quotes").fetchone() == (2,)
        assert _row_hashes(connection) == digest_map(
            row_digests("eod_quotes", failed_target.semantic_rows())
        )

        checks = {
            "complete_bootstrap_hashes": True,
            "adjusted_close_delta": True,
            "volume_delta": True,
            "raw_ohlc_deltas_refresh": True,
            "metadata_only_replay_no_refresh": True,
            "stale_row_deleted": True,
            "rollback_preserved_committed_state": True,
            "rerun_converged_without_duplicates": True,
        }
        report_path = tmp_path / "xdl-pr065-delta-replay-rollback.json"
        _write_report(report_path, checks, counters)
        report = json.loads(report_path.read_text(encoding="utf-8"))
        assert report["status"] == "PASS"
    finally:
        if blocker is not None:
            blocker.rollback()
            blocker.close()
        connection.close()
