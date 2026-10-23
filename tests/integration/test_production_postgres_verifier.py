import hashlib
import json
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from xetra_loader.contracts.corporate_actions import DividendEvent, SplitEvent
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.gold.dividends import build_dividend_gold
from xetra_loader.gold.listings import build_listing_gold
from xetra_loader.gold.quotes import build_quote_gold
from xetra_loader.gold.splits import build_split_gold
from xetra_loader.medallion.core import (
    JSONValue,
    Layer,
    MedallionLayout,
    canonical_json,
    read_manifest_payload,
    write_parquet_rows,
)
from xetra_loader.ops.bootstrap import PostgresEodhdBootstrapRuntime
from xetra_loader.ops.verify_postgres_sync import (
    DATASETS,
    DatasetVerification,
    ProductionAcceptanceReport,
    RoleVerification,
    TimestampVerification,
    _decimal_text,
    _key_differences,
    load_gold_snapshots,
    semantic_fingerprint,
    write_production_report,
)

pytestmark = pytest.mark.integration


def _records() -> tuple[ListingRecord, QuoteRecord, DividendEvent, SplitEvent]:
    listing = ListingRecord(
        isin="DE0000000001",
        exchange="XETRA",
        code="AAA",
        name="Fixture",
        instrument_type="ETF",
        currency="EUR",
        country="Germany",
    )
    quote = QuoteRecord(
        isin=listing.isin,
        exchange=listing.exchange,
        code=listing.code,
        trade_date=date(2026, 8, 21),
        open=Decimal("10"),
        high=Decimal("12"),
        low=Decimal("9"),
        close=Decimal("11"),
        adjusted_close=Decimal("11"),
        volume=100,
    )
    dividend = DividendEvent(
        isin=listing.isin,
        exchange=listing.exchange,
        code=listing.code,
        event_date=date(2026, 8, 20),
        value=Decimal("1.25"),
        currency="EUR",
    )
    split = SplitEvent(
        isin=listing.isin,
        exchange=listing.exchange,
        code=listing.code,
        event_date=date(2026, 8, 19),
        split_ratio="2:1",
        split_factor=Decimal("2"),
    )
    return listing, quote, dividend, split


def _write_dataset(
    layout: MedallionLayout,
    dataset: str,
    rows: list[dict[str, JSONValue]],
    fingerprint: str,
    retracted_keys: list[list[str]] | None = None,
) -> None:
    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    runtime._layout = layout
    for layer in (Layer.SILVER, Layer.GOLD):
        if layer is Layer.SILVER:
            runtime.persist_silver(dataset, rows)
        else:
            runtime.persist_gold(
                dataset,
                rows,
                row_count=len(rows),
                semantic_fingerprint=fingerprint,
                retracted_keys=tuple(tuple(key) for key in (retracted_keys or [])),
            )


def test_gold_snapshot_recomputes_exact_builder_fingerprints(tmp_path: Path) -> None:
    listing, quote, dividend, split = _records()
    listing_gold = build_listing_gold([listing])
    quote_gold = build_quote_gold([quote])
    dividend_gold = build_dividend_gold([dividend])
    split_gold = build_split_gold([split])
    layout = MedallionLayout(tmp_path)

    rows_by_dataset = {
        "listings": (
            [dict(row) for row in listing_gold.semantic_rows()],
            listing_gold.semantic_fingerprint,
        ),
        "eod_quotes": (
            [dict(row) for row in quote_gold.semantic_rows()],
            quote_gold.semantic_fingerprint,
        ),
        "dividends": (
            [dict(row) for row in dividend_gold.semantic_rows()],
            dividend_gold.semantic_fingerprint,
        ),
        "splits": (
            [dict(row) for row in split_gold.semantic_rows()],
            split_gold.semantic_fingerprint,
        ),
    }
    for dataset, (rows, fingerprint) in rows_by_dataset.items():
        _write_dataset(layout, dataset, rows, fingerprint)
        assert semantic_fingerprint(dataset, rows) == fingerprint

    snapshots = load_gold_snapshots(tmp_path)
    assert tuple(snapshots) == DATASETS
    assert all(snapshot.source_count == 1 for snapshot in snapshots.values())
    assert all(snapshot.row_count == 1 for snapshot in snapshots.values())
    assert all(snapshot.fingerprint_valid for snapshot in snapshots.values())


def test_gold_snapshot_reloads_tombstones_and_rejects_sidecar_tampering(tmp_path: Path) -> None:
    listing, _, dividend, _ = _records()
    layout = MedallionLayout(tmp_path)
    retracted = [[listing.isin, listing.exchange, listing.code, "a" * 64]]
    active_rows = [dict(row) for row in build_dividend_gold([dividend]).semantic_rows()]
    fingerprint = semantic_fingerprint("dividends", active_rows, retracted)

    for dataset in ("listings", "eod_quotes", "splits"):
        _write_dataset(layout, dataset, [], semantic_fingerprint(dataset, []))
    _write_dataset(layout, "dividends", active_rows, fingerprint, retracted)

    snapshots = load_gold_snapshots(tmp_path)
    assert snapshots["dividends"].retracted_keys == (tuple(retracted[0]),)

    write_parquet_rows(
        layout.retractions_path(Layer.GOLD, "dividends"),
        [
            {
                "isin": "DE0000000002",
                "exchange": "XETRA",
                "code": "AAA",
                "event_key": "a" * 64,
            }
        ],
    )
    with pytest.raises(ValueError, match="retractions fingerprint mismatch"):
        load_gold_snapshots(tmp_path)


def test_gold_persistence_writes_sorted_tombstone_sidecar(tmp_path: Path) -> None:
    layout = MedallionLayout(tmp_path)
    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    runtime._layout = layout
    retracted = (
        ("DE0000000002", "XETRA", "BBB", "b" * 64),
        ("DE0000000001", "XETRA", "AAA", "a" * 64),
    )
    fingerprint = semantic_fingerprint("dividends", [], [list(key) for key in sorted(retracted)])

    runtime.persist_gold(
        "dividends",
        [],
        row_count=0,
        semantic_fingerprint=fingerprint,
        retracted_keys=retracted,
    )

    from xetra_loader.medallion.core import read_snapshot_retractions

    assert read_snapshot_retractions(layout, Layer.GOLD, "dividends") == [
        {"isin": key[0], "exchange": key[1], "code": key[2], "event_key": key[3]}
        for key in sorted(retracted)
    ]
    manifest = read_manifest_payload(layout.manifest_path(Layer.GOLD, "dividends"))
    assert manifest["semantic_metadata"]["retractions_fingerprint"] == hashlib.sha256(
        canonical_json([list(key) for key in sorted(retracted)]).encode("utf-8")
    ).hexdigest()


def test_key_differences_streams_sorted_keys() -> None:
    assert _key_differences(
        iter((("A",), ("B",), ("D",))),
        iter((("A",), ("C",), ("D",), ("E",))),
    ) == (1, 2)


def test_decimal_text_normalizes_postgres_numeric_scale() -> None:
    assert _decimal_text(Decimal("0.000010")) == "0.00001"


def _passing_dataset() -> DatasetVerification:
    return DatasetVerification(
        source_count=1,
        source_rows_fingerprint="b" * 64,
        gold_count=1,
        gold_rows_fingerprint="b" * 64,
        postgres_count=1,
        missing_keys=0,
        extra_keys=0,
        duplicate_keys=0,
        gold_fingerprint="a" * 64,
        postgres_fingerprint="a" * 64,
        gold_manifest_valid=True,
        gold_min_date="2026-08-21",
        gold_max_date="2026-08-21",
        postgres_min_date="2026-08-21",
        postgres_max_date="2026-08-21",
    )


def _passing_report() -> ProductionAcceptanceReport:
    return ProductionAcceptanceReport(
        target_host="10.10.1.3",
        target_port=54321,
        resolved_addresses=("10.10.1.3",),
        target_matches=True,
        initial_run_ids={dataset: f"run-{dataset}" for dataset in DATASETS},
        committed_runs={dataset: True for dataset in DATASETS},
        datasets={dataset: _passing_dataset() for dataset in DATASETS},
        orphan_counts={"eod_quotes": 0, "dividends": 0, "splits": 0},
        timestamps=TimestampVerification(
            session_timezone="UTC",
            checked_columns=("xetra_loader.listings.fetched_at_utc",),
            invalid_columns=(),
            missing_columns=(),
        ),
        role=RoleVerification(
            select_tables={dataset: True for dataset in DATASETS},
            insert_denied=True,
            update_denied=True,
            delete_denied=True,
            ddl_denied=True,
            sync_schema_select_denied=True,
        ),
        replay_mutations={dataset: 0 for dataset in DATASETS},
    )


def test_acceptance_report_is_sanitized_and_fails_closed(tmp_path: Path) -> None:
    passing = _passing_report()
    assert passing.passed
    output = write_production_report(passing, tmp_path / "report.json")
    text = output.read_text(encoding="utf-8")
    decoded = json.loads(text)
    assert decoded["status"] == "PASS"
    assert "password" not in text.lower()
    assert "dsn" not in text.lower()
    assert "api_token" not in text.lower()

    full_import_only = replace(
        passing,
        replay_mutations={},
        weekly_replay_performed=False,
    )
    assert full_import_only.passed
    full_import_path = write_production_report(full_import_only, tmp_path / "full-only.json")
    full_import_report = json.loads(full_import_path.read_text(encoding="utf-8"))
    assert full_import_report["weekly_replay_performed"] is False
    assert full_import_report["unchanged_replay_mutations"] == {}

    failing = ProductionAcceptanceReport(
        target_host=passing.target_host,
        target_port=passing.target_port,
        resolved_addresses=passing.resolved_addresses,
        target_matches=passing.target_matches,
        initial_run_ids=passing.initial_run_ids,
        committed_runs=passing.committed_runs,
        datasets={
            **passing.datasets,
            "eod_quotes": replace(_passing_dataset(), missing_keys=1),
        },
        orphan_counts=passing.orphan_counts,
        timestamps=passing.timestamps,
        role=passing.role,
        replay_mutations=passing.replay_mutations,
    )
    assert not failing.passed

    missing_silver_rows = replace(
        passing,
        datasets={
            **passing.datasets,
            "eod_quotes": replace(_passing_dataset(), source_count=0),
        },
    )
    assert not missing_silver_rows.passed

    different_silver_rows = replace(
        passing,
        datasets={
            **passing.datasets,
            "eod_quotes": replace(_passing_dataset(), source_rows_fingerprint="c" * 64),
        },
    )
    assert not different_silver_rows.passed


def test_full_sync_parser_can_disable_delta_replay() -> None:
    from xetra_loader.ops.verify_postgres_sync import _parser

    args = _parser().parse_args(
        [
            "--execute-full-sync",
            "--confirm-destructive-reset",
            "--skip-weekly-replay",
            "--backup-root",
            "/tmp/backup",
        ]
    )
    assert args.skip_weekly_replay
