"""Recoverable full-lake JSON-to-Parquet migration tests."""

import hashlib
import os
from collections.abc import Iterable, Iterator, Mapping
from contextlib import AbstractContextManager, nullcontext
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from threading import Barrier, Lock
from typing import cast

import polars as pl
import pytest
import scripts.migrate_lake_to_parquet as lake_migration
from scripts.migrate_lake_to_parquet import (
    _archive_legacy_artifacts,
    _canonical_quote_payload,
    _convert_canonical_bronze_payloads,
    _convert_snapshot,
    _decode_bronze_payload_json,
    _legacy_artifact,
    _nested_quote_canonical_target,
    _obsolete_checkpoint_archives,
    _semantically_equal,
    _unreferenced_canonical_artifacts,
    _unreferenced_snapshot_sidecars,
    _validate_canonical_lake,
    _verify_nested_quote_legacy_sources,
    migrate_lake,
)

import xetra_loader.ops.bootstrap as bootstrap_module
from xetra_loader.contracts.corporate_actions import DividendEvent, SplitEvent
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.gold.dividends import build_dividend_gold
from xetra_loader.gold.splits import build_split_gold
from xetra_loader.medallion.core import (
    BronzePartition,
    GoldArtifact,
    JSONValue,
    Layer,
    Manifest,
    MedallionLayout,
    SilverPartition,
    canonical_json,
    read_dataset_rows,
    read_parquet_rows,
    write_manifest_parquet,
    write_parquet_rows,
    write_raw_listing_partitions,
)
from xetra_loader.ops.bootstrap import PostgresEodhdBootstrapRuntime, SnapshotWrite
from xetra_loader.ops.verify_postgres_sync import (
    DATASETS,
    semantic_fingerprint,
    validate_gold_fingerprints_streaming,
)


def _legacy_lake(root: Path) -> MedallionLayout:
    layout = MedallionLayout(root)
    listing = ListingRecord("DE0000000001", "XETRA", "AAA")
    quote = QuoteRecord(
        isin=listing.isin,
        exchange=listing.exchange,
        code=listing.code,
        trade_date=date(2026, 8, 21),
        open=Decimal("9.5"),
        high=Decimal("10.5"),
        low=Decimal("9.0"),
        close=Decimal("10.0"),
        adjusted_close=Decimal("10.0"),
        volume=100,
    )
    dividend = build_dividend_gold(
        [
            DividendEvent(
                listing.isin,
                listing.exchange,
                listing.code,
                date(2026, 8, 20),
                Decimal("1.25"),
            )
        ]
    )
    split = build_split_gold(
        [
            SplitEvent(
                listing.isin,
                listing.exchange,
                listing.code,
                date(2026, 8, 19),
                "2:1",
                Decimal("2"),
            )
        ]
    )
    gold_rows: dict[str, list[dict[str, JSONValue]]] = {
        "listings": [listing.semantic_dict()],
        "eod_quotes": [quote.semantic_dict()],
        "dividends": [dict(row) for row in dividend.semantic_rows()],
        "splits": [dict(row) for row in split.semantic_rows()],
    }
    for dataset in DATASETS:
        for layer in (Layer.SILVER, Layer.GOLD):
            directory = layout.dataset_path(layer, dataset)
            directory.mkdir(parents=True, exist_ok=True)
            rows = gold_rows[dataset]
            if layer is Layer.SILVER and dataset == "dividends":
                dividend_row = rows[0]
                rows = [{**dividend_row, "late_schema_probe": None} for _ in range(100)] + [
                    {**dividend_row, "late_schema_probe": "2016-02-23"}
                ]
            (directory / "data.json").write_text(canonical_json(rows) + "\n", encoding="utf-8")

        semantic_metadata: dict[str, JSONValue] = {
            "row_count": len(gold_rows[dataset]),
            "builder_semantic_fingerprint": semantic_fingerprint(dataset, gold_rows[dataset]),
        }
        if dataset in {"dividends", "splits"}:
            retractions = layout.legacy_retractions_path(Layer.GOLD, dataset)
            retractions.write_text("[]\n", encoding="utf-8")
            semantic_metadata["retractions_fingerprint"] = hashlib.sha256(
                canonical_json([]).encode("utf-8")
            ).hexdigest()
        manifest = Manifest(dataset, Layer.GOLD, semantic_metadata, {})
        legacy_manifest_path = layout.legacy_manifest_path(Layer.GOLD, dataset)
        legacy_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        legacy_manifest_path.write_text(manifest.to_json() + "\n", encoding="utf-8")

    bronze_listings = layout.dataset_path(Layer.BRONZE, "listings") / "data.json"
    bronze_listings.parent.mkdir(parents=True, exist_ok=True)
    bronze_listings.write_text(
        '[{"Code":"AAA","Exchange":"XETRA","ISIN":"ISIN"}]\n',
        encoding="utf-8",
    )

    bronze_partition = (
        layout.dataset_path(Layer.BRONZE, "eod_quotes") / "partitions" / "AAA!XETRA!ISIN.parquet"
    )
    bronze_partition.parent.mkdir(parents=True, exist_ok=True)
    bronze_partition.with_suffix(".json").write_text(
        '[{"isin":"ISIN","exchange":"XETRA","code":"AAA","payload":{"close":12}}]\n',
        encoding="utf-8",
    )
    return layout


def test_migration_writes_canonical_parquet_and_archives_legacy_artifacts(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    backup_parent = tmp_path / "backups"
    layout = _legacy_lake(root)
    obsolete_daily = (
        root / "bronze/dataset_type=eod_quotes/exchange=XETRA/instrument_type=unknown/"
        "symbol=AAA/timeframe=1d/year=2026/month=2026-08/date=2026-08-21/data.parquet"
    )
    write_parquet_rows(obsolete_daily, [{"code": "AAA", "payload_json": "{}"}])
    obsolete_checkpoint = root / "weekly.checkpoint.aborted-20260824T1644Z.parquet"
    write_parquet_rows(
        obsolete_checkpoint,
        [{"version": 1, "completed": ["listings"]}],
    )
    probe_checkpoint = root / "weekly.checkpoint.aborted-role-probe-20261023.parquet"
    write_parquet_rows(
        probe_checkpoint,
        [{"version": 1, "completed": ["listings"], "details": {"listings": {"probe": True}}}],
    )

    report = migrate_lake(root, backup_parent)

    assert report["status"] == "PASS"
    assert report["datasets_fingerprint_valid"] == len(DATASETS)
    assert report["obsolete_checkpoint_archives_preserved"] == 2
    backup = Path(str(report["recovery_backup"]))
    assert (backup / obsolete_checkpoint.name).is_file()
    assert (backup / probe_checkpoint.name).is_file()
    assert not obsolete_checkpoint.exists()
    assert not probe_checkpoint.exists()
    assert (backup / obsolete_daily.relative_to(root)).is_file()
    assert not obsolete_daily.exists()
    assert (backup / "silver" / "eod_quotes" / "data.json").exists()
    assert (backup / "gold" / "dividends" / "retractions.json").exists()
    assert not (root / "silver" / "eod_quotes" / "data.json").exists()
    assert not (root / "gold" / "dividends" / "data.json").exists()
    assert not list(root.rglob("partitions/*.json"))
    assert not list(root.rglob("versions/*/partitions/*.parquet"))

    assert read_dataset_rows(layout, Layer.SILVER, "eod_quotes") == read_dataset_rows(
        layout, Layer.GOLD, "eod_quotes"
    )
    assert read_dataset_rows(layout, Layer.GOLD, "eod_quotes")[0]["close"] == "10"
    silver_dividends = read_dataset_rows(layout, Layer.SILVER, "dividends")
    assert len(silver_dividends) == 101
    assert any(row["late_schema_probe"] == "2016-02-23" for row in silver_dividends)

    listing_backup = backup / "bronze" / "listings" / "data.json"
    listing_snapshot_date = datetime.fromtimestamp(listing_backup.stat().st_mtime, UTC).date()
    raw_listing = layout.bronze_path(
        BronzePartition("listings", "XETRA", "unknown", "AAA", "static", listing_snapshot_date)
    )
    raw_listing_frame = pl.read_parquet(raw_listing)
    assert raw_listing_frame.to_dicts() == [
        {
            "isin": "ISIN",
            "exchange": "XETRA",
            "code": "AAA",
            "provider_payload": {"Code": "AAA", "Exchange": "XETRA", "ISIN": "ISIN"},
        }
    ]
    assert (
        write_raw_listing_partitions(
            layout,
            "listings",
            [{"Code": "AAA", "Exchange": "XETRA", "ISIN": "ISIN", "Name": "changed"}],
            skip_existing=True,
            snapshot_date=listing_snapshot_date,
        )
        == 0
    )
    assert pl.read_parquet(raw_listing).equals(raw_listing_frame)
    quote_source = backup / "bronze" / "eod_quotes" / "partitions" / "AAA!XETRA!ISIN.json"
    quote_snapshot_date = datetime.fromtimestamp(quote_source.stat().st_mtime, UTC).date()
    raw_quote = layout.bronze_path(
        BronzePartition("eod_quotes", "XETRA", "unknown", "AAA", "1d", quote_snapshot_date)
    )
    assert read_parquet_rows(raw_quote) == [
        {
            "isin": "ISIN",
            "exchange": "XETRA",
            "code": "AAA",
            "provider_payload": {"close": 12},
        }
    ]


def test_migration_repartitions_existing_versioned_parquet(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = MedallionLayout(tmp_path)
    dataset = "eod_quotes"
    version = "legacy-generation"
    rows = [
        {
            "isin": "DE0000000001",
            "exchange": "XETRA",
            "code": "AAA",
            "trade_date": "2026-08-21",
            "close": "10.00",
        },
        {
            "isin": "DE0000000002",
            "exchange": "XETRA",
            "code": "AAA",
            "trade_date": "2026-08-22",
            "close": "11.00",
        },
    ]
    legacy_partition = (
        layout.snapshot_version_path(Layer.SILVER, dataset, version)
        / "partitions"
        / "AAA!XETRA!DE0000000001.parquet"
    )
    write_parquet_rows(legacy_partition, [rows[0]])
    alias_partition = legacy_partition.with_name("AAA!XETRA!DE0000000002.parquet")
    write_parquet_rows(alias_partition, [rows[1]])
    legacy_manifest = Manifest(
        dataset=dataset,
        layer=Layer.SILVER,
        semantic_metadata={"row_count": len(rows)},
        run_metadata={"storage_version": version},
    )
    legacy_manifest_path = layout.legacy_manifest_path(Layer.SILVER, dataset)
    legacy_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_manifest_path.write_text(legacy_manifest.to_json(), encoding="utf-8")
    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    runtime._layout = layout

    persisted_with: dict[str, bool] = {}
    persist = PostgresEodhdBootstrapRuntime._persist_partitioned_snapshot

    # Mirror the production method signature to verify the migration-only opt-out.
    def capture_sync_policy(  # noqa: PLR0913
        self: PostgresEodhdBootstrapRuntime,
        layer: Layer,
        dataset: str,
        snapshot_rows: Iterable[Mapping[str, JSONValue]],
        *,
        snapshot: SnapshotWrite,
        durable_sync: bool = True,
        presorted: bool = False,
    ) -> None:
        persisted_with["durable_sync"] = durable_sync
        persisted_with["presorted"] = presorted
        persist(
            self,
            layer,
            dataset,
            snapshot_rows,
            snapshot=snapshot,
            durable_sync=durable_sync,
            presorted=presorted,
        )

    monkeypatch.setattr(
        PostgresEodhdBootstrapRuntime,
        "_persist_partitioned_snapshot",
        capture_sync_policy,
    )

    assert _convert_snapshot(runtime, layout, Layer.SILVER, dataset)

    assert read_dataset_rows(layout, Layer.SILVER, dataset) == rows
    assert legacy_partition.exists()
    assert persisted_with["durable_sync"] is False
    assert persisted_with["presorted"] is True


def test_presorted_snapshot_writer_flushes_one_partition_before_consuming_the_next(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    runtime._layout = MedallionLayout(tmp_path)
    written_symbols: list[str] = []
    write_rows = bootstrap_module.write_parquet_rows

    def record_partition(
        path: Path,
        rows: Iterable[Mapping[str, JSONValue]],
        **kwargs: bool,
    ) -> None:
        write_rows(path, rows, **kwargs)
        written_symbols.append(path.stem.split("-")[0])

    monkeypatch.setattr(bootstrap_module, "write_parquet_rows", record_partition)

    def ordered_rows() -> Iterator[dict[str, JSONValue]]:
        yield {
            "isin": "DE0000000001",
            "exchange": "XETRA",
            "code": "AAA",
            "trade_date": "2026-08-21",
        }
        yield {
            "isin": "DE0000000002",
            "exchange": "XETRA",
            "code": "BBB",
            "trade_date": "2026-08-21",
        }
        assert written_symbols == ["AAA"], "writer buffered more than one symbol partition"
        yield {
            "isin": "DE0000000003",
            "exchange": "XETRA",
            "code": "CCC",
            "trade_date": "2026-08-21",
        }

    runtime._persist_partitioned_snapshot(
        Layer.SILVER,
        "eod_quotes",
        ordered_rows(),
        snapshot=SnapshotWrite({"row_count": 3}),
        durable_sync=False,
        presorted=True,
    )

    assert written_symbols == ["AAA", "BBB", "CCC"]


def test_partition_writer_uses_configured_bounded_parallelism() -> None:
    barrier = Barrier(2)
    guard = Lock()
    active = 0
    peak_active = 0

    def write(symbol: str) -> str:
        nonlocal active, peak_active
        with guard:
            active += 1
            peak_active = max(peak_active, active)
        try:
            if symbol in {"AAA", "BBB"}:
                barrier.wait(timeout=3)
            return symbol
        finally:
            with guard:
                active -= 1

    paths = bootstrap_module._run_partition_writes(
        (lambda symbol=symbol: write(symbol) for symbol in ("AAA", "BBB", "CCC")),
        workers=2,
    )

    assert paths == ["AAA", "BBB", "CCC"]
    assert peak_active == 2


def test_snapshot_sort_uses_polars_and_preserves_canonical_order() -> None:
    rows: list[dict[str, JSONValue]] = [
        {
            "isin": "DE0000000002",
            "exchange": "XETRA",
            "code": "BBB",
            "trade_date": "2026-08-21",
            "value": "z",
        },
        {
            "isin": "DE0000000001",
            "exchange": "XETRA",
            "code": "AAA",
            "trade_date": "2026-08-22",
            "value": "a",
        },
        {
            "isin": "DE0000000001",
            "exchange": "XETRA",
            "code": "AAA",
            "trade_date": "2026-08-21",
            "value": "b",
        },
        {
            "isin": "DE0000000002",
            "exchange": "XETRA",
            "code": "BBB",
            "trade_date": "2026-08-21",
            "value": "a",
        },
    ]

    actual = bootstrap_module._polars_sorted_snapshot_rows("eod_quotes", rows)
    expected = sorted(rows, key=lambda row: bootstrap_module._snapshot_sort_key("eod_quotes", row))

    assert actual == expected
    assert bootstrap_module._polars_sorted_snapshot_rows("eod_quotes", reversed(rows)) == expected


def test_empty_legacy_archive_does_not_create_backup_directory(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    backup = tmp_path / "backup"
    root.mkdir()

    assert _archive_legacy_artifacts(root, backup) == 0
    assert not backup.exists()


def test_migration_archives_unreferenced_canonical_snapshot_artifacts(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    layout = MedallionLayout(root)
    silver = layout.silver_path(SilverPartition("eod_quotes", "XETRA", "AAA", "1d", 2026))
    gold_artifact = GoldArtifact(
        "xetra-eod_quotes-gold-v1",
        "XETRA",
        "AAA",
        "a" * 64,
        "b" * 64,
    )
    gold = layout.gold_path(gold_artifact)
    dividend_artifact = GoldArtifact(
        "xetra-dividends-gold-v1",
        "XETRA",
        "AAA",
        "c" * 64,
        "d" * 64,
    )
    dividend_gold = layout.gold_path(dividend_artifact)
    orphan_artifact = GoldArtifact(
        gold_artifact.dataset_id,
        "XETRA",
        "AAA",
        "a" * 64,
        "c" * 64,
    )
    orphan = layout.gold_path(orphan_artifact)
    row = {
        "isin": "DE0000000001",
        "exchange": "XETRA",
        "code": "AAA",
        "trade_date": "2026-08-21",
    }
    for path in (silver, gold, dividend_gold, orphan):
        write_parquet_rows(path, [row])
    dividend_sidecar = layout.retractions_path(Layer.GOLD, "dividends")
    write_parquet_rows(
        dividend_sidecar,
        [
            {
                "isin": "DE0000000002",
                "exchange": "XETRA",
                "code": "AAA",
                "event_key": "e" * 64,
            }
        ],
    )

    for layer, dataset, artifact_paths, semantic_metadata in (
        (Layer.SILVER, "eod_quotes", [silver], {"row_count": 1}),
        (
            Layer.GOLD,
            "eod_quotes",
            [gold],
            {"dataset_id": gold_artifact.dataset_id, "row_count": 1},
        ),
        (
            Layer.GOLD,
            "dividends",
            [dividend_gold],
            {"dataset_id": dividend_artifact.dataset_id, "row_count": 1},
        ),
    ):
        manifest = Manifest(
            dataset,
            layer,
            semantic_metadata,
            {
                "artifact_paths": [path.relative_to(root).as_posix() for path in artifact_paths],
                "retractions_path": (
                    dividend_sidecar.relative_to(root).as_posix()
                    if dataset == "dividends"
                    else None
                ),
            },
        )
        manifest_path = layout.manifest_path(layer, dataset)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        write_manifest_parquet(manifest_path, manifest)

    orphaned = _unreferenced_canonical_artifacts(root)
    orphan_sidecar = layout.retractions_path(Layer.GOLD, "splits")
    write_parquet_rows(
        orphan_sidecar,
        [
            {
                "isin": "DE0000000001",
                "exchange": "XETRA",
                "code": "AAA",
                "event_key": "d" * 64,
            }
        ],
    )
    orphaned_sidecars = _unreferenced_snapshot_sidecars(root)
    assert orphaned == [orphan]
    assert orphaned_sidecars == [orphan_sidecar]
    backup = tmp_path / "backups" / "run-1"
    assert (
        _archive_legacy_artifacts(
            root,
            backup,
            additional_sources=(*orphaned, *orphaned_sidecars),
        )
        == 2
    )
    assert (backup / orphan.relative_to(root)).is_file()
    assert (backup / orphan_sidecar.relative_to(root)).is_file()
    assert not orphan.exists()
    assert not orphan_sidecar.exists()
    assert _validate_canonical_lake(root) == 3


def test_archive_rejects_sources_outside_lake(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    outside = tmp_path / "outside.parquet"
    outside.write_bytes(b"preserve")

    with pytest.raises(ValueError, match="outside the lake"):
        _archive_legacy_artifacts(
            root,
            tmp_path / "backup",
            additional_sources=[outside],
        )

    assert outside.read_bytes() == b"preserve"
    assert not (tmp_path / "backup").exists()


def test_validator_accepts_valid_root_level_parquet_restart_checkpoint(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    checkpoint = root / "weekly.checkpoint.parquet"
    write_parquet_rows(
        checkpoint,
        [{"version": 1, "completed": ["listings"], "details": {"listings": {"count": 1}}}],
    )

    assert _validate_canonical_lake(root) == 0


def test_validator_rejects_invalid_root_level_parquet_checkpoint(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    write_parquet_rows(
        root / "weekly.checkpoint.parquet",
        [{"version": 99, "completed": ["listings"], "details": {"listings": {"count": 1}}}],
    )

    with pytest.raises(ValueError, match="invalid loader checkpoint"):
        _validate_canonical_lake(root)


def test_aborted_parquet_checkpoints_are_preserved_outside_lake_and_active_is_strict(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    active = root / "weekly.checkpoint.parquet"
    write_parquet_rows(
        active,
        [{"version": 1, "completed": ["listings"], "details": {"listings": {"count": 1}}}],
    )
    malformed_archive = root / "weekly.checkpoint.aborted-20260824T1644Z.parquet"
    write_parquet_rows(malformed_archive, [{"version": 1, "completed": ["listings"]}])
    valid_archive = root / "weekly.checkpoint.aborted-role-probe-20261023.parquet"
    write_parquet_rows(
        valid_archive,
        [{"version": 1, "completed": ["listings"], "details": {"listings": {"probe": True}}}],
    )

    archived_sources = _obsolete_checkpoint_archives(root)
    backup = tmp_path / "private-backup" / "20261023T120000000000Z"
    assert _archive_legacy_artifacts(root, backup, additional_sources=archived_sources) == 2

    assert active.is_file()
    assert not malformed_archive.exists()
    assert not valid_archive.exists()
    assert read_parquet_rows(backup / malformed_archive.name) == [
        {"version": 1, "completed": ["listings"]}
    ]
    assert read_parquet_rows(backup / valid_archive.name)[0]["details"] == {
        "listings": {"probe": True}
    }
    assert _validate_canonical_lake(root) == 0

    write_parquet_rows(active, [{"version": 1, "completed": ["listings"]}])
    with pytest.raises(ValueError, match="checkpoint details must match completed stages"):
        _validate_canonical_lake(root)


def test_validator_still_rejects_unrecognized_root_level_parquet(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    write_parquet_rows(root / "unrelated.parquet", [{"value": 1}])

    with pytest.raises(ValueError, match="non-canonical Parquet row artifacts"):
        _validate_canonical_lake(root)


def _canonical_bronze_fixture(
    root: Path,
    dataset: str,
    payload_text: str = '{"close":10.0,"provider_tag":"fixture"}',
) -> Path:
    layout = MedallionLayout(root)
    path = layout.bronze_path(
        BronzePartition(dataset, "XETRA", "ETF", "AAA", "1d", date(2026, 10, 1))
    )
    write_parquet_rows(
        path,
        [
            {
                "isin": "DE0000000001",
                "exchange": "XETRA",
                "code": "AAA",
                "payload_json": payload_text,
            },
            {
                "isin": "DE0000000001",
                "exchange": "XETRA",
                "code": "AAA",
                "payload_json": payload_text,
            },
        ],
    )
    return path


def test_canonical_bronze_payload_conversion_covers_all_datasets_and_preserves_multiplicity(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    backup = tmp_path / "private-backup" / "20261023T120000000000Z"
    sources = {dataset: _canonical_bronze_fixture(root, dataset) for dataset in DATASETS}

    converted = _convert_canonical_bronze_payloads(root, backup, cores=3)

    assert converted == len(DATASETS)
    assert _validate_canonical_lake(root) == len(DATASETS)
    assert backup.stat().st_mode & 0o777 == 0o700
    for _dataset, source in sources.items():
        live = read_parquet_rows(source)
        original = read_parquet_rows(backup / source.relative_to(root))
        assert len(live) == len(original) == 2
        assert all(
            row["provider_payload"]["provider_tag"] == "fixture" for row in live
        )
        assert all("payload_json" not in row for row in live)
        assert all("payload_json" in row for row in original)
        assert "provider_payload" in pl.read_parquet_schema(source)
        assert "payload_json" not in pl.read_parquet_schema(source)
        assert not list(source.parent.glob(".data.parquet.*.tmp"))


def test_bronze_payload_conversion_preserves_decimal_precision_and_struct_key_union(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    backup = tmp_path / "private-backup" / "20261023T120000000000Z"
    source = _canonical_bronze_fixture(
        root,
        "splits",
        '{"precise":0.12345678901234567890123456789,"large":'
        '12345678901234567890123456789,"nested":{"a":1}}',
    )
    source_rows = read_parquet_rows(source)
    source_rows[1]["payload_json"] = '{"nested":{"b":2}}'
    write_parquet_rows(source, source_rows)

    assert _convert_canonical_bronze_payloads(root, backup, cores=1) == 1

    payloads = [row["provider_payload"] for row in read_parquet_rows(source)]
    assert payloads[0]["nested"] == {"a": 1, "b": None}
    assert payloads[1]["nested"] == {"a": None, "b": 2}
    decoded = _decode_bronze_payload_json(
        '{"precise":0.12345678901234567890123456789,"large":'
        '12345678901234567890123456789}',
        source,
        1,
    )
    assert decoded["precise"] == Decimal("0.12345678901234567890123456789")
    assert decoded["large"] == 12345678901234567890123456789
    assert _semantically_equal({"a": 1}, {"a": 1, "b": None})


def test_empty_canonical_bronze_payload_file_is_backed_up_then_removed(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    backup = tmp_path / "private-backup" / "20261023T120000000000Z"
    source = MedallionLayout(root).bronze_path(
        BronzePartition("splits", "XETRA", "ETF", "EMPTY", "event", date(2026, 10, 1))
    )
    empty = pl.DataFrame(
        schema={
            "isin": pl.String,
            "exchange": pl.String,
            "code": pl.String,
            "payload_json": pl.String,
        }
    )
    source.parent.mkdir(parents=True)
    empty.write_parquet(source)
    original_bytes = source.read_bytes()

    assert _convert_canonical_bronze_payloads(root, backup, cores=2) == 1

    assert not source.exists()
    recovered = backup / source.relative_to(root)
    assert recovered.read_bytes() == original_bytes
    assert pl.read_parquet(recovered).height == 0
    assert "payload_json" in pl.read_parquet_schema(recovered)
    assert backup.stat().st_mode & 0o777 == 0o700
    assert _validate_canonical_lake(root) == 0


def test_invalid_canonical_bronze_payload_fails_closed_without_changing_source(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    source = _canonical_bronze_fixture(root, "splits", "not-json")
    original_bytes = source.read_bytes()
    backup = tmp_path / "private-backup" / "20261023T120000000000Z"

    with pytest.raises(ValueError, match="invalid Bronze payload_json"):
        _convert_canonical_bronze_payloads(root, backup, cores=2)

    assert source.read_bytes() == original_bytes
    assert not backup.exists()
    assert not list(source.parent.glob(".data.parquet.*.tmp"))


def test_canonical_bronze_payload_conversion_backup_and_retry_are_restart_safe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "lake"
    source = _canonical_bronze_fixture(root, "splits")
    original_bytes = source.read_bytes()
    first_backup = tmp_path / "private-backup" / "20261023T120000000000Z"
    real_replace = os.replace

    def interrupt_live_replace(source_path: str | Path, target_path: str | Path) -> None:
        if Path(target_path) == source:
            raise OSError("simulated interruption before promotion")
        real_replace(source_path, target_path)

    monkeypatch.setattr(lake_migration.os, "replace", interrupt_live_replace)
    with pytest.raises(OSError, match="simulated interruption"):
        _convert_canonical_bronze_payloads(root, first_backup, cores=1)

    assert source.read_bytes() == original_bytes
    assert (first_backup / source.relative_to(root)).read_bytes() == original_bytes

    monkeypatch.setattr(lake_migration.os, "replace", real_replace)
    retry_backup = tmp_path / "private-backup" / "20261023T120001000000Z"
    assert _convert_canonical_bronze_payloads(root, retry_backup, cores=1) == 1
    assert len(read_parquet_rows(source)) == 2
    assert all("provider_payload" in row for row in read_parquet_rows(source))
    assert _convert_canonical_bronze_payloads(
        root,
        tmp_path / "private-backup" / "20261023T120002000000Z",
        cores=1,
    ) == 0


def test_canonical_bronze_validator_rejects_legacy_or_missing_payload_schema(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    source = _canonical_bronze_fixture(root, "splits")

    with pytest.raises(ValueError, match="legacy payload_json remains"):
        _validate_canonical_lake(root)

    native_without_payload = source.with_name("native-without-provider.parquet")
    write_parquet_rows(
        native_without_payload,
        [{"isin": "DE0000000001", "exchange": "XETRA", "code": "AAA"}],
    )
    native_without_payload.rename(source)
    with pytest.raises(ValueError, match="native provider_payload is missing"):
        _validate_canonical_lake(root)

    write_parquet_rows(
        source,
        [{"isin": "DE0000000001", "exchange": "XETRA", "code": "AAA", "provider_payload": "{}"}],
    )
    with pytest.raises(ValueError, match="provider_payload must be a native struct"):
        _validate_canonical_lake(root)


def _nested_quote_legacy_fixture(
    root: Path,
    legacy_quotes: list[dict[str, JSONValue]],
    canonical_quotes: list[dict[str, JSONValue]] | None,
) -> tuple[Path, Path | None]:
    source = (
        root
        / "bronze/dataset_type=eod_quotes/exchange=XETRA/instrument_type=ETF/"
        "symbol=ASRT/symbol=ASRR/timeframe=1d/year=2026/month=2026-10/data.parquet"
    )
    write_parquet_rows(
        source,
        [
            {
                "isin": "DE0000000001",
                "exchange": "XETRA",
                "code": "ASRR",
                "payload_json": canonical_json(quote),
            }
            for quote in legacy_quotes
        ],
    )
    target = _nested_quote_canonical_target(root, source)
    if canonical_quotes is not None:
        write_parquet_rows(
            target,
            [
                {
                    "isin": "DE0000000001",
                    "exchange": "XETRA",
                    "code": "ASRR",
                    "provider_payload": quote,
                }
                for quote in canonical_quotes
            ],
        )
        return source, target
    return source, None


def _quote_payload(day: str, close: float) -> dict[str, JSONValue]:
    return {
        "code": "ASRR",
        "exchange": "XETRA",
        "isin": "DE0000000001",
        "type": "ETF",
        "date": day,
        "open": 8.5,
        "high": 12.0,
        "low": 8.0,
        "close": close,
        "adjusted_close": close,
        "volume": 100,
    }


def test_quote_payload_normalization_is_limited_to_known_numeric_fields() -> None:
    normalized = _canonical_quote_payload(
        {
            "close": 10.0,
            "volume": Decimal("100.00"),
            "provider_metric": 1.0,
            "metadata": {"close": Decimal("10.00")},
        }
    )

    assert normalized["close"] == "10"
    assert normalized["volume"] == "100"
    assert normalized["provider_metric"] == 1.0
    assert normalized["metadata"] == {"close": Decimal("10.00")}


def test_nested_quote_archival_requires_proof_and_accepts_canonical_superset(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    legacy = [_quote_payload("2026-10-01", 10.0), _quote_payload("2026-10-02", 11.0)]
    newer = _quote_payload("2026-10-03", 12.0)
    canonical = []
    for quote in [*legacy, newer]:
        canonical.append(
            {
                key: (
                    cast(JSONValue, Decimal(str(value)).quantize(Decimal("0.01")))
                    if key in {"open", "high", "low", "close", "adjusted_close", "volume"}
                    and isinstance(value, (int, float))
                    else value
                )
                for key, value in quote.items()
            }
        )
    source, target = _nested_quote_legacy_fixture(root, legacy, canonical)
    assert target is not None

    verified = _verify_nested_quote_legacy_sources(root)

    assert verified == [source]
    assert target is not None
    assert "instrument_type=ETF" in target.as_posix()
    assert sum(part.startswith("symbol=") for part in target.relative_to(root).parts) == 1
    assert not _legacy_artifact(source, root)
    backup = tmp_path / "private-backup" / "20261023T120000000000Z"
    assert _archive_legacy_artifacts(root, backup) == 0
    assert source.is_file()
    assert _archive_legacy_artifacts(root, backup, additional_sources=verified) == 1
    assert not source.exists()
    assert (backup / source.relative_to(root)).is_file()
    assert target.is_file()


def test_nested_quote_archival_fails_closed_when_canonical_target_is_missing(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    source, _ = _nested_quote_legacy_fixture(
        root,
        [_quote_payload("2026-10-01", 10.0)],
        None,
    )

    with pytest.raises(ValueError, match="have no canonical target"):
        _verify_nested_quote_legacy_sources(root)

    assert source.is_file()
    assert _archive_legacy_artifacts(root, tmp_path / "backup") == 0
    assert source.is_file()
    assert not (tmp_path / "backup").exists()


def test_nested_quote_archival_fails_closed_when_any_payload_row_is_missing(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    legacy = [_quote_payload("2026-10-01", 10.0), _quote_payload("2026-10-02", 11.0)]
    source, _ = _nested_quote_legacy_fixture(root, legacy, legacy[:1])

    with pytest.raises(ValueError, match="1 missing"):
        _verify_nested_quote_legacy_sources(root)

    assert source.is_file()
    assert _archive_legacy_artifacts(root, tmp_path / "backup") == 0
    assert source.is_file()
    assert not (tmp_path / "backup").exists()


def test_streaming_gold_fingerprint_validation_matches_canonical_hashes(
    tmp_path: Path,
) -> None:
    layout = MedallionLayout(tmp_path)
    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    runtime._layout = layout
    listings: list[dict[str, JSONValue]] = [
        {
            "isin": "DE0000000001",
            "exchange": "XETRA",
            "code": "AAA",
            "name": "Fixture",
            "instrument_type": "etf",
        }
    ]
    rows_by_dataset = {
        "listings": listings,
        "eod_quotes": [
            {
                "isin": "DE0000000001",
                "exchange": "XETRA",
                "code": "AAA",
                "trade_date": "2026-08-21",
                "adjusted_close": "11.00",
            }
        ],
        "dividends": [
            {
                "isin": "DE0000000001",
                "exchange": "XETRA",
                "code": "AAA",
                "event_key": "b" * 64,
                "event_date": "2026-08-20",
                "amount": "1.25",
                "payment_date": "2026-08-28",
            },
            {
                "isin": "DE0000000003",
                "exchange": "XETRA",
                "code": "BBB",
                "event_key": "d" * 64,
                "event_date": "2026-08-20",
                "amount": "0.50",
                "payment_date": None,
            },
        ],
        "splits": [
            {
                "isin": "DE0000000001",
                "exchange": "XETRA",
                "code": "AAA",
                "event_key": "c" * 64,
                "event_date": "2026-08-19",
                "split_factor": "2",
            }
        ],
    }
    retracted = [("DE0000000002", "XETRA", "BBB", "a" * 64)]
    for dataset, rows in rows_by_dataset.items():
        runtime.persist_gold(
            dataset,
            rows,
            row_count=len(rows),
            semantic_fingerprint=semantic_fingerprint(
                dataset,
                rows,
                [list(key) for key in retracted] if dataset == "dividends" else (),
            ),
            retracted_keys=retracted if dataset == "dividends" else (),
        )

    assert validate_gold_fingerprints_streaming(tmp_path) == len(DATASETS)


def test_lake_migration_cli_passes_explicit_core_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    observed: dict[str, object] = {}

    def migrate(
        root: Path,
        backup_root: Path,
        *,
        cores: int | None = None,
    ) -> dict[str, JSONValue]:
        observed["root"] = root
        observed["backup_root"] = backup_root
        observed["cores"] = cores
        return {"status": "PASS"}

    def resolve_root(root: str | None = None) -> str:
        return str(root)

    monkeypatch.setattr(lake_migration, "resolve_medallion_root", resolve_root)
    monkeypatch.setattr(lake_migration, "migrate_lake", migrate)

    assert (
        lake_migration.main(
            [
                "--root",
                str(tmp_path / "lake"),
                "--backup-root",
                str(tmp_path / "backup"),
                "--cores",
                "3",
            ]
        )
        == 0
    )
    assert observed["root"] == tmp_path / "lake"
    assert observed["backup_root"] == tmp_path / "backup"
    assert observed["cores"] == 3
    assert '"status":"PASS"' in capsys.readouterr().out


def test_lake_migration_holds_both_writer_and_authoritative_rewrite_locks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "lake"
    for layer in Layer:
        (root / layer.value).mkdir(parents=True)
    lock_paths: list[Path] = []

    def recording_lock(path: Path) -> AbstractContextManager[None]:
        lock_paths.append(path)
        return nullcontext()

    monkeypatch.setattr(lake_migration, "LoaderLock", recording_lock)
    monkeypatch.setattr(lake_migration, "_convert_bronze_partitions", lambda *_a, **_k: 0)
    monkeypatch.setattr(lake_migration, "_convert_snapshot", lambda *_a, **_k: False)
    monkeypatch.setattr(lake_migration, "validate_gold_fingerprints_streaming", lambda _root: 4)
    monkeypatch.setattr(lake_migration, "_unreferenced_canonical_artifacts", lambda _root: [])
    monkeypatch.setattr(lake_migration, "_unreferenced_snapshot_sidecars", lambda _root: [])
    monkeypatch.setattr(
        lake_migration,
        "_archive_legacy_artifacts",
        lambda *_a, **_k: 0,
    )
    monkeypatch.setattr(lake_migration, "_validate_canonical_lake", lambda _root: 0)

    report = migrate_lake(root, tmp_path / "backup", cores=1)

    assert report["status"] == "PASS"
    assert lock_paths == [
        root.resolve() / "authoritative-rewrite.lock",
        root.resolve() / "weekly.lock",
    ]
