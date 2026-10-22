"""Recoverable full-lake JSON-to-Parquet migration tests."""

import hashlib
from collections.abc import Iterable, Iterator, Mapping
from contextlib import AbstractContextManager, nullcontext
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
import scripts.migrate_lake_to_parquet as lake_migration
from scripts.migrate_lake_to_parquet import (
    _archive_legacy_artifacts,
    _convert_snapshot,
    _unreferenced_canonical_artifacts,
    _unreferenced_snapshot_sidecars,
    _validate_canonical_lake,
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
        layout.dataset_path(Layer.BRONZE, "eod_quotes")
        / "partitions"
        / "AAA!XETRA!ISIN.parquet"
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
        root
        / "bronze/dataset_type=eod_quotes/exchange=XETRA/instrument_type=unknown/"
        "symbol=AAA/timeframe=1d/year=2026/month=2026-08/date=2026-08-21/data.parquet"
    )
    write_parquet_rows(obsolete_daily, [{"code": "AAA", "payload_json": "{}"}])

    report = migrate_lake(root, backup_parent)

    assert report["status"] == "PASS"
    assert report["datasets_fingerprint_valid"] == len(DATASETS)
    backup = Path(str(report["recovery_backup"]))
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
            "payload_json": '{"Code":"AAA","Exchange":"XETRA","ISIN":"ISIN"}',
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
            "payload_json": '{"close":12}',
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
    silver = layout.silver_path(
        SilverPartition("eod_quotes", "XETRA", "AAA", "1d", 2026)
    )
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
                "artifact_paths": [
                    path.relative_to(root).as_posix() for path in artifact_paths
                ],
                "retractions_path": (
                    dividend_sidecar.relative_to(root).as_posix()
                    if dataset == "dividends"
                    else None
                ),
            },
        )
        manifest_path = layout.manifest_path(layer, dataset)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(manifest.to_json(), encoding="utf-8")

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
            }
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

    assert lake_migration.main(
        [
            "--root",
            str(tmp_path / "lake"),
            "--backup-root",
            str(tmp_path / "backup"),
            "--cores",
            "3",
        ]
    ) == 0
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
