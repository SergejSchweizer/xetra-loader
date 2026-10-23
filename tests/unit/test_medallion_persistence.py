"""Filesystem contracts for atomic, partitioned medallion persistence."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

import xetra_loader.medallion.core as medallion_core
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.medallion.core import (
    BronzePartition,
    Layer,
    Manifest,
    MedallionLayout,
    atomic_write_parquet,
    atomic_write_text,
    bronze_provider_payload,
    migrate_json_manifests,
    read_dataset_rows,
    read_legacy_bronze_provider_payload,
    read_manifest_payload,
    read_parquet_rows,
    write_manifest_parquet,
    write_raw_listing_partitions,
)
from xetra_loader.ops.bootstrap import PostgresEodhdBootstrapRuntime


def test_atomic_write_failure_preserves_last_committed_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "gold" / "quotes" / "data.json"
    atomic_write_text(target, "old\n")

    def fail_replace(source: Path, destination: Path) -> None:
        del source, destination
        raise OSError("injected replace failure")

    monkeypatch.setattr(medallion_core.os, "replace", fail_replace)
    with pytest.raises(OSError, match="injected replace failure"):
        atomic_write_text(target, "new\n")

    assert target.read_text(encoding="utf-8") == "old\n"
    assert not list(target.parent.glob(".data.json.*.tmp"))


def test_atomic_parquet_write_failure_preserves_last_committed_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "gold" / "quotes" / "data.parquet"
    atomic_write_parquet(target, pl.DataFrame({"value": ["old"]}))

    def fail_replace(source: Path, destination: Path) -> None:
        del source, destination
        raise OSError("injected replace failure")

    monkeypatch.setattr(medallion_core.os, "replace", fail_replace)
    with pytest.raises(OSError, match="injected replace failure"):
        atomic_write_parquet(target, pl.DataFrame({"value": ["new"]}))

    assert pl.read_parquet(target).to_dicts() == [{"value": "old"}]
    assert not list(target.parent.glob(".data.parquet.*.tmp"))


def test_manifest_publishes_only_verified_parquet_metadata(tmp_path: Path) -> None:
    layout = MedallionLayout(tmp_path)
    legacy_path = layout.legacy_metadata_manifest_path(Layer.GOLD, "quotes")
    legacy_path.parent.mkdir(parents=True)
    legacy_path.write_text('{"legacy":true}', encoding="utf-8")
    manifest = Manifest(
        dataset="quotes",
        layer=Layer.GOLD,
        semantic_metadata={"row_count": 1},
        run_metadata={"artifact_paths": ["gold/quotes.parquet"]},
    )

    write_manifest_parquet(layout.manifest_path(Layer.GOLD, "quotes"), manifest)

    assert layout.manifest_path(Layer.GOLD, "quotes").is_file()
    assert not legacy_path.exists()
    assert read_manifest_payload(layout.manifest_path(Layer.GOLD, "quotes")) == {
        "dataset": "quotes",
        "layer": "gold",
        "semantic_fingerprint": manifest.semantic_fingerprint(),
        "semantic_metadata": {"row_count": 1},
        "run_metadata": {"artifact_paths": ["gold/quotes.parquet"]},
    }


def test_legacy_manifest_migration_verifies_before_removing_json(tmp_path: Path) -> None:
    layout = MedallionLayout(tmp_path)
    legacy_path = layout.legacy_metadata_manifest_path(Layer.SILVER, "quotes")
    legacy_path.parent.mkdir(parents=True)
    legacy_path.write_text(
        '{"dataset":"quotes","layer":"silver","semantic_fingerprint":"abc",'
        '"semantic_metadata":{"row_count":12},'
        '"run_metadata":{"artifact_paths":["silver/quotes.parquet"]}}',
        encoding="utf-8",
    )

    assert migrate_json_manifests(tmp_path) == 1

    parquet_path = layout.manifest_path(Layer.SILVER, "quotes")
    assert parquet_path.is_file()
    assert not legacy_path.exists()
    assert read_manifest_payload(parquet_path) == {
        "dataset": "quotes",
        "layer": "silver",
        "semantic_fingerprint": "abc",
        "semantic_metadata": {"row_count": 12},
        "run_metadata": {"artifact_paths": ["silver/quotes.parquet"]},
    }


def test_atomic_parquet_can_defer_directory_sync_for_replayable_migration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "bronze" / "data.parquet"
    real_fsync = medallion_core.os.fsync
    synced_descriptors: list[int] = []

    def track_fsync(descriptor: int) -> None:
        synced_descriptors.append(descriptor)
        real_fsync(descriptor)

    monkeypatch.setattr(medallion_core.os, "fsync", track_fsync)
    atomic_write_parquet(
        target,
        pl.DataFrame({"value": ["preserved"]}),
        sync_directory=False,
    )

    assert len(synced_descriptors) == 1
    assert pl.read_parquet(target).to_dicts() == [{"value": "preserved"}]


def test_atomic_parquet_can_defer_file_and_directory_sync_for_replayable_migration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "bronze" / "replayable.parquet"
    synced_descriptors: list[int] = []
    monkeypatch.setattr(medallion_core.os, "fsync", synced_descriptors.append)

    atomic_write_parquet(
        target,
        pl.DataFrame({"value": ["replayable"]}),
        sync_file=False,
        sync_directory=False,
    )

    assert synced_descriptors == []
    assert pl.read_parquet(target).to_dicts() == [{"value": "replayable"}]


def test_silver_snapshot_uses_canonical_annual_partitions(tmp_path: Path) -> None:
    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    runtime._layout = MedallionLayout(tmp_path)
    rows = [
        {
            "isin": "DE0000000002",
            "exchange": "XETRA",
            "code": "BBB",
            "trade_date": "2026-08-22",
        },
        {
            "isin": "DE0000000001",
            "exchange": "XETRA",
            "code": "AAA",
            "trade_date": "2026-08-21",
        },
        {
            "isin": "DE0000000001",
            "exchange": "XETRA",
            "code": "AAA",
            "trade_date": "2026-09-01",
        },
    ]

    runtime.persist_silver("eod_quotes", rows)

    manifest = read_manifest_payload(runtime._layout.manifest_path(Layer.SILVER, "eod_quotes"))
    artifact_paths = manifest["run_metadata"]["artifact_paths"]
    assert "storage_version" not in manifest["run_metadata"]
    assert len(artifact_paths) == 2
    assert all(
        path.startswith("silver/dataset_type=eod_quotes/exchange=XETRA/symbol=")
        and "/timeframe=1d/year=2026/" in path
        and path.endswith(("AAA-2026.parquet", "BBB-2026.parquet"))
        for path in artifact_paths
    )
    aaa_path = next(path for path in artifact_paths if path.endswith("AAA-2026.parquet"))
    assert pl.read_parquet(tmp_path / aaa_path).height == 2
    assert all((tmp_path / path).is_file() for path in artifact_paths)
    assert not list(tmp_path.rglob("versions"))
    assert read_dataset_rows(runtime._layout, Layer.SILVER, "eod_quotes") == [
        {"isin": "DE0000000001", "exchange": "XETRA", "code": "AAA", "trade_date": "2026-08-21"},
        {"isin": "DE0000000001", "exchange": "XETRA", "code": "AAA", "trade_date": "2026-09-01"},
        {"isin": "DE0000000002", "exchange": "XETRA", "code": "BBB", "trade_date": "2026-08-22"},
    ]

    runtime.persist_silver(
        "eod_quotes",
        [
            {
                "isin": "DE0000000001",
                "exchange": "XETRA",
                "code": "AAA",
                "trade_date": "2027-01-04",
            }
        ],
    )
    replacement_manifest = read_manifest_payload(
        runtime._layout.manifest_path(Layer.SILVER, "eod_quotes")
    )
    replacement_paths = replacement_manifest["run_metadata"]["artifact_paths"]
    assert len(replacement_paths) == 1
    assert replacement_paths[0].endswith("AAA-2027.parquet")
    assert not (tmp_path / aaa_path).exists()
    assert not (tmp_path / next(path for path in artifact_paths if "symbol=BBB" in path)).exists()
    silver_artifacts = tuple(
        path.relative_to(tmp_path).as_posix()
        for path in (tmp_path / "silver/dataset_type=eod_quotes").rglob("*.parquet")
    )
    assert silver_artifacts == tuple(replacement_paths)
    assert not list(tmp_path.rglob("*.json"))


def test_gold_snapshot_uses_content_addressed_symbol_layout(tmp_path: Path) -> None:
    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    runtime._layout = MedallionLayout(tmp_path)
    runtime._cores = 2
    rows = [
        {
            "isin": "DE0000000001",
            "exchange": "XETRA",
            "code": "AAA",
            "trade_date": "2026-08-21",
            "close": "10.00",
        },
        {
            "isin": "DE0000000001",
            "exchange": "XETRA",
            "code": "AAA",
            "trade_date": "2026-08-22",
            "close": "11.00",
        },
    ]

    runtime.persist_gold(
        "eod_quotes",
        rows,
        row_count=len(rows),
        semantic_fingerprint="a" * 64,
    )

    manifest = read_manifest_payload(runtime._layout.manifest_path(Layer.GOLD, "eod_quotes"))
    artifact_paths = manifest["run_metadata"]["artifact_paths"]
    assert len(artifact_paths) == 1
    assert "storage_version" not in manifest["run_metadata"]
    assert manifest["semantic_metadata"]["dataset_id"] == "xetra-eod_quotes-gold-v1"
    artifact = Path(artifact_paths[0])
    assert artifact.parts[:5] == (
        "gold",
        "dataset_id=xetra-eod_quotes-gold-v1",
        "dataset_type=gold_symbol_dataset",
        "exchange=XETRA",
        "symbol=AAA",
    )
    assert artifact.name.startswith("AAA_GOLD_")
    assert len(artifact.stem.removeprefix("AAA_GOLD_").split("_")) == 2
    assert pl.read_parquet(tmp_path / artifact).height == 2
    assert not list(tmp_path.rglob("versions"))
    assert not list(tmp_path.rglob("*.json"))

    revised_rows = [dict(row) for row in rows]
    revised_rows[-1]["close"] = "12.00"
    runtime.persist_gold(
        "eod_quotes",
        revised_rows,
        row_count=len(revised_rows),
        semantic_fingerprint="c" * 64,
    )
    revised_manifest = read_manifest_payload(
        runtime._layout.manifest_path(Layer.GOLD, "eod_quotes")
    )
    revised_paths = revised_manifest["run_metadata"]["artifact_paths"]
    assert len(revised_paths) == 1
    assert revised_paths[0] != artifact_paths[0]
    assert not (tmp_path / artifact).exists()
    assert len(
        tuple((tmp_path / "gold/dataset_id=xetra-eod_quotes-gold-v1").rglob("*.parquet"))
    ) == 1

    runtime.persist_gold(
        "eod_quotes",
        [],
        row_count=0,
        semantic_fingerprint="d" * 64,
    )
    empty_manifest = read_manifest_payload(
        runtime._layout.manifest_path(Layer.GOLD, "eod_quotes")
    )
    assert empty_manifest["run_metadata"]["artifact_paths"] == []
    assert not tuple((tmp_path / "gold/dataset_id=xetra-eod_quotes-gold-v1").rglob("*.parquet"))


def test_raw_listings_are_polars_parquet_in_monthly_bronze_layout(tmp_path: Path) -> None:
    layout = MedallionLayout(tmp_path)
    as_of = date(2026, 8, 23)
    assert write_raw_listing_partitions(
        layout,
        "listings",
        [
            {
                "Code": "AAA",
                "Exchange": "XETRA",
                "ISIN": "DE0000000001",
                "Type": "ETF",
            }
        ],
        snapshot_date=as_of,
    ) == 1
    path = layout.bronze_path(
        BronzePartition("listings", "XETRA", "etf", "AAA", "static", as_of)
    )
    assert pl.read_parquet(path).to_dicts() == [
        {
            "isin": "DE0000000001",
            "exchange": "XETRA",
            "code": "AAA",
            "provider_payload": {
                "Code": "AAA",
                "Exchange": "XETRA",
                "ISIN": "DE0000000001",
                "Type": "ETF",
            },
        }
    ]
    assert not list(tmp_path.rglob("*.json"))


def test_quote_bronze_delta_merges_partition_and_stores_native_payload(tmp_path: Path) -> None:
    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    runtime._layout = MedallionLayout(tmp_path)
    listing = ListingRecord("DE0000000001", "XETRA", "AAA")
    runtime._record_partition_bronze(
        "eod_quotes",
        listing,
        ({"date": "2026-08-21", "close": 10.0}, {"date": "2026-08-22", "close": 11.0}),
    )
    runtime._record_partition_bronze(
        "eod_quotes",
        listing,
        ({"date": "2026-08-22", "close": 11.5}, {"date": "2026-08-23", "close": 12.0}),
    )

    path = runtime._layout.bronze_path(
        BronzePartition("eod_quotes", "XETRA", "unknown", "AAA", "1d", date(2026, 8, 22))
    )
    rows = read_parquet_rows(path)
    payloads = [bronze_provider_payload(row, path) for row in rows]

    assert payloads == [
        {"date": "2026-08-21", "close": 10.0},
        {"date": "2026-08-22", "close": 11.5},
        {"date": "2026-08-23", "close": 12.0},
    ]
    frame = pl.read_parquet(path)
    assert "payload_json" not in frame.columns
    assert "provider_payload" in frame.columns
    assert not list(tmp_path.rglob("*.json"))


def test_runtime_bronze_reader_rejects_legacy_json_text_payload(tmp_path: Path) -> None:
    row = {"payload_json": '{"date":"2026-08-21"}'}
    path = tmp_path / "legacy.parquet"

    with pytest.raises(ValueError, match="Bronze provider payload is invalid"):
        bronze_provider_payload(row, path)

    assert read_legacy_bronze_provider_payload(row, path) == {"date": "2026-08-21"}
