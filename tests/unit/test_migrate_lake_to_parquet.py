"""Recoverable full-lake JSON-to-Parquet migration tests."""

import hashlib
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
from scripts.migrate_lake_to_parquet import _convert_snapshot, migrate_lake

from xetra_loader.contracts.corporate_actions import DividendEvent, SplitEvent
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.gold.dividends import build_dividend_gold
from xetra_loader.gold.splits import build_split_gold
from xetra_loader.medallion.core import (
    BronzePartition,
    JSONValue,
    Layer,
    Manifest,
    MedallionLayout,
    canonical_json,
    read_dataset_rows,
    read_parquet_rows,
    write_parquet_rows,
    write_raw_listing_partitions,
)
from xetra_loader.ops.bootstrap import PostgresEodhdBootstrapRuntime
from xetra_loader.ops.verify_postgres_sync import DATASETS, semantic_fingerprint


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

    bronze_partition = layout.partition_path(Layer.BRONZE, "eod_quotes", "AAA!XETRA!ISIN")
    bronze_partition.parent.mkdir(parents=True, exist_ok=True)
    bronze_partition.with_suffix(".json").write_text(
        '[{"isin":"ISIN","exchange":"XETRA","code":"AAA","payload":{"close":12}}]\n',
        encoding="utf-8",
    )
    return layout


def test_migration_writes_polars_parquet_and_archives_json(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    backup_parent = tmp_path / "backups"
    layout = _legacy_lake(root)

    report = migrate_lake(root, backup_parent)

    assert report["status"] == "PASS"
    assert report["datasets_fingerprint_valid"] == len(DATASETS)
    backup = Path(str(report["json_backup"]))
    assert (backup / "silver" / "eod_quotes" / "data.json").exists()
    assert (backup / "gold" / "dividends" / "retractions.json").exists()
    assert not (root / "silver" / "eod_quotes" / "data.json").exists()
    assert not (root / "gold" / "dividends" / "data.json").exists()
    assert not list(root.rglob("partitions/*.json"))

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


def test_migration_repartitions_existing_versioned_parquet(tmp_path: Path) -> None:
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
        }
    ]
    legacy_partition = (
        layout.snapshot_version_path(Layer.SILVER, dataset, version)
        / "partitions"
        / "AAA!XETRA!DE0000000001.parquet"
    )
    write_parquet_rows(legacy_partition, rows)
    legacy_manifest = Manifest(
        dataset=dataset,
        layer=Layer.SILVER,
        semantic_metadata={"row_count": 1},
        run_metadata={"storage_version": version},
    )
    legacy_manifest_path = layout.legacy_manifest_path(Layer.SILVER, dataset)
    legacy_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_manifest_path.write_text(legacy_manifest.to_json(), encoding="utf-8")
    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    runtime._layout = layout

    assert _convert_snapshot(runtime, layout, Layer.SILVER, dataset)

    assert read_dataset_rows(layout, Layer.SILVER, dataset) == rows
    assert legacy_partition.exists()
