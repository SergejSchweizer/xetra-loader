"""Filesystem contracts for atomic, partitioned medallion persistence."""

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

import xetra_loader.medallion.core as medallion_core
from xetra_loader.medallion.core import (
    BronzePartition,
    Layer,
    MedallionLayout,
    atomic_write_parquet,
    atomic_write_text,
    read_dataset_rows,
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


def test_silver_snapshot_uses_canonical_monthly_partitions(tmp_path: Path) -> None:
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
    ]

    runtime.persist_silver("eod_quotes", rows)

    manifest = json.loads(
        runtime._layout.manifest_path(Layer.SILVER, "eod_quotes").read_text(encoding="utf-8")
    )
    artifact_paths = manifest["run_metadata"]["artifact_paths"]
    assert len(artifact_paths) == 2
    assert all(
        path.startswith("silver/dataset_type=eod_quotes/exchange=XETRA/symbol=")
        and "/timeframe=1d/year=2026/month=2026-08/" in path
        for path in artifact_paths
    )
    assert all((tmp_path / path).is_file() for path in artifact_paths)
    assert read_dataset_rows(runtime._layout, Layer.SILVER, "eod_quotes") == [
        {"isin": "DE0000000001", "exchange": "XETRA", "code": "AAA", "trade_date": "2026-08-21"},
        {"isin": "DE0000000002", "exchange": "XETRA", "code": "BBB", "trade_date": "2026-08-22"},
    ]


def test_gold_snapshot_uses_content_addressed_symbol_layout(tmp_path: Path) -> None:
    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    runtime._layout = MedallionLayout(tmp_path)
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

    manifest = json.loads(
        runtime._layout.manifest_path(Layer.GOLD, "eod_quotes").read_text(encoding="utf-8")
    )
    artifact_paths = manifest["run_metadata"]["artifact_paths"]
    assert len(artifact_paths) == 1
    artifact = Path(artifact_paths[0])
    assert artifact.parts[:6] == (
        "gold",
        "dataset_id=xetra-eod_quotes-gold-v1",
        "dataset_type=gold_symbol_dataset",
        "feature_set_version=v1.0.0",
        "exchange=XETRA",
        "symbol=AAA",
    )
    assert artifact.name.startswith("AAA_GOLD_")
    assert len(artifact.stem.removeprefix("AAA_GOLD_").split("_")) == 2
    assert pl.read_parquet(tmp_path / artifact).height == 2


def test_raw_listings_are_polars_parquet_in_daily_bronze_layout(tmp_path: Path) -> None:
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
            "payload_json": (
                '{"Code":"AAA","Exchange":"XETRA","ISIN":"DE0000000001",'
                '"Type":"ETF"}'
            ),
        }
    ]
