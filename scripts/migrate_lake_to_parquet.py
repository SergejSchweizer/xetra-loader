#!/usr/bin/env python3
"""Convert the existing Bronze/Silver/Gold lake from JSON to Polars Parquet."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

from xetra_loader.config import resolve_cores, resolve_medallion_root

os.environ["POLARS_MAX_THREADS"] = str(resolve_cores())

import polars as pl

from xetra_loader.medallion.core import (
    BronzePartition,
    JSONValue,
    Layer,
    MedallionLayout,
    canonical_json,
    read_dataset_rows,
    read_legacy_dataset_rows,
    read_legacy_snapshot_retractions,
    read_parquet_rows,
    write_parquet_rows,
    write_raw_listing_partitions,
)
from xetra_loader.ops.bootstrap import PostgresEodhdBootstrapRuntime, SnapshotWrite
from xetra_loader.ops.verify_postgres_sync import DATASETS, load_gold_snapshots
from xetra_loader.pipeline.restart import LoaderLock


@dataclass(frozen=True, slots=True)
class _BronzeMigrationContext:
    layout: MedallionLayout
    instrument_types: Mapping[tuple[str, str, str], str]


def _json_value(value: object) -> JSONValue:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (date, datetime, Decimal)):
        return str(value) if isinstance(value, Decimal) else value.isoformat()
    if isinstance(value, (dict, list)):
        return cast(JSONValue, value)
    raise ValueError(f"unsupported JSON-compatible value: {type(value).__name__}")


def _frame_rows(frame: pl.DataFrame) -> Iterator[dict[str, JSONValue]]:
    if frame.height == 0:
        return iter(())
    required = {"isin", "exchange", "code"}
    if not required.issubset(frame.columns):
        missing = sorted(required - set(frame.columns))
        raise ValueError(f"snapshot is missing listing identity columns: {missing}")
    ordered = frame.sort(["isin", "exchange", "code"])

    def rows() -> Iterator[dict[str, JSONValue]]:
        for row in ordered.iter_rows(named=True):
            yield {key: _json_value(value) for key, value in row.items()}

    return rows()


def _convert_bronze_partitions(root: Path) -> int:
    layout = MedallionLayout(root)
    listing_manifest = layout.manifest_path(Layer.SILVER, "listings")
    listing_rows = (
        read_dataset_rows(layout, Layer.SILVER, "listings")
        if listing_manifest.exists()
        else read_legacy_dataset_rows(layout, Layer.SILVER, "listings")
    )
    instrument_types = _listing_instrument_types(listing_rows)
    context = _BronzeMigrationContext(layout, instrument_types)
    groups = _group_bronze_sources(_legacy_bronze_sources(root))
    cores = resolve_cores()
    converted = 0
    with ThreadPoolExecutor(max_workers=cores, thread_name_prefix="xdl-bronze-migrate") as pool:
        results = pool.map(
            _convert_bronze_sources,
            ((context, group) for group in groups),
        )
        for index, written in enumerate(results, start=1):
            converted += written
            if index % 100 == 0 or index == len(groups):
                print(
                    canonical_json(
                        {
                            "stage": "bronze-migration",
                            "source_groups": index,
                            "source_groups_total": len(groups),
                            "parquet_files_written": converted,
                        }
                    ),
                    file=sys.stderr,
                    flush=True,
                )

    listings = root / Layer.BRONZE.value / "listings" / "data.json"
    if listings.exists():
        payload = json.loads(listings.read_text(encoding="utf-8"))
        if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
            raise ValueError(f"invalid raw listings artifact: {listings}")
        converted += write_raw_listing_partitions(
            layout,
            "listings",
            cast(list[dict[str, JSONValue]], payload),
            skip_existing=True,
            snapshot_date=datetime.fromtimestamp(listings.stat().st_mtime, UTC).date(),
        )
    return converted


def _legacy_bronze_sources(root: Path) -> list[Path]:
    return sorted(
        path
        for dataset in DATASETS
        for path in (
            *sorted((root / "bronze" / dataset / "partitions").glob("*.json")),
            *sorted((root / "bronze" / dataset / "partitions").glob("*.parquet")),
        )
    )


def _group_bronze_sources(sources: list[Path]) -> list[list[Path]]:
    groups: dict[tuple[str, str, str], list[Path]] = {}
    for source in sources:
        dataset = source.parent.parent.name
        identity = source.stem.split("!")
        key = (dataset, identity[0], identity[1]) if len(identity) == 3 else (dataset, "raw", "")
        groups.setdefault(key, []).append(source)
    return [groups[key] for key in sorted(groups)]


def _listing_instrument_types(
    listing_rows: list[dict[str, JSONValue]],
) -> dict[tuple[str, str, str], str]:
    instrument_types: dict[tuple[str, str, str], str] = {}
    for row in listing_rows:
        identity = tuple(row.get(field) for field in ("isin", "exchange", "code"))
        instrument_type = row.get("instrument_type")
        if all(isinstance(value, str) for value in identity) and isinstance(
            instrument_type, str
        ):
            instrument_types[cast(tuple[str, str, str], identity)] = instrument_type.lower()
    return instrument_types


def _convert_bronze_sources(
    job: tuple[_BronzeMigrationContext, list[Path]],
) -> int:
    context, sources = job
    grouped: dict[Path, dict[str, dict[str, JSONValue]]] = {}
    for source in sources:
        payload: object = (
            read_parquet_rows(source)
            if source.suffix == ".parquet"
            else json.loads(source.read_text(encoding="utf-8"))
        )
        if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
            raise ValueError(f"invalid Bronze partition: {source}")
        dataset = source.parent.parent.name
        fallback_date = datetime.fromtimestamp(source.stat().st_mtime, UTC).date()
        for outer in cast(list[dict[str, JSONValue]], payload):
            for item in _provider_payload_rows(outer):
                path, normalized = _bronze_entry(
                    context,
                    dataset,
                    outer,
                    item,
                    fallback_date,
                )
                unique_key = canonical_json(cast(JSONValue, normalized))
                grouped.setdefault(path, {})[unique_key] = normalized

    written = 0
    for path, incoming in grouped.items():
        prior = read_parquet_rows(path) if path.exists() else []
        unique = {canonical_json(cast(JSONValue, row)): row for row in prior}
        unique.update(incoming)
        ordered = [unique[key] for key in sorted(unique)]
        if ordered != prior:
            write_parquet_rows(path, ordered)
            written += 1
    return written


def _provider_payload_rows(outer: Mapping[str, JSONValue]) -> list[dict[str, JSONValue]]:
    raw = outer.get("payload", outer.get("payload_json"))
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            raw = {"raw_text": raw}
    candidates = raw if isinstance(raw, list) else [raw]
    return [
        candidate if isinstance(candidate, dict) else {"value": candidate}
        for candidate in candidates
    ]


def _bronze_entry(
    context: _BronzeMigrationContext,
    dataset: str,
    outer: Mapping[str, JSONValue],
    item: Mapping[str, JSONValue],
    fallback_date: date,
) -> tuple[Path, dict[str, JSONValue]]:
    exchange = _first_text(outer.get("exchange"), item.get("Exchange"), item.get("exchange"))
    symbol = _first_text(outer.get("code"), item.get("Code"), item.get("code"))
    isin = _first_text(outer.get("isin"), item.get("ISIN"), item.get("Isin"), item.get("isin"))
    if symbol is None:
        digest = hashlib.sha256(canonical_json(cast(JSONValue, dict(item))).encode()).hexdigest()
        symbol = f"unidentified-{digest[:16]}"
    exchange = exchange or "unknown"
    type_value = _first_text(item.get("Type"), item.get("type"), outer.get("instrument_type"))
    instrument_type = (
        type_value.lower()
        if type_value is not None
        else context.instrument_types.get((isin or "", exchange, symbol), "unknown")
    )
    raw_date = _first_text(item.get("date"), item.get("event_date"))
    partition_date = date.fromisoformat(raw_date[:10]) if raw_date else fallback_date
    timeframe = _dataset_timeframe(dataset)
    path = context.layout.bronze_path(
        BronzePartition(
            dataset_type=dataset,
            exchange=exchange,
            instrument_type=instrument_type,
            symbol=symbol,
            timeframe=timeframe,
            partition_date=partition_date,
        )
    )
    return path, {
        "isin": isin,
        "exchange": exchange,
        "code": symbol,
        "payload_json": canonical_json(cast(JSONValue, dict(item))),
    }


def _first_text(*values: JSONValue) -> str | None:
    return next(
        (value.strip() for value in values if isinstance(value, str) and value.strip()),
        None,
    )


def _dataset_timeframe(dataset: str) -> str:
    if dataset == "listings":
        return "static"
    if dataset == "eod_quotes":
        return "1d"
    return "event"


def _convert_snapshot(
    runtime: PostgresEodhdBootstrapRuntime,
    layout: MedallionLayout,
    layer: Layer,
    dataset: str,
) -> bool:
    directory = layout.dataset_path(layer, dataset)
    source = directory / "data.json"
    manifest_path = layout.manifest_path(layer, dataset)
    if _canonical_snapshot_ready(layout, manifest_path):
        return False
    legacy_manifest_path = layout.legacy_manifest_path(layer, dataset)
    manifest = (
        json.loads(legacy_manifest_path.read_text(encoding="utf-8"))
        if legacy_manifest_path.exists()
        else {}
    )
    if source.exists():
        # Inspect the full JSON input so late non-null strings are not inferred as Null.
        frame = pl.read_json(source, infer_schema_length=None)
    else:
        rows = read_legacy_dataset_rows(layout, layer, dataset)
        if not rows and not legacy_manifest_path.exists():
            raise FileNotFoundError(f"missing {layer.value} snapshot: {source}")
        frame = pl.DataFrame(rows, infer_schema_length=None)
    if layer is Layer.GOLD:
        semantic_metadata, retractions = _gold_migration_metadata(
            layout,
            dataset,
            manifest,
            frame.height,
            manifest_path,
        )
    else:
        semantic_metadata = {"row_count": frame.height}
        retractions = None

    runtime._persist_partitioned_snapshot(
        layer,
        dataset,
        _frame_rows(frame),
        snapshot=SnapshotWrite(
            semantic_metadata=semantic_metadata,
            retractions=retractions,
        ),
    )
    return True


def _canonical_snapshot_ready(layout: MedallionLayout, manifest_path: Path) -> bool:
    if not manifest_path.exists():
        return False
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    run_metadata = manifest.get("run_metadata", {})
    paths = run_metadata.get("artifact_paths") if isinstance(run_metadata, dict) else None
    return isinstance(paths, list) and all(
        isinstance(path, str) and (layout.root / path).is_file() for path in paths
    )


def _gold_migration_metadata(
    layout: MedallionLayout,
    dataset: str,
    manifest: Mapping[str, object],
    row_count: int,
    manifest_path: Path,
) -> tuple[dict[str, JSONValue], list[dict[str, JSONValue]] | None]:
    semantic_metadata = manifest.get("semantic_metadata")
    if not isinstance(semantic_metadata, dict):
        raise ValueError(f"Gold manifest has no semantic metadata: {manifest_path}")
    if semantic_metadata.get("row_count") != row_count:
        raise ValueError(f"Gold row count does not match manifest for {dataset}")
    if dataset not in {"dividends", "splits"}:
        return cast(dict[str, JSONValue], semantic_metadata), None
    retractions_path = layout.legacy_retractions_path(Layer.GOLD, dataset)
    if retractions_path.exists():
        payload = json.loads(retractions_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError(f"invalid Gold retractions: {retractions_path}")
        retractions: list[dict[str, JSONValue]] = []
        for key in payload:
            if not isinstance(key, list) or len(key) != 4 or not all(
                isinstance(part, str) for part in key
            ):
                raise ValueError(f"invalid Gold retraction key: {retractions_path}")
            retractions.append(
                {"isin": key[0], "exchange": key[1], "code": key[2], "event_key": key[3]}
            )
    else:
        retractions = read_legacy_snapshot_retractions(layout, Layer.GOLD, dataset)
    return cast(dict[str, JSONValue], semantic_metadata), retractions


def _archive_json_artifacts(root: Path, backup_root: Path) -> int:
    sources = sorted(
        path
        for path in root.rglob("*.json")
        if path.name in {"data.json", "retractions.json"}
        or (path.parent.name == "partitions")
    )
    if not sources:
        return 0
    backup_root.mkdir(parents=True, mode=0o700, exist_ok=False)
    os.chmod(backup_root, 0o700)
    for source in sources:
        relative = source.relative_to(root)
        destination = backup_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(destination))
    return len(sources)


def migrate_lake(root: Path, backup_root: Path) -> dict[str, JSONValue]:
    """Publish and validate Parquet snapshots before moving JSON originals to backup."""

    resolved_root = root.resolve()
    resolved_backup_root = backup_root.resolve()
    if not resolved_root.is_dir() or not all(
        (resolved_root / layer.value).is_dir() for layer in Layer
    ):
        raise ValueError("migration root must contain bronze, silver, and gold directories")
    if resolved_backup_root == resolved_root or resolved_root in resolved_backup_root.parents:
        raise ValueError("JSON backup root must be outside the lake root")
    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    runtime._layout = MedallionLayout(resolved_root)
    with LoaderLock(resolved_root / "weekly.lock"):
        bronze_partitions = _convert_bronze_partitions(resolved_root)
        converted_snapshots = 0
        for dataset in DATASETS:
            for layer in (Layer.SILVER, Layer.GOLD):
                converted_snapshots += int(
                    _convert_snapshot(runtime, runtime._layout, layer, dataset)
                )
                print(
                    canonical_json(
                        {
                            "stage": "snapshot-migration",
                            "dataset": dataset,
                            "layer": layer.value,
                            "converted": converted_snapshots,
                        }
                    ),
                    file=sys.stderr,
                    flush=True,
                )

        snapshots = load_gold_snapshots(resolved_root)
        invalid = [
            dataset for dataset, snapshot in snapshots.items() if not snapshot.fingerprint_valid
        ]
        if invalid:
            raise ValueError(f"Parquet Gold fingerprint validation failed: {', '.join(invalid)}")

        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        actual_backup = resolved_backup_root / stamp
        archived = _archive_json_artifacts(resolved_root, actual_backup)
        return {
            "status": "PASS",
            "root": str(resolved_root),
            "bronze_parquet_artifacts": bronze_partitions,
            "silver_gold_snapshots": converted_snapshots,
            "datasets_fingerprint_valid": len(snapshots),
            "json_artifacts_archived": archived,
            "json_backup": str(actual_backup) if archived else None,
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="migrate-lake-to-parquet")
    parser.add_argument("--root", help="Lake root; defaults to medallion.root in config.yaml")
    parser.add_argument(
        "--backup-root",
        default="/home/dev_xetra/.xdl-lake-json-backups",
        help="Private parent directory for recoverable legacy JSON artifacts",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = migrate_lake(Path(resolve_medallion_root(args.root)), Path(args.backup_root))
    print(canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
