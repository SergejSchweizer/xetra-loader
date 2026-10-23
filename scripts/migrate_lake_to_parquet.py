#!/usr/bin/env python3
"""Convert the existing Bronze/Silver/Gold lake from JSON to Polars Parquet."""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from itertools import groupby
from pathlib import Path, PurePosixPath
from threading import Lock
from typing import cast

from xetra_loader.config import resolve_cores, resolve_medallion_root
from xetra_loader.contracts.numeric import canonical_decimal

# Polars initializes its thread pool at import, so resolve the CLI override first.
_core_parser = argparse.ArgumentParser(add_help=False)
_core_parser.add_argument("--cores", type=int)
_core_arguments, _ = _core_parser.parse_known_args()
_POLARS_CORES = resolve_cores(_core_arguments.cores)
os.environ["POLARS_MAX_THREADS"] = str(_POLARS_CORES)

import polars as pl  # noqa: E402 - thread budget is set before Polars initializes

from xetra_loader.medallion.core import (  # noqa: E402 - see Polars initialization above
    BronzePartition,
    JSONValue,
    Layer,
    MedallionLayout,
    canonical_json,
    migrate_json_manifests,
    read_dataset_rows,
    read_legacy_bronze_provider_payload,
    read_legacy_dataset_rows,
    read_legacy_snapshot_retractions,
    read_manifest_payload,
    read_parquet_rows,
    write_native_parquet_rows,
    write_parquet_rows,
    write_raw_listing_partitions,
)
from xetra_loader.ops.bootstrap import (  # noqa: E402
    PostgresEodhdBootstrapRuntime,
    SnapshotWrite,
    _silver_partition_key,
)
from xetra_loader.ops.verify_postgres_sync import (  # noqa: E402
    DATASETS,
    validate_gold_fingerprints_streaming,
)
from xetra_loader.pipeline.restart import (  # noqa: E402
    LoaderLock,
    _checkpoint_from_payload,
    _read_checkpoint_parquet,
)


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


def _frame_rows(
    frame: pl.DataFrame,
    *,
    dataset: str,
    presorted: bool = False,
) -> Iterator[dict[str, JSONValue]]:
    if frame.height == 0:
        return iter(())
    required = {"isin", "exchange", "code"}
    if not required.issubset(frame.columns):
        missing = sorted(required - set(frame.columns))
        raise ValueError(f"snapshot is missing listing identity columns: {missing}")
    sort_columns = ["exchange", "code"]
    date_column = "trade_date" if dataset == "eod_quotes" else "event_date"
    if dataset != "listings" and date_column in frame.columns:
        sort_columns.append(date_column)
    sort_columns.append("isin")
    ordered = frame if presorted else frame.sort(sort_columns)

    def rows() -> Iterator[dict[str, JSONValue]]:
        for row in ordered.iter_rows(named=True):
            yield {key: _json_value(value) for key, value in row.items()}

    return rows()


def _convert_bronze_partitions(root: Path, *, cores: int) -> int:
    layout = MedallionLayout(root)
    context = _bronze_migration_context(layout)
    groups = _group_bronze_sources(_legacy_bronze_sources(root))
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


def _bronze_migration_context(layout: MedallionLayout) -> _BronzeMigrationContext:
    listing_manifest = layout.manifest_path(Layer.SILVER, "listings")
    listing_rows = (
        read_dataset_rows(layout, Layer.SILVER, "listings")
        if listing_manifest.exists()
        else read_legacy_dataset_rows(layout, Layer.SILVER, "listings")
    )
    return _BronzeMigrationContext(layout, _listing_instrument_types(listing_rows))


def _has_legacy_bronze_payload(path: Path) -> bool:
    return "payload_json" in pl.read_parquet_schema(path)


def _canonical_bronze_payload_files(root: Path, *, cores: int) -> list[Path]:
    bronze_root = root / Layer.BRONZE.value
    if not bronze_root.is_dir():
        return []
    candidates = sorted(
        path
        for path in bronze_root.rglob("data.parquet")
        if _canonical_bronze_path(PurePosixPath(path.relative_to(root).as_posix()))
    )
    with ThreadPoolExecutor(max_workers=cores, thread_name_prefix="xdl-bronze-schema") as pool:
        legacy_flags = pool.map(
            _has_legacy_bronze_payload,
            candidates,
            buffersize=max(cores * 2, 1),
        )
        return [path for path, is_legacy in zip(candidates, legacy_flags, strict=True) if is_legacy]


def _decode_bronze_payload_json(value: object, path: Path, row_number: int) -> dict[str, object]:
    if not isinstance(value, str):
        raise ValueError(f"invalid Bronze payload_json value at row {row_number}: {path}")
    try:
        payload = json.loads(value, parse_float=Decimal)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid Bronze payload_json at row {row_number}: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Bronze payload_json must be an object at row {row_number}: {path}")
    return payload


def _semantically_equal(left: object, right: object) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, (int, float, Decimal)) and isinstance(right, (int, float, Decimal)):
        try:
            return Decimal(str(left)) == Decimal(str(right))
        except Exception:
            return False
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        # Polars structs union keys across rows; a missing key becomes null.
        return all(
            _semantically_equal(left.get(key), right.get(key))
            for key in left.keys() | right.keys()
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            _semantically_equal(a, b) for a, b in zip(left, right, strict=True)
        )
    return left == right


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _copy_recovery_original(source: Path, root: Path, backup_root: Path) -> None:
    if not backup_root.is_dir() or backup_root.is_symlink():
        raise ValueError(f"invalid recovery backup directory: {backup_root}")
    destination = backup_root / source.relative_to(root)
    destination.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    os.chmod(destination.parent, 0o700)
    if destination.exists():
        raise FileExistsError(f"recovery backup file already exists: {destination}")
    try:
        os.link(source, destination)
    except OSError as exc:
        unsupported_link = {
            errno.EXDEV,
            errno.EPERM,
            errno.EACCES,
            errno.EOPNOTSUPP,
            errno.ENOTSUP,
        }
        if exc.errno not in unsupported_link:
            raise
    else:
        _fsync_directory(destination.parent)
        return
    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".backup.tmp",
    )
    temporary = Path(temporary_name)
    try:
        with source.open("rb") as original, os.fdopen(descriptor, "wb") as backup:
            shutil.copyfileobj(original, backup)
            backup.flush()
            os.fsync(backup.fileno())
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _convert_canonical_bronze_payload_file(
    source: Path,
    root: Path,
    backup_root: Path,
    staging_root: Path,
    backup_lock: Lock,
) -> None:
    original_frame = pl.read_parquet(source, low_memory=True)
    if "provider_payload" in original_frame.columns:
        raise ValueError(f"ambiguous Bronze payload columns: {source}")
    if original_frame.height == 0:
        with backup_lock:
            _copy_recovery_original(source, root, backup_root)
        source.unlink()
        _fsync_directory(source.parent)
        return
    rows: list[dict[str, object]] = []
    for row_number, source_row in enumerate(original_frame.to_dicts(), start=1):
        row = dict(source_row)
        row["provider_payload"] = _decode_bronze_payload_json(
            row.pop("payload_json"), source, row_number
        )
        rows.append(row)
    try:
        staged_frame = pl.DataFrame(rows, infer_schema_length=None, strict=False)
    except Exception as exc:
        raise ValueError(f"could not stage native Bronze provider payload: {source}") from exc

    descriptor, temporary_name = tempfile.mkstemp(
        dir=staging_root,
        prefix=f".{source.name}.",
        suffix=".tmp",
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        staged_frame.write_parquet(temporary, compression="zstd", statistics=True)
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        staged_rows = pl.read_parquet(temporary, low_memory=True).to_dicts()
        if len(staged_rows) != len(rows) or not all(
            _semantically_equal(expected, actual)
            for expected, actual in zip(rows, staged_rows, strict=True)
        ):
            raise ValueError(f"staged Bronze payload verification failed: {source}")
        with backup_lock:
            _copy_recovery_original(source, root, backup_root)
        os.replace(temporary, source)
        _fsync_directory(source.parent)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _convert_canonical_bronze_payloads(root: Path, backup_root: Path, *, cores: int) -> int:
    """Convert canonical JSON-text Bronze payloads with bounded parallelism."""

    sources = _canonical_bronze_payload_files(root, cores=cores)
    if not sources:
        return 0
    staging_root = Path(
        tempfile.mkdtemp(prefix=f".{root.name}.bronze-stage-", dir=root.parent)
    )
    os.chmod(staging_root, 0o700)
    if os.stat(staging_root).st_dev != os.stat(root).st_dev:
        shutil.rmtree(staging_root)
        raise ValueError("Bronze staging directory must be on the lake filesystem")
    backup_root.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    backup_root.mkdir(mode=0o700, exist_ok=False)
    os.chmod(backup_root, 0o700)
    _fsync_directory(backup_root.parent)
    backup_lock = Lock()
    converted = 0
    try:
        with ThreadPoolExecutor(max_workers=cores, thread_name_prefix="xdl-bronze-native") as pool:
            results = pool.map(
                lambda source: _convert_canonical_bronze_payload_file(
                    source,
                    root,
                    backup_root,
                    staging_root,
                    backup_lock,
                ),
                sources,
                buffersize=max(cores * 2, 1),
            )
            for result in results:
                del result
                converted += 1
                if converted % 1000 == 0 or converted == len(sources):
                    print(
                        canonical_json(
                            {
                                "stage": "canonical-bronze-payload-migration",
                                "files_converted": converted,
                                "files_total": len(sources),
                            }
                        ),
                        file=sys.stderr,
                        flush=True,
                    )
    except BaseException:
        shutil.rmtree(staging_root)
        try:
            backup_root.rmdir()
        except OSError:
            pass
        else:
            _fsync_directory(backup_root.parent)
        raise
    shutil.rmtree(staging_root)
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
        prior = (
            [
                {
                    "isin": row.get("isin"),
                    "exchange": row.get("exchange"),
                    "code": row.get("code"),
                    "provider_payload": read_legacy_bronze_provider_payload(row, path),
                }
                for row in read_parquet_rows(path)
            ]
            if path.exists()
            else []
        )
        unique = {canonical_json(cast(JSONValue, row)): row for row in prior}
        unique.update(incoming)
        ordered = [unique[key] for key in sorted(unique)]
        if ordered != prior:
            # Legacy sources remain authoritative until final validation, so
            # missing or unflushed Parquet output is safely replayable.
            write_native_parquet_rows(path, ordered, sync_file=False, sync_directory=False)
            written += 1
    return written


def _provider_payload_rows(outer: Mapping[str, JSONValue]) -> list[dict[str, JSONValue]]:
    raw = outer.get("provider_payload", outer.get("payload", outer.get("payload_json")))
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
        "provider_payload": dict(item),
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
    if _canonical_snapshot_ready(layout, layer, dataset, manifest_path):
        return False
    legacy_manifest_path = layout.legacy_manifest_path(layer, dataset)
    if legacy_manifest_path.exists():
        manifest = json.loads(legacy_manifest_path.read_text(encoding="utf-8"))
    elif manifest_path.exists():
        manifest = read_manifest_payload(manifest_path)
    else:
        manifest = {}
    if source.exists():
        # Inspect the full JSON input so late non-null strings are not inferred as Null.
        frame = pl.read_json(source, infer_schema_length=None)
    else:
        legacy_run_metadata = manifest.get("run_metadata")
        storage_version = (
            legacy_run_metadata.get("storage_version")
            if isinstance(legacy_run_metadata, dict)
            else None
        )
        legacy_partitions = (
            layout.snapshot_version_path(layer, dataset, storage_version) / "partitions"
            if isinstance(storage_version, str)
            else None
        )
        partition_sources = (
            sorted(legacy_partitions.glob("*.parquet"))
            if legacy_partitions is not None
            else []
        )
        if partition_sources:
            return _convert_versioned_parquet_snapshot(
                runtime,
                layout,
                layer,
                dataset,
                manifest,
                manifest_path,
                partition_sources,
            )
        else:
            rows = (
                read_dataset_rows(layout, layer, dataset)
                if manifest_path.exists()
                else read_legacy_dataset_rows(layout, layer, dataset)
            )
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
        _frame_rows(frame, dataset=dataset),
        snapshot=SnapshotWrite(
            semantic_metadata=semantic_metadata,
            retractions=retractions,
        ),
        # Legacy rows remain authoritative until Gold fingerprints validate;
        # replay can safely recreate missing directory entries after a crash.
        durable_sync=False,
        presorted=True,
    )
    return True


def _convert_versioned_parquet_snapshot(  # noqa: C901, PLR0913, PLR0917
    runtime: PostgresEodhdBootstrapRuntime,
    layout: MedallionLayout,
    layer: Layer,
    dataset: str,
    manifest: Mapping[str, object],
    manifest_path: Path,
    partition_sources: list[Path],
) -> bool:
    """Convert old one-instrument files with memory bounded to one partition."""

    run_metadata = manifest.get("run_metadata")
    if not isinstance(run_metadata, dict):
        raise ValueError(f"legacy {layer.value}/{dataset} manifest has no run metadata")
    row_count = 0
    expected_schema: tuple[str, ...] | None = None
    staged: dict[tuple[str, ...], list[Path]] = {}
    sort_columns = ["exchange", "code"]
    if dataset != "listings":
        sort_columns.append("trade_date" if dataset == "eod_quotes" else "event_date")
    sort_columns.append("isin")
    temporary_parent = layout.root.parent.parent / ".xdl-snapshot-migration-tmp"
    temporary_parent.mkdir(mode=0o700, parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(
        prefix="xdl-snapshot-migration-",
        dir=temporary_parent,
    ) as temporary_root:
        stage_root = Path(temporary_root)
        chunk_number = 0
        for source_index, source in enumerate(partition_sources, start=1):
            frame = pl.read_parquet(source, low_memory=True)
            if frame.height == 0:
                continue
            schema = tuple(sorted(frame.columns))
            if expected_schema is None:
                expected_schema = schema
            elif schema != expected_schema:
                raise ValueError(
                    f"legacy {layer.value}/{dataset} partitions have inconsistent schemas"
                )
            row_count += frame.height
            ordered = frame.sort(sort_columns)
            for key, group in groupby(
                _frame_rows(ordered, dataset=dataset, presorted=True),
                key=lambda row: _snapshot_partition_key(layer, dataset, row),
            ):
                chunk_path = stage_root / f"{chunk_number:08d}.parquet"
                write_parquet_rows(
                    chunk_path,
                    group,
                    sync_file=False,
                    sync_directory=False,
                )
                staged.setdefault(key, []).append(chunk_path)
                chunk_number += 1
            if source_index % 1_000 == 0 or source_index == len(partition_sources):
                print(
                    canonical_json(
                        {
                            "stage": "snapshot-partition-staging",
                            "dataset": dataset,
                            "layer": layer.value,
                            "source_partitions_completed": source_index,
                            "source_partitions_total": len(partition_sources),
                            "rows_read": row_count,
                        }
                    ),
                    file=sys.stderr,
                    flush=True,
                )

        if expected_schema is None:
            if not manifest_path.exists():
                raise FileNotFoundError(f"missing {layer.value} snapshot partitions")

        def staged_rows() -> Iterator[dict[str, JSONValue]]:
            for key in sorted(staged):
                chunks = staged[key]
                frame = (
                    pl.scan_parquet([str(path) for path in chunks])
                    .sort(sort_columns)
                    .collect(engine="streaming")
                )
                yield from _frame_rows(frame, dataset=dataset, presorted=True)

        if layer is Layer.GOLD:
            semantic_metadata, retractions = _gold_migration_metadata(
                layout,
                dataset,
                manifest,
                row_count,
                manifest_path,
            )
        else:
            semantic_metadata = {"row_count": row_count}
            retractions = None
        runtime._persist_partitioned_snapshot(
            layer,
            dataset,
            staged_rows(),
            snapshot=SnapshotWrite(
                semantic_metadata=semantic_metadata,
                retractions=retractions,
            ),
            durable_sync=False,
            presorted=True,
        )
    return True


def _snapshot_partition_key(
    layer: Layer,
    dataset: str,
    row: Mapping[str, JSONValue],
) -> tuple[str, ...]:
    if layer is Layer.SILVER:
        exchange, symbol, year = _silver_partition_key(dataset, row)
        return exchange, symbol, f"{year:04d}"
    exchange = row.get("exchange")
    symbol = row.get("code")
    if (
        not isinstance(exchange, str)
        or not exchange
        or not isinstance(symbol, str)
        or not symbol
    ):
        raise ValueError(f"Gold {dataset} row has an incomplete symbol identity")
    return exchange, symbol


def _canonical_snapshot_ready(
    layout: MedallionLayout,
    layer: Layer,
    dataset: str,
    manifest_path: Path,
) -> bool:
    if not manifest_path.exists():
        return False
    manifest = read_manifest_payload(manifest_path)
    run_metadata = manifest.get("run_metadata", {})
    paths = run_metadata.get("artifact_paths") if isinstance(run_metadata, dict) else None
    if not isinstance(paths, list):
        return False
    semantic_metadata = manifest.get("semantic_metadata")
    if layer is Layer.GOLD and (
        not isinstance(semantic_metadata, dict)
        or semantic_metadata.get("dataset_id") != f"xetra-{dataset}-gold-v1"
    ):
        return False
    return all(
        isinstance(path, str)
        and _canonical_snapshot_path(layer, PurePosixPath(path))
        and (layout.root / path).is_file()
        for path in paths
    )


def _canonical_snapshot_path(layer: Layer, path: PurePosixPath) -> bool:
    """Reject manifests that still point at the retired monthly/versioned layouts."""

    if layer is Layer.SILVER:
        return _canonical_silver_path(path)
    if layer is Layer.GOLD:
        return _canonical_gold_path(path)
    return False


def _canonical_silver_path(path: PurePosixPath) -> bool:
    parts = path.parts
    valid_partition = len(parts) == 7 and parts[0] == "silver" and all(
        parts[index].startswith(prefix)
        for index, prefix in (
            (1, "dataset_type="),
            (2, "exchange="),
            (3, "symbol="),
            (4, "timeframe="),
            (5, "year="),
        )
    )
    return bool(
        valid_partition
        and parts[6]
        == f"{parts[3].removeprefix('symbol=')}-{parts[5].removeprefix('year=')}.parquet"
    )


def _canonical_gold_path(path: PurePosixPath) -> bool:
    parts = path.parts
    valid_partition = (
        len(parts) == 6
        and parts[0] == "gold"
        and parts[1].startswith("dataset_id=")
        and parts[2] == "dataset_type=gold_symbol_dataset"
        and parts[3].startswith("exchange=")
        and parts[4].startswith("symbol=")
    )
    symbol = parts[4].removeprefix("symbol=") if len(parts) > 4 else ""
    return bool(
        valid_partition
        and re.fullmatch(
            re.escape(f"{symbol}_GOLD_") + r"[0-9a-f]{64}_[0-9a-f]{64}\.parquet",
            parts[5],
        )
        is not None
    )


def _canonical_bronze_path(path: PurePosixPath) -> bool:
    parts = path.parts
    return (
        len(parts) == 9
        and parts[0] == "bronze"
        and parts[1].startswith("dataset_type=")
        and parts[2].startswith("exchange=")
        and parts[3].startswith("instrument_type=")
        and parts[4].startswith("symbol=")
        and parts[5].startswith("timeframe=")
        and re.fullmatch(r"year=\d{4}", parts[6]) is not None
        and re.fullmatch(r"month=\d{4}-\d{2}", parts[7]) is not None
        and parts[8] == "data.parquet"
    )


def _validate_canonical_bronze_schema(path: Path) -> None:
    schema = pl.read_parquet_schema(path)
    if "payload_json" in schema:
        raise ValueError(f"legacy payload_json remains in canonical Bronze schema: {path}")
    if "provider_payload" not in schema:
        raise ValueError(f"native provider_payload is missing from canonical Bronze schema: {path}")
    if not isinstance(schema["provider_payload"], pl.Struct):
        raise ValueError(
            f"provider_payload must be a native struct in canonical Bronze schema: {path}"
        )


def _is_root_restart_checkpoint(path: PurePosixPath) -> bool:
    """Recognize only the active root-level restart checkpoint."""

    return len(path.parts) == 1 and path.name == "weekly.checkpoint.parquet"


def _obsolete_checkpoint_archives(root: Path) -> list[Path]:
    """Find root-level aborted checkpoints for preservation outside the live lake."""

    return sorted(
        path
        for path in root.glob("weekly.checkpoint.aborted*.parquet")
        if path.is_file()
    )


def _validate_restart_checkpoint(path: Path) -> None:
    """Validate a recognized Parquet checkpoint using the runtime contract."""

    _checkpoint_from_payload(_read_checkpoint_parquet(path))


def _validate_canonical_lake(root: Path) -> int:  # noqa: C901
    """Reject any row artifact left outside the goal's canonical lake layout."""

    row_artifact_count = 0
    canonical_snapshot_artifacts: set[str] = set()
    canonical_bronze_artifacts: list[Path] = []
    invalid_paths: list[str] = []
    for artifact in root.rglob("*.parquet"):
        relative = PurePosixPath(artifact.relative_to(root).as_posix())
        if _is_root_restart_checkpoint(relative):
            _validate_restart_checkpoint(artifact)
            continue
        if "_metadata" in relative.parts:
            continue
        layer_is_canonical = (
            _canonical_bronze_path(relative)
            if relative.parts[0] == Layer.BRONZE.value
            else _canonical_silver_path(relative)
            if relative.parts[0] == Layer.SILVER.value
            else _canonical_gold_path(relative)
            if relative.parts[0] == Layer.GOLD.value
            else False
        )
        if layer_is_canonical:
            row_artifact_count += 1
            if relative.parts[0] == Layer.BRONZE.value:
                canonical_bronze_artifacts.append(artifact)
            else:
                canonical_snapshot_artifacts.add(relative.as_posix())
        elif len(invalid_paths) < 10:
            invalid_paths.append(relative.as_posix())
    if invalid_paths:
        raise ValueError(
            "non-canonical Parquet row artifacts remain in the lake: "
            + ", ".join(invalid_paths)
        )

    with ThreadPoolExecutor(
        max_workers=_POLARS_CORES,
        thread_name_prefix="xdl-bronze-schema-validate",
    ) as pool:
        for _ in pool.map(
            _validate_canonical_bronze_schema,
            canonical_bronze_artifacts,
            buffersize=max(_POLARS_CORES * 2, 1),
        ):
            pass

    referenced_artifacts = set(_canonical_snapshot_references(root))
    orphaned_artifacts = canonical_snapshot_artifacts - referenced_artifacts
    if orphaned_artifacts:
        raise ValueError(
            "canonical Silver/Gold artifacts are not referenced by a manifest: "
            + ", ".join(sorted(orphaned_artifacts)[:10])
        )
    orphaned_sidecars = _unreferenced_snapshot_sidecars(root)
    if orphaned_sidecars:
        raise ValueError(
            "unreferenced Gold retraction sidecars remain in the lake: "
            + ", ".join(path.relative_to(root).as_posix() for path in orphaned_sidecars[:10])
        )

    legacy_rows = [
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file()
        and (
            (path.suffix == ".json" and _legacy_artifact(path, root))
            or (path.name.startswith(".data.parquet.") and path.name.endswith(".tmp"))
        )
    ]
    if legacy_rows:
        raise ValueError(
            "legacy row or temporary artifacts remain in the lake: "
            + ", ".join(legacy_rows[:10])
        )
    return row_artifact_count


def _canonical_snapshot_references(root: Path) -> dict[str, str]:  # noqa: C901, PLR0912
    """Load and validate the authoritative Silver/Gold manifest artifact set."""

    referenced: dict[str, str] = {}
    resolved_root = root.resolve()
    for layer in (Layer.SILVER, Layer.GOLD):
        metadata_root = root / layer.value / "_metadata"
        for manifest_path in sorted(metadata_root.rglob("manifest.parquet")):
            manifest = read_manifest_payload(manifest_path)
            if not isinstance(manifest, dict) or manifest.get("layer") != layer.value:
                raise ValueError(f"invalid canonical {layer.value} manifest: {manifest_path}")
            dataset = manifest.get("dataset")
            run_metadata = manifest.get("run_metadata")
            artifact_paths = (
                run_metadata.get("artifact_paths") if isinstance(run_metadata, dict) else None
            )
            if not isinstance(dataset, str) or not isinstance(artifact_paths, list):
                raise ValueError(f"invalid canonical {layer.value} manifest: {manifest_path}")
            owner = f"{layer.value}/{dataset}"
            for artifact_path in artifact_paths:
                if not isinstance(artifact_path, str):
                    raise ValueError(f"invalid artifact path in {manifest_path}")
                relative = PurePosixPath(artifact_path)
                if not _canonical_snapshot_path(layer, relative):
                    raise ValueError(f"non-canonical artifact path in {manifest_path}")
                resolved_artifact = (root / Path(*relative.parts)).resolve()
                if (
                    resolved_root not in resolved_artifact.parents
                    or not resolved_artifact.is_file()
                ):
                    raise ValueError(f"missing or unsafe artifact in {manifest_path}")
                previous_owner = referenced.get(relative.as_posix())
                if previous_owner is not None:
                    raise ValueError(
                        f"duplicate canonical artifact reference: {relative} "
                        f"({previous_owner}, {owner})"
                    )
                referenced[relative.as_posix()] = owner
            retractions_path = run_metadata.get("retractions_path")
            if retractions_path is not None:
                expected_retractions = (
                    PurePosixPath("gold")
                    / "_metadata"
                    / f"dataset_type={dataset}"
                    / "retractions.parquet"
                )
                if (
                    layer is not Layer.GOLD
                    or dataset not in {"dividends", "splits"}
                    or not isinstance(retractions_path, str)
                    or PurePosixPath(retractions_path) != expected_retractions
                ):
                    raise ValueError(f"invalid Gold retraction sidecar path in {manifest_path}")
                resolved_sidecar = (root / Path(*expected_retractions.parts)).resolve()
                if resolved_root not in resolved_sidecar.parents or not resolved_sidecar.is_file():
                    raise ValueError(f"missing or unsafe retraction sidecar in {manifest_path}")
                if expected_retractions.as_posix() in referenced:
                    raise ValueError(
                        f"duplicate canonical artifact reference: {expected_retractions}"
                    )
                referenced[expected_retractions.as_posix()] = owner
    return referenced


def _unreferenced_canonical_artifacts(root: Path) -> list[Path]:
    """Find stale canonical snapshots so the recoverable archive can remove duplicates."""

    referenced = set(_canonical_snapshot_references(root))
    orphaned: list[Path] = []
    for layer in (Layer.SILVER, Layer.GOLD):
        for artifact in (root / layer.value).rglob("*.parquet"):
            relative = PurePosixPath(artifact.relative_to(root).as_posix())
            if _canonical_snapshot_path(layer, relative) and relative.as_posix() not in referenced:
                orphaned.append(artifact)
    return sorted(orphaned)


def _unreferenced_snapshot_sidecars(root: Path) -> list[Path]:
    """Find retraction Parquets not owned by a current Gold manifest."""

    referenced = set(_canonical_snapshot_references(root))
    metadata_root = root / Layer.GOLD.value / "_metadata"
    return sorted(
        path
        for path in metadata_root.rglob("retractions.parquet")
        if path.is_file() and path.relative_to(root).as_posix() not in referenced
    )


def _is_nested_quote_legacy_source(path: Path, root: Path) -> bool:
    relative = PurePosixPath(path.relative_to(root).as_posix())
    return (
        path.suffix == ".parquet"
        and len(relative.parts) > 2
        and relative.parts[0] == "bronze"
        and relative.parts[1] == "dataset_type=eod_quotes"
        and sum(part.startswith("symbol=") for part in relative.parts) > 1
    )


def _nested_quote_legacy_sources(root: Path) -> list[Path]:
    """Find quote Parquets hidden below more than one legacy symbol directory."""

    bronze_quotes = root / "bronze" / "dataset_type=eod_quotes"
    if not bronze_quotes.is_dir():
        return []
    return sorted(
        path
        for path in bronze_quotes.rglob("*.parquet")
        if _is_nested_quote_legacy_source(path, root)
    )


def _nested_quote_expected_rows(
    source: Path,
    context: _BronzeMigrationContext,
) -> Counter[str]:
    legacy_rows = read_parquet_rows(source)
    if not legacy_rows:
        raise ValueError(f"nested legacy quote partition is empty: {source}")
    expected: Counter[str] = Counter()
    fallback_date = datetime.fromtimestamp(source.stat().st_mtime, UTC).date()
    for outer in legacy_rows:
        payload_text = outer.get("payload_json")
        if not isinstance(payload_text, str):
            raise ValueError(f"nested legacy quote row lacks payload_json: {source}")
        try:
            payload_value = json.loads(payload_text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"nested legacy quote payload is invalid: {source}") from exc
        if not isinstance(payload_value, dict) and not (
            isinstance(payload_value, list)
            and payload_value
            and all(isinstance(item, dict) for item in payload_value)
        ):
            raise ValueError(f"nested legacy quote payload is not an object list: {source}")
        for item in _provider_payload_rows(outer):
            _, normalized = _bronze_entry(
                context,
                "eod_quotes",
                outer,
                item,
                fallback_date,
            )
            normalized["provider_payload"] = _canonical_quote_payload(item)
            expected[canonical_json(normalized)] += 1
    return expected


_QUOTE_NUMERIC_FIELDS = frozenset({"open", "high", "low", "close", "adjusted_close", "volume"})


def _canonical_quote_payload(payload: Mapping[str, object]) -> dict[str, JSONValue]:
    """Normalize only defined quote numeric fields across JSON/Parquet scalar types."""

    normalized: dict[str, JSONValue] = {}
    for key, value in payload.items():
        if (
            key in _QUOTE_NUMERIC_FIELDS
            and isinstance(value, (int, float, Decimal, str))
            and not isinstance(value, bool)
        ):
            normalized[key] = canonical_decimal(Decimal(str(value)))
        else:
            normalized[key] = cast(JSONValue, value)
    return normalized


def _nested_quote_canonical_target(root: Path, source: Path) -> Path:
    """Collapse repeated symbol directories without changing other partition keys."""

    relative = PurePosixPath(source.relative_to(root).as_posix())
    symbol_indexes = [
        index for index, part in enumerate(relative.parts) if part.startswith("symbol=")
    ]
    if len(symbol_indexes) < 2:
        raise ValueError(f"nested quote source has no repeated symbol partition: {source}")
    final_symbol_index = symbol_indexes[-1]
    target_parts = tuple(
        part
        for index, part in enumerate(relative.parts)
        if not part.startswith("symbol=") or index == final_symbol_index
    )
    canonical_relative = PurePosixPath(*target_parts)
    if not _canonical_bronze_path(canonical_relative):
        raise ValueError(f"nested quote source does not collapse to canonical Bronze: {source}")
    return root.joinpath(*canonical_relative.parts)


def _verify_nested_quote_target(
    root: Path,
    target: Path,
    expected: Counter[str],
) -> None:
    if not target.is_file():
        raise ValueError(
            "nested legacy quote rows have no canonical target: "
            f"{target.relative_to(root).as_posix()}"
        )
    actual: Counter[str] = Counter()
    for row in read_parquet_rows(target):
        normalized = {
            "isin": row.get("isin"),
            "exchange": row.get("exchange"),
            "code": row.get("code"),
            "provider_payload": _canonical_quote_payload(
                read_legacy_bronze_provider_payload(row, target)
            ),
        }
        actual[canonical_json(cast(JSONValue, normalized))] += 1
    missing = sum(max(count - actual[key], 0) for key, count in expected.items())
    if missing:
        raise ValueError(
            "nested legacy quote rows are not fully present in canonical target "
            f"({missing} missing): {target.relative_to(root).as_posix()}"
        )


def _verify_nested_quote_legacy_sources(root: Path) -> list[Path]:
    """Prove each nested legacy payload row already exists in canonical Bronze."""

    sources = _nested_quote_legacy_sources(root)
    if not sources:
        return []
    context = _bronze_migration_context(MedallionLayout(root))
    expected_by_target: dict[Path, Counter[str]] = {}
    for source in sources:
        target = _nested_quote_canonical_target(root, source)
        expected_by_target.setdefault(target, Counter()).update(
            _nested_quote_expected_rows(source, context)
        )
    for target, expected in expected_by_target.items():
        _verify_nested_quote_target(root, target, expected)
    return sources


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


def _legacy_artifact(path: Path, root: Path) -> bool:
    relative = path.relative_to(root)
    parts = relative.parts
    if (
        not parts
        or parts[0] not in {layer.value for layer in Layer}
        or _is_nested_quote_legacy_source(path, root)
    ):
        return False
    if "_metadata" in parts:
        return False
    if path.name.startswith(".data.parquet.") and path.name.endswith(".tmp"):
        return True
    if path.suffix == ".json":
        return path.name in {"data.json", "manifest.json", "retractions.json"} or (
            "partitions" in parts
        )
    if path.suffix != ".parquet":
        return False
    return (
        "partitions" in parts
        or "versions" in parts
        or any(part.startswith("date=") for part in parts)
        or any(part.startswith("feature_set_version=") for part in parts)
        or (parts[0] == "silver" and any(part.startswith("month=") for part in parts))
        or (len(parts) == 3 and path.name == "data.parquet")
    )


def _archive_legacy_artifacts(
    root: Path,
    backup_root: Path,
    *,
    additional_sources: Iterable[Path] = (),
    allow_existing_backup: bool = False,
) -> int:
    resolved_root = root.resolve()
    sources = {
        path for path in root.rglob("*") if path.is_file() and _legacy_artifact(path, root)
    }
    for path in additional_sources:
        resolved_path = path.resolve()
        if resolved_root not in resolved_path.parents or not resolved_path.is_file():
            raise ValueError(f"additional archive source is outside the lake: {path}")
        sources.add(resolved_path)
    source_count = 0
    actual_backup: Path | None = None
    for source in sorted(sources):
        if actual_backup is None:
            actual_backup = backup_root
            if actual_backup.exists():
                if not allow_existing_backup or not actual_backup.is_dir():
                    raise FileExistsError(f"recovery backup already exists: {actual_backup}")
            else:
                actual_backup.mkdir(parents=True, mode=0o700, exist_ok=False)
            os.chmod(actual_backup, 0o700)
        relative = source.relative_to(root)
        destination = actual_backup / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(destination))
        source_count += 1
    return source_count


def migrate_lake(
    root: Path,
    backup_root: Path,
    *,
    cores: int | None = None,
) -> dict[str, JSONValue]:
    """Publish and validate Parquet snapshots before moving JSON originals to backup."""

    resolved_root = root.resolve()
    resolved_backup_root = backup_root.resolve()
    worker_count = resolve_cores(cores)
    if not resolved_root.is_dir() or not all(
        (resolved_root / layer.value).is_dir() for layer in Layer
    ):
        raise ValueError("migration root must contain bronze, silver, and gold directories")
    if resolved_backup_root == resolved_root or resolved_root in resolved_backup_root.parents:
        raise ValueError("JSON backup root must be outside the lake root")
    runtime = object.__new__(PostgresEodhdBootstrapRuntime)
    runtime._layout = MedallionLayout(resolved_root)
    # The canonical weekly pipeline and the destructive full-refresh verifier
    # use different locks. Hold both while rewriting lake artifacts so neither
    # scheduler path can publish into the same tree concurrently.
    with LoaderLock(resolved_root / "authoritative-rewrite.lock"), LoaderLock(
        resolved_root / "weekly.lock"
    ):
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        actual_backup = resolved_backup_root / stamp
        if actual_backup.exists():
            raise FileExistsError(f"recovery backup already exists: {actual_backup}")
        canonical_bronze_payloads_migrated = _convert_canonical_bronze_payloads(
            resolved_root,
            actual_backup,
            cores=worker_count,
        )
        migrated_manifests = migrate_json_manifests(resolved_root)
        if migrated_manifests:
            print(
                canonical_json(
                    {"stage": "control-metadata-migration", "manifests": migrated_manifests}
                ),
                file=sys.stderr,
                flush=True,
            )
        bronze_partitions = _convert_bronze_partitions(
            resolved_root,
            cores=worker_count,
        )
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

        validated_datasets = validate_gold_fingerprints_streaming(resolved_root)

        verified_nested_quote_sources = _verify_nested_quote_legacy_sources(resolved_root)
        obsolete_checkpoint_archives = _obsolete_checkpoint_archives(resolved_root)
        orphaned_artifacts = _unreferenced_canonical_artifacts(resolved_root)
        orphaned_sidecars = _unreferenced_snapshot_sidecars(resolved_root)
        archived = _archive_legacy_artifacts(
            resolved_root,
            actual_backup,
            additional_sources=(
                *orphaned_artifacts,
                *orphaned_sidecars,
                *verified_nested_quote_sources,
                *obsolete_checkpoint_archives,
            ),
            allow_existing_backup=canonical_bronze_payloads_migrated > 0,
        )
        canonical_row_artifacts = _validate_canonical_lake(resolved_root)
        return {
            "status": "PASS",
            "root": str(resolved_root),
            "bronze_parquet_artifacts": bronze_partitions,
            "silver_gold_snapshots": converted_snapshots,
            "datasets_fingerprint_valid": validated_datasets,
            "canonical_bronze_payload_files_migrated": canonical_bronze_payloads_migrated,
            "legacy_artifacts_archived": archived,
            "orphaned_canonical_artifacts_archived": len(orphaned_artifacts),
            "orphaned_retraction_sidecars_archived": len(orphaned_sidecars),
            "verified_nested_quote_partitions_archived": len(verified_nested_quote_sources),
            "obsolete_checkpoint_archives_preserved": len(obsolete_checkpoint_archives),
            "recovery_backup": str(actual_backup)
            if archived or canonical_bronze_payloads_migrated
            else None,
            "canonical_row_artifacts": canonical_row_artifacts,
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="migrate-lake-to-parquet")
    parser.add_argument("--root", help="Lake root; defaults to medallion.root in config.yaml")
    parser.add_argument(
        "--backup-root",
        default="/home/dev_xetra/.xdl-lake-recovery-backups",
        help="Private parent directory for recoverable legacy lake artifacts",
    )
    parser.add_argument(
        "--cores",
        type=int,
        default=None,
        help="CPU budget for Polars and independent migration tasks (default: config or 6)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    cores = resolve_cores(args.cores)
    result = migrate_lake(
        Path(resolve_medallion_root(args.root)),
        Path(args.backup_root),
        cores=cores,
    )
    print(canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
