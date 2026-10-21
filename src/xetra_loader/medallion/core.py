"""Deterministic Bronze/Silver/Gold layout and manifest primitives."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import TextIO, cast
from urllib.parse import quote

import polars as pl

type JSONValue = str | int | float | bool | None | list[JSONValue] | dict[str, JSONValue]


class Layer(StrEnum):
    """Supported medallion layers."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass(frozen=True, slots=True)
class BronzePartition:
    """Partition dimensions for a single Bronze day file."""

    dataset_type: str
    exchange: str
    instrument_type: str
    symbol: str
    timeframe: str
    partition_date: date


@dataclass(frozen=True, slots=True)
class SilverPartition:
    """Partition dimensions for one symbol's annual Silver file."""

    dataset_type: str
    exchange: str
    symbol: str
    timeframe: str
    year: int


@dataclass(frozen=True, slots=True)
class GoldArtifact:
    """Partition and content-address dimensions for a Gold symbol artifact."""

    dataset_id: str
    exchange: str
    symbol: str
    feature_hash: str
    source_hash: str


@dataclass(frozen=True, slots=True)
class MedallionLayout:
    """Resolve dataset paths without allowing traversal outside the configured root."""

    root: Path

    def bronze_path(self, partition: BronzePartition) -> Path:
        """Resolve one monthly Bronze file using the canonical Hive partition keys."""

        return (
            self.root
            / Layer.BRONZE.value
            / _hive("dataset_type", partition.dataset_type)
            / _hive("exchange", partition.exchange)
            / _hive("instrument_type", partition.instrument_type)
            / _hive("symbol", partition.symbol)
            / _hive("timeframe", partition.timeframe)
            / _hive("year", f"{partition.partition_date.year:04d}")
            / _hive("month", partition.partition_date.strftime("%Y-%m"))
            / "data.parquet"
        )

    def silver_path(self, partition: SilverPartition) -> Path:
        """Resolve one annual Silver Parquet file using the canonical layout."""

        return (
            self.root
            / Layer.SILVER.value
            / _hive("dataset_type", partition.dataset_type)
            / _hive("exchange", partition.exchange)
            / _hive("symbol", partition.symbol)
            / _hive("timeframe", partition.timeframe)
            / _hive("year", f"{partition.year:04d}")
            / f"{_path_value(partition.symbol)}-{partition.year:04d}.parquet"
        )

    def gold_path(self, artifact: GoldArtifact) -> Path:
        """Resolve one content-addressed Gold symbol dataset."""

        filename = (
            f"{_path_value(artifact.symbol)}_GOLD_"
            f"{_validate_digest(artifact.feature_hash)}_"
            f"{_validate_digest(artifact.source_hash)}.parquet"
        )
        return (
            self.root
            / Layer.GOLD.value
            / _hive("dataset_id", artifact.dataset_id)
            / _hive("dataset_type", "gold_symbol_dataset")
            / _hive("exchange", artifact.exchange)
            / _hive("symbol", artifact.symbol)
            / filename
        )

    def metadata_path(self, layer: Layer, dataset: str) -> Path:
        """Resolve the JSON control-metadata directory, outside row partitions."""

        return self.root / layer.value / "_metadata" / _hive("dataset_type", dataset)

    def dataset_path(self, layer: Layer, dataset: str) -> Path:
        """Resolve the retired pre-Hive dataset directory for migration tools."""

        _validate_component(dataset)
        return self.root / layer.value / dataset

    def manifest_path(self, layer: Layer, dataset: str) -> Path:
        return self.metadata_path(layer, dataset) / "manifest.json"

    def legacy_manifest_path(self, layer: Layer, dataset: str) -> Path:
        return self.dataset_path(layer, dataset) / "manifest.json"

    def retractions_path(self, layer: Layer, dataset: str) -> Path:
        return self.metadata_path(layer, dataset) / "retractions.parquet"

    def legacy_retractions_path(self, layer: Layer, dataset: str) -> Path:
        return self.dataset_path(layer, dataset) / "retractions.json"

    def snapshot_version_path(self, layer: Layer, dataset: str, version: str) -> Path:
        _validate_component(version)
        return self.dataset_path(layer, dataset) / "versions" / version


def atomic_write_parquet(path: Path, frame: pl.DataFrame) -> None:
    """Atomically replace a Parquet artifact after Polars has fully written it."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        frame.write_parquet(temporary, compression="zstd", statistics=True)
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def write_parquet_rows(path: Path, rows: Iterable[Mapping[str, JSONValue]]) -> None:
    """Write semantic rows with Polars, encoding nested Bronze payloads losslessly."""

    normalized: list[dict[str, object]] = []
    for row in rows:
        normalized.append(
            {
                key: canonical_json(value) if isinstance(value, (dict, list)) else value
                for key, value in row.items()
            }
        )
    if not normalized:
        return
    atomic_write_parquet(path, pl.DataFrame(normalized, infer_schema_length=None, strict=False))


def write_raw_listing_partitions(
    layout: MedallionLayout,
    dataset: str,
    payload_rows: Iterable[Mapping[str, JSONValue]],
    *,
    skip_existing: bool = False,
    snapshot_date: date | None = None,
) -> int:
    """Persist raw provider listing objects in the canonical monthly Bronze layout."""

    grouped: dict[Path, list[dict[str, JSONValue]]] = {}
    as_of = snapshot_date or datetime.now(UTC).date()
    for payload in payload_rows:
        code = payload.get("Code") or payload.get("code")
        exchange = payload.get("Exchange") or payload.get("exchange")
        isin = payload.get("ISIN") or payload.get("Isin") or payload.get("isin")
        code_text = code if isinstance(code, str) else ""
        exchange_text = exchange if isinstance(exchange, str) else ""
        isin_text = isin if isinstance(isin, str) else ""
        encoded = canonical_json(cast(JSONValue, dict(payload))).encode("utf-8")
        symbol = code_text or f"unidentified-{hashlib.sha256(encoded).hexdigest()[:16]}"
        path = layout.bronze_path(
            BronzePartition(
                dataset_type=dataset,
                exchange=exchange_text or "unknown",
                instrument_type=_listing_instrument_type(payload),
                symbol=symbol,
                timeframe="static",
                partition_date=as_of,
            )
        )
        grouped.setdefault(path, []).append(
            {
                "isin": isin_text or None,
                "exchange": exchange_text or "unknown",
                "code": code_text or symbol,
                "payload_json": canonical_json(cast(JSONValue, dict(payload))),
            }
        )

    written = 0
    for path, rows in grouped.items():
        if skip_existing and path.exists():
            continue
        write_parquet_rows(path, rows)
        written += 1
    return written


def read_parquet_rows(path: Path) -> list[dict[str, JSONValue]]:
    """Read one Parquet artifact with Polars and restore canonical JSON scalars."""

    rows: list[dict[str, JSONValue]] = []
    for row in pl.read_parquet(path, low_memory=True).to_dicts():
        normalized: dict[str, JSONValue] = {}
        for key, value in row.items():
            if value is None or isinstance(value, (str, int, float, bool)):
                normalized[key] = value
            elif hasattr(value, "isoformat"):
                normalized[key] = value.isoformat()
            else:
                value_type = type(value).__name__
                raise ValueError(f"unsupported Parquet value in {path}: {key}={value_type}")
        rows.append(normalized)
    return rows


def read_parquet_partitions(directory: Path) -> list[dict[str, JSONValue]]:
    """Read deterministic partition order from a Polars-backed Parquet dataset."""

    rows: list[dict[str, JSONValue]] = []
    for path in sorted(directory.glob("*.parquet")):
        rows.extend(read_parquet_rows(path))
    return rows


def read_dataset_rows(
    layout: MedallionLayout,
    layer: Layer,
    dataset: str,
) -> list[dict[str, JSONValue]]:
    """Read only the canonical manifest-backed lake artifacts."""

    manifest_path = layout.manifest_path(layer, dataset)
    if not manifest_path.exists():
        if _has_legacy_dataset(layout, layer, dataset):
            raise ValueError(f"legacy lake layout requires migration: {layer.value}/{dataset}")
        return []
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    run_metadata = manifest.get("run_metadata")
    artifact_paths = run_metadata.get("artifact_paths") if isinstance(run_metadata, dict) else None
    if not isinstance(artifact_paths, list):
        raise ValueError(f"canonical manifest has no artifact_paths for {layer.value}/{dataset}")
    return _read_manifest_artifacts(layout, artifact_paths)


def read_legacy_dataset_rows(
    layout: MedallionLayout,
    layer: Layer,
    dataset: str,
) -> list[dict[str, JSONValue]]:
    """Read the retired layout only for the one-time migration utility."""

    legacy_manifest = layout.legacy_manifest_path(layer, dataset)
    if legacy_manifest.exists():
        manifest = json.loads(legacy_manifest.read_text(encoding="utf-8"))
        run_metadata = manifest.get("run_metadata")
        version = run_metadata.get("storage_version") if isinstance(run_metadata, dict) else None
        if isinstance(version, str):
            return read_parquet_partitions(
                layout.snapshot_version_path(layer, dataset, version) / "partitions"
            )
    directory = layout.dataset_path(layer, dataset)
    parquet_path = directory / "data.parquet"
    if parquet_path.exists():
        return read_parquet_rows(parquet_path)
    json_path = directory / "data.json"
    if json_path.exists():
        payload = json.loads(json_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
            raise ValueError(f"invalid legacy JSON data artifact for {layer.value}/{dataset}")
        return cast(list[dict[str, JSONValue]], payload)
    return []


def read_snapshot_retractions(
    layout: MedallionLayout,
    layer: Layer,
    dataset: str,
) -> list[dict[str, JSONValue]]:
    """Read the canonical Parquet retraction sidecar named by its manifest."""

    manifest_path = layout.manifest_path(layer, dataset)
    if not manifest_path.exists():
        if _has_legacy_dataset(layout, layer, dataset):
            raise ValueError(f"legacy lake layout requires migration: {layer.value}/{dataset}")
        return []
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    run_metadata = manifest.get("run_metadata")
    retraction_path = (
        run_metadata.get("retractions_path") if isinstance(run_metadata, dict) else None
    )
    if isinstance(retraction_path, str):
        path = _manifest_artifact_path(layout, retraction_path)
        return read_parquet_rows(path) if path.exists() else []
    return []


def read_legacy_snapshot_retractions(
    layout: MedallionLayout,
    layer: Layer,
    dataset: str,
) -> list[dict[str, JSONValue]]:
    """Read retired Parquet or JSON retraction sidecars for migration only."""

    legacy_manifest = layout.legacy_manifest_path(layer, dataset)
    if legacy_manifest.exists():
        manifest = json.loads(legacy_manifest.read_text(encoding="utf-8"))
        run_metadata = manifest.get("run_metadata")
        version = run_metadata.get("storage_version") if isinstance(run_metadata, dict) else None
        if isinstance(version, str):
            path = layout.snapshot_version_path(layer, dataset, version) / "retractions.parquet"
            return read_parquet_rows(path) if path.exists() else []
    legacy_path = layout.legacy_retractions_path(layer, dataset)
    if not legacy_path.exists():
        return []
    payload = json.loads(legacy_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not all(isinstance(key, list) for key in payload):
        raise ValueError(f"invalid legacy retractions for {layer.value}/{dataset}")
    rows: list[dict[str, JSONValue]] = []
    for key in payload:
        if len(key) != 4 or not all(isinstance(part, str) for part in key):
            raise ValueError(f"invalid legacy retraction key for {layer.value}/{dataset}")
        rows.append({"isin": key[0], "exchange": key[1], "code": key[2], "event_key": key[3]})
    return rows


def _has_legacy_dataset(layout: MedallionLayout, layer: Layer, dataset: str) -> bool:
    directory = layout.dataset_path(layer, dataset)
    return (
        layout.legacy_manifest_path(layer, dataset).exists()
        or (directory / "data.json").exists()
        or (directory / "data.parquet").exists()
        or (directory / "versions").exists()
    )


@dataclass(frozen=True, slots=True)
class Manifest:
    """Separate semantic dataset identity from execution-only metadata."""

    dataset: str
    layer: Layer
    semantic_metadata: Mapping[str, JSONValue]
    run_metadata: Mapping[str, JSONValue]

    def semantic_fingerprint(self) -> str:
        payload: dict[str, JSONValue] = {
            "dataset": self.dataset,
            "layer": self.layer.value,
            "semantic_metadata": dict(self.semantic_metadata),
        }
        return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()

    def to_json(self) -> str:
        payload: dict[str, JSONValue] = {
            "dataset": self.dataset,
            "layer": self.layer.value,
            "semantic_fingerprint": self.semantic_fingerprint(),
            "semantic_metadata": dict(self.semantic_metadata),
            "run_metadata": dict(self.run_metadata),
        }
        return canonical_json(payload)


def canonical_json(value: JSONValue) -> str:
    """Serialize JSON deterministically for stable fingerprints and manifests."""

    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def atomic_write(path: Path, write: Callable[[TextIO], None]) -> None:
    """Durably replace one artifact without exposing a partial published file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        text=True,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            write(handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def atomic_write_text(path: Path, content: str) -> None:
    """Atomically replace a complete text artifact."""

    def write(handle: TextIO) -> None:
        handle.write(content)

    atomic_write(path, write)


def _validate_component(value: str) -> None:
    if not value or value in {".", ".."}:
        raise ValueError("dataset path component must be non-empty and relative")
    if Path(value).name != value or "/" in value or "\\" in value:
        raise ValueError("dataset path component must not contain path separators")


def _path_value(value: str) -> str:
    _validate_component(value)
    return quote(value, safe="-._~")


def _hive(key: str, value: str) -> str:
    return f"{key}={_path_value(value)}"


def _validate_digest(value: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("Gold hashes must be lowercase SHA-256 hex digests")
    return value


def _listing_instrument_type(payload: Mapping[str, JSONValue]) -> str:
    raw = payload.get("Type") or payload.get("type")
    if isinstance(raw, str) and raw.strip():
        return raw.strip().lower()
    return "unknown"


def _manifest_artifact_path(layout: MedallionLayout, relative_path: str) -> Path:
    candidate = (layout.root / relative_path).resolve()
    root = layout.root.resolve()
    if candidate != root and root not in candidate.parents:
        raise ValueError("manifest artifact path escapes the configured lake root")
    return candidate


def _read_manifest_artifacts(
    layout: MedallionLayout,
    artifact_paths: list[object],
) -> list[dict[str, JSONValue]]:
    if not all(isinstance(path, str) for path in artifact_paths):
        raise ValueError("manifest artifact_paths must contain only strings")
    rows: list[dict[str, JSONValue]] = []
    for relative_path in artifact_paths:
        path = _manifest_artifact_path(layout, cast(str, relative_path))
        rows.extend(read_parquet_rows(path))
    return rows
