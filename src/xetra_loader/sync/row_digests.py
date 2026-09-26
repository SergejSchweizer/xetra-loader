"""Deterministic per-row digest state for complete Gold reconciliation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Final

from xetra_loader.sync.core import JSONValue

SEMANTIC_METADATA_FIELDS: Final[frozenset[str]] = frozenset(
    {"fetched_at_utc", "published_at_utc", "run_id"}
)
DATASET_KEY_FIELDS: Final[dict[str, tuple[str, ...]]] = {
    "listings": ("isin", "exchange", "code"),
    "eod_quotes": ("isin", "exchange", "code", "trade_date"),
    "dividends": ("isin", "exchange", "code", "event_key"),
    "splits": ("isin", "exchange", "code", "event_key"),
}
_DIGEST_PREFIX = b"xetra-loader:row-digest:v1\x00"


@dataclass(frozen=True, slots=True)
class RowDigest:
    """One persisted digest identified by dataset and canonical business key."""

    dataset: str
    entity_key: str
    row_sha256: str

    def __post_init__(self) -> None:
        if self.dataset not in DATASET_KEY_FIELDS:
            raise ValueError(f"unsupported digest dataset: {self.dataset}")
        if not self.entity_key:
            raise ValueError("entity_key must be non-empty")
        if len(self.row_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.row_sha256
        ):
            raise ValueError("row_sha256 must be lowercase SHA-256")


def _semantic_row(row: Mapping[str, JSONValue]) -> dict[str, JSONValue]:
    return {
        key: value
        for key, value in row.items()
        if key not in SEMANTIC_METADATA_FIELDS
    }


def canonical_entity_key(dataset: str, row: Mapping[str, JSONValue]) -> str:
    """Encode a dataset identity without delimiter or provider-order ambiguity."""

    fields = DATASET_KEY_FIELDS.get(dataset)
    if fields is None:
        raise ValueError(f"unsupported digest dataset: {dataset}")
    try:
        values = [row[field] for field in fields]
    except KeyError as exc:
        raise ValueError(f"row is missing digest key field: {exc.args[0]}") from exc
    if any(value is None or (isinstance(value, str) and not value) for value in values):
        raise ValueError(f"row has an empty digest key for {dataset}")
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"))


def row_sha256(dataset: str, row: Mapping[str, JSONValue]) -> str:
    """Hash only canonical semantic fields and include the dataset namespace."""

    if dataset not in DATASET_KEY_FIELDS:
        raise ValueError(f"unsupported digest dataset: {dataset}")
    payload = json.dumps(
        {"dataset": dataset, "row": _semantic_row(row)},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(_DIGEST_PREFIX + payload).hexdigest()


def row_digests(
    dataset: str,
    rows: Iterable[Mapping[str, JSONValue]],
) -> tuple[RowDigest, ...]:
    """Build a deterministic, duplicate-free digest tuple for a Gold dataset."""

    result: list[RowDigest] = []
    seen: set[str] = set()
    for row in rows:
        entity_key = canonical_entity_key(dataset, row)
        if entity_key in seen:
            raise ValueError(f"duplicate digest entity key in {dataset}: {entity_key}")
        seen.add(entity_key)
        result.append(RowDigest(dataset, entity_key, row_sha256(dataset, row)))
    return tuple(sorted(result, key=lambda digest: digest.entity_key))


def digest_map(digests: Iterable[RowDigest]) -> dict[str, str]:
    """Convert digest state to a map while rejecting duplicate identities."""

    result: dict[str, str] = {}
    for digest in digests:
        if digest.entity_key in result:
            raise ValueError(f"duplicate persisted digest key: {digest.entity_key}")
        result[digest.entity_key] = digest.row_sha256
    return result
