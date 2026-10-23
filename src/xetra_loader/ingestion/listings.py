"""XETRA listing ingestion from EODHD into deterministic Bronze/Silver state."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Protocol, cast

from xetra_loader.contracts.listings import (
    ListingRecord,
    merge_listing_lifecycle,
    normalize_listings,
)

type JSONValue = str | int | float | bool | None | list[JSONValue] | dict[str, JSONValue]


class JsonTransport(Protocol):
    """Provider seam required by listing ingestion."""

    def get_json(
        self,
        path: str,
        params: Mapping[str, str | int | float] | None = None,
    ) -> JSONValue: ...


@dataclass(frozen=True, slots=True)
class ListingIngestionResult:
    """Deterministic raw Bronze rows plus normalized Silver listing rows."""

    bronze_rows: tuple[dict[str, JSONValue], ...]
    silver_records: tuple[ListingRecord, ...]


def ingest_xetra_listings(
    transport: JsonTransport,
    *,
    previous_records: Iterable[ListingRecord] = (),
) -> ListingIngestionResult:
    """Fetch active and delisted XETRA symbols and retain vanished identities inactive."""

    active_payload = transport.get_json("exchange-symbol-list/XETRA")
    delisted_payload = transport.get_json("exchange-symbol-list/XETRA", {"delisted": 1})
    if not isinstance(active_payload, list) or not isinstance(delisted_payload, list):
        raise ValueError("EODHD XETRA listing response must be a JSON array")

    bronze_rows: list[dict[str, JSONValue]] = []
    active_rows: list[dict[str, object]] = []
    delisted_rows: list[dict[str, object]] = []
    for item in active_payload:
        if not isinstance(item, dict):
            raise ValueError("each EODHD XETRA listing must be a JSON object")
        bronze_rows.append(item)
        active_rows.append(cast(dict[str, object], item))
    for item in delisted_payload:
        if not isinstance(item, dict):
            raise ValueError("each EODHD XETRA listing must be a JSON object")
        bronze_rows.append(item)
        delisted_rows.append(cast(dict[str, object], item))

    bronze_rows.sort(key=_canonical_row)
    return ListingIngestionResult(
        bronze_rows=tuple(bronze_rows),
        silver_records=merge_listing_lifecycle(
            normalize_listings(active_rows, is_active=True),
            normalize_listings(delisted_rows, is_active=False),
            previous_records,
        ),
    )


def _canonical_row(row: dict[str, JSONValue]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
