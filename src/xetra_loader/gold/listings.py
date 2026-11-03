"""Validated deterministic Gold builder for XETRA listings."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from xetra_loader.config import resolve_cores
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.gold._fingerprint import semantic_fingerprint
from xetra_loader.gold._ordering import order_records

type JSONValue = str | int | float | bool | None | list[JSONValue] | dict[str, JSONValue]


@dataclass(frozen=True, slots=True)
class ListingGoldResult:
    """Validated Gold rows plus deterministic validation metadata."""

    rows: tuple[ListingRecord, ...]
    row_count: int
    semantic_fingerprint: str

    def semantic_rows(self) -> tuple[dict[str, JSONValue], ...]:
        return tuple(row.semantic_dict() for row in self.rows)


def build_listing_gold(
    records: Iterable[ListingRecord], *, cores: int | None = None
) -> ListingGoldResult:
    """Validate the serving key and produce deterministic Gold metadata."""

    worker_count = resolve_cores(cores)
    ordered = order_records(records, lambda record: record.key, cores=worker_count)
    seen: set[tuple[str, str, str]] = set()
    for record in ordered:
        if not record.isin.strip() or not record.exchange.strip() or not record.code.strip():
            raise ValueError("Gold listing identity fields must be non-empty")
        if record.key in seen:
            raise ValueError(f"duplicate Gold listing key: {record.key}")
        seen.add(record.key)

    return ListingGoldResult(
        rows=ordered,
        row_count=len(ordered),
        semantic_fingerprint=semantic_fingerprint(
            ordered, _semantic_row, cores=worker_count
        ),
    )


def _semantic_row(record: ListingRecord) -> dict[str, JSONValue]:
    return record.semantic_dict()
