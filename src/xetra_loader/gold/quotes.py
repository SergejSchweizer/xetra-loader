"""Validated deterministic Gold builder for EOD quotes."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime

from xetra_loader.config import resolve_cores
from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.gold._fingerprint import semantic_fingerprint
from xetra_loader.gold._ordering import order_records

type JSONValue = str | int | float | bool | None | list[JSONValue] | dict[str, JSONValue]


@dataclass(frozen=True, slots=True)
class QuoteGoldResult:
    """Validated Gold quote rows and stable semantic metadata."""

    rows: tuple[QuoteRecord, ...]
    row_count: int
    semantic_fingerprint: str

    def semantic_rows(self) -> tuple[dict[str, JSONValue], ...]:
        return tuple(row.semantic_dict() for row in self.rows)


def build_quote_gold(
    records: Iterable[QuoteRecord], *, cores: int | None = None
) -> QuoteGoldResult:
    """Validate serving keys and UTC-midnight timestamp anchors."""

    worker_count = resolve_cores(cores)
    ordered = order_records(records, lambda record: record.key, cores=worker_count)
    seen: set[tuple[str, str, str, date]] = set()
    for record in ordered:
        if record.key in seen:
            raise ValueError(f"duplicate quote key: {record.key}")
        seen.add(record.key)
        expected = datetime.combine(record.trade_date, datetime.min.time(), tzinfo=UTC)
        if record.timestamp_eod != expected:
            raise ValueError("Gold quote timestamp_eod must equal trade_date at UTC midnight")
        if record.volume is not None and record.volume < 0:
            raise ValueError("Gold quote volume must be non-negative")

    return QuoteGoldResult(
        rows=ordered,
        row_count=len(ordered),
        semantic_fingerprint=semantic_fingerprint(
            ordered, _semantic_row, cores=worker_count
        ),
    )


def _semantic_row(record: QuoteRecord) -> dict[str, JSONValue]:
    return record.semantic_dict()
