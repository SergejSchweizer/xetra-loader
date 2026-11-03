"""EOD quote ingestion with full-history and seven-day correction overlap modes."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Protocol

from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.numeric import provider_decimal
from xetra_loader.contracts.quotes import QuoteRecord, overlap_start, validate_unique_quotes

type JSONValue = str | int | float | bool | None | list[JSONValue] | dict[str, JSONValue]

class JsonTransport(Protocol):
    def get_json(
        self,
        path: str,
        params: Mapping[str, str | int | float] | None = None,
    ) -> JSONValue: ...


@dataclass(frozen=True, slots=True)
class QuoteIngestionResult:
    """Raw Bronze state, normalized Silver rows, and semantic delta classification."""

    bronze_rows: tuple[dict[str, JSONValue], ...]
    silver_records: tuple[QuoteRecord, ...]
    inserted_keys: tuple[tuple[str, str, str, date], ...]
    corrected_keys: tuple[tuple[str, str, str, date], ...]


def ingest_quotes(
    transport: JsonTransport,
    listing: ListingRecord,
    *,
    last_business_date: date | None = None,
    previous_records: Iterable[QuoteRecord] = (),
) -> QuoteIngestionResult:
    """Fetch full history or an inclusive seven-calendar-day correction window."""

    params: dict[str, str | int | float] = {}
    if last_business_date is not None:
        params["from"] = overlap_start(last_business_date).isoformat()
    payload = transport.get_json(f"eod/{listing.code}.{listing.exchange}", params or None)
    if not isinstance(payload, list):
        raise ValueError("EODHD quote response must be a JSON array")

    bronze_rows: list[dict[str, JSONValue]] = []
    silver: list[QuoteRecord] = []
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("each EODHD quote must be a JSON object")
        bronze_rows.append(item)
        silver.append(_normalize_quote(listing, item))

    silver_records = validate_unique_quotes(silver)
    previous_by_key = {record.key: record for record in previous_records}
    inserted: list[tuple[str, str, str, date]] = []
    corrected: list[tuple[str, str, str, date]] = []
    for record in silver_records:
        previous = previous_by_key.get(record.key)
        if previous is None:
            inserted.append(record.key)
        elif previous != record:
            corrected.append(record.key)

    bronze_rows.sort(key=_canonical_row)
    return QuoteIngestionResult(
        bronze_rows=tuple(bronze_rows),
        silver_records=silver_records,
        inserted_keys=tuple(inserted),
        corrected_keys=tuple(corrected),
    )


def _normalize_quote(
    listing: ListingRecord,
    row: Mapping[str, JSONValue],
) -> QuoteRecord:
    trade_date = date.fromisoformat(_required_text(row, "date"))
    open_price = _decimal(row.get("open"))
    high = _decimal(row.get("high"))
    low = _decimal(row.get("low"))
    close = _decimal(row.get("close"))
    volume = _integer(row.get("volume"))
    return QuoteRecord(
        isin=listing.isin,
        exchange=listing.exchange,
        code=listing.code,
        trade_date=trade_date,
        open=open_price,
        high=high,
        low=low,
        close=close,
        adjusted_close=_decimal(row.get("adjusted_close", row.get("adjustedClose"))),
        volume=volume,
    )


def _required_text(row: Mapping[str, JSONValue], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"quote field {key} must be non-empty text")
    return value.strip()


def _decimal(value: JSONValue) -> Decimal | None:
    if value is None or value == "":
        return None
    return provider_decimal(value, field="quote numeric field")


def _integer(value: JSONValue) -> int | None:
    if value is None or value == "":
        return None
    decimal = provider_decimal(value, field="quote volume")
    if decimal != decimal.to_integral_value():
        raise ValueError("quote volume must be an exact integer")
    volume = int(decimal)
    if volume < 0:
        raise ValueError("quote volume must be non-negative")
    return volume


def _canonical_row(row: dict[str, JSONValue]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
