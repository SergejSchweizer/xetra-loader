"""EOD quote ingestion with full-history and seven-day correction overlap modes."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Protocol

from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.numeric import provider_decimal
from xetra_loader.contracts.quotes import QuoteRecord, overlap_start, validate_unique_quotes

type JSONValue = str | int | float | bool | None | list[JSONValue] | dict[str, JSONValue]

_LOGGER = logging.getLogger(__name__)


class JsonTransport(Protocol):
    def get_json(
        self,
        path: str,
        params: Mapping[str, str | int | float] | None = None,
    ) -> JSONValue: ...


@dataclass(frozen=True, slots=True)
class QuoteIngestionResult:
    """Raw Bronze state, normalized Silver rows, and semantic delta classification."""

    bronze_payload: str
    silver_records: tuple[QuoteRecord, ...]
    inserted_keys: tuple[tuple[str, str, str, date], ...]
    corrected_keys: tuple[tuple[str, str, str, date], ...]


def ingest_quotes(
    transport: JsonTransport,
    listing: ListingRecord,
    *,
    last_business_date: date | None = None,
    previous_records: Iterable[QuoteRecord] = (),
    quarantine_invalid_ohlc: bool = False,
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
        try:
            silver.append(
                _normalize_quote(
                    listing,
                    item,
                    sanitize_invalid_ohlc=quarantine_invalid_ohlc,
                )
            )
        except ValueError as exc:
            if not quarantine_invalid_ohlc or not _is_invalid_ohlc(exc):
                raise
            _LOGGER.warning(
                "quarantined EODHD OHLC violation for %s.%s on %s",
                listing.code,
                listing.exchange,
                item.get("date"),
            )

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
        bronze_payload=json.dumps(
            bronze_rows,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ),
        silver_records=silver_records,
        inserted_keys=tuple(inserted),
        corrected_keys=tuple(corrected),
    )


def _normalize_quote(
    listing: ListingRecord,
    row: Mapping[str, JSONValue],
    *,
    sanitize_invalid_ohlc: bool,
) -> QuoteRecord:
    trade_date = date.fromisoformat(_required_text(row, "date"))
    open_price = _decimal(row.get("open"))
    high = _decimal(row.get("high"))
    low = _decimal(row.get("low"))
    close_value = _required_decimal(row, "close")
    close: Decimal | None = close_value
    volume = _integer(row.get("volume"))
    open_price, high, low = _missing_ohlc_sentinel(
        open_price,
        high,
        low,
        close_value,
        volume=volume,
    )
    invalid_fields: list[str] = []
    if high is not None and low is not None and low > high:
        if not sanitize_invalid_ohlc:
            raise ValueError("quote low must not exceed high")
        high = None
        low = None
        invalid_fields.extend(("high", "low"))
    elif high is not None and low is not None:
        if open_price is not None and not low <= open_price <= high:
            if not sanitize_invalid_ohlc:
                raise ValueError("open must be between low and high")
            open_price = None
            invalid_fields.append("open")
        if close is not None and not low <= close <= high:
            if not sanitize_invalid_ohlc:
                raise ValueError("close must be between low and high")
            close = None
            invalid_fields.append("close")
    if invalid_fields:
        _LOGGER.warning(
            "nulled invalid EODHD OHLC field(s)=%s for %s.%s on %s",
            ",".join(invalid_fields),
            listing.code,
            listing.exchange,
            trade_date,
        )
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


def _missing_ohlc_sentinel(
    open_price: Decimal | None,
    high: Decimal | None,
    low: Decimal | None,
    close: Decimal,
    *,
    volume: int | None,
) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    """Map EODHD's zero-volume missing-OHLC sentinels without masking bad prices."""

    if open_price == high == low == Decimal(0) and close > 0:
        return None, None, None
    if high == low == Decimal(0) and close > 0 and volume == 0:
        return None, None, None
    return open_price, high, low


def _is_invalid_ohlc(error: ValueError) -> bool:
    return str(error).endswith("must be between low and high")


def _required_text(row: Mapping[str, JSONValue], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"quote field {key} must be non-empty text")
    return value.strip()


def _decimal(value: JSONValue) -> Decimal | None:
    if value is None or value == "":
        return None
    return provider_decimal(value, field="quote numeric field")


def _required_decimal(row: Mapping[str, JSONValue], key: str) -> Decimal:
    value = _decimal(row.get(key))
    if value is None:
        raise ValueError(f"quote field {key} is required")
    return value


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
