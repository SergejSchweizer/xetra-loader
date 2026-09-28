"""Deterministic Bronze/Silver/Gold listing dataset contract."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from typing import cast

type JSONValue = str | int | float | bool | None | list[JSONValue] | dict[str, JSONValue]


@dataclass(frozen=True, slots=True)
class ListingRecord:
    """Normalized listing retained whenever the provider supplies a non-empty ISIN."""

    isin: str
    exchange: str
    code: str
    name: str | None = None
    instrument_type: str | None = None
    currency: str | None = None
    country: str | None = None
    is_active: bool = True

    @property
    def key(self) -> tuple[str, str, str]:
        return self.isin, self.exchange, self.code

    def semantic_dict(self) -> dict[str, JSONValue]:
        return {
            "isin": self.isin,
            "exchange": self.exchange,
            "code": self.code,
            "name": self.name,
            "instrument_type": self.instrument_type,
            "currency": self.currency,
            "country": self.country,
            "is_active": self.is_active,
        }


def normalize_listing(
    provider_row: Mapping[str, object],
    *,
    is_active: bool = True,
) -> ListingRecord | None:
    """Normalize one EODHD listing; only a missing/empty ISIN causes exclusion."""

    isin = _optional_text(provider_row.get("ISIN"))
    if isin is None:
        isin = _optional_text(provider_row.get("Isin"))
    if isin is None:
        return None
    exchange = _required_text("Exchange", provider_row.get("Exchange"))
    code = _required_text("Code", provider_row.get("Code"))
    return ListingRecord(
        isin=isin.upper(),
        exchange=exchange.upper(),
        code=code,
        name=_optional_text(provider_row.get("Name")),
        instrument_type=_optional_text(provider_row.get("Type")),
        currency=_optional_text(provider_row.get("Currency")),
        country=_optional_text(provider_row.get("Country")),
        is_active=is_active,
    )


def normalize_listings(
    provider_rows: Iterable[Mapping[str, object]],
    *,
    is_active: bool = True,
) -> tuple[ListingRecord, ...]:
    """Normalize and order listings, rejecting conflicting identity collapse.

    EODHD may repeat an identical row in a response; those exact semantic
    duplicates are explicitly treated as provider deduplication. Rows with
    the same identity but different metadata fail closed.
    """

    unique: dict[tuple[str, str, str], ListingRecord] = {}
    for row in provider_rows:
        record = normalize_listing(row, is_active=is_active)
        if record is None:
            continue
        previous = unique.get(record.key)
        if previous is not None and previous != record:
            raise ValueError(f"conflicting listing rows for identity: {record.key}")
        unique[record.key] = record
    return tuple(sorted(unique.values(), key=lambda record: record.key))


def serialize_listings(records: Iterable[ListingRecord]) -> str:
    """Serialize semantic listing rows in deterministic key order."""

    ordered = sorted(records, key=lambda record: record.key)
    return json.dumps(
        [record.semantic_dict() for record in ordered],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def deserialize_listings(payload: str) -> tuple[ListingRecord, ...]:
    """Round-trip the deterministic semantic representation."""

    decoded = cast(list[dict[str, JSONValue]], json.loads(payload))
    records = (
        ListingRecord(
            isin=_decoded_required(row, "isin"),
            exchange=_decoded_required(row, "exchange"),
            code=_decoded_required(row, "code"),
            name=_decoded_optional(row, "name"),
            instrument_type=_decoded_optional(row, "instrument_type"),
            currency=_decoded_optional(row, "currency"),
            country=_decoded_optional(row, "country"),
            is_active=_decoded_active(row),
        )
        for row in decoded
    )
    return tuple(sorted(records, key=lambda record: record.key))


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _required_text(name: str, value: object) -> str:
    text = _optional_text(value)
    if text is None:
        raise ValueError(f"provider listing field {name} must be non-empty")
    return text


def _decoded_required(row: Mapping[str, JSONValue], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"serialized listing field {key} must be non-empty text")
    return value


def _decoded_optional(row: Mapping[str, JSONValue], key: str) -> str | None:
    value = row.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"serialized listing field {key} must be text or null")
    return value


def _decoded_active(row: Mapping[str, JSONValue]) -> bool:
    value = row.get("is_active", True)
    if not isinstance(value, bool):
        raise ValueError("serialized listing field is_active must be boolean")
    return value


def merge_listing_lifecycle(
    active: Iterable[ListingRecord],
    delisted: Iterable[ListingRecord],
    previous_records: Iterable[ListingRecord] = (),
) -> tuple[ListingRecord, ...]:
    """Merge lifecycle views with active-provider precedence.

    If an identity appears in both provider responses, the active row wins;
    prior identities absent from both current responses are retained inactive.
    """

    merged = {record.key: replace(record, is_active=True) for record in active}
    for record in delisted:
        merged.setdefault(record.key, replace(record, is_active=False))
    for record in previous_records:
        merged.setdefault(record.key, replace(record, is_active=False))
    return tuple(sorted(merged.values(), key=lambda record: record.key))
