"""Dividend ingestion with deterministic correction and retraction reconciliation."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Protocol

from xetra_loader.contracts.corporate_actions import (
    ActionStatus,
    DividendEvent,
    retract_dividend,
)
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.numeric import provider_decimal
from xetra_loader.ingestion.corporate_actions import (
    action_overlap_start,
    reconcile_action_window,
)

type JSONValue = str | int | float | bool | None | list[JSONValue] | dict[str, JSONValue]

class JsonTransport(Protocol):
    def get_json(
        self,
        path: str,
        params: Mapping[str, str | int | float] | None = None,
    ) -> JSONValue: ...


@dataclass(frozen=True, slots=True)
class DividendIngestionResult:
    bronze_payload: str
    silver_records: tuple[DividendEvent, ...]
    added_count: int
    removed_count: int
    correction_count: int
    retraction_count: int

    @property
    def change_set_count(self) -> int:
        """Return the number of added and removed event keys."""

        return self.added_count + self.removed_count


def ingest_dividends(
    transport: JsonTransport,
    listing: ListingRecord,
    *,
    last_event_date: date | None = None,
    previous_records: Iterable[DividendEvent] = (),
) -> DividendIngestionResult:
    """Fetch history and reconcile authoritative event sets by content-addressed key."""

    params: dict[str, str | int | float] = {}
    boundary = action_overlap_start(last_event_date)
    if boundary is not None:
        params["from"] = boundary.isoformat()
    payload = transport.get_json(f"div/{listing.code}.{listing.exchange}", params or None)
    if not isinstance(payload, list):
        raise ValueError("EODHD dividend response must be a JSON array")

    bronze_rows: list[dict[str, JSONValue]] = []
    current: list[DividendEvent] = []
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("each EODHD dividend must be a JSON object")
        bronze_rows.append(item)
        current.append(_normalize_dividend(listing, item))

    active_previous = tuple(
        record for record in previous_records if record.status is ActionStatus.ACTIVE
    )

    bronze_rows.sort(key=_canonical_row)
    reconciliation = reconcile_action_window(
        active_previous,
        current,
        last_event_date=last_event_date,
        retract=retract_dividend,
    )
    return DividendIngestionResult(
        bronze_payload=json.dumps(
            bronze_rows,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ),
        silver_records=reconciliation.records,
        added_count=reconciliation.added_count,
        removed_count=reconciliation.removed_count,
        correction_count=reconciliation.correction_count,
        retraction_count=reconciliation.retraction_count,
    )


def _normalize_dividend(
    listing: ListingRecord,
    row: Mapping[str, JSONValue],
) -> DividendEvent:
    return DividendEvent(
        isin=listing.isin,
        exchange=listing.exchange,
        code=listing.code,
        event_date=date.fromisoformat(_required_text(row, "date")),
        value=_required_decimal(row, "value"),
        currency=_optional_text(row.get("currency")),
        period=_optional_text(row.get("period")),
        declaration_date=_optional_date(row.get("declarationDate")),
        record_date=_optional_date(row.get("recordDate")),
        payment_date=_optional_date(row.get("paymentDate")),
    )


def _required_text(row: Mapping[str, JSONValue], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"dividend field {key} must be non-empty text")
    return value.strip()


def _required_decimal(row: Mapping[str, JSONValue], key: str) -> Decimal:
    value = row.get(key)
    return provider_decimal(value, field=f"dividend field {key}")


def _optional_text(value: JSONValue) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, (list, dict)):
        raise ValueError("dividend text field must be scalar")
    return str(value).strip() or None


def _optional_date(value: JSONValue) -> date | None:
    text = _optional_text(value)
    return None if text is None else date.fromisoformat(text)


def _canonical_row(row: dict[str, JSONValue]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
