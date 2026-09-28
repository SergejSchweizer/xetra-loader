"""Split ingestion with deterministic correction and retraction reconciliation."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Protocol

from xetra_loader.contracts.corporate_actions import ActionStatus, SplitEvent, retract_split
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
class SplitIngestionResult:
    bronze_payload: str
    silver_records: tuple[SplitEvent, ...]
    added_count: int
    removed_count: int
    correction_count: int
    retraction_count: int

    @property
    def change_set_count(self) -> int:
        """Return the number of added and removed event keys."""

        return self.added_count + self.removed_count


def ingest_splits(
    transport: JsonTransport,
    listing: ListingRecord,
    *,
    last_event_date: date | None = None,
    previous_records: Iterable[SplitEvent] = (),
) -> SplitIngestionResult:
    """Fetch history and reconcile authoritative event sets by content-addressed key."""

    params: dict[str, str | int | float] = {}
    boundary = action_overlap_start(last_event_date)
    if boundary is not None:
        params["from"] = boundary.isoformat()
    payload = transport.get_json(f"splits/{listing.code}.{listing.exchange}", params or None)
    if not isinstance(payload, list):
        raise ValueError("EODHD split response must be a JSON array")

    bronze_rows: list[dict[str, JSONValue]] = []
    current: list[SplitEvent] = []
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("each EODHD split must be a JSON object")
        bronze_rows.append(item)
        current.append(_normalize_split(listing, item))

    active_previous = tuple(
        record for record in previous_records if record.status is ActionStatus.ACTIVE
    )

    bronze_rows.sort(key=_canonical_row)
    reconciliation = reconcile_action_window(
        active_previous,
        current,
        last_event_date=last_event_date,
        retract=retract_split,
    )
    return SplitIngestionResult(
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


def _normalize_split(listing: ListingRecord, row: Mapping[str, JSONValue]) -> SplitEvent:
    ratio = _required_text(row, "split")
    return SplitEvent(
        isin=listing.isin,
        exchange=listing.exchange,
        code=listing.code,
        event_date=date.fromisoformat(_required_text(row, "date")),
        split_ratio=ratio,
        split_factor=_split_factor(row.get("split_factor", row.get("splitFactor")), ratio),
    )


def _required_text(row: Mapping[str, JSONValue], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"split field {key} must be non-empty text")
    return value.strip()


def _split_factor(value: JSONValue, ratio: str) -> Decimal | None:
    if value is not None and value != "":
        return provider_decimal(value, field="split factor")
    if ":" not in ratio:
        return None
    numerator, denominator = ratio.split(":", 1)
    denominator_decimal = provider_decimal(denominator.strip(), field="split ratio denominator")
    if denominator_decimal == 0:
        raise ValueError("split ratio denominator must not be zero")
    return provider_decimal(numerator.strip(), field="split ratio numerator") / denominator_decimal


def _canonical_row(row: dict[str, JSONValue]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
