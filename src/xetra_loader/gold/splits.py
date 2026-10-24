"""Validated deterministic Gold builder for split events."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass

from xetra_loader.config import resolve_cores
from xetra_loader.contracts.corporate_actions import ActionStatus, SplitEvent
from xetra_loader.contracts.numeric import canonical_decimal
from xetra_loader.gold._fingerprint import semantic_fingerprint
from xetra_loader.gold._ordering import order_records

type JSONValue = str | int | float | bool | None | list[JSONValue] | dict[str, JSONValue]


@dataclass(frozen=True, slots=True)
class SplitGoldResult:
    """Active serving rows plus deterministic retraction metadata."""

    rows: tuple[SplitEvent, ...]
    retracted_keys: tuple[tuple[str, str, str, str], ...]
    row_count: int
    semantic_fingerprint: str

    def semantic_rows(self) -> tuple[dict[str, JSONValue], ...]:
        return tuple(_serving_row(row) for row in self.rows)


def build_split_gold(
    records: Iterable[SplitEvent], *, cores: int | None = None
) -> SplitGoldResult:
    """Validate split event keys and separate active rows from retractions."""

    worker_count = resolve_cores(cores)
    ordered = order_records(
        records,
        lambda event: (*event.key, event.status.value),
        cores=worker_count,
    )
    seen: set[tuple[str, str, str, str]] = set()
    active: list[SplitEvent] = []
    retracted: list[tuple[str, str, str, str]] = []
    for event in ordered:
        if event.key in seen:
            raise ValueError(f"duplicate Gold split key: {event.key}")
        seen.add(event.key)
        if len(event.event_key) != 64:
            raise ValueError("Gold split event_key must be SHA-256")
        if not event.split_ratio.strip():
            raise ValueError("Gold split ratio must be non-empty")
        if event.status is ActionStatus.ACTIVE:
            active.append(event)
        else:
            retracted.append(event.key)

    active_rows = tuple(active)
    retracted_keys = tuple(sorted(retracted))
    encoded_retractions = json.dumps(
        [list(key) for key in retracted_keys],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return SplitGoldResult(
        rows=active_rows,
        retracted_keys=retracted_keys,
        row_count=len(active_rows),
        semantic_fingerprint=semantic_fingerprint(
            active_rows,
            _serving_row,
            cores=worker_count,
            prefix=b'{"retracted_keys":' + encoded_retractions + b',"rows":[',
            suffix=b"]}",
        ),
    )


def _serving_row(event: SplitEvent) -> dict[str, JSONValue]:
    return {
        "isin": event.isin,
        "exchange": event.exchange,
        "code": event.code,
        "event_key": event.event_key,
        "event_date": event.event_date.isoformat(),
        "split_ratio": event.split_ratio,
        "split_factor": (
            None if event.split_factor is None else canonical_decimal(event.split_factor)
        ),
    }
