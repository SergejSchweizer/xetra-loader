"""Shared bounded reconciliation for dividend and split provider responses."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Protocol, TypeVar, cast

_OVERLAP_DAYS = 7


class ActionRecord(Protocol):
    """Minimum interface required by the content-addressed action reconciler."""

    @property
    def event_date(self) -> date: ...

    @property
    def key(self) -> tuple[str, str, str, str]: ...

    @property
    def status(self) -> object: ...


ActionT = TypeVar("ActionT")


@dataclass(frozen=True, slots=True)
class ActionReconciliationResult[ActionT]:
    """Merged action state and deterministic change-set metrics."""

    records: tuple[ActionT, ...]
    added_count: int
    removed_count: int
    correction_count: int
    retraction_count: int

    @property
    def change_set_count(self) -> int:
        """Return the number of added and removed content-addressed keys."""

        return self.added_count + self.removed_count


def action_overlap_start(last_event_date: date | None) -> date | None:
    """Return the inclusive lower bound of the seven-day correction window."""

    return None if last_event_date is None else last_event_date - timedelta(days=_OVERLAP_DAYS)


def reconcile_action_window[ActionT](
    previous: Iterable[ActionT],
    current: Iterable[ActionT],
    *,
    last_event_date: date | None,
    retract: Callable[[ActionT], ActionT],
) -> ActionReconciliationResult[ActionT]:
    """Reconcile only the provider-authoritative part of an action response.

    A bounded provider response is authoritative from ``action_overlap_start``
    onward. Historical rows before that boundary are retained and are never
    inferred as retracted merely because they were omitted from the response.
    Corrections are paired only when an added and removed key share an event
    date; unrelated additions and removals remain separate metrics.
    """

    boundary = action_overlap_start(last_event_date)
    previous_rows = tuple(previous)
    current_rows = tuple(
        record
        for record in current
        if boundary is None or _view(record).event_date >= boundary
    )
    retained = tuple(
        record
        for record in previous_rows
        if boundary is None or _view(record).event_date < boundary
    )

    previous_in_window = {
        _view(record).key: record
        for record in previous_rows
        if boundary is None or _view(record).event_date >= boundary
    }
    current_by_key = {_view(record).key: record for record in current_rows}
    removed_keys = tuple(sorted(previous_in_window.keys() - current_by_key.keys()))
    added_keys = tuple(sorted(current_by_key.keys() - previous_in_window.keys()))

    removed_by_date: defaultdict[date, int] = defaultdict(int)
    added_by_date: defaultdict[date, int] = defaultdict(int)
    for key in removed_keys:
        removed_by_date[_view(previous_in_window[key]).event_date] += 1
    for key in added_keys:
        added_by_date[_view(current_by_key[key]).event_date] += 1
    correction_count = sum(
        min(removed_by_date[event_date], added_by_date[event_date])
        for event_date in removed_by_date.keys() & added_by_date.keys()
    )

    merged: dict[tuple[str, str, str, str], ActionT] = {
        _view(record).key: record for record in retained
    }
    merged.update(current_by_key)
    merged.update({key: retract(previous_in_window[key]) for key in removed_keys})
    records = tuple(
        sorted(
            merged.values(),
            key=lambda record: (
                _view(record).event_date,
                _view(record).key,
                str(_view(record).status),
            ),
        )
    )
    return ActionReconciliationResult(
        records=records,
        added_count=len(added_keys),
        removed_count=len(removed_keys),
        correction_count=correction_count,
        retraction_count=len(removed_keys) - correction_count,
    )


def _view[ActionT](record: ActionT) -> ActionRecord:
    """Expose the structural action protocol to the generic implementation."""

    return cast(ActionRecord, record)
