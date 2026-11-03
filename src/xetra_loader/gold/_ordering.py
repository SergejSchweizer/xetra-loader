"""Bounded-core deterministic ordering for large Gold record sets."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from typing import TypeVar

import polars as pl

RecordT = TypeVar("RecordT")
_PARALLEL_SORT_THRESHOLD = 400_000


def order_records(
    records: Iterable[RecordT],
    key: Callable[[RecordT], tuple[object, ...]],
    *,
    cores: int,
) -> tuple[RecordT, ...]:
    """Sort records with Polars' native parallel sort when the input is large.

    Only typed key columns and an ordinal enter Polars; the original records
    remain untouched so Decimal and domain-object semantics are preserved.
    Entry points configure ``POLARS_MAX_THREADS`` before importing Polars. If
    this helper is called outside such an entry point, it falls back to Python
    sorting whenever the initialized Polars pool would exceed the requested
    core budget.
    """

    if cores < 1:
        raise ValueError("Gold transformation core budget must be positive")
    materialized = tuple(records)
    if (
        len(materialized) < _PARALLEL_SORT_THRESHOLD
        or cores == 1
        or pl.thread_pool_size() != cores
    ):
        return tuple(sorted(materialized, key=key))
    if _already_ordered(materialized, key):
        return materialized

    sort_keys = [key(record) for record in materialized]
    if not sort_keys:
        return ()
    width = len(sort_keys[0])
    if any(len(sort_key) != width for sort_key in sort_keys):
        raise ValueError("Gold sort keys must have a stable width")
    columns: dict[str, Sequence[object]] = {
        f"key_{index}": [sort_key[index] for sort_key in sort_keys]
        for index in range(width)
    }
    columns["ordinal"] = list(range(len(materialized)))
    ordinals = (
        pl.DataFrame(columns)
        .sort([f"key_{index}" for index in range(width)], maintain_order=True)
        .get_column("ordinal")
        .to_list()
    )
    return tuple(materialized[int(ordinal)] for ordinal in ordinals)


def _already_ordered(
    records: tuple[RecordT, ...],
    key: Callable[[RecordT], tuple[object, ...]],
) -> bool:
    """Avoid sorting provider batches already emitted in canonical key order."""

    if len(records) < 2:
        return True
    previous = key(records[0])
    for record in records[1:]:
        current = key(record)
        if current < previous:
            return False
        previous = current
    return True
