from __future__ import annotations

from dataclasses import dataclass

import xetra_loader.gold._ordering as ordering


@dataclass(frozen=True)
class _Record:
    first: str
    second: int
    payload: object


def test_parallel_ordering_matches_python_sort_and_preserves_objects(monkeypatch) -> None:
    monkeypatch.setattr(ordering, "_PARALLEL_SORT_THRESHOLD", 1)
    monkeypatch.setattr(ordering.pl, "thread_pool_size", lambda: 4)
    records = [
        _Record("b", 2, object()),
        _Record("a", 4, object()),
        _Record("a", 1, object()),
    ]

    actual = ordering.order_records(
        records,
        lambda row: (row.first, row.second),
        cores=4,
    )

    assert actual == tuple(sorted(records, key=lambda row: (row.first, row.second)))
    assert [row.payload for row in actual] == [
        records[2].payload,
        records[1].payload,
        records[0].payload,
    ]


def test_ordering_falls_back_when_polars_pool_exceeds_core_budget(monkeypatch) -> None:
    monkeypatch.setattr(ordering, "_PARALLEL_SORT_THRESHOLD", 1)
    monkeypatch.setattr(ordering.pl, "thread_pool_size", lambda: 6)

    def unexpected_dataframe(*args, **kwargs):
        raise AssertionError("Polars sort must respect the requested core budget")

    monkeypatch.setattr(ordering.pl, "DataFrame", unexpected_dataframe)
    records = [_Record("b", 1, None), _Record("a", 1, None)]

    assert ordering.order_records(records, lambda row: (row.first,), cores=2) == tuple(
        sorted(records, key=lambda row: row.first)
    )


def test_already_ordered_records_skip_polars_sort(monkeypatch) -> None:
    monkeypatch.setattr(ordering, "_PARALLEL_SORT_THRESHOLD", 1)
    monkeypatch.setattr(ordering.pl, "thread_pool_size", lambda: 4)

    def unexpected_dataframe(*args, **kwargs):
        raise AssertionError("already ordered records do not need a Polars sort")

    monkeypatch.setattr(ordering.pl, "DataFrame", unexpected_dataframe)
    records = [_Record("a", 1, None), _Record("b", 1, None)]

    assert ordering.order_records(records, lambda row: (row.first,), cores=4) == tuple(records)
