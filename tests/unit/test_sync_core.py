from datetime import UTC, datetime

import pytest

import xetra_loader.sync.core as sync_core
from xetra_loader.sync import SyncCounters, semantic_fingerprint


def test_semantic_fingerprint_is_order_independent() -> None:
    left, left_count = semantic_fingerprint(
        [{"id": 2, "value": "b"}, {"id": 1, "value": "a"}]
    )
    right, right_count = semantic_fingerprint(
        [{"value": "a", "id": 1}, {"value": "b", "id": 2}]
    )
    assert left == right
    assert left_count == right_count == 2


def test_run_metadata_is_not_part_of_semantic_fingerprint() -> None:
    rows = [{"id": 1, "value": "a"}]
    fingerprint, _ = semantic_fingerprint(rows)
    run_a = {"run_id": "a", "fetched_at": datetime(2026, 8, 22, 10, 0, tzinfo=UTC)}
    run_b = {"run_id": "b", "fetched_at": datetime(2026, 8, 22, 11, 0, tzinfo=UTC)}
    assert run_a != run_b
    assert semantic_fingerprint(rows)[0] == fingerprint


def test_counters_reject_negative_values() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        SyncCounters(updated=-1)


def test_counters_total_all_generic_mutations() -> None:
    counters = SyncCounters(inserted=1, updated=2, deleted=3, retracted=4)
    assert counters.total_mutations == 10


@pytest.mark.parametrize(("cores", "parallel_workers"), [(1, "0"), (6, "5")])
def test_postgres_session_uses_bounded_parallel_query_and_maintenance_workers(
    monkeypatch: pytest.MonkeyPatch,
    cores: int,
    parallel_workers: str,
) -> None:
    class RecordingConnection:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...]]] = []
            self.commits = 0

        def execute(self, query: str, params: tuple[object, ...] = ()) -> None:
            self.calls.append((query, params))

        def commit(self) -> None:
            self.commits += 1

    connection = RecordingConnection()
    monkeypatch.setattr(sync_core, "resolve_postgres_writer_dsn", lambda _dsn: "dsn")
    monkeypatch.setattr(sync_core, "resolve_feature_temp_file_limit", lambda: "32GB")
    monkeypatch.setattr(sync_core.psycopg, "connect", lambda *_args, **_kwargs: connection)

    result = sync_core.connect_postgres(cores=cores)

    assert result is connection
    assert (
        "SELECT set_config(%s, %s, false)",
        ("max_parallel_workers_per_gather", parallel_workers),
    ) in connection.calls
    assert (
        "SELECT set_config('xetra_loader.feature_temp_file_limit', %s, false)",
        ("32GB",),
    ) in connection.calls
    assert (
        "SELECT set_config(%s, %s, false)",
        ("max_parallel_maintenance_workers", parallel_workers),
    ) in connection.calls
    assert connection.commits == 1
