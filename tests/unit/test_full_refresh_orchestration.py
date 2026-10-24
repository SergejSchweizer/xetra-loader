from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

import xetra_loader.ops.verify_postgres_sync as verifier


@pytest.mark.parametrize("acceptance_passes", [True, False])
def test_full_refresh_gates_cleanup_on_independent_acceptance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    acceptance_passes: bool,
) -> None:
    events: list[str] = []
    report = SimpleNamespace(passed=acceptance_passes)

    class _Connection:
        autocommit = False

        def execute(self, *_args: object, **_kwargs: object) -> None:
            return None

        def close(self) -> None:
            events.append("connection_closed")

    class _Runtime:
        @classmethod
        def from_admin_environment(cls, **_kwargs: object) -> _Runtime:
            return cls()

        def promote_staging_state(self) -> None:
            events.append("promote")

        def close(self) -> None:
            events.append("runtime_closed")

    class _ResetRuntime:
        def __init__(self, _runtime: object, *, cores: int) -> None:
            assert cores == 1

        def promote_staging_state(self) -> None:
            events.append("promote")

        def close(self) -> None:
            events.append("runtime_closed")

    monkeypatch.setattr(verifier, "_require_scheduler_disabled", lambda: None)
    monkeypatch.setattr(verifier, "_require_private_backup_root", lambda _path: None)
    monkeypatch.setattr(verifier, "LoaderLock", lambda _path: nullcontext())
    monkeypatch.setattr(verifier, "connect_postgres", lambda **_kwargs: _Connection())
    monkeypatch.setattr(verifier, "_verify_target", lambda _connection: ("local", 5432, (), True))
    monkeypatch.setattr(verifier, "_backup_owned_state", lambda *_args: events.append("backup"))
    monkeypatch.setattr(
        verifier,
        "_archive_weekly_checkpoints",
        lambda *_args: events.append("archive_checkpoint"),
    )
    monkeypatch.setattr(verifier, "PostgresEodhdBootstrapRuntime", _Runtime)
    monkeypatch.setattr(verifier, "_AdminResetWriterPublishRuntime", _ResetRuntime)
    monkeypatch.setattr(
        verifier,
        "run_full_bootstrap",
        lambda *_args, **_kwargs: events.append("build_and_publish"),
    )
    monkeypatch.setattr(verifier, "verify_postgres_sync", lambda *_args, **_kwargs: report)
    monkeypatch.setattr(
        verifier,
        "write_production_report",
        lambda _report, _path: events.append("write_report"),
    )
    monkeypatch.setattr(
        verifier,
        "drop_previous_serving_schemas",
        lambda _connection: events.append("drop_previous"),
    )
    monkeypatch.setattr(
        verifier,
        "_prune_full_sync_backups",
        lambda _path: events.append("prune_backups"),
    )

    arguments = {
        "medallion_root": tmp_path / "lake",
        "output_path": tmp_path / "acceptance.json",
        "backup_root": tmp_path / "backups",
        "weekly_replay": False,
        "cores": 1,
    }
    if acceptance_passes:
        result = verifier.execute_production_full_sync_and_verify(**arguments)
        assert result is report
        assert events.index("write_report") < events.index("drop_previous")
        assert events.index("drop_previous") < events.index("prune_backups")
    else:
        with pytest.raises(verifier.ProductionVerificationError, match="acceptance report"):
            verifier.execute_production_full_sync_and_verify(**arguments)
        assert "write_report" in events
        assert "drop_previous" not in events
        assert "prune_backups" not in events

    assert events.index("backup") < events.index("build_and_publish")
    assert events.index("build_and_publish") < events.index("promote")
    assert events.index("runtime_closed") < events.index("write_report")
