from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import xetra_loader.ops.verify_postgres_sync as verifier


def test_full_sync_backup_retention_keeps_four_newest_owned_directories(
    tmp_path: Path,
) -> None:
    backup_root = tmp_path / "backups"
    backup_root.mkdir()
    backup_names = tuple(
        f"xetra-loader-202610{day:02d}T090000Z" for day in range(1, 7)
    )
    for name in backup_names:
        (backup_root / name).mkdir()

    checkpoint_names = tuple(
        f"weekly-checkpoint-202610{day:02d}T090000Z-{day}.json"
        for day in range(1, 7)
    )
    for name in checkpoint_names:
        (backup_root / name).write_text("checkpoint", encoding="utf-8")

    unrelated = backup_root / "weekly-checkpoint-keep.json"
    unrelated.write_text("checkpoint", encoding="utf-8")
    malformed_name = backup_root / "xetra-loader-20261006T090000Z-extra"
    malformed_name.mkdir()

    removed = verifier._prune_full_sync_backups(backup_root)

    assert removed == (*backup_names[:2], *checkpoint_names[:2])
    assert tuple(sorted(path.name for path in backup_root.iterdir() if path.is_dir())) == (
        *backup_names[2:],
        malformed_name.name,
    )
    assert all((backup_root / name).is_file() for name in checkpoint_names[2:])
    assert unrelated.read_text(encoding="utf-8") == "checkpoint"


def test_full_sync_backup_retention_never_follows_a_backup_symlink(
    tmp_path: Path,
) -> None:
    backup_root = tmp_path / "backups"
    backup_root.mkdir()
    outside = tmp_path / "outside-backup"
    outside.mkdir()
    symlink_backup = backup_root / "xetra-loader-20261001T090000Z"
    symlink_backup.symlink_to(outside, target_is_directory=True)

    assert verifier._prune_full_sync_backups(backup_root) == ()
    assert symlink_backup.is_symlink()
    assert outside.is_dir()


def test_full_sync_backup_retention_rejects_a_symlink_root(tmp_path: Path) -> None:
    actual_root = tmp_path / "actual-backups"
    actual_root.mkdir()
    linked_root = tmp_path / "linked-backups"
    linked_root.symlink_to(actual_root, target_is_directory=True)

    try:
        verifier._prune_full_sync_backups(linked_root)
    except verifier.ProductionVerificationError as exc:
        assert "symbolic link" in str(exc)
    else:
        raise AssertionError("a symlink backup root must not be pruned")


class _FakeConnection:
    autocommit = False

    def execute(self, *_args: object, **_kwargs: object) -> None:
        return None

    def close(self) -> None:
        return None


class _NoopLock:
    def __init__(self, *_args: object, **_kwargs: object) -> None:
        return None

    def __enter__(self) -> _NoopLock:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


def _patch_full_sync(
    monkeypatch: pytest.MonkeyPatch,
    *,
    passed: bool,
) -> None:
    class FakeAdminRuntime:
        def close(self) -> None:
            return None

    report = SimpleNamespace(
        passed=passed,
        as_dict=lambda: {"status": "PASS" if passed else "FAIL"},
    )
    monkeypatch.setattr(verifier, "_require_scheduler_disabled", lambda: None)
    monkeypatch.setattr(verifier, "LoaderLock", _NoopLock)
    monkeypatch.setattr(verifier, "connect_postgres", lambda **_kwargs: _FakeConnection())
    monkeypatch.setattr(
        verifier,
        "_verify_target",
        lambda _connection: ("10.10.1.3", 54321, ("10.10.1.3",), True),
    )
    monkeypatch.setattr(verifier, "_backup_owned_state", lambda *_args: None)
    monkeypatch.setattr(
        verifier.PostgresEodhdBootstrapRuntime,
        "from_admin_environment",
        lambda **_kwargs: FakeAdminRuntime(),
    )
    monkeypatch.setattr(verifier, "run_full_bootstrap", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        verifier,
        "verify_postgres_sync",
        lambda *_args, **_kwargs: report,
    )


def _make_backup_set(backup_root: Path) -> tuple[str, ...]:
    backup_root.mkdir()
    names = tuple(f"xetra-loader-202610{day:02d}T090000Z" for day in range(1, 7))
    for name in names:
        (backup_root / name).mkdir()
    return names


def test_production_sync_prunes_only_after_a_verified_pass_report_is_written(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    backup_root = tmp_path / "backups"
    names = _make_backup_set(backup_root)
    output_path = tmp_path / "report.json"
    _patch_full_sync(monkeypatch, passed=True)

    report = verifier.execute_production_full_sync_and_verify(
        medallion_root=tmp_path / "lake",
        output_path=output_path,
        backup_root=backup_root,
        weekly_replay=False,
    )

    assert report.passed
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "PASS"
    assert not any((backup_root / name).exists() for name in names[:2])
    assert all((backup_root / name).is_dir() for name in names[2:])


def test_failed_production_sync_does_not_prune_backups(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    backup_root = tmp_path / "backups"
    names = _make_backup_set(backup_root)
    output_path = tmp_path / "report.json"
    _patch_full_sync(monkeypatch, passed=False)

    with pytest.raises(verifier.ProductionVerificationError, match="not PASS"):
        verifier.execute_production_full_sync_and_verify(
            medallion_root=tmp_path / "lake",
            output_path=output_path,
            backup_root=backup_root,
            weekly_replay=False,
        )

    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "FAIL"
    assert all((backup_root / name).is_dir() for name in names)


def test_backup_cleanup_failure_does_not_fail_a_completed_production_sync(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    backup_root = tmp_path / "backups"
    _make_backup_set(backup_root)
    output_path = tmp_path / "report.json"
    _patch_full_sync(monkeypatch, passed=True)

    def fail_cleanup(_backup_root: Path) -> tuple[str, ...]:
        raise PermissionError("cleanup unavailable")

    monkeypatch.setattr(verifier, "_prune_full_sync_backups", fail_cleanup)

    report = verifier.execute_production_full_sync_and_verify(
        medallion_root=tmp_path / "lake",
        output_path=output_path,
        backup_root=backup_root,
        weekly_replay=False,
    )

    assert report.passed
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "PASS"
    assert "backup retention cleanup failed" in capsys.readouterr().err
