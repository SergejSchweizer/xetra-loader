from types import SimpleNamespace

import pytest

import xetra_loader.ops.verify_postgres_sync as verifier


@pytest.mark.parametrize(
    "command",
    (
        "/home/dev_xetra/xetra-loader/.venv/bin/xdl-weekly --cores 6",
        "/home/dev_xetra/xetra-loader/.venv/bin/xdl-enqueue-weekly",
    ),
)
def test_destructive_full_sync_rejects_both_recurring_weekly_commands(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
) -> None:
    result = SimpleNamespace(
        returncode=0,
        stdout=f"0 9 * * 0 {command}\n",
        stderr="",
    )
    monkeypatch.setattr(verifier.subprocess, "run", lambda *args, **kwargs: result)

    with pytest.raises(verifier.ProductionVerificationError, match="disable the recurring"):
        verifier._require_scheduler_disabled()


def test_destructive_full_sync_ignores_commented_weekly_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = SimpleNamespace(
        returncode=0,
        stdout="# 0 11 * * 0 xdl-enqueue-weekly\n0 22 * * 0 /usr/bin/true\n",
        stderr="",
    )
    monkeypatch.setattr(verifier.subprocess, "run", lambda *args, **kwargs: result)

    verifier._require_scheduler_disabled()


def test_destructive_full_sync_fails_closed_when_crontab_cannot_be_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = SimpleNamespace(returncode=2, stdout="", stderr="permission denied")
    monkeypatch.setattr(verifier.subprocess, "run", lambda *args, **kwargs: result)

    with pytest.raises(verifier.ProductionVerificationError, match="unable to inspect"):
        verifier._require_scheduler_disabled()


def test_destructive_full_sync_allows_user_without_a_crontab(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = SimpleNamespace(
        returncode=1,
        stdout="",
        stderr="no crontab for dev_xetra",
    )
    monkeypatch.setattr(verifier.subprocess, "run", lambda *args, **kwargs: result)

    verifier._require_scheduler_disabled()
