from pathlib import Path

import pytest

from xetra_loader.config import (
    ConfigurationError,
    resolve_eodhd_token,
    resolve_feature_work_mem,
    resolve_medallion_root,
    resolve_postgres_admin_dsn,
    resolve_postgres_dsn,
    resolve_postgres_writer_dsn,
)


def _write_config(path: Path) -> None:
    path.write_text(
        """
eodhd:
  api_token: file-token
postgres:
  host: 127.0.0.1
  port: 6543
  user: loader
  password: p@ss/word
  database: market
  feature_work_mem: 512MB
medallion:
  root: /tmp/medallion
""",
        encoding="utf-8",
    )


def test_ignored_yaml_config_resolves_loader_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "config.yaml"
    _write_config(config_path)
    monkeypatch.setenv("XDL_CONFIG_FILE", str(config_path))
    monkeypatch.delenv("EODHD_API_TOKEN", raising=False)
    monkeypatch.delenv("XDL_POSTGRES_DSN", raising=False)
    monkeypatch.delenv("XDL_POSTGRES_WRITER_DSN", raising=False)
    monkeypatch.delenv("XDL_POSTGRES_ADMIN_DSN", raising=False)
    monkeypatch.delenv("XDL_MEDALLION_ROOT", raising=False)

    assert resolve_eodhd_token() == "file-token"
    assert resolve_postgres_dsn() == (
        "postgresql://loader:p%40ss%2Fword@127.0.0.1:6543/market"
    )
    assert resolve_postgres_writer_dsn() == resolve_postgres_dsn()
    assert resolve_postgres_admin_dsn() == (
        "postgresql://loader:p%40ss%2Fword@127.0.0.1:6543/market"
    )
    assert resolve_medallion_root() == "/tmp/medallion"
    assert resolve_feature_work_mem() == "512MB"


def test_environment_values_override_yaml_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "config.yaml"
    _write_config(config_path)
    monkeypatch.setenv("XDL_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("EODHD_API_TOKEN", "env-token")
    monkeypatch.setenv("XDL_POSTGRES_DSN", "postgresql://env-dsn")
    monkeypatch.setenv("XDL_POSTGRES_ADMIN_DSN", "postgresql://admin-dsn")
    monkeypatch.setenv("XDL_MEDALLION_ROOT", "/env/medallion")

    assert resolve_eodhd_token() == "env-token"
    assert resolve_postgres_dsn() == "postgresql://env-dsn"
    assert resolve_postgres_admin_dsn() == "postgresql://admin-dsn"
    assert resolve_medallion_root() == "/env/medallion"


@pytest.mark.parametrize("value", ["15MB", "5GB", "0MB", "512", "not-a-size"])
def test_feature_work_mem_validation_fails_closed(value: str) -> None:
    with pytest.raises(ConfigurationError):
        resolve_feature_work_mem(value)


def test_feature_work_mem_environment_overrides_yaml(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "config.yaml"
    _write_config(config_path)
    monkeypatch.setenv("XDL_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("XDL_FEATURE_WORK_MEM", "1GB")

    assert resolve_feature_work_mem() == "1GB"
