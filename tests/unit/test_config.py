from pathlib import Path

import pytest

from xetra_loader.config import (
    ConfigurationError,
    resolve_api_base_url,
    resolve_api_token,
    resolve_cores,
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
    assert resolve_postgres_dsn() == ("postgresql://loader:p%40ss%2Fword@127.0.0.1:6543/market")
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


def test_cores_resolve_from_yaml_and_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "config.yaml"
    _write_config(config_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8") + "\nweekly:\n  cores: 5\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("XDL_CONFIG_FILE", str(config_path))
    monkeypatch.delenv("XDL_CORES", raising=False)
    assert resolve_cores() == 5
    monkeypatch.setenv("XDL_CORES", "4")
    assert resolve_cores() == 4


def test_cores_default_to_six(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "config.yaml"
    _write_config(config_path)
    monkeypatch.setenv("XDL_CONFIG_FILE", str(config_path))
    monkeypatch.delenv("XDL_CORES", raising=False)
    assert resolve_cores() == 6


@pytest.mark.parametrize("value", ["0", "65", "not-an-int"])
def test_cores_validation_fails_closed(value: str) -> None:
    with pytest.raises(ConfigurationError):
        resolve_cores(value)


def test_api_configuration_resolves_token_and_base_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "config.yaml"
    _write_config(config_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8")
        + '\napi:\n  token: "only-for-this-test-12345678901234567890"\n  base_url: "http://127.0.0.1:8765"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("XDL_CONFIG_FILE", str(config_path))
    monkeypatch.delenv("XDL_API_TOKEN", raising=False)
    monkeypatch.delenv("XDL_API_BASE_URL", raising=False)

    assert resolve_api_token() == "only-for-this-test-12345678901234567890"
    assert resolve_api_base_url() == "http://127.0.0.1:8765"
    monkeypatch.setenv("XDL_API_TOKEN", "environment-token-12345678901234567890")
    assert resolve_api_token() == "environment-token-12345678901234567890"


def test_api_configuration_fails_closed_without_token_or_valid_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "config.yaml"
    _write_config(config_path)
    monkeypatch.setenv("XDL_CONFIG_FILE", str(config_path))
    monkeypatch.delenv("XDL_API_TOKEN", raising=False)
    monkeypatch.setenv("XDL_API_BASE_URL", "file:///tmp/no")

    with pytest.raises(ConfigurationError, match="api.token"):
        resolve_api_token()
    with pytest.raises(ConfigurationError, match="http"):
        resolve_api_base_url()
