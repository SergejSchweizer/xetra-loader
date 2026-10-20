"""Machine-readable loader acceptance contract generation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from xetra_loader.medallion.core import JSONValue, canonical_json

_CRON_PATH = Path("deploy/cron/xetra-loader.cron")
_SCHEDULE_FIELDS = 5


@dataclass(frozen=True, slots=True)
class LoaderAcceptanceReport:
    """Sanitized deterministic cross-repository loader contract report."""

    scenarios: Mapping[str, bool]
    tables: tuple[str, ...]
    timestamp_type: str
    database_timezone: str
    app_role: str
    app_role_select_only: bool
    scheduler_timezone: str
    scheduler_expression: str
    portfell_imports: int

    @property
    def passed(self) -> bool:
        return (
            all(self.scenarios.values())
            and self.timestamp_type == "TIMESTAMPTZ(6)"
            and self.database_timezone == "UTC"
            and self.app_role_select_only
            and self.scheduler_timezone == "Europe/Vienna"
            and self.scheduler_expression == "0 11 * * 0"
            and self.portfell_imports == 0
        )

    def as_dict(self) -> dict[str, JSONValue]:
        return {
            "status": "PASS" if self.passed else "FAIL",
            "scenarios": dict(sorted(self.scenarios.items())),
            "serving_contract": {
                "tables": list(self.tables),
                "timestamp_type": self.timestamp_type,
                "database_timezone": self.database_timezone,
            },
            "consumer_contract": {
                "role": self.app_role,
                "select_only": self.app_role_select_only,
                "portfell_imports": self.portfell_imports,
            },
            "scheduler_contract": {
                "timezone": self.scheduler_timezone,
                "expression": self.scheduler_expression,
            },
        }


def read_scheduler_contract(path: Path = _CRON_PATH) -> tuple[str, str]:
    """Read the timezone and five-field expression deployed in the cron contract."""

    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) < 2 or not lines[0].startswith("CRON_TZ="):
        raise ValueError("cron contract must start with CRON_TZ and contain one schedule")
    schedule_fields = lines[1].split(maxsplit=_SCHEDULE_FIELDS)
    if len(schedule_fields) <= _SCHEDULE_FIELDS:
        raise ValueError("cron contract must contain a command after five schedule fields")
    return lines[0].removeprefix("CRON_TZ="), " ".join(schedule_fields[:_SCHEDULE_FIELDS])


def write_acceptance_report(report: LoaderAcceptanceReport, path: Path) -> Path:
    """Write one stable sanitized JSON artifact suitable for Portfell contract tests."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(report.as_dict()) + "\n", encoding="utf-8")
    return path
