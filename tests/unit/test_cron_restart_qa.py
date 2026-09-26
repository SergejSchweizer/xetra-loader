import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.ingestion.quotes import ingest_quotes
from xetra_loader.ops.acceptance import read_scheduler_contract
from xetra_loader.pipeline.orchestrator import (
    PipelineStageError,
    PipelineStages,
    run_weekly_pipeline,
)
from xetra_loader.pipeline.restart import run_restartable_pipeline

CRON_PATH = Path("deploy/cron/xetra-loader.cron")
STAGES = (
    "listings",
    "dividends",
    "splits",
    "quotes",
    "gold_validation",
    "postgres_listings_sync",
    "postgres_quotes_sync",
    "postgres_dividends_sync",
    "postgres_splits_sync",
    "verification",
)


class _Transport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, str] | None]] = []

    def get_json(self, path: str, params: dict[str, str] | None = None) -> list[dict[str, object]]:
        self.calls.append((path, params))
        return [
            {
                "date": "2026-08-22",
                "open": "10",
                "high": "11",
                "low": "9",
                "close": "10.5",
                "adjusted_close": "10.5",
                "volume": 100,
            }
        ]


def _stages(
    calls: list[str],
    fail_at: str | None = None,
    *,
    rehydratable: bool = False,
) -> PipelineStages:
    def stage(name: str):
        def run() -> dict[str, str]:
            calls.append(name)
            if name == fail_at:
                raise RuntimeError(f"failed-{name}")
            return {"stage": name}

        return run

    return PipelineStages(
        listings=stage("listings"),
        quotes=stage("quotes"),
        dividends=stage("dividends"),
        splits=stage("splits"),
        gold_validation=stage("gold_validation"),
        postgres_listings_sync=stage("postgres_listings_sync"),
        postgres_quotes_sync=stage("postgres_quotes_sync"),
        postgres_dividends_sync=stage("postgres_dividends_sync"),
        postgres_splits_sync=stage("postgres_splits_sync"),
        verification=stage("verification"),
        rehydrate=(lambda details: None) if rehydratable else None,
    )


def _quote() -> QuoteRecord:
    return QuoteRecord(
        isin="DE0000000001",
        exchange="XETRA",
        code="AAA",
        trade_date=date(2026, 8, 22),
        open=Decimal("10"),
        high=Decimal("11"),
        low=Decimal("9"),
        close=Decimal("10.5"),
        adjusted_close=Decimal("10.5"),
        volume=100,
    )


def test_cron_restart_and_delta_contract_report(tmp_path: Path) -> None:
    timezone, expression = read_scheduler_contract(CRON_PATH)
    cron_line = CRON_PATH.read_text(encoding="utf-8").splitlines()[1]

    calls: list[str] = []
    with pytest.raises(PipelineStageError) as failure:
        run_weekly_pipeline(_stages(calls, "gold_validation"))
    gold_gate = calls == ["listings", "dividends", "splits", "quotes", "gold_validation"]
    assert failure.value.stage == "gold_validation"

    resumed_calls: list[str] = []
    checkpoint = tmp_path / "weekly.checkpoint.json"
    with pytest.raises(PipelineStageError):
        run_restartable_pipeline(
            _stages(resumed_calls, "quotes"),
            lock_path=tmp_path / "weekly.lock",
            checkpoint_path=checkpoint,
        )
    completed_before_failure = resumed_calls == ["listings", "dividends", "splits", "quotes"]
    final_calls: list[str] = []
    summary = run_restartable_pipeline(
        _stages(final_calls, rehydratable=True),
        lock_path=tmp_path / "weekly.lock",
        checkpoint_path=checkpoint,
    )
    restart_resumed = summary.succeeded and final_calls == list(STAGES[3:])

    transport = _Transport()
    result = ingest_quotes(
        transport,
        ListingRecord("DE0000000001", "XETRA", "AAA"),
        last_business_date=date(2026, 8, 22),
        previous_records=(_quote(),),
    )
    incremental_request = (
        result.corrected_keys == ()
        and transport.calls == [("eod/AAA.XETRA", {"from": "2026-08-15"})]
    )

    sync_source = Path("src/xetra_loader/sync/core.py").read_text(encoding="utf-8")
    feature_refresh_order = (
        'if dataset == "eod_quotes" and counters.total_mutations:' in sync_source
        and 'cursor.execute("SELECT xetra_loader.refresh_xetra_features()")' in sync_source
    )
    log_paths = (
        Path("artifacts/cron/xdl-weekly.log"),
        Path("artifacts/cron/xdl-bootstrap.log"),
    )
    secret_markers = ("eodhd_api_token", "password=", "postgresql://", "postgres://", "writer_dsn")
    logs_sanitized = all(
        not any(marker in path.read_text(encoding="utf-8").lower() for marker in secret_markers)
        for path in log_paths
        if path.exists()
    )
    checks = {
        "cron_vienna_sunday_0800": timezone == "Europe/Vienna" and expression == "0 8 * * 0",
        "cron_invokes_weekly_not_bootstrap": (
            "xdl-weekly" in cron_line and "xdl-bootstrap" not in cron_line
        ),
        "gold_failure_blocks_publication": gold_gate,
        "restart_checkpoint_rehydrates": completed_before_failure and restart_resumed,
        "incremental_overlap_request": incremental_request,
        "feature_refresh_after_quote_mutation": feature_refresh_order,
        "operational_logs_sanitized": logs_sanitized,
    }
    report: dict[str, Any] = {
        "work_order": "xdl-pr066-qa-xetra-cron-and-restart",
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "cron": {"timezone": timezone, "expression": expression},
        "stage_order": list(STAGES),
        "request_mode": "seven-calendar-day-overlap",
        "feature_refresh": "after-raw-quote-mutation-in-transaction",
        "secrets_included": False,
    }
    report_path = tmp_path / "xdl-pr066-cron-restart.json"
    report_path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    assert json.loads(report_path.read_text(encoding="utf-8"))["status"] == "PASS", checks
