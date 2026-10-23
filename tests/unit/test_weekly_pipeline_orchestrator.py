import json
from collections.abc import Callable

import pytest

from xetra_loader.pipeline.orchestrator import (
    PipelineStageError,
    PipelineStages,
    PipelineSummary,
    StageReport,
    run_weekly_pipeline,
)
from xetra_loader.sync.core import JSONValue

EXPECTED_ORDER = [
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
]


def _stages(calls: list[str], *, fail_at: str | None = None) -> PipelineStages:
    def stage(name: str) -> Callable[[], dict[str, JSONValue]]:
        def run() -> dict[str, JSONValue]:
            calls.append(name)
            if name == fail_at:
                raise RuntimeError(f"boom-{name}")
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
    )


def test_weekly_pipeline_executes_exact_order_and_reports_every_stage() -> None:
    calls: list[str] = []
    summary = run_weekly_pipeline(_stages(calls))

    assert calls == EXPECTED_ORDER
    assert summary.succeeded
    assert [report.name for report in summary.reports] == EXPECTED_ORDER
    assert [report.status for report in summary.reports] == ["success"] * len(EXPECTED_ORDER)
    assert summary.as_dict()["status"] == "success"


def test_public_pipeline_report_aggregates_private_checkpoint_provenance() -> None:
    provenance = [
        {
            "key": ["DE0000000001", "XETRA", "AAA", f"2026-01-{day:02d}"],
            "fetched_at_utc": "2026-10-23T17:00:00+00:00",
        }
        for day in range(1, 32)
    ]
    summary = PipelineSummary(
        (
            StageReport(
                "quotes",
                "success",
                {"fetched_at_by_key": provenance, "rows": 31},
            ),
        )
    )

    details = summary.as_dict()["stages"][0]["details"]
    assert details == {"fetch_provenance_entries": 31, "rows": 31}
    assert "fetched_at_by_key" not in details
    assert len(provenance) == 31


def test_public_pipeline_report_aggregates_compact_quote_provenance() -> None:
    summary = PipelineSummary(
        (
            StageReport(
                "quotes",
                "success",
                {
                    "fetched_at_by_window": [
                        {
                            "listing_key": ["DE0000000001", "XETRA", "AAA"],
                            "start_date": "2026-01-01",
                            "end_date": "2026-12-31",
                            "fetched_at_utc": "2026-10-23T17:00:00+00:00",
                        }
                    ],
                    "rows": 100,
                },
            ),
        )
    )

    details = summary.as_dict()["stages"][0]["details"]
    assert details == {"fetch_provenance_entries": 1, "rows": 100}
    assert "fetched_at_by_window" not in details


def test_weekly_pipeline_failure_blocks_every_downstream_stage() -> None:
    calls: list[str] = []

    with pytest.raises(PipelineStageError) as exc_info:
        run_weekly_pipeline(_stages(calls, fail_at="gold_validation"))

    assert calls == EXPECTED_ORDER[:5]
    assert exc_info.value.stage == "gold_validation"
    assert [report.name for report in exc_info.value.summary.reports] == EXPECTED_ORDER[:5]
    assert exc_info.value.summary.reports[-1].status == "failed"
    assert exc_info.value.summary.as_dict()["status"] == "failed"


def test_failed_stage_report_redacts_exception_text() -> None:
    secret = "credential=must-not-appear"
    stages = _stages([], fail_at="gold_validation")

    def fail_with_secret() -> dict[str, JSONValue]:
        raise RuntimeError(secret)

    stages = PipelineStages(
        listings=stages.listings,
        quotes=stages.quotes,
        dividends=stages.dividends,
        splits=stages.splits,
        gold_validation=fail_with_secret,
        postgres_listings_sync=stages.postgres_listings_sync,
        postgres_quotes_sync=stages.postgres_quotes_sync,
        postgres_dividends_sync=stages.postgres_dividends_sync,
        postgres_splits_sync=stages.postgres_splits_sync,
        verification=stages.verification,
    )

    with pytest.raises(PipelineStageError) as exc_info:
        run_weekly_pipeline(stages)

    report = json.dumps(exc_info.value.summary.as_dict(), sort_keys=True)
    assert secret not in report
    assert secret not in str(exc_info.value)
    assert exc_info.value.summary.reports[-1].details == {"error_type": "RuntimeError"}
