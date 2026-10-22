"""Fail-closed weekly pipeline orchestration with explicit stage ordering."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from xetra_loader.sync.core import JSONValue

type StageDetails = Mapping[str, JSONValue] | None
type StageCallable = Callable[[], StageDetails]
type StageRehydrator = Callable[[Mapping[str, Mapping[str, JSONValue]]], None]


@dataclass(frozen=True, slots=True)
class PipelineStages:
    """All weekly pipeline stages in dependency order."""

    listings: StageCallable
    quotes: StageCallable
    dividends: StageCallable
    splits: StageCallable
    gold_validation: StageCallable
    postgres_listings_sync: StageCallable
    postgres_quotes_sync: StageCallable
    postgres_dividends_sync: StageCallable
    postgres_splits_sync: StageCallable
    verification: StageCallable
    rehydrate: StageRehydrator | None = None


@dataclass(frozen=True, slots=True)
class StageReport:
    """Structured result for one completed pipeline stage."""

    name: str
    status: str
    details: Mapping[str, JSONValue]

    def semantic_row(self) -> dict[str, JSONValue]:
        return {
            "name": self.name,
            "status": self.status,
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class PipelineSummary:
    """Structured summary of all stages reached by one pipeline run."""

    reports: tuple[StageReport, ...]

    @property
    def succeeded(self) -> bool:
        return bool(self.reports) and all(report.status == "success" for report in self.reports)

    def as_dict(self) -> dict[str, JSONValue]:
        return {
            "status": "success" if self.succeeded else "failed",
            "stages": [report.semantic_row() for report in self.reports],
        }


class PipelineStageError(RuntimeError):
    """Raised immediately when one stage fails; downstream stages are never called."""

    def __init__(self, stage: str, summary: PipelineSummary, cause: Exception) -> None:
        super().__init__(f"weekly pipeline failed at stage {stage}: {cause}")
        self.stage = stage
        self.summary = summary
        self.__cause__ = cause


def run_weekly_pipeline(stages: PipelineStages) -> PipelineSummary:
    """Execute the exact weekly stage order and stop immediately on failure."""

    reports: list[StageReport] = []
    for name, stage in _ordered_stages(stages):
        try:
            details = stage()
        except Exception as exc:
            reports.append(StageReport(name=name, status="failed", details={"error": str(exc)}))
            raise PipelineStageError(name, PipelineSummary(tuple(reports)), exc) from exc
        reports.append(
            StageReport(
                name=name,
                status="success",
                details={} if details is None else dict(details),
            )
        )
    return PipelineSummary(tuple(reports))


def _ordered_stages(stages: PipelineStages) -> tuple[tuple[str, StageCallable], ...]:
    return (
        ("listings", stages.listings),
        ("dividends", stages.dividends),
        ("splits", stages.splits),
        ("quotes", stages.quotes),
        ("gold_validation", stages.gold_validation),
        ("postgres_listings_sync", stages.postgres_listings_sync),
        ("postgres_quotes_sync", stages.postgres_quotes_sync),
        ("postgres_dividends_sync", stages.postgres_dividends_sync),
        ("postgres_splits_sync", stages.postgres_splits_sync),
        ("verification", stages.verification),
    )
