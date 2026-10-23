"""Recover full-sync quote rows quarantined by the previous OHLC normalizer."""

from __future__ import annotations

import argparse
import json
import logging
import re
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from psycopg import Connection

from xetra_loader.config import resolve_cores, resolve_medallion_root
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.gold.quotes import QuoteGoldResult, build_quote_gold
from xetra_loader.gold.validation import GoldValidationSummary
from xetra_loader.ingestion.quotes import _normalize_quote
from xetra_loader.logging_config import configure_logging
from xetra_loader.medallion.core import (
    BronzePartition,
    JSONValue,
    Layer,
    MedallionLayout,
    read_dataset_rows,
    read_parquet_rows,
)
from xetra_loader.ops.bootstrap import PostgresEodhdBootstrapRuntime
from xetra_loader.ops.verify_postgres_sync import (
    DATASETS,
    ProductionVerificationError,
    _verify_target,
    load_gold_snapshots,
    verify_postgres_sync,
    write_production_report,
)
from xetra_loader.pipeline.restart import LoaderLock
from xetra_loader.pipeline.runtime import _quotes_from_rows
from xetra_loader.sync import connect_postgres
from xetra_loader.sync.core import SyncCounters, SyncOutcome

_LOGGER = logging.getLogger("xetra_loader")
_QUARANTINE_LINE = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) "
    r"WARNING xetra_loader\.ingestion\.quotes quarantined EODHD OHLC violation "
    r"for (?P<identity>.+) on (?P<trade_date>\d{4}-\d{2}-\d{2})$"
)


@dataclass(frozen=True, slots=True, order=True)
class QuarantinedQuoteKey:
    """The provider identity and business date named by a quarantine log line."""

    identity: str
    trade_date: date


def read_full_refresh_window(wrapper_log: Path) -> tuple[datetime, datetime]:
    """Read the UTC start/end markers and convert them to the host's log timezone."""

    markers: dict[str, datetime] = {}
    for line in wrapper_log.read_text(encoding="utf-8").splitlines():
        for name in ("full_refresh_start_utc", "full_refresh_end_utc"):
            prefix = f"{name}="
            if line.startswith(prefix):
                value = datetime.fromisoformat(line.removeprefix(prefix).replace("Z", "+00:00"))
                if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
                    raise ValueError(f"{name} must be a UTC timestamp")
                markers[name] = value.astimezone().replace(tzinfo=None)
    start = markers.get("full_refresh_start_utc")
    end = markers.get("full_refresh_end_utc")
    if start is None or end is None or end < start:
        raise ValueError("wrapper log must contain an ordered UTC start and end")
    return start, end


def read_quarantined_quote_keys(
    log_path: Path,
    *,
    started_local: datetime,
    finished_local: datetime,
) -> tuple[QuarantinedQuoteKey, ...]:
    """Select only OHLC quarantine entries emitted during the named full refresh."""

    if started_local.tzinfo is not None or finished_local.tzinfo is not None:
        raise ValueError("log-window bounds must be local naive timestamps")
    if finished_local < started_local:
        raise ValueError("log-window end precedes start")

    issues: set[QuarantinedQuoteKey] = set()
    for line in log_path.read_text(encoding="utf-8").splitlines():
        if "quarantined EODHD OHLC violation" not in line:
            continue
        match = _QUARANTINE_LINE.fullmatch(line)
        if match is None:
            raise ValueError("malformed EODHD OHLC quarantine log entry")
        logged_at = datetime.strptime(match.group("timestamp"), "%Y-%m-%d %H:%M:%S,%f")
        if not started_local <= logged_at <= finished_local:
            continue
        issues.add(
            QuarantinedQuoteKey(
                match.group("identity"),
                date.fromisoformat(match.group("trade_date")),
            )
        )
    return tuple(sorted(issues))


def recover_from_bronze(
    layout: MedallionLayout,
    listings: tuple[ListingRecord, ...],
    quarantined: tuple[QuarantinedQuoteKey, ...],
) -> tuple[QuoteRecord, ...]:
    """Re-normalize just the quarantined raw rows; fail closed on missing/ambiguous data."""

    listings_by_identity = _listing_map(listings)
    issues_by_path = _issues_by_bronze_path(layout, quarantined, listings_by_identity)
    recovered: dict[QuarantinedQuoteKey, set[QuoteRecord]] = {
        issue: set() for issue in quarantined
    }
    for path, path_issues in sorted(issues_by_path.items()):
        listing = listings_by_identity[next(iter(path_issues)).identity]
        recovered.update(_recover_bronze_partition(path, path_issues, listing))

    missing = [issue for issue, records in recovered.items() if not records]
    ambiguous = [issue for issue, records in recovered.items() if len(records) > 1]
    if missing or ambiguous:
        raise ValueError(
            "Bronze recovery was incomplete or ambiguous "
            f"(missing={len(missing)}, ambiguous={len(ambiguous)})"
        )
    return tuple(
        sorted(
            (next(iter(records)) for records in recovered.values()),
            key=lambda record: record.key,
        )
    )


def _listing_map(listings: tuple[ListingRecord, ...]) -> dict[str, ListingRecord]:
    result: dict[str, ListingRecord] = {}
    for listing in listings:
        label = f"{listing.code}.{listing.exchange}"
        if label in result and result[label] != listing:
            raise ValueError(f"ambiguous listing identity for quarantined symbol {label}")
        result[label] = listing
    return result


def _issues_by_bronze_path(
    layout: MedallionLayout,
    quarantined: tuple[QuarantinedQuoteKey, ...],
    listings: Mapping[str, ListingRecord],
) -> dict[Path, set[QuarantinedQuoteKey]]:
    result: dict[Path, set[QuarantinedQuoteKey]] = defaultdict(set)
    for issue in quarantined:
        listing = listings.get(issue.identity)
        if listing is None:
            raise ValueError(f"quarantined identity is absent from Gold listings: {issue.identity}")
        path = layout.bronze_path(
            BronzePartition(
                dataset_type="eod_quotes",
                exchange=listing.exchange,
                instrument_type=listing.instrument_type or "unknown",
                symbol=listing.code,
                timeframe="1d",
                partition_date=issue.trade_date,
            )
        )
        result[path].add(issue)
    return result


def _recover_bronze_partition(
    path: Path,
    issues: set[QuarantinedQuoteKey],
    listing: ListingRecord,
) -> dict[QuarantinedQuoteKey, set[QuoteRecord]]:
    if not path.is_file():
        raise ValueError(f"Bronze partition is missing for quarantined quotes: {path}")
    recovered: dict[QuarantinedQuoteKey, set[QuoteRecord]] = {
        issue: set() for issue in issues
    }
    wanted_dates = {issue.trade_date.isoformat() for issue in issues}
    for bronze_row in read_parquet_rows(path):
        if not _bronze_row_matches_listing(bronze_row, listing):
            continue
        payload = _bronze_payload(bronze_row, path)
        raw_date = payload.get("date")
        if not isinstance(raw_date, str) or raw_date not in wanted_dates:
            continue
        issue = QuarantinedQuoteKey(
            f"{listing.code}.{listing.exchange}", date.fromisoformat(raw_date)
        )
        if issue in issues:
            record = _normalize_quote(
                listing,
                cast(Mapping[str, JSONValue], payload),
                sanitize_invalid_ohlc=True,
            )
            recovered[issue].add(record)
    return recovered


def _bronze_row_matches_listing(
    row: Mapping[str, JSONValue], listing: ListingRecord
) -> bool:
    return (
        row.get("isin") == listing.isin
        and row.get("exchange") == listing.exchange
        and row.get("code") == listing.code
    )


def _bronze_payload(row: Mapping[str, JSONValue], path: Path) -> dict[str, JSONValue]:
    encoded = row.get("payload_json")
    if not isinstance(encoded, str):
        raise ValueError(f"Bronze payload_json is invalid in {path}")
    payload = json.loads(encoded)
    if not isinstance(payload, dict):
        raise ValueError(f"Bronze quote payload is not an object in {path}")
    return cast(dict[str, JSONValue], payload)


def repair_full_sync_quarantines(
    *,
    wrapper_log: Path,
    source_report: Path,
    log_path: Path,
    output_path: Path,
    cores: int | None = None,
) -> dict[str, JSONValue]:
    """Repair a passed full import from Bronze and independently verify PostgreSQL."""

    _require_passed_source_report(source_report)
    started_local, finished_local = read_full_refresh_window(wrapper_log)
    quarantined = read_quarantined_quote_keys(
        log_path,
        started_local=started_local,
        finished_local=finished_local,
    )
    if not quarantined:
        raise ValueError("the full-sync window contains no quarantined OHLC rows")

    root = Path(resolve_medallion_root()).resolve()
    worker_count = resolve_cores(cores)
    with LoaderLock(root / "authoritative-rewrite.lock"):
        snapshots = _validated_snapshots(root)
        layout = MedallionLayout(root)
        listings = _gold_listings(layout)
        recovered = recover_from_bronze(layout, listings, quarantined)
        quote_gold, authoritative = _repaired_quote_snapshot(snapshots, recovered)
        report = _publish_and_verify(
            root=root,
            quote_gold=quote_gold,
            authoritative=authoritative,
            output_path=output_path,
            cores=worker_count,
        )
        result: dict[str, JSONValue] = {
            "status": "PASS" if report.passed else "FAIL",
            "quarantined_keys": len(quarantined),
            "recovered_rows": len(recovered),
            "quote_gold_rows": quote_gold.row_count,
            "postgres_report": str(output_path),
        }
        if not report.passed:
            raise ProductionVerificationError("post-repair PostgreSQL acceptance is not PASS")
        return result


def _require_passed_source_report(path: Path) -> None:
    source_payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(source_payload, dict) or source_payload.get("status") != "PASS":
        raise ProductionVerificationError("source full-sync report is not PASS")


def _validated_snapshots(root: Path) -> dict[str, Any]:
    snapshots = load_gold_snapshots(root)
    for dataset in DATASETS:
        snapshot = snapshots[dataset]
        if not snapshot.fingerprint_valid:
            raise ValueError(f"existing Gold fingerprint is invalid for {dataset}")
        if (
            snapshot.source_count != snapshot.row_count
            or snapshot.source_rows_fingerprint != snapshot.gold_rows_fingerprint
        ):
            raise ValueError(f"existing Silver and Gold disagree for {dataset}")
    return snapshots


def _gold_listings(layout: MedallionLayout) -> tuple[ListingRecord, ...]:
    return tuple(
        ListingRecord(
            isin=_required_text(row, "isin"),
            exchange=_required_text(row, "exchange"),
            code=_required_text(row, "code"),
            name=_optional_text(row, "name"),
            instrument_type=_optional_text(row, "instrument_type"),
            currency=_optional_text(row, "currency"),
            country=_optional_text(row, "country"),
            is_active=_required_bool(row, "is_active"),
        )
        for row in read_dataset_rows(layout, Layer.GOLD, "listings")
    )


def _repaired_quote_snapshot(
    snapshots: Mapping[str, Any],
    recovered: tuple[QuoteRecord, ...],
) -> tuple[QuoteGoldResult, GoldValidationSummary]:
    old_quotes = _quotes_from_rows(
        [dict(row) for row in snapshots["eod_quotes"].rows]
    )
    old_keys = {record.key for record in old_quotes}
    if old_keys.intersection(record.key for record in recovered):
        raise ValueError("a quarantined quote key already exists in current Gold")
    quote_gold = build_quote_gold((*old_quotes, *recovered))
    if quote_gold.row_count != len(old_quotes) + len(recovered):
        raise ValueError("repaired Gold quote count is not the expected complete count")
    row_counts = {name: snapshots[name].row_count for name in DATASETS}
    fingerprints = {name: snapshots[name].computed_fingerprint for name in DATASETS}
    row_counts["eod_quotes"] = quote_gold.row_count
    fingerprints["eod_quotes"] = quote_gold.semantic_fingerprint
    return quote_gold, GoldValidationSummary(row_counts, fingerprints)


def _publish_and_verify(
    *,
    root: Path,
    quote_gold: QuoteGoldResult,
    authoritative: GoldValidationSummary,
    output_path: Path,
    cores: int,
) -> Any:
    admin = connect_postgres(admin=True, cores=cores)
    try:
        admin.autocommit = True
        if not _verify_target(admin)[3]:
            raise ProductionVerificationError(
                "configured admin connection is not the required target"
            )
        admin.execute("ALTER TABLE xetra_loader.eod_quotes ALTER COLUMN close DROP NOT NULL")
        admin.execute("SET TIME ZONE 'UTC'")

        writer = PostgresEodhdBootstrapRuntime.from_environment(cores=cores)
        try:
            writer.persist_silver("eod_quotes", quote_gold.semantic_rows())
            writer.persist_gold(
                "eod_quotes",
                quote_gold.semantic_rows(),
                row_count=quote_gold.row_count,
                semantic_fingerprint=quote_gold.semantic_fingerprint,
            )
            writer.publish_quotes(quote_gold, authoritative_snapshot=authoritative)
        finally:
            writer.close()

        outcomes = _latest_sync_outcomes(admin)
        if set(outcomes) != set(DATASETS):
            raise ValueError("PostgreSQL does not have a committed run for every dataset")
        initial = cast(Any, SimpleNamespace(sync_outcomes=outcomes))
        report = verify_postgres_sync(
            admin,
            medallion_root=root,
            initial=initial,
            replay_mutations={},
            weekly_replay_performed=False,
        )
        write_production_report(report, output_path)
        output_path.chmod(0o600)
        return report
    finally:
        admin.close()


def _latest_sync_outcomes(connection: Connection[Any]) -> dict[str, SyncOutcome]:
    rows = connection.execute(
        "SELECT DISTINCT ON (dataset) run_id, dataset, semantic_fingerprint, row_count, status, "
        "inserted_count, updated_count, deleted_count, retracted_count "
        "FROM xetra_loader_sync.loader_runs "
        "ORDER BY dataset, finished_at_utc DESC, run_id DESC"
    ).fetchall()
    outcomes: dict[str, SyncOutcome] = {}
    for row in rows:
        outcome = SyncOutcome(
            run_id=str(row[0]),
            dataset=str(row[1]),
            semantic_fingerprint=str(row[2]).strip(),
            row_count=int(row[3]),
            status=str(row[4]),
            counters=SyncCounters(
                inserted=int(row[5]),
                updated=int(row[6]),
                deleted=int(row[7]),
                retracted=int(row[8]),
            ),
        )
        outcomes[outcome.dataset] = outcome
    return outcomes


def _required_text(row: Mapping[str, JSONValue], name: str) -> str:
    value = row.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Gold listing field {name} must be non-empty text")
    return value


def _optional_text(row: Mapping[str, JSONValue], name: str) -> str | None:
    value = row.get(name)
    if value is not None and not isinstance(value, str):
        raise ValueError(f"Gold listing field {name} must be text or null")
    return value


def _required_bool(row: Mapping[str, JSONValue], name: str) -> bool:
    value = row.get(name)
    if not isinstance(value, bool):
        raise ValueError(f"Gold listing field {name} must be boolean")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="xdl-repair-quote-quarantines")
    parser.add_argument("--wrapper-log", type=Path, required=True)
    parser.add_argument("--source-report", type=Path, required=True)
    parser.add_argument("--log", type=Path, default=Path(".logs/xdl-verify-production-sync.log"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cores", type=int, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the guarded Bronze repair and print a sanitized summary."""

    configure_logging("xdl-repair-quote-quarantines")
    args = _parser().parse_args(argv)
    try:
        result = repair_full_sync_quarantines(
            wrapper_log=args.wrapper_log,
            source_report=args.source_report,
            log_path=args.log,
            output_path=args.output,
            cores=args.cores,
        )
    except Exception as exc:
        _LOGGER.error("quote_quarantine_repair_failed error_type=%s", type(exc).__name__)
        print(json.dumps({"status": "FAIL", "error_type": type(exc).__name__}, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
