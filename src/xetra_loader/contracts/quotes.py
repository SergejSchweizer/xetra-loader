"""Deterministic EOD quote dataset contract."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

from xetra_loader.contracts.numeric import canonical_decimal, require_finite

type JSONValue = str | int | float | bool | None | list[JSONValue] | dict[str, JSONValue]

OVERLAP_DAYS = 7


@dataclass(frozen=True, slots=True)
class QuoteRecord:
    """Semantic EOD quote row keyed by listing identity and business date."""

    isin: str
    exchange: str
    code: str
    trade_date: date
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    adjusted_close: Decimal | None
    volume: int | None

    def __post_init__(self) -> None:
        identities = (("isin", self.isin), ("exchange", self.exchange), ("code", self.code))
        for name, identity in identities:
            if not identity.strip():
                raise ValueError(f"{name} must be non-empty")
        if self.volume is not None and self.volume < 0:
            raise ValueError("volume must be non-negative")
        for name, price in (
            ("open", self.open),
            ("high", self.high),
            ("low", self.low),
            ("close", self.close),
            ("adjusted_close", self.adjusted_close),
        ):
            if price is not None:
                require_finite(price, field=name)
                if price < 0:
                    raise ValueError(f"{name} must be non-negative")
        if self.low is not None and self.high is not None:
            for name, price in (("open", self.open), ("close", self.close)):
                if price is not None and not self.low <= price <= self.high:
                    raise ValueError(f"{name} must be between low and high")

    @property
    def key(self) -> tuple[str, str, str, date]:
        return self.isin, self.exchange, self.code, self.trade_date

    @property
    def timestamp_eod(self) -> datetime:
        """Canonical UTC anchor; explicitly not an exchange close timestamp."""

        return datetime.combine(self.trade_date, time.min, tzinfo=UTC)

    def semantic_dict(self) -> dict[str, JSONValue]:
        return {
            "isin": self.isin,
            "exchange": self.exchange,
            "code": self.code,
            "trade_date": self.trade_date.isoformat(),
            "timestamp_eod": self.timestamp_eod.isoformat(),
            "open": _decimal_text(self.open),
            "high": _decimal_text(self.high),
            "low": _decimal_text(self.low),
            "close": _decimal_text(self.close),
            "adjusted_close": _decimal_text(self.adjusted_close),
            "volume": self.volume,
        }


@dataclass(frozen=True, slots=True)
class QuoteRunMetadata:
    """Execution metadata intentionally excluded from quote semantic identity."""

    run_id: str
    fetched_at_utc: datetime

    def __post_init__(self) -> None:
        if self.fetched_at_utc.tzinfo is None or self.fetched_at_utc.utcoffset() is None:
            raise ValueError("fetched_at_utc must be timezone-aware")
        if self.fetched_at_utc.utcoffset() != timedelta(0):
            raise ValueError("fetched_at_utc must use UTC")


def overlap_start(last_business_date: date) -> date:
    """Return the inclusive seven-calendar-day correction boundary."""

    return last_business_date - timedelta(days=OVERLAP_DAYS)


def validate_unique_quotes(records: Iterable[QuoteRecord]) -> tuple[QuoteRecord, ...]:
    """Reject duplicate business keys and return deterministic key order."""

    ordered = tuple(sorted(records, key=lambda record: record.key))
    seen: set[tuple[str, str, str, date]] = set()
    for record in ordered:
        if record.key in seen:
            raise ValueError(f"duplicate quote key: {record.key}")
        seen.add(record.key)
    return ordered


def serialize_quotes(records: Iterable[QuoteRecord]) -> str:
    """Serialize only semantic fields in deterministic order."""

    ordered = validate_unique_quotes(records)
    return json.dumps(
        [record.semantic_dict() for record in ordered],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else canonical_decimal(value)
