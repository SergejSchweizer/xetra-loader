from datetime import date
from decimal import Decimal

import pytest

from xetra_loader.contracts.corporate_actions import DividendEvent, SplitEvent
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.gold.dividends import build_dividend_gold
from xetra_loader.gold.quotes import build_quote_gold
from xetra_loader.ingestion.quotes import ingest_quotes
from xetra_loader.ingestion.splits import ingest_splits


class FixtureTransport:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def get_json(self, path: str, params: object = None) -> object:
        del path, params
        return self.payload


def _listing() -> ListingRecord:
    return ListingRecord("DE0000000001", "XETRA", "AAA")


def _quote(*, close: Decimal = Decimal("10")) -> QuoteRecord:
    return QuoteRecord(
        isin="DE0000000001",
        exchange="XETRA",
        code="AAA",
        trade_date=date(2026, 8, 21),
        open=Decimal("9"),
        high=Decimal("11"),
        low=Decimal("8"),
        close=close,
        adjusted_close=close,
        volume=1,
    )


def test_harmless_decimal_spellings_have_identical_semantic_fingerprints() -> None:
    one = DividendEvent(
        isin="DE0000000001",
        exchange="XETRA",
        code="AAA",
        event_date=date(2026, 8, 20),
        value=Decimal("1"),
    )
    equivalent = DividendEvent(
        isin=one.isin,
        exchange=one.exchange,
        code=one.code,
        event_date=one.event_date,
        value=Decimal("1.00"),
    )

    assert one.event_key == equivalent.event_key
    assert build_dividend_gold([one]).semantic_fingerprint == build_dividend_gold(
        [equivalent]
    ).semantic_fingerprint
    assert build_quote_gold([_quote(close=Decimal("10.0"))]).semantic_fingerprint == (
        build_quote_gold([_quote(close=Decimal("10.00"))]).semantic_fingerprint
    )


@pytest.mark.parametrize("value", (Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")))
def test_non_finite_provider_numbers_fail(value: Decimal) -> None:
    payload = [
        {
            "date": "2026-08-21",
            "open": Decimal("9"),
            "high": Decimal("11"),
            "low": Decimal("8"),
            "close": value,
            "volume": 1,
        }
    ]

    with pytest.raises(ValueError, match="finite"):
        ingest_quotes(FixtureTransport(payload), _listing())


def test_fractional_volume_fails_but_inconsistent_ohlc_is_preserved() -> None:
    fractional_volume = [
        {"date": "2026-08-21", "open": 9, "high": 11, "low": 8, "close": 10, "volume": 1.5}
    ]
    inconsistent = [
        {"date": "2026-08-21", "open": 9, "high": 11, "low": 8, "close": 12, "volume": 1}
    ]

    with pytest.raises(ValueError, match="exact integer"):
        ingest_quotes(FixtureTransport(fractional_volume), _listing())
    quote = ingest_quotes(FixtureTransport(inconsistent), _listing()).silver_records[0]
    assert quote.close == Decimal("12")


def test_negative_prices_are_preserved_but_non_positive_split_factors_fail() -> None:
    assert _quote(close=Decimal("-1")).close == Decimal("-1")
    with pytest.raises(ValueError, match="positive"):
        SplitEvent(
            isin="DE0000000001",
            exchange="XETRA",
            code="AAA",
            event_date=date(2026, 8, 20),
            split_ratio="0:1",
            split_factor=Decimal("0"),
        )
    with pytest.raises(ValueError, match="positive"):
        ingest_splits(
            FixtureTransport([{"date": "2026-08-20", "split": "0:1"}]),
            _listing(),
        )
