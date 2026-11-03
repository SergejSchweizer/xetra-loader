from datetime import date
from decimal import Decimal

from xetra_loader.contracts.corporate_actions import DividendEvent
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.quotes import QuoteRecord


def test_listing_contract_carries_active_lifecycle_state() -> None:
    assert ListingRecord("DE0000000001", "XETRA", "AAA", is_active=False).is_active is False


def test_quote_contract_preserves_provider_ohlc_values_even_if_inconsistent() -> None:
    quote = QuoteRecord(
        isin="DE0000000001",
        exchange="XETRA",
        code="AAA",
        trade_date=date(2026, 8, 22),
        open=Decimal("8"),
        high=Decimal("12"),
        low=Decimal("9"),
        close=Decimal("10.25"),
        adjusted_close=Decimal("10.25"),
        volume=100,
    )
    assert quote.open == Decimal("8")


def test_event_contract_generates_a_sha256_identity() -> None:
    event = DividendEvent(
        isin="DE0000000001",
        exchange="XETRA",
        code="AAA",
        event_date=date(2026, 8, 1),
        value=Decimal("1.25"),
    )
    assert len(event.event_key) == 64
