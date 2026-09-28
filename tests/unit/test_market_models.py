from datetime import date
from decimal import Decimal

import pytest

from xetra_loader.market import DividendRow, ListingRow, QuoteRow


def test_listing_contract_carries_active_lifecycle_state() -> None:
    assert ListingRow("DE0000000001", "XETRA", "AAA", is_active=False).is_active is False


def test_quote_rejects_invalid_ohlc_order() -> None:
    with pytest.raises(ValueError, match="between low and high"):
        QuoteRow(
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


def test_event_contract_generates_a_sha256_identity() -> None:
    event = DividendRow(
        isin="DE0000000001",
        exchange="XETRA",
        code="AAA",
        event_date=date(2026, 8, 1),
        value=Decimal("1.25"),
    )
    assert len(event.event_key) == 64
