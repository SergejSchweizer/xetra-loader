from datetime import date
from decimal import Decimal

import pytest

from xetra_loader.contracts.corporate_actions import DividendEvent, SplitEvent
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.gold.dividends import build_dividend_gold
from xetra_loader.gold.listings import build_listing_gold
from xetra_loader.gold.quotes import build_quote_gold
from xetra_loader.gold.splits import build_split_gold
from xetra_loader.gold.validation import validate_complete_gold
from xetra_loader.sync.core import AuthoritativeSnapshotRequired, run_sync


def _snapshot():
    listing = build_listing_gold([ListingRecord("DE0000000001", "XETRA", "AAA")])
    quote = build_quote_gold(
        [
            QuoteRecord(
                isin="DE0000000001",
                exchange="XETRA",
                code="AAA",
                trade_date=date(2026, 1, 2),
                open=Decimal("10"),
                high=Decimal("11"),
                low=Decimal("9"),
                close=Decimal("10"),
                adjusted_close=Decimal("10"),
                volume=100,
            )
        ]
    )
    return validate_complete_gold(
        listing,
        quote,
        build_dividend_gold(
            [
                DividendEvent(
                    isin="DE0000000001",
                    exchange="XETRA",
                    code="AAA",
                    event_date=date(2026, 1, 1),
                    value=Decimal("1"),
                )
            ]
        ),
        build_split_gold(
            [
                SplitEvent(
                    isin="DE0000000001",
                    exchange="XETRA",
                    code="AAA",
                    event_date=date(2026, 1, 1),
                    split_ratio="2:1",
                    split_factor=Decimal("2"),
                )
            ]
        ),
    )


def test_mismatched_snapshot_is_rejected_before_dml() -> None:
    with pytest.raises(AuthoritativeSnapshotRequired):
        run_sync(
            object(),  # type: ignore[arg-type]
            dataset="eod_quotes",
            semantic_rows=({"id": 1},),
            mutate=lambda _cursor: object(),  # type: ignore[return-value]
            authoritative_snapshot=_snapshot(),
        )


def test_snapshot_proof_is_bound_to_dataset_fingerprint() -> None:
    snapshot = _snapshot()
    fingerprint = snapshot.semantic_fingerprints["eod_quotes"]
    assert snapshot.matches("eod_quotes", row_count=1, semantic_fingerprint=fingerprint)
    assert not snapshot.matches("eod_quotes", row_count=2, semantic_fingerprint=fingerprint)
