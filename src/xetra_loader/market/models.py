"""Compatibility aliases for the canonical serving-domain contracts."""

from xetra_loader.contracts.corporate_actions import DividendEvent, SplitEvent
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.contracts.quotes import QuoteRecord

ListingRow = ListingRecord
QuoteRow = QuoteRecord
DividendRow = DividendEvent
SplitRow = SplitEvent

__all__ = ["DividendRow", "ListingRow", "QuoteRow", "SplitRow"]
