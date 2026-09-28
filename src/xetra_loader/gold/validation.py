"""Cross-dataset validation for complete Gold serving state."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from xetra_loader.contracts.corporate_actions import DividendEvent, SplitEvent
from xetra_loader.contracts.quotes import QuoteRecord
from xetra_loader.gold.dividends import DividendGoldResult
from xetra_loader.gold.listings import ListingGoldResult
from xetra_loader.gold.quotes import QuoteGoldResult
from xetra_loader.gold.splits import SplitGoldResult
from xetra_loader.medallion.core import JSONValue

type ListingIdentity = tuple[str, str, str]
type ChildRecord = QuoteRecord | DividendEvent | SplitEvent


@dataclass(frozen=True, slots=True)
class GoldValidationSummary:
    """Exact cross-dataset evidence produced before serving publication."""

    row_counts: Mapping[str, int]
    semantic_fingerprints: Mapping[str, str]

    def matches(self, dataset: str, *, row_count: int, semantic_fingerprint: str) -> bool:
        """Return whether this proof belongs to the exact Gold dataset being published."""

        return (
            self.row_counts.get(dataset) == row_count
            and self.semantic_fingerprints.get(dataset) == semantic_fingerprint
        )

    def as_dict(self) -> dict[str, JSONValue]:
        return {
            "row_counts": dict(sorted(self.row_counts.items())),
            "semantic_fingerprints": dict(sorted(self.semantic_fingerprints.items())),
        }


def validate_complete_gold(
    listings: ListingGoldResult,
    quotes: QuoteGoldResult,
    dividends: DividendGoldResult,
    splits: SplitGoldResult,
) -> GoldValidationSummary:
    """Require every active child row to reference a retained listing Gold identity."""

    listing_keys = {listing.key for listing in listings.rows}
    _require_listing_references("eod_quotes", quotes.rows, listing_keys)
    _require_listing_references("dividends", dividends.rows, listing_keys)
    _require_listing_references("splits", splits.rows, listing_keys)

    return GoldValidationSummary(
        row_counts={
            "listings": listings.row_count,
            "eod_quotes": quotes.row_count,
            "dividends": dividends.row_count,
            "splits": splits.row_count,
        },
        semantic_fingerprints={
            "listings": listings.semantic_fingerprint,
            "eod_quotes": quotes.semantic_fingerprint,
            "dividends": dividends.semantic_fingerprint,
            "splits": splits.semantic_fingerprint,
        },
    )


def _require_listing_references(
    dataset: str,
    rows: Iterable[ChildRecord],
    listing_keys: set[ListingIdentity],
) -> None:
    orphan_keys = tuple(sorted(_identities(rows) - listing_keys))
    if orphan_keys:
        rendered = ", ".join("/".join(key) for key in orphan_keys)
        raise ValueError(f"Gold {dataset} contains orphan listing identities: {rendered}")


def _identities(rows: Iterable[ChildRecord]) -> set[ListingIdentity]:
    identities: set[ListingIdentity] = set()
    for row in rows:
        identities.add((row.isin, row.exchange, row.code))
    return identities
