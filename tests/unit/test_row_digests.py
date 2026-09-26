from datetime import date

import pytest

from xetra_loader.sync.row_digests import (
    canonical_entity_key,
    digest_map,
    row_digests,
    row_sha256,
)


def _quote(close: str = "10.0", **metadata: str) -> dict[str, str]:
    return {
        "isin": "DE0000000001",
        "exchange": "XETRA",
        "code": "AAA",
        "trade_date": date(2026, 8, 22).isoformat(),
        "close": close,
        **metadata,
    }


def test_entity_key_is_canonical_json_not_delimiter_text() -> None:
    key = canonical_entity_key("eod_quotes", _quote())
    assert key == '["DE0000000001","XETRA","AAA","2026-08-22"]'


def test_metadata_does_not_change_row_digest() -> None:
    first = row_sha256("eod_quotes", _quote(fetched_at_utc="a"))
    second = row_sha256("eod_quotes", _quote(published_at_utc="b", run_id="c"))
    assert first == second


def test_semantic_change_changes_only_one_digest() -> None:
    rows = [_quote(), {**_quote(), "trade_date": date(2026, 8, 23).isoformat()}]
    changed = [{**rows[0], "close": "10.5"}, rows[1]]
    before = {digest.entity_key: digest.row_sha256 for digest in row_digests("eod_quotes", rows)}
    after = {digest.entity_key: digest.row_sha256 for digest in row_digests("eod_quotes", changed)}
    assert before.keys() == after.keys()
    assert sum(before[key] != after[key] for key in before) == 1


def test_duplicate_identity_and_missing_identity_fail_closed() -> None:
    with pytest.raises(ValueError, match="duplicate digest entity key"):
        row_digests("eod_quotes", [_quote(), _quote()])
    with pytest.raises(ValueError, match="missing digest key field"):
        canonical_entity_key("eod_quotes", {"isin": "DE0000000001"})


def test_digest_map_rejects_duplicate_persisted_state() -> None:
    digests = row_digests("listings", [{"isin": "DE1", "exchange": "XETRA", "code": "AAA"}])
    assert digest_map(digests) == {digests[0].entity_key: digests[0].row_sha256}
    with pytest.raises(ValueError, match="duplicate persisted digest key"):
        digest_map((digests[0], digests[0]))
