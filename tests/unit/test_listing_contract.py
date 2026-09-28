from xetra_loader.contracts.listings import (
    deserialize_listings,
    normalize_listings,
    serialize_listings,
)


def test_only_missing_or_empty_isin_is_excluded() -> None:
    rows = [
        {"ISIN": " de0000000001 ", "Exchange": "XETRA", "Code": "AAA", "Type": "ETF"},
        {"ISIN": "", "Exchange": "XETRA", "Code": "EMPTY"},
        {"ISIN": None, "Exchange": "XETRA", "Code": "NONE"},
        {"ISIN": "DE0000000002", "Exchange": "XETRA", "Code": "STOCK", "Type": "Common Stock"},
    ]
    records = normalize_listings(rows)
    assert [record.code for record in records] == ["AAA", "STOCK"]
    assert records[0].isin == "DE0000000001"


def test_provider_title_case_isin_is_normalized() -> None:
    records = normalize_listings(
        [{"Isin": " de0000000003 ", "Exchange": "XETRA", "Code": "TITLECASE"}]
    )
    assert [record.key for record in records] == [("DE0000000003", "XETRA", "TITLECASE")]


def test_duplicate_isin_with_distinct_code_is_retained() -> None:
    rows = [
        {"ISIN": "DE0000000001", "Exchange": "XETRA", "Code": "BBB"},
        {"ISIN": "DE0000000001", "Exchange": "XETRA", "Code": "AAA"},
    ]
    records = normalize_listings(rows)
    assert [record.key for record in records] == [
        ("DE0000000001", "XETRA", "AAA"),
        ("DE0000000001", "XETRA", "BBB"),
    ]


def test_conflicting_duplicate_identity_fails_closed() -> None:
    rows = [
        {"ISIN": "DE0000000001", "Exchange": "XETRA", "Code": "AAA", "Name": "One"},
        {"ISIN": "DE0000000001", "Exchange": "XETRA", "Code": "AAA", "Name": "Two"},
    ]

    try:
        normalize_listings(rows)
    except ValueError as exc:
        assert "conflicting listing rows" in str(exc)
    else:
        raise AssertionError("conflicting duplicate identity was accepted")


def test_exact_duplicate_identity_is_provider_deduplicated() -> None:
    row = {"ISIN": "DE0000000001", "Exchange": "XETRA", "Code": "AAA"}
    assert normalize_listings([row, row]) == normalize_listings([row])


def test_round_trip_and_order_are_deterministic() -> None:
    rows = [
        {"ISIN": "DE0000000002", "Exchange": "XETRA", "Code": "BBB", "Name": "B"},
        {"ISIN": "DE0000000001", "Exchange": "XETRA", "Code": "AAA", "Name": "A"},
    ]
    records = normalize_listings(rows)
    payload = serialize_listings(reversed(records))
    assert deserialize_listings(payload) == records
    assert serialize_listings(deserialize_listings(payload)) == payload


def test_listing_lifecycle_round_trips_as_semantic_contract() -> None:
    records = normalize_listings(
        [{"ISIN": "DE0000000001", "Exchange": "XETRA", "Code": "OLD"}],
        is_active=False,
    )

    assert records[0].semantic_dict()["is_active"] is False
    assert deserialize_listings(serialize_listings(records)) == records
