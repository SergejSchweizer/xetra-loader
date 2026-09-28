from collections.abc import Mapping
from datetime import date

from xetra_loader.contracts.corporate_actions import ActionStatus
from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.ingestion.dividends import JSONValue, ingest_dividends


class FixtureTransport:
    def __init__(self, payload: JSONValue) -> None:
        self.payload = payload
        self.calls: list[tuple[str, Mapping[str, str | int | float] | None]] = []

    def get_json(
        self,
        path: str,
        params: Mapping[str, str | int | float] | None = None,
    ) -> JSONValue:
        self.calls.append((path, params))
        return self.payload


def _listing() -> ListingRecord:
    return ListingRecord("DE0000000001", "XETRA", "AAA")


def _payload(value: float = 1.25) -> JSONValue:
    return [
        {
            "date": "2026-08-20",
            "value": value,
            "currency": "EUR",
            "period": "Annual",
            "declarationDate": "2026-07-01",
            "recordDate": "2026-08-19",
            "paymentDate": "2026-08-25",
        }
    ]


def _same_date_payload(first: float = 1.25, second: float = 2.5) -> JSONValue:
    return [
        {
            "date": "2026-08-20",
            "value": first,
            "currency": "EUR",
            "period": "Annual",
        },
        {
            "date": "2026-08-20",
            "value": second,
            "currency": "EUR",
            "period": "Special",
        },
    ]


def _dated_payload() -> JSONValue:
    return [
        {"date": "2026-08-10", "value": 0.75, "currency": "EUR"},
        {"date": "2026-08-20", "value": 1.25, "currency": "EUR"},
    ]


def test_full_history_and_overlap_requests() -> None:
    full_transport = FixtureTransport(_payload())
    ingest_dividends(full_transport, _listing())
    assert full_transport.calls == [("div/AAA.XETRA", None)]

    overlap_transport = FixtureTransport(_payload())
    ingest_dividends(overlap_transport, _listing(), last_event_date=date(2026, 8, 22))
    assert overlap_transport.calls == [("div/AAA.XETRA", {"from": "2026-08-15"})]


def test_unchanged_replay_is_stable() -> None:
    first = ingest_dividends(FixtureTransport(_payload()), _listing())
    replay = ingest_dividends(
        FixtureTransport(_payload()),
        _listing(),
        previous_records=first.silver_records,
    )
    assert replay.correction_count == 0
    assert replay.retraction_count == 0
    assert replay.silver_records == first.silver_records
    assert replay.bronze_payload == first.bronze_payload


def test_correction_is_reconciled_once() -> None:
    first = ingest_dividends(FixtureTransport(_payload(1.25)), _listing())
    corrected = ingest_dividends(
        FixtureTransport(_payload(1.30)),
        _listing(),
        previous_records=first.silver_records,
    )
    assert corrected.correction_count == 1
    assert corrected.retraction_count == 0
    assert len(corrected.silver_records) == 2
    assert sum(record.status is ActionStatus.RETRACTED for record in corrected.silver_records) == 1


def test_removed_overlap_event_is_retracted() -> None:
    first = ingest_dividends(FixtureTransport(_payload()), _listing())
    removed = ingest_dividends(
        FixtureTransport([]),
        _listing(),
        previous_records=first.silver_records,
    )
    assert removed.correction_count == 0
    assert removed.retraction_count == 1
    assert len(removed.silver_records) == 1
    assert removed.silver_records[0].status is ActionStatus.RETRACTED


def test_same_date_events_reconcile_as_a_content_addressed_set() -> None:
    first = ingest_dividends(FixtureTransport(_same_date_payload()), _listing())
    replay = ingest_dividends(
        FixtureTransport(list(reversed(_same_date_payload()))),
        _listing(),
        previous_records=first.silver_records,
    )
    corrected = ingest_dividends(
        FixtureTransport(_same_date_payload(first=1.30)),
        _listing(),
        previous_records=first.silver_records,
    )
    removed = ingest_dividends(
        FixtureTransport(_same_date_payload()[:1]),
        _listing(),
        previous_records=first.silver_records,
    )

    assert replay.silver_records == first.silver_records
    assert corrected.correction_count == 1
    assert corrected.retraction_count == 0
    assert sum(event.status is ActionStatus.RETRACTED for event in corrected.silver_records) == 1
    assert removed.correction_count == 0
    assert removed.retraction_count == 1
    assert sum(event.status is ActionStatus.RETRACTED for event in removed.silver_records) == 1


def test_bounded_response_preserves_history_before_overlap_boundary() -> None:
    first = ingest_dividends(FixtureTransport(_dated_payload()), _listing())
    bounded = ingest_dividends(
        FixtureTransport(_payload()),
        _listing(),
        last_event_date=date(2026, 8, 22),
        previous_records=first.silver_records,
    )

    assert {event.event_date for event in bounded.silver_records} == {
        date(2026, 8, 10),
        date(2026, 8, 20),
    }
    assert bounded.removed_count == 1
    assert bounded.correction_count == 1
    assert bounded.retraction_count == 0
    assert all(
        event.status is ActionStatus.ACTIVE
        for event in bounded.silver_records
        if event.event_date == date(2026, 8, 10)
    )


def test_unrelated_add_and_remove_are_not_counted_as_one_correction() -> None:
    first = ingest_dividends(FixtureTransport(_dated_payload()), _listing())
    current = FixtureTransport(
        [{"date": "2026-08-21", "value": 2.0, "currency": "EUR"}]
    )
    result = ingest_dividends(
        current,
        _listing(),
        previous_records=first.silver_records,
    )

    assert result.added_count == 1
    assert result.removed_count == 2
    assert result.change_set_count == 3
    assert result.correction_count == 0
    assert result.retraction_count == 2
