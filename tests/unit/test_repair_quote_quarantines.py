from datetime import date, datetime
from decimal import Decimal

import pytest

from xetra_loader.contracts.listings import ListingRecord
from xetra_loader.medallion.core import BronzePartition, MedallionLayout, write_parquet_rows
from xetra_loader.ops.repair_quote_quarantines import (
    QuarantinedQuoteKey,
    read_full_refresh_window,
    read_quarantined_quote_keys,
    recover_from_bronze,
)


def test_full_refresh_window_reads_utc_markers_and_returns_local_window(tmp_path) -> None:
    wrapper_log = tmp_path / "full-refresh.log"
    wrapper_log.write_text(
        "full_refresh_start_utc=2026-10-22T19:39:20Z\n"
        "full_refresh_end_utc=2026-10-22T20:39:20Z\n",
        encoding="utf-8",
    )

    start, end = read_full_refresh_window(wrapper_log)

    assert start.tzinfo is None
    assert end.tzinfo is None
    assert (end - start).total_seconds() == 3600


def test_quarantine_log_is_scoped_to_one_refresh_window_and_deduplicated(tmp_path) -> None:
    log_path = tmp_path / "xdl.log"
    log_path.write_text(
        "2026-10-22 21:39:19,999 WARNING xetra_loader.ingestion.quotes "
        "quarantined EODHD OHLC violation for ETF1.XETRA on 2017-05-05\n"
        "2026-10-22 21:39:20,000 WARNING xetra_loader.ingestion.quotes "
        "quarantined EODHD OHLC violation for ETF1.XETRA on 2017-05-05\n"
        "2026-10-22 21:39:20,001 WARNING xetra_loader.ingestion.quotes "
        "quarantined EODHD OHLC violation for ETF2.XETRA on 2017-05-06\n"
        "2026-10-22 22:00:00,001 WARNING xetra_loader.ingestion.quotes "
        "quarantined EODHD OHLC violation for ETF3.XETRA on 2017-05-07\n",
        encoding="utf-8",
    )

    keys = read_quarantined_quote_keys(
        log_path,
        started_local=datetime(2026, 10, 22, 21, 39, 20),
        finished_local=datetime(2026, 10, 22, 22, 0, 0),
    )

    assert keys == (
        QuarantinedQuoteKey("ETF1.XETRA", date(2017, 5, 5)),
        QuarantinedQuoteKey("ETF2.XETRA", date(2017, 5, 6)),
    )


def test_recovery_preserves_good_fields_and_nulls_only_invalid_close(tmp_path) -> None:
    layout = MedallionLayout(tmp_path)
    listing = ListingRecord(
        isin="DE0000000001",
        exchange="XETRA",
        code="ETF1",
        instrument_type="ETF",
    )
    trade_date = date(2017, 5, 5)
    path = layout.bronze_path(
        BronzePartition(
            dataset_type="eod_quotes",
            exchange="XETRA",
            instrument_type="ETF",
            symbol="ETF1",
            timeframe="1d",
            partition_date=trade_date,
        )
    )
    raw = {
        "date": "2017-05-05",
        "open": "95.62",
        "high": "95.62",
        "low": "95.62",
        "close": "95.2765",
        "adjusted_close": "95.2765",
        "volume": 123,
    }
    write_parquet_rows(
        path,
        [
            {
                "isin": listing.isin,
                "exchange": listing.exchange,
                "code": listing.code,
                "provider_payload": raw,
            }
        ],
    )

    records = recover_from_bronze(
        layout,
        (listing,),
        (QuarantinedQuoteKey("ETF1.XETRA", trade_date),),
    )

    assert len(records) == 1
    assert records[0].close is None
    assert records[0].adjusted_close == Decimal("95.2765")
    assert records[0].open == records[0].high == records[0].low == Decimal("95.62")
    assert records[0].volume == 123


def test_recovery_fails_closed_when_raw_partition_is_missing(tmp_path) -> None:
    layout = MedallionLayout(tmp_path)
    listing = ListingRecord("DE0000000001", "XETRA", "ETF1", instrument_type="ETF")

    with pytest.raises(ValueError, match="Bronze partition is missing"):
        recover_from_bronze(
            layout,
            (listing,),
            (QuarantinedQuoteKey("ETF1.XETRA", date(2017, 5, 5)),),
        )
