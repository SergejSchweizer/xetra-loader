from datetime import date
from pathlib import Path

import pytest

from xetra_loader.medallion import Layer, Manifest, MedallionLayout
from xetra_loader.medallion.core import (
    BronzePartition,
    GoldArtifact,
    SilverPartition,
    read_dataset_rows,
    read_legacy_dataset_rows,
    read_manifest_payload,
)


def test_layout_resolves_each_layer() -> None:
    layout = MedallionLayout(Path("/data/xetra"))
    assert layout.dataset_path(Layer.BRONZE, "listings") == Path("/data/xetra/bronze/listings")
    assert layout.dataset_path(Layer.SILVER, "quotes") == Path("/data/xetra/silver/quotes")
    assert layout.manifest_path(Layer.GOLD, "splits") == Path(
        "/data/xetra/gold/_metadata/dataset_type=splits/manifest.parquet"
    )


def test_layout_resolves_required_partition_paths() -> None:
    layout = MedallionLayout(Path("/data/xetra"))
    assert layout.bronze_path(
        BronzePartition("eod_quotes", "XETRA", "etf", "AAA", "1d", date(2026, 8, 21))
    ) == Path(
        "/data/xetra/bronze/dataset_type=eod_quotes/exchange=XETRA/"
        "instrument_type=etf/symbol=AAA/timeframe=1d/year=2026/month=2026-08/"
        "data.parquet"
    )
    assert layout.silver_path(SilverPartition("eod_quotes", "XETRA", "AAA", "1d", 2026)) == Path(
        "/data/xetra/silver/dataset_type=eod_quotes/exchange=XETRA/symbol=AAA/"
        "timeframe=1d/year=2026/AAA-2026.parquet"
    )
    assert layout.gold_path(
        GoldArtifact(
            "xetra-eod-quotes-gold-v1",
            "XETRA",
            "AAA",
            "a" * 64,
            "b" * 64,
        )
    ) == Path(
        "/data/xetra/gold/dataset_id=xetra-eod-quotes-gold-v1/"
        "dataset_type=gold_symbol_dataset/exchange=XETRA/symbol=AAA/AAA_GOLD_"
        + "a" * 64
        + "_"
        + "b" * 64
        + ".parquet"
    )


@pytest.mark.parametrize("dataset", ["", ".", "..", "../quotes", "nested/quotes", "nested\\quotes"])
def test_layout_rejects_invalid_dataset_path(dataset: str) -> None:
    with pytest.raises(ValueError):
        MedallionLayout(Path("/data/xetra")).dataset_path(Layer.GOLD, dataset)


def test_runtime_reader_rejects_legacy_json_and_migration_reader_can_read_it(
    tmp_path: Path,
) -> None:
    layout = MedallionLayout(tmp_path)
    legacy_file = layout.dataset_path(Layer.SILVER, "eod_quotes") / "data.json"
    legacy_file.parent.mkdir(parents=True)
    legacy_file.write_text('[{"code":"AAA"}]', encoding="utf-8")

    assert read_legacy_dataset_rows(layout, Layer.SILVER, "eod_quotes") == [{"code": "AAA"}]
    with pytest.raises(ValueError, match="requires migration"):
        read_dataset_rows(layout, Layer.SILVER, "eod_quotes")


def test_manifest_reader_rejects_legacy_json_metadata(tmp_path: Path) -> None:
    layout = MedallionLayout(tmp_path)
    legacy_path = layout.legacy_metadata_manifest_path(Layer.GOLD, "quotes")
    legacy_path.parent.mkdir(parents=True)
    legacy_path.write_text(
        '{"dataset":"quotes","layer":"gold","semantic_metadata":{},'
        '"run_metadata":{"artifact_paths":[]}}',
        encoding="utf-8",
    )

    with pytest.raises(FileNotFoundError, match="manifest.parquet"):
        read_manifest_payload(layout.manifest_path(Layer.GOLD, "quotes"))


def test_semantic_fingerprint_ignores_run_metadata() -> None:
    first = Manifest(
        dataset="quotes",
        layer=Layer.GOLD,
        semantic_metadata={"rows": 10, "max_trade_date": "2026-08-21"},
        run_metadata={"run_id": "a", "fetched_at": "2026-08-22T10:00:00Z"},
    )
    second = Manifest(
        dataset="quotes",
        layer=Layer.GOLD,
        semantic_metadata={"max_trade_date": "2026-08-21", "rows": 10},
        run_metadata={"run_id": "b", "fetched_at": "2026-08-22T11:00:00Z"},
    )
    assert first.semantic_fingerprint() == second.semantic_fingerprint()
    assert first.to_json() != second.to_json()


def test_semantic_change_changes_fingerprint() -> None:
    base = Manifest("listings", Layer.GOLD, {"rows": 2}, {"run_id": "a"})
    changed = Manifest("listings", Layer.GOLD, {"rows": 3}, {"run_id": "a"})
    assert base.semantic_fingerprint() != changed.semantic_fingerprint()
