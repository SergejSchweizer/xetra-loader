from __future__ import annotations

from pathlib import Path
from runpy import run_path

_BENCHMARK = run_path(
    str(Path(__file__).resolve().parents[2] / "scripts/quality/benchmark_postgres_features.py")
)


def test_explain_worker_counts_include_nested_plan_nodes() -> None:
    plan = {
        "Workers Planned": 5,
        "Workers Launched": 4,
        "Plans": [
            {"Workers Planned": 2, "Workers Launched": 1, "Plans": []},
        ],
    }

    assert _BENCHMARK["_worker_counts"](plan) == (7, 5)


def test_explain_json_decoder_accepts_postgres_json_payload() -> None:
    document = '[{"Plan":{"Node Type":"Gather","Workers Planned":5}}]'

    assert _BENCHMARK["_decode_plan"](document) == {
        "Node Type": "Gather",
        "Workers Planned": 5,
    }
