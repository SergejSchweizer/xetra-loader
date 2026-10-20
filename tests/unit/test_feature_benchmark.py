from polars.testing import assert_frame_equal
from scripts.quality.benchmark_features import (
    compute_global_plan,
    compute_per_instrument_plans,
    input_frame,
    validate_reference,
)


def test_benchmark_implementations_are_equivalent() -> None:
    frame = input_frame(rows=800, instruments=8)

    assert_frame_equal(
        compute_global_plan(frame),
        compute_per_instrument_plans(frame),
        check_exact=False,
        rel_tol=1e-12,
        abs_tol=1e-12,
    )


def test_benchmark_formulas_match_independent_reference() -> None:
    validate_reference()
