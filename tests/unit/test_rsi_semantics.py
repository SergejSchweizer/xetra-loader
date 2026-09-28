from pathlib import Path

from tests.qa.feature_reference import _wilder_rsi


def test_monotone_and_flat_rsi_edges() -> None:
    assert _wilder_rsi([1.0, 2.0, 3.0, 4.0], 3, 3) == 100.0
    assert _wilder_rsi([4.0, 3.0, 2.0, 1.0], 3, 3) == 0.0
    assert _wilder_rsi([2.0, 2.0, 2.0, 2.0], 3, 3) is None


def test_invalid_transition_resets_until_a_fresh_period_is_available() -> None:
    values = [10.0, 11.0, 12.0, None, 13.0, 14.0, 15.0, 16.0]
    assert _wilder_rsi(values, 3, 3) is None
    assert _wilder_rsi(values, 4, 3) is None
    assert _wilder_rsi(values, 6, 3) is None
    assert _wilder_rsi(values, 7, 3) == 100.0


def test_zero_or_negative_prices_are_invalid_transitions() -> None:
    values = [10.0, 11.0, 0.0, 12.0, 13.0, 14.0, 15.0]
    assert _wilder_rsi(values, 4, 3) is None
    assert _wilder_rsi(values, 6, 3) == 100.0


def test_sql_preserves_invalid_gain_loss_values_and_flat_null_rule() -> None:
    sql = Path("sql/schema/004_xetra_features.sql").read_text(encoding="utf-8")
    assert "array_agg(r.gain ORDER BY r.observation_number)" in sql
    assert "array_agg(coalesce(r.gain" not in sql
    assert "WHEN average_gain = 0 AND average_loss = 0 THEN NULL" in sql
    assert "WHEN average_gain = 0 THEN 0.0" in sql
