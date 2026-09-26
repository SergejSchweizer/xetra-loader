import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from xetra_loader.features.catalog import FEATURE_SPECS
from xetra_loader.features.standardization import (
    FeatureFold,
    FeatureRow,
    standardize_train_fold,
)

FEATURES = tuple(spec.name for spec in FEATURE_SPECS)


def _rows(values: tuple[float, ...]) -> tuple[FeatureRow, ...]:
    return tuple(
        FeatureRow(
            key=("DE0000000001", "XETRA", "AAA", date(2026, 1, 1) + timedelta(days=index)),
            values={name: value + offset for offset, name in enumerate(FEATURES)},
        )
        for index, value in enumerate(values)
    )


def _fold() -> FeatureFold:
    keys = tuple(row.key for row in _rows((1.0, 3.0, 9.0, 11.0)))
    return FeatureFold(
        train=frozenset(keys[:2]),
        validation=frozenset(keys[2:3]),
        test=frozenset(keys[3:]),
    )


def test_fit_is_train_only_and_all_derived_features_are_supported() -> None:
    rows = _rows((1.0, 3.0, 9.0, 11.0))
    changed_non_train = _rows((1.0, 3.0, 900.0, 1100.0))
    first = standardize_train_fold(rows, _fold())
    second = standardize_train_fold(changed_non_train, _fold())

    assert first.features == FEATURES
    assert first.source_fingerprint != second.source_fingerprint
    assert first.statistics == second.statistics
    assert first.statistics[FEATURES[0]].mean == 2.0
    assert first.statistics[FEATURES[0]].standard_deviation == 1.0
    assert first.rows[2].values[FEATURES[0]] == 7.0
    assert second.rows[2].values[FEATURES[0]] == 898.0


def test_training_change_changes_applied_result_deterministically() -> None:
    fold = _fold()
    baseline = standardize_train_fold(_rows((1.0, 3.0, 9.0, 11.0)), fold)
    changed = standardize_train_fold(_rows((1.0, 5.0, 9.0, 11.0)), fold)

    assert changed.statistics[FEATURES[0]].mean == 3.0
    assert changed.rows[2].values[FEATURES[0]] == 3.0
    assert baseline.rows[2].values[FEATURES[0]] != changed.rows[2].values[FEATURES[0]]


def test_zero_variance_is_reported_and_standardizes_to_null() -> None:
    rows = _rows((4.0, 4.0, 9.0, 11.0))
    result = standardize_train_fold(rows, _fold(), features=(FEATURES[0],))

    statistics = result.statistics[FEATURES[0]]
    assert statistics.zero_variance
    assert statistics.standard_deviation == 0.0
    assert all(row.values[FEATURES[0]] is None for row in result.rows)


def test_fold_rejects_overlap_and_unassigned_rows() -> None:
    rows = _rows((1.0, 3.0, 9.0, 11.0))
    keys = tuple(row.key for row in rows)
    with pytest.raises(ValueError, match="disjoint"):
        FeatureFold(
            train=frozenset(keys[:2]),
            validation=frozenset(keys[1:3]),
            test=frozenset(keys[3:]),
        )
    with pytest.raises(ValueError, match="exactly match"):
        standardize_train_fold(rows[:3], _fold())


def test_sanitized_standardization_report_is_pass() -> None:
    report = json.loads(
        Path("artifacts/acceptance/xdl-pr069-train-fold-standardization.json").read_text(
            encoding="utf-8"
        )
    )

    assert report["status"] == "PASS"
    assert report["derived_features"] == len(FEATURES)
    assert report["global_view_fit_statistics"] is False
    assert report["secrets_included"] is False
