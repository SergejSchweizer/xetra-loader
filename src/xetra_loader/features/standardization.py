"""Leakage-safe standardization of PostgreSQL feature-view rows by train fold."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final

from xetra_loader.features.catalog import FEATURE_SPECS

type FeatureKey = tuple[str, str, str, date]
type FeatureValue = float | None

DERIVED_FEATURE_COLUMNS: Final[tuple[str, ...]] = tuple(
    spec.name for spec in FEATURE_SPECS
)


@dataclass(frozen=True, slots=True)
class FeatureRow:
    """One row read from ``xetra_loader.xetra_features``."""

    key: FeatureKey
    values: Mapping[str, FeatureValue]


@dataclass(frozen=True, slots=True)
class FeatureFold:
    """Explicit, disjoint train/validation/test row membership."""

    train: frozenset[FeatureKey]
    validation: frozenset[FeatureKey]
    test: frozenset[FeatureKey]

    def __post_init__(self) -> None:
        partitions = (self.train, self.validation, self.test)
        if not self.train:
            raise ValueError("feature fold must contain at least one training row")
        if any(
            left & right
            for index, left in enumerate(partitions)
            for right in partitions[index + 1 :]
        ):
            raise ValueError("feature fold partitions must be disjoint")

    @property
    def all_keys(self) -> frozenset[FeatureKey]:
        """Return the complete explicitly assigned row set."""

        return self.train | self.validation | self.test

    def split_for(self, key: FeatureKey) -> str:
        """Return the declared partition for one row key."""

        if key in self.train:
            return "train"
        if key in self.validation:
            return "validation"
        if key in self.test:
            return "test"
        raise ValueError(f"row {key!r} is outside the explicit feature fold")


@dataclass(frozen=True, slots=True)
class FeatureStatistics:
    """Training-only fit statistics for one derived feature."""

    mean: float | None
    standard_deviation: float | None
    training_count: int
    zero_variance: bool


@dataclass(frozen=True, slots=True)
class StandardizedFeatureRow:
    """One fold-labelled row after applying training-only statistics."""

    key: FeatureKey
    split: str
    values: Mapping[str, FeatureValue]


@dataclass(frozen=True, slots=True)
class FoldStandardizationResult:
    """Deterministic fold result; statistics never belong to the global view."""

    source_fingerprint: str
    features: tuple[str, ...]
    statistics: Mapping[str, FeatureStatistics]
    rows: tuple[StandardizedFeatureRow, ...]


def standardize_train_fold(
    rows: Iterable[FeatureRow],
    fold: FeatureFold,
    *,
    features: Sequence[str] = DERIVED_FEATURE_COLUMNS,
) -> FoldStandardizationResult:
    """Fit on train rows only and apply unchanged statistics to every partition.

    The input rows must exactly equal the explicit fold membership.  This makes
    an accidental global fit or an unassigned future row a hard error.  The
    population standard deviation is used; a zero-variance feature is reported
    and yields ``None`` for every standardized value.
    """

    selected = _validate_features(features)
    materialized = tuple(sorted(rows, key=lambda row: row.key))
    keys = {row.key for row in materialized}
    if len(keys) != len(materialized):
        raise ValueError("feature rows must have unique keys")
    if keys != fold.all_keys:
        raise ValueError("feature rows must exactly match explicit fold membership")

    by_key = {row.key: row for row in materialized}
    statistics = {
        name: _fit_feature(name, (by_key[key] for key in sorted(fold.train)))
        for name in selected
    }
    standardized = tuple(
        StandardizedFeatureRow(
            key=row.key,
            split=fold.split_for(row.key),
            values={name: _apply(statistics[name], row.values[name]) for name in selected},
        )
        for row in materialized
    )
    return FoldStandardizationResult(
        source_fingerprint=_source_fingerprint(materialized, selected),
        features=selected,
        statistics=statistics,
        rows=standardized,
    )


def _validate_features(features: Sequence[str]) -> tuple[str, ...]:
    selected = tuple(features)
    if not selected or len(set(selected)) != len(selected):
        raise ValueError("standardization features must be non-empty and unique")
    unsupported = set(selected) - set(DERIVED_FEATURE_COLUMNS)
    if unsupported:
        raise ValueError(f"unsupported standardization features: {sorted(unsupported)}")
    return selected


def _fit_feature(
    name: str,
    rows: Iterable[FeatureRow],
) -> FeatureStatistics:
    values = [
        _finite_value(row.values[name], name)
        for row in rows
        if row.values[name] is not None
    ]
    if not values:
        return FeatureStatistics(None, None, 0, False)
    mean = math.fsum(values) / len(values)
    variance = math.fsum((value - mean) ** 2 for value in values) / len(values)
    standard_deviation = math.sqrt(variance)
    return FeatureStatistics(
        mean=mean,
        standard_deviation=standard_deviation,
        training_count=len(values),
        zero_variance=standard_deviation == 0.0,
    )


def _apply(statistics: FeatureStatistics, value: FeatureValue) -> FeatureValue:
    if value is None or statistics.mean is None or statistics.standard_deviation in (None, 0.0):
        return None
    return (
        _finite_value(value, "standardization") - statistics.mean
    ) / statistics.standard_deviation


def _finite_value(value: FeatureValue, name: str) -> float:
    if value is None:
        raise ValueError(f"training feature {name!r} contains NULL")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"training feature {name!r} is not finite")
    return numeric


def _source_fingerprint(rows: Sequence[FeatureRow], features: Sequence[str]) -> str:
    payload = [
        {
            "key": [row.key[0], row.key[1], row.key[2], row.key[3].isoformat()],
            "values": {name: row.values[name] for name in features},
        }
        for row in rows
    ]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
