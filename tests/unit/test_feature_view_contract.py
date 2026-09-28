import re
from pathlib import Path

from xetra_loader.features.catalog import (
    FORBIDDEN_LEVEL_COLUMNS,
    feature_columns,
)

VIEW_SQL = Path("sql/schema/004_xetra_features.sql").read_text(encoding="utf-8")


def test_feature_view_is_materialized_and_has_all_catalog_columns() -> None:
    assert "CREATE MATERIALIZED VIEW xetra_loader.xetra_features AS" in VIEW_SQL
    for column in feature_columns():
        assert column in VIEW_SQL


def test_feature_view_does_not_expose_ohlc_level_columns() -> None:
    for forbidden in FORBIDDEN_LEVEL_COLUMNS:
        assert re.search(rf"(?<![a-z_]){re.escape(forbidden)}(?![a-z_])", VIEW_SQL) is None


def test_feature_view_has_identity_index_and_runtime_refresh_function() -> None:
    assert "CREATE UNIQUE INDEX xetra_features_identity_idx" in VIEW_SQL
    assert "CREATE OR REPLACE FUNCTION xetra_loader.refresh_xetra_features()" in VIEW_SQL
    assert (
        'GRANT SELECT ON xetra_loader.xetra_features TO '
        '"xetra-data-loader", portfell_app, xetra_loader'
        in VIEW_SQL
    )
