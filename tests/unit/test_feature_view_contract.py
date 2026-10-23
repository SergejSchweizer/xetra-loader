import re
from pathlib import Path

from xetra_loader.features.catalog import (
    BASE_LEVEL_COLUMNS,
    FEATURE_SPECS,
    FORBIDDEN_LEVEL_COLUMNS,
    feature_columns,
)

VIEW_SQL = Path("sql/schema/004_xetra_features.sql").read_text(encoding="utf-8")


def test_catalog_matches_the_fixed_105_column_view_contract() -> None:
    identity_and_availability = ("isin", "exchange", "code", "trade_date", "available_at_utc")
    assert len(BASE_LEVEL_COLUMNS) == 2
    assert len(FEATURE_SPECS) == 98
    assert len(identity_and_availability + feature_columns()) == 105


def test_feature_view_is_materialized_and_has_all_catalog_columns() -> None:
    assert "CREATE MATERIALIZED VIEW xetra_loader.xetra_features_build AS" in VIEW_SQL
    assert "r.available_at_utc," in VIEW_SQL
    for column in feature_columns():
        assert column in VIEW_SQL


def test_final_sql_projection_exactly_matches_the_105_column_catalog() -> None:
    build_start = VIEW_SQL.index("CREATE MATERIALIZED VIEW xetra_loader.xetra_features_build AS")
    select_start = VIEW_SQL.index("\nSELECT\n", build_start) + len("\nSELECT\n")
    projection_end = VIEW_SQL.index("\nFROM normalized AS r", select_start)
    projection = VIEW_SQL[select_start:projection_end]

    expressions: list[str] = []
    start = 0
    depth = 0
    in_string = False
    for index, character in enumerate(projection):
        if character == "'":
            in_string = not in_string
        elif not in_string and character == "(":
            depth += 1
        elif not in_string and character == ")":
            depth -= 1
        elif not in_string and depth == 0 and character == ",":
            expressions.append(projection[start:index].strip())
            start = index + 1
    expressions.append(projection[start:].strip())

    names: list[str] = []
    for expression in expressions:
        alias = re.search(r"\bAS\s+([a-z_][a-z0-9_]*)\s*$", expression, re.IGNORECASE)
        if alias is not None:
            names.append(alias.group(1))
            continue
        column = re.search(r"\.([a-z_][a-z0-9_]*)\s*$", expression, re.IGNORECASE)
        assert column is not None, f"unrecognized output expression: {expression}"
        names.append(column.group(1))

    expected = (
        "isin",
        "exchange",
        "code",
        "trade_date",
        "available_at_utc",
        *feature_columns(),
    )
    assert len(names) == 105
    assert len(set(names)) == 105
    assert set(names) == set(expected)


def test_feature_view_does_not_expose_ohlc_level_columns() -> None:
    for forbidden in FORBIDDEN_LEVEL_COLUMNS:
        assert re.search(rf"(?<![a-z_]){re.escape(forbidden)}(?![a-z_])", VIEW_SQL) is None


def test_available_at_utc_is_projected_from_the_raw_quote_table() -> None:
    assert "q.available_at_utc," in VIEW_SQL
    assert "r.available_at_utc," in VIEW_SQL
    assert "EXTRACT(ISODOW FROM r.trade_date)::integer" not in VIEW_SQL


def test_feature_view_has_identity_index_and_runtime_refresh_function() -> None:
    assert "CREATE UNIQUE INDEX xetra_features_build_identity_idx" in VIEW_SQL
    assert "RENAME TO xetra_features_identity_idx" in VIEW_SQL
    assert "CREATE OR REPLACE FUNCTION xetra_loader.refresh_xetra_features()" in VIEW_SQL
    assert (
        'GRANT SELECT ON xetra_loader.xetra_features TO "xetra-data-loader", portfell_app'
        in VIEW_SQL
    )


def test_ema_arrays_are_expanded_behind_a_materialization_boundary() -> None:
    assert "ema_values AS MATERIALIZED (" in VIEW_SQL
    start = VIEW_SQL.index("ema_values AS MATERIALIZED (")
    end = VIEW_SQL.index("), ema_value_lags AS", start)
    expansion = VIEW_SQL[start:end]
    assert "FROM ema_arrays AS a" in expansion
    assert "CROSS JOIN LATERAL unnest(" in expansion


def test_market_breadth_uses_narrow_returns_before_wide_feature_materialization() -> None:
    assert "), normalized AS MATERIALIZED (" in VIEW_SQL
    start = VIEW_SQL.index("), daily_cross AS (")
    end = VIEW_SQL.index("), cross_rolling AS (", start)
    daily_cross = VIEW_SQL[start:end]
    assert "FROM returns" in daily_cross
    assert "FROM normalized" not in daily_cross


def test_feature_view_build_is_swapped_only_after_population_and_indexing() -> None:
    build = VIEW_SQL.index("CREATE MATERIALIZED VIEW xetra_loader.xetra_features_build AS")
    index = VIEW_SQL.index("CREATE UNIQUE INDEX xetra_features_build_identity_idx")
    old_view_drop = VIEW_SQL.index("DROP MATERIALIZED VIEW IF EXISTS xetra_loader.xetra_features;")
    rename = VIEW_SQL.index(
        "ALTER MATERIALIZED VIEW xetra_loader.xetra_features_build RENAME TO xetra_features;"
    )
    assert build < index < old_view_drop < rename
