import re
from pathlib import Path

from xetra_loader.features.catalog import (
    BASE_LEVEL_COLUMNS,
    FORBIDDEN_LEVEL_COLUMNS,
    feature_columns,
)

VIEW_SQL = Path("sql/schema/004_xetra_features.sql").read_text(encoding="utf-8")
IDENTITY_COLUMNS = ("isin", "exchange", "code", "trade_date")


def test_schema_qa_has_exact_identity_and_feature_column_contract() -> None:
    expected = IDENTITY_COLUMNS + feature_columns()
    assert len(expected) == len(set(expected))
    projection_start = VIEW_SQL.index("SELECT\n    r.isin, r.exchange, r.code, r.trade_date,")
    projection_end = VIEW_SQL.index("FROM normalized AS r", projection_start)
    projection = VIEW_SQL[projection_start:projection_end].split("SELECT", 1)[1]
    expressions: list[str] = []
    start = depth = 0
    in_string = False
    for index, character in enumerate(projection):
        if character == "'":
            in_string = not in_string
        elif not in_string:
            if character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
            elif character == "," and depth == 0:
                expressions.append(projection[start:index].strip())
                start = index + 1
    expressions.append(projection[start:].strip())
    actual: list[str] = []
    for expression in expressions:
        alias = re.search(r"\bAS\s+([a-z_][a-z_0-9]*)\s*$", expression, re.IGNORECASE)
        actual.append(
            alias.group(1)
            if alias is not None
            else expression.rsplit(".", maxsplit=1)[-1].strip()
        )
    assert actual[:6] == [*IDENTITY_COLUMNS, *BASE_LEVEL_COLUMNS]
    assert len(actual) == len(set(actual))
    assert set(actual) == set(expected)


def test_schema_qa_allows_only_the_two_exposed_base_levels() -> None:
    assert BASE_LEVEL_COLUMNS == ("adjusted_close_level", "volume_level")
    for forbidden in FORBIDDEN_LEVEL_COLUMNS:
        assert re.search(rf"(?<![a-z_]){re.escape(forbidden)}(?![a-z_])", VIEW_SQL) is None


def test_schema_qa_scopes_raw_ohlc_to_approved_intraday_families() -> None:
    assert "range_1 AS range_1obs" in VIEW_SQL
    assert "intraday_1 AS intraday_1obs" in VIEW_SQL
    assert "gap_1 AS gap_1obs" in VIEW_SQL
    assert "AS stoch_k_5" in VIEW_SQL
    assert "AS donchian_breakout_up" in VIEW_SQL
    assert "adjusted_close_level" in VIEW_SQL
    assert "volume_level" in VIEW_SQL
    assert (
        'ALTER MATERIALIZED VIEW xetra_loader.xetra_features OWNER TO "xetra-data-loader"'
        in VIEW_SQL
    )


def test_schema_qa_has_repeatable_privilege_contract_and_catalog_version() -> None:
    assert "COMMENT ON MATERIALIZED VIEW xetra_loader.xetra_features" in VIEW_SQL
    assert "catalog version=4" in VIEW_SQL
    assert (
        'GRANT SELECT ON xetra_loader.xetra_features TO "xetra-data-loader", portfell_app'
        in VIEW_SQL
    )
    assert "REVOKE ALL ON xetra_loader.xetra_features FROM PUBLIC" in VIEW_SQL
    assert "GRANT EXECUTE ON FUNCTION xetra_loader.refresh_xetra_features()" in VIEW_SQL


def test_schema_qa_uses_validated_configurable_refresh_memory_policy() -> None:
    assert "work_mem = '1GB'" not in VIEW_SQL
    assert "xetra_loader.resolve_feature_work_mem()" in VIEW_SQL
    assert "16MB and 4GB" in VIEW_SQL
    assert "xetra_loader.compute_cores" in VIEW_SQL
    assert "max_parallel_workers_per_gather" in VIEW_SQL
