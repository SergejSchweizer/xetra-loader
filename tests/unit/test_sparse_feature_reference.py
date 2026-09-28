from datetime import date
from math import log

from tests.qa.feature_reference import make_sparse_fixture, reference_features


def test_sparse_reference_keys_and_cross_section_are_date_aligned() -> None:
    rows = make_sparse_fixture()
    features = reference_features(rows)

    assert ("BBB", date(2026, 1, 2)) not in features
    assert features[("AAA", date(2026, 1, 3))]["breadth_daily"] == 1.0

    bbb_after_gap = next(
        row for row in rows if row.code == "BBB" and row.trade_date == date(2026, 1, 23)
    )
    bbb_previous = next(
        row for row in rows if row.code == "BBB" and row.trade_date == date(2026, 1, 21)
    )
    assert features[("BBB", bbb_after_gap.trade_date)]["adjusted_close_log_return_1obs"] == log(
        bbb_after_gap.adjusted_close / bbb_previous.adjusted_close
    )

    aaa_return = features[("AAA", date(2026, 1, 22))][
        "adjusted_close_log_return_1obs"
    ]
    assert features[("AAA", date(2026, 1, 22))]["breadth_daily"] == (
        1.0 if aaa_return is not None and aaa_return > 0 else 0.0
    )
