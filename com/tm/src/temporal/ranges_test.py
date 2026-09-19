"""Unit test định nghĩa date range (§3.3) với ngày cụ thể.

Engine và reference dùng chung `window_of`, nên property test không bắt được lỗi định nghĩa window —
test này là nơi duy nhất khoá định nghĩa đó.
"""

import sys

import pytest

from com.tm.src.temporal.model import DateRange, Kind, ModelError, parse_day
from com.tm.src.temporal.ranges import (
    ALL_DATE_RANGES,
    CustomRange,
    Window,
    custom_window,
    label_window,
    pv_tag_bitmaps,
    window_of,
)

DR = DateRange


def w(l, r):
    return Window(parse_day(l), parse_day(r))


@pytest.mark.parametrize(
    "ds,dr,want",
    [
        ("2026-09-15", DR.A1, w("2026-09-15", "2026-09-15")),
        ("2026-09-15", DR.A7, w("2026-09-09", "2026-09-15")),
        ("2026-09-15", DR.A15, w("2026-09-01", "2026-09-15")),
        ("2026-09-15", DR.A30, w("2026-08-17", "2026-09-15")),
        ("2026-09-15", DR.A60, w("2026-07-18", "2026-09-15")),
        ("2026-09-15", DR.A90, w("2026-06-18", "2026-09-15")),
        ("2026-09-15", DR.A120, w("2026-05-19", "2026-09-15")),
        ("2026-09-15", DR.A180, w("2026-03-20", "2026-09-15")),
        ("2026-09-15", DR.IN_MONTH, w("2026-09-01", "2026-09-15")),
        ("2026-09-01", DR.IN_MONTH, w("2026-09-01", "2026-09-01")),
        ("2026-09-15", DR.LAST_MONTH, w("2026-08-01", "2026-08-31")),
        ("2026-09-01", DR.LAST_MONTH, w("2026-08-01", "2026-08-31")),
        ("2026-09-30", DR.LAST_MONTH, w("2026-08-01", "2026-08-31")),
        ("2026-03-01", DR.LAST_MONTH, w("2026-02-01", "2026-02-28")),
        ("2028-03-31", DR.LAST_MONTH, w("2028-02-01", "2028-02-29")),  # năm nhuận
        ("2027-01-10", DR.LAST_MONTH, w("2026-12-01", "2026-12-31")),  # qua năm
        ("2026-05-31", DR.LAST_MONTH, w("2026-04-01", "2026-04-30")),
    ],
)
def test_window_of(ds, dr, want):
    assert window_of(dr, parse_day(ds)) == want


def test_always_active():
    ds = parse_day("2026-09-15")
    got = window_of(DR.ALWAYS_ACTIVE, ds)
    assert got.always_active and got.r == ds and got.l < parse_day("1971-01-01")
    assert got.clamp(parse_day("2026-01-01")) == Window(parse_day("2026-01-01"), ds, True)


def test_all_date_ranges_defined():
    ds = parse_day("2026-09-15")
    names = {DR.Name(v) for v in DR.values()} - {"DATE_RANGE_UNSPECIFIED"}
    assert {DR.Name(dr) for dr in ALL_DATE_RANGES} == names
    for dr in ALL_DATE_RANGES:
        assert window_of(dr, ds).r <= ds
    with pytest.raises(ModelError):
        window_of(DR.DATE_RANGE_UNSPECIFIED, ds)


def test_custom_window():
    ds = parse_day("2026-09-15")
    assert custom_window(CustomRange(ds - 399, ds), ds) == Window(ds - 399, ds)
    for bad in [CustomRange(ds - 400, ds), CustomRange(ds, ds + 1), CustomRange(ds, ds - 1)]:
        with pytest.raises(ModelError):
            custom_window(bad, ds)


@pytest.mark.parametrize("kind", [Kind.PARTIAL_VALUE, Kind.PARTIAL_VALUE_BY_TAG])
def test_label_window_rejects_value_kinds(kind):
    with pytest.raises(ModelError):
        label_window(kind, {}, {})


@pytest.mark.parametrize("kind", [Kind.MUTEX_EVENT, Kind.MUTEX_STATE, Kind.NOT_MUTEX_EVENT, Kind.NOT_MUTEX_STATE])
def test_pv_tag_bitmaps_rejects_label_kinds(kind):
    with pytest.raises(ModelError):
        pv_tag_bitmaps(kind, {}, {})


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
