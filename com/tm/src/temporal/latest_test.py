"""Unit test LATEST / POS / STATE + checkpoint tuần (§4.3)."""

import sys

import pytest
from pyroaring import BitMap

from com.tm.src.temporal.latest import (
    KEEP_DAYS,
    LatestStore,
    MissingStateError,
    fold_mutex,
    fold_not_mutex,
    fold_state,
    fold_for,
    is_checkpoint_day,
)
from com.tm.src.temporal.model import ANY_TAG, Kind, LabelDaily, StateDaily, parse_day, to_date

A, B = 1, 2


def bm(*xs):
    return BitMap(xs)


def test_checkpoint_day_is_sunday():
    assert is_checkpoint_day(parse_day("2026-09-06"))
    assert is_checkpoint_day(parse_day("2026-09-13"))
    assert not is_checkpoint_day(parse_day("2026-09-15"))


def test_fold_mutex_new_add_clears_other_tags():
    prev = {A: bm(1, 2), B: bm(3)}
    daily = LabelDaily(add={B: bm(1), ANY_TAG: bm(1)}, dele={B: bm(3)})
    assert fold_mutex(prev, daily) == {A: bm(2), B: bm(1)}


def test_fold_mutex_add_then_remove_same_day_drops_user():
    prev = {A: bm(1)}
    daily = LabelDaily(add={B: bm(1), ANY_TAG: bm(1)}, dele={B: bm(1)})
    assert fold_mutex(prev, daily) == {}  # không quay lại A


def test_fold_not_mutex_is_per_tag():
    prev = {A: bm(1), B: bm(1)}
    daily = LabelDaily(add={A: bm(2)}, dele={B: bm(1)})
    assert fold_not_mutex(prev, daily) == {A: bm(1, 2)}


def test_fold_state():
    prev = {A: bm(1, 2)}
    daily = StateDaily(added={B: bm(1)}, removed={A: bm(1)})
    assert fold_state(prev, daily) == {A: bm(2), B: bm(1)}


@pytest.mark.parametrize("kind", [Kind.PARTIAL_VALUE, Kind.PARTIAL_VALUE_BY_TAG])
def test_fold_for_rejects_value_kinds(kind):
    with pytest.raises(ValueError):
        fold_for(kind)


def test_state_at_uses_checkpoint_and_short_forward_fold():
    start = parse_day("2026-06-01")
    ds = parse_day("2026-09-15")
    # user = ngày trong tháng; mỗi ngày STATE thêm đúng 1 user → STATE(d) kiểm được trực tiếp
    dailies = {d: StateDaily(added={A: bm(d - start + 1)}) for d in range(start, ds + 1)}
    calls = []

    def daily_of(d):
        calls.append(d)
        return dailies[d]

    store = LatestStore(Kind.MUTEX_STATE, daily_of)
    for d in range(start, ds + 1):
        store.fold(d)
        if is_checkpoint_day(d):
            store.checkpoint(d)

    for r in range(start - 3, ds + 1):
        calls.clear()
        got = store.state_at(r)
        want = {A: BitMap(range(1, r - start + 2))} if r >= start else {}
        assert got == want, to_date(r)
        # checkpoint Chủ nhật gần nhất + forward-fold ≤ 6 ngày
        assert len(calls) <= 6, (to_date(r), len(calls))
    assert store.state_at(ds - KEEP_DAYS + 1) is not None


def test_state_at_errors():
    store = LatestStore(Kind.MUTEX_EVENT, lambda d: LabelDaily())
    start = parse_day("2026-09-01")  # thứ Ba
    for d in range(start, start + 20):
        store.fold(d)  # không checkpoint
    with pytest.raises(MissingStateError):
        store.state_at(start + 20)  # chưa fold
    with pytest.raises(MissingStateError):
        store.state_at(start + 9)  # cần checkpoint Chủ nhật 09-06 nhưng không có
    with pytest.raises(MissingStateError):
        store.fold(start + 25)  # nhảy cóc
    with pytest.raises(ValueError):
        store.checkpoint(start)  # không phải Chủ nhật


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
