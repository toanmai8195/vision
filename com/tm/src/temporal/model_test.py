"""Unit test model: ds theo ICT, kind, value range, reduce trong ngày (§4.1), validate input."""

import sys
from decimal import Decimal

import pytest
from pyroaring import BitMap

from com.tm.src.temporal.model import (
    ANY_TAG,
    AggFunc,
    AttributeSpec,
    AttributeType,
    DataType,
    DateRange,
    FeedMode,
    Kind,
    ModelError,
    StateInterval,
    TagDict,
    TagEvent,
    ValueEvent,
    ValueRange,
    ds_of_ts_ms,
    kind_of,
    merge_values,
    parse_day,
    parse_ts_ms,
    reduce_mutex_day,
    reduce_not_mutex_day,
    reduce_pv_day,
    state_delta,
    ts_ms_of,
    validate_intervals,
    validate_tag_event,
)

D = parse_day("2026-09-14")
A, B = 1, 2


def ev(i, uidx, hhmm, add=(), remove=()):
    hh, mm = hhmm
    return TagEvent(f"e{i:03d}", uidx, ts_ms_of(D, hh * 3600 + mm * 60), frozenset(add), frozenset(remove))


def test_ds_uses_ict():
    assert parse_day("2026-09-15") == 20711
    # e-9003: 18:30Z ngày 14 = 01:30 ICT ngày 15
    assert ds_of_ts_ms(parse_ts_ms("2026-09-14T18:30:00Z")) == parse_day("2026-09-15")
    assert ds_of_ts_ms(parse_ts_ms("2026-09-14T16:59:59Z")) == parse_day("2026-09-14")
    assert ds_of_ts_ms(parse_ts_ms("2026-09-14T17:00:00Z")) == parse_day("2026-09-15")
    assert ds_of_ts_ms(ts_ms_of(D, 0)) == D and ds_of_ts_ms(ts_ms_of(D, 86399)) == D
    with pytest.raises(ModelError):
        parse_ts_ms("2026-09-14T10:00:00")


@pytest.mark.parametrize(
    "dt,fm,kind",
    [
        (DataType.MUTEX, FeedMode.EVENT, Kind.MUTEX_EVENT),
        (DataType.MUTEX, FeedMode.STATE, Kind.MUTEX_STATE),
        (DataType.NOT_MUTEX, FeedMode.EVENT, Kind.NOT_MUTEX_EVENT),
        (DataType.NOT_MUTEX, FeedMode.STATE, Kind.NOT_MUTEX_STATE),
        (DataType.PARTIAL_VALUE, FeedMode.EVENT, Kind.PARTIAL_VALUE),
        (DataType.PARTIAL_VALUE_BY_TAG, FeedMode.EVENT, Kind.PARTIAL_VALUE_BY_TAG),
    ],
)
def test_kind_of(dt, fm, kind):
    assert kind_of(dt, fm) is kind


@pytest.mark.parametrize(
    "dt,fm",
    [
        (DataType.PARTIAL_VALUE, FeedMode.STATE),
        (DataType.PARTIAL_VALUE_BY_TAG, FeedMode.STATE),
        (DataType.MUTEX, FeedMode.FEED_MODE_UNSPECIFIED),
        (DataType.DATA_TYPE_UNSPECIFIED, FeedMode.EVENT),
    ],
)
def test_kind_of_rejects(dt, fm):
    with pytest.raises(ModelError):
        kind_of(dt, fm)


def test_value_range():
    vr = ValueRange(Decimal(500000), True, Decimal(2000000), False)
    assert vr.contains(Decimal(500000)) and not vr.contains(Decimal(2000000)) and not vr.contains(Decimal(499999))
    assert ValueRange(from_value=Decimal(0), from_inclusive=False).contains(Decimal("0.000001"))
    assert not ValueRange(from_value=Decimal(0), from_inclusive=False).contains(Decimal(0))
    assert ValueRange(to_value=Decimal(0), to_inclusive=True).contains(Decimal(-5))
    assert ValueRange(Decimal(1), True, Decimal(1), True).contains(Decimal(1))
    for bad in [
        dict(),
        dict(from_value=Decimal(2), to_value=Decimal(1)),
        dict(from_value=Decimal(1), to_value=Decimal(1), to_inclusive=False),
    ]:
        with pytest.raises(ModelError):
            ValueRange(**bad)


def test_attribute_spec_value_ranges():
    vr = {1: ValueRange(from_value=Decimal(0))}
    AttributeSpec(1, "pv", Kind.PARTIAL_VALUE, {"a": 1}, frozenset({DateRange.A7}), vr)
    with pytest.raises(ModelError):
        AttributeSpec(1, "pv", Kind.PARTIAL_VALUE, {"a": 1, "b": 2}, frozenset({DateRange.A7}), vr)
    for kind in (Kind.MUTEX_EVENT, Kind.NOT_MUTEX_STATE, Kind.PARTIAL_VALUE_BY_TAG):
        with pytest.raises(ModelError):
            AttributeSpec(1, "x", kind, {"a": 1}, frozenset({DateRange.A7}), vr)
    with pytest.raises(ModelError):
        AttributeSpec(1, "x", Kind.MUTEX_EVENT, {"a": 0}, frozenset({DateRange.A7}))


# §4.1 MUTEX: (events trong ngày, ADD, DEL, ADD(d,0))
MUTEX_CASES = {
    "add_last_wins": ([ev(1, 1, (8, 0), add=[A]), ev(2, 1, (9, 0), add=[B])], {B: [1]}, {}),
    "remove_before_add": ([ev(1, 1, (8, 0), remove=[A]), ev(2, 1, (9, 0), add=[A])], {A: [1]}, {}),
    "add_then_remove": ([ev(1, 1, (9, 0), add=[A]), ev(2, 1, (10, 0), remove=[A])], {A: [1]}, {A: [1]}),
    "remove_only": ([ev(1, 1, (9, 0), remove=[A])], {}, {A: [1]}),
    # ADD a, ADD b, REMOVE b: ADD cuối là b và bị REMOVE → không quay lại a
    "add_a_add_b_remove_b": (
        [ev(1, 1, (8, 0), add=[A]), ev(2, 1, (9, 0), add=[B]), ev(3, 1, (10, 0), remove=[B])],
        {B: [1]},
        {B: [1]},
    ),
    # REMOVE tag khác sau ADD không ảnh hưởng
    "remove_other_tag": ([ev(1, 1, (9, 0), add=[B]), ev(2, 1, (10, 0), remove=[A])], {B: [1]}, {A: [1]}),
    # cùng timestamp → event_id quyết định thứ tự
    "tie_by_event_id": ([ev(2, 1, (9, 0), remove=[A]), ev(1, 1, (9, 0), add=[A])], {A: [1]}, {A: [1]}),
    "event_add_b_remove_a": ([ev(1, 1, (8, 0), add=[A]), ev(2, 1, (9, 0), add=[B], remove=[A])], {B: [1]}, {A: [1]}),
}


@pytest.mark.parametrize("name", MUTEX_CASES)
def test_reduce_mutex_day(name):
    events, add, dele = MUTEX_CASES[name]
    out = reduce_mutex_day(events)
    want_add = {t: BitMap(u) for t, u in add.items()}
    if want_add:
        want_add[ANY_TAG] = BitMap.union(*want_add.values())
    assert out.add == want_add
    assert out.dele == {t: BitMap(u) for t, u in dele.items()}
    # DQ: Σ cnt(ADD(d,t)) == cnt(ADD(d,0))
    assert sum(len(b) for t, b in out.add.items() if t != ANY_TAG) == len(out.add.get(ANY_TAG, BitMap()))


NOT_MUTEX_CASES = {
    "independent_tags": ([ev(1, 1, (8, 0), add=[A, B]), ev(2, 1, (9, 0), remove=[A])], {B: [1]}, {A: [1]}),
    "remove_then_add": ([ev(1, 1, (8, 0), remove=[A]), ev(2, 1, (9, 0), add=[A])], {A: [1]}, {}),
    "two_users": ([ev(1, 1, (8, 0), add=[A]), ev(2, 2, (9, 0), remove=[A])], {A: [1]}, {A: [2]}),
}


@pytest.mark.parametrize("name", NOT_MUTEX_CASES)
def test_reduce_not_mutex_day(name):
    events, add, dele = NOT_MUTEX_CASES[name]
    out = reduce_not_mutex_day(events)
    assert out.add == {t: BitMap(u) for t, u in add.items()}
    assert out.dele == {t: BitMap(u) for t, u in dele.items()}
    for t in out.add:
        assert not out.add[t] & out.dele.get(t, BitMap())  # DQ ADD ∩ DEL = ∅
    assert out.sig() == {t: out.add.get(t, BitMap()) | out.dele.get(t, BitMap()) for t in out.add.keys() | out.dele.keys()}


@pytest.mark.parametrize(
    "kind,event",
    [
        (Kind.MUTEX_EVENT, ev(1, 1, (8, 0), add=[A, B])),  # MUTEX: một event ADD tối đa 1 tag
        (Kind.NOT_MUTEX_EVENT, ev(1, 1, (8, 0), add=[A], remove=[A])),  # thứ tự trong event không xác định
        (Kind.NOT_MUTEX_EVENT, ev(1, 1, (8, 0))),
        (Kind.MUTEX_STATE, ev(1, 1, (8, 0), add=[A])),  # STATE nhận SCD2, không nhận event
        (Kind.PARTIAL_VALUE, ev(1, 1, (8, 0), add=[A])),
    ],
)
def test_validate_tag_event_rejects(kind, event):
    with pytest.raises(ModelError):
        validate_tag_event(kind, event)


def test_state_delta():
    ivs = [
        StateInterval(1, A, D - 10, D),
        StateInterval(1, B, D),
        StateInterval(2, A, D - 3, D),  # hai version liền nhau cùng tag → triệt tiêu
        StateInterval(2, A, D),
        StateInterval(3, B, D - 5),
    ]
    validate_intervals(Kind.MUTEX_STATE, ivs)
    out = state_delta(ivs, D)
    assert out.added == {B: BitMap([1])}
    assert out.removed == {A: BitMap([1])}
    snap = state_delta(ivs, D, snapshot=True)
    assert snap.added == {A: BitMap([2]), B: BitMap([1, 3])} and not snap.removed


@pytest.mark.parametrize(
    "kind,ivs",
    [
        (Kind.MUTEX_STATE, [StateInterval(1, A, D - 5), StateInterval(1, B, D)]),  # 2 tag cùng lúc
        (Kind.NOT_MUTEX_STATE, [StateInterval(1, A, D - 5, D + 1), StateInterval(1, A, D)]),
        (Kind.NOT_MUTEX_STATE, [StateInterval(1, A, D, D)]),
        (Kind.MUTEX_EVENT, [StateInterval(1, A, D)]),
    ],
)
def test_validate_intervals_rejects(kind, ivs):
    with pytest.raises(ModelError):
        validate_intervals(kind, ivs)


def test_validate_intervals_not_mutex_allows_many_tags():
    validate_intervals(Kind.NOT_MUTEX_STATE, [StateInterval(1, A, D - 5), StateInterval(1, B, D)])


def test_reduce_pv_day():
    events = [
        ValueEvent("a", 1, ts_ms_of(D), 0, Decimal("0.000001")),
        ValueEvent("b", 1, ts_ms_of(D), 0, Decimal("99999999999999999999.999999")),
        ValueEvent("c", 2, ts_ms_of(D), 0, Decimal(-5)),
        ValueEvent("d", 2, ts_ms_of(D), 0, Decimal(5)),
    ]
    # DECIMAL(27,6): cộng không lệch; SUM = 0 vẫn có mặt (user có event)
    assert reduce_pv_day(Kind.PARTIAL_VALUE, events) == {0: {1: Decimal("100000000000000000000.000000"), 2: Decimal(0)}}
    by_tag = [ValueEvent("a", 1, ts_ms_of(D), 1, Decimal(3)), ValueEvent("b", 1, ts_ms_of(D), 2, Decimal(4))]
    assert reduce_pv_day(Kind.PARTIAL_VALUE_BY_TAG, by_tag) == {1: {1: Decimal(3)}, 2: {1: Decimal(4)}}
    with pytest.raises(ModelError):
        reduce_pv_day(Kind.PARTIAL_VALUE, by_tag)
    with pytest.raises(ModelError):
        reduce_pv_day(Kind.PARTIAL_VALUE_BY_TAG, events)
    with pytest.raises(ModelError):
        reduce_pv_day(Kind.MUTEX_EVENT, events)



# --------------------------------------------------------------------------- P1b: aggFunc, EXTENDED

PV_EVENTS = [
    ValueEvent("a", 1, ts_ms_of(D, 10), 0, Decimal(300)),
    ValueEvent("b", 1, ts_ms_of(D, 20), 0, Decimal(-50)),
    ValueEvent("c", 1, ts_ms_of(D, 30), 0, Decimal(700)),
    ValueEvent("d", 2, ts_ms_of(D, 40), 0, Decimal(0)),
]


@pytest.mark.parametrize(
    "agg,want",
    [
        (AggFunc.SUM, {1: Decimal(950), 2: Decimal(0)}),
        (AggFunc.COUNT, {1: Decimal(3), 2: Decimal(1)}),
        (AggFunc.MIN, {1: Decimal(-50), 2: Decimal(0)}),
        (AggFunc.MAX, {1: Decimal(700), 2: Decimal(0)}),
    ],
)
def test_reduce_pv_day_agg(agg, want):
    assert reduce_pv_day(Kind.PARTIAL_VALUE, PV_EVENTS, agg) == {0: want}


@pytest.mark.parametrize(
    "agg,want",
    [
        (AggFunc.SUM, {1: Decimal(8), 2: Decimal(5)}),
        (AggFunc.COUNT, {1: Decimal(8), 2: Decimal(5)}),
        (AggFunc.MIN, {1: Decimal(3), 2: Decimal(5)}),
        (AggFunc.MAX, {1: Decimal(5), 2: Decimal(5)}),
    ],
)
def test_merge_values(agg, want):
    a = {7: {1: Decimal(3)}}
    b = {7: {1: Decimal(5), 2: Decimal(5)}}
    assert merge_values(a, b, agg) == {7: want}
    assert a == {7: {1: Decimal(3)}}  # không sửa input


def test_unsupported_agg_is_explicit_error():
    with pytest.raises(ModelError):
        reduce_pv_day(Kind.PARTIAL_VALUE, PV_EVENTS, 99)


R_ = frozenset({DateRange.A7})


def test_attribute_spec_agg_func():
    pv = AttributeSpec(1, "pv", Kind.PARTIAL_VALUE_BY_TAG, {"a": 1}, R_)
    assert pv.agg_func == AggFunc.SUM  # UNSPECIFIED → SUM
    assert AttributeSpec(1, "pv", Kind.PARTIAL_VALUE_BY_TAG, {"a": 1}, R_, agg_func=AggFunc.MAX).agg_func == AggFunc.MAX
    for kind in (Kind.MUTEX_EVENT, Kind.NOT_MUTEX_STATE):
        with pytest.raises(ModelError):
            AttributeSpec(1, "x", kind, {"a": 1}, R_, agg_func=AggFunc.SUM)
    with pytest.raises(ModelError):
        AttributeSpec(1, "pv", Kind.PARTIAL_VALUE_BY_TAG, {"a": 1}, R_, agg_func=99)


@pytest.mark.parametrize("kind", list(Kind), ids=lambda k: k.value)
def test_attribute_spec_extended(kind):
    ok = kind in (Kind.NOT_MUTEX_EVENT, Kind.NOT_MUTEX_STATE, Kind.PARTIAL_VALUE_BY_TAG)
    if ok:
        a = AttributeSpec(1, "ext", kind, {}, R_, attribute_type=AttributeType.EXTENDED)
        assert a.is_extended
        with pytest.raises(ModelError):
            a.tag_id("oa_1")  # phải tra qua TagDict
        with pytest.raises(ModelError):
            AttributeSpec(1, "ext", kind, {"a": 1}, R_, attribute_type=AttributeType.EXTENDED)
    else:
        with pytest.raises(ModelError):
            AttributeSpec(1, "ext", kind, {}, R_, attribute_type=AttributeType.EXTENDED)


def test_tag_dict_append_only():
    d = TagDict()
    assert d.encode("oa_1") == 1 and d.encode("oa_2") == 2 and d.encode("oa_1") == 1
    assert d.lookup("oa_3") is None and d.name(2) == "oa_2"
    assert d.snapshot() == {"oa_1": 1, "oa_2": 2}
    with pytest.raises(ModelError):
        d.encode("")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
