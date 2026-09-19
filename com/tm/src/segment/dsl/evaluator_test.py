"""Evaluator thuần: AND / OR / SUB, tagOp, valueRange cho đủ 4 loại dữ liệu (source giả lập)."""

import sys
from decimal import Decimal

import pytest
from pyroaring import BitMap

from com.tm.proto.vision.segment.v1 import segment_pb2
from com.tm.src.segment.dsl.evaluator import evaluate, evaluate_condition
from com.tm.src.segment.dsl.validate import DslError
from com.tm.src.temporal.model import ANY_TAG, AttributeSpec, AttributeType, DateRange, Kind, TagDict, ValueRange, parse_day
from com.tm.src.temporal.ranges import CustomRange

DS = parse_day("2026-09-15")
RANGES = frozenset({DateRange.A7, DateRange.A30})
Ops = segment_pb2.Rule.Operator
TagOp = segment_pb2.Condition.TagOp


class FakeSource:
    """Bitmap/SUM cố định theo (window, tag); ghi lại số lần gọi để kiểm cache."""

    def __init__(self, bitmaps=None, sums=None, tag_dict=None):
        self.bitmaps = bitmaps or {}
        self.sums = sums or {}
        self.tag_dict = tag_dict
        self.calls = 0

    def extended_tag_id(self, tag_string):
        return self.tag_dict.lookup(tag_string)

    def tag_bitmap(self, ref, ds, tag_id):
        self.calls += 1
        return BitMap(self.bitmaps.get((ref, tag_id), []))

    def pv_sums(self, ref, ds, tag_id):
        self.calls += 1
        return {u: Decimal(v) for u, v in self.sums.get((ref, tag_id), {}).items()}


def spec(name, kind, value_ranges=None):
    return AttributeSpec(1, name, kind, {"a": 1, "b": 2, "c": 3}, RANGES, value_ranges or {})


PV_BUCKETS = {1: ValueRange(to_value=Decimal(100)), 2: ValueRange(Decimal(100), True, Decimal(1000)), 3: ValueRange(Decimal(1000))}
CATALOG = {
    "mutex_event": spec("mutex_event", Kind.MUTEX_EVENT),
    "mutex_state": spec("mutex_state", Kind.MUTEX_STATE),
    "not_mutex_event": spec("not_mutex_event", Kind.NOT_MUTEX_EVENT),
    "not_mutex_state": spec("not_mutex_state", Kind.NOT_MUTEX_STATE),
    "pv": spec("pv", Kind.PARTIAL_VALUE, PV_BUCKETS),
    "pvbt": spec("pvbt", Kind.PARTIAL_VALUE_BY_TAG),
}
A7 = DateRange.A7
LABEL = FakeSource(bitmaps={(A7, 1): [1, 2], (A7, 2): [2, 3], (A7, 3): [4]})
SOURCES = {
    "mutex_event": LABEL,
    "mutex_state": LABEL,
    "not_mutex_event": LABEL,
    "not_mutex_state": LABEL,
    "pv": FakeSource(bitmaps={(A7, 1): [3], (A7, 2): [1, 2]}, sums={(A7, ANY_TAG): {1: 500, 2: 1000, 3: 50}}),
    # user 1: a=400, b=400 (tổng 800 nhưng không cộng gộp giữa tag); user 2: a=600; user 3: b=0
    "pvbt": FakeSource(sums={(A7, 1): {1: 400, 2: 600}, (A7, 2): {1: 400, 3: 0}}),
}


def cond(attr, tags=(), tag_op=None, vr=None, **window):
    c = segment_pb2.Condition(attr=attr, tags=list(tags), **(window or {"date_range": A7}))
    if tag_op:
        c.tag_op = TagOp.Value(tag_op)
    if vr:
        for k, v in vr.items():
            setattr(c.value_range, k, v)
    return c


GTE_500 = {"from_value": "500", "from_inclusive": True}
LT_1 = {"to_value": "1", "to_inclusive": False}


@pytest.mark.parametrize(
    "c,want",
    [
        (cond("mutex_event", ["a", "b"]), [1, 2, 3]),
        (cond("mutex_state", ["c"], "OR"), [4]),
        (cond("not_mutex_event", ["a", "b"], "AND"), [2]),
        (cond("not_mutex_state", ["a", "b"], "OR"), [1, 2, 3]),
        (cond("pv", ["a", "b"]), [1, 2, 3]),  # bucket định sẵn
        (cond("pv", vr=GTE_500), [1, 2]),  # ad-hoc
        (cond("pv", vr=LT_1), []),  # user không có event không thuộc range nào kể cả chứa 0
        (cond("pvbt", ["a"], vr=GTE_500), [2]),
        (cond("pvbt", ["a", "b"], "OR", vr=GTE_500), [2]),  # user 1: 400 + 400 không cộng gộp
        (cond("pvbt", ["a", "b"], "AND", vr={"from_value": "400", "from_inclusive": True}), [1]),
        (cond("pvbt", ["b"], vr={"from_value": "0", "from_inclusive": True}), [1, 3]),  # SUM = 0 vẫn có event
    ],
)
def test_condition(c, want):
    assert evaluate_condition(c, CATALOG, SOURCES, DS) == BitMap(want)


def test_condition_uses_custom_range():
    ref = CustomRange(parse_day("2026-09-01"), parse_day("2026-09-10"))
    src = FakeSource(bitmaps={(ref, 1): [9]})
    c = cond("mutex_event", ["a"], custom_date_range=segment_pb2.CustomDateRange(from_date="2026-09-01", to_date="2026-09-10"))
    assert evaluate_condition(c, CATALOG, {"mutex_event": src}, DS) == BitMap([9])


def test_condition_validates_first():
    with pytest.raises(DslError):
        evaluate_condition(cond("mutex_event", ["a", "b"], "AND"), CATALOG, SOURCES, DS)


def leaf(c):
    return segment_pb2.Rule(condition=c)


def test_operators_and_cache():
    x = leaf(cond("not_mutex_event", ["a"]))  # {1,2}
    y = leaf(cond("not_mutex_event", ["b"]))  # {2,3}
    z = leaf(cond("not_mutex_event", ["c"]))  # {4}
    rule = segment_pb2.Rule(
        operator=Ops.OR,
        children=[
            segment_pb2.Rule(operator=Ops.SUB, children=[segment_pb2.Rule(operator=Ops.OR, children=[x, y, z]), x, z]),
            segment_pb2.Rule(operator=Ops.AND, children=[x, y]),
        ],
    )
    LABEL.calls = 0
    # ({1,2,3,4} − ({1,2} ∪ {4})) ∪ ({1,2} ∩ {2,3}) = {3} ∪ {2}
    assert evaluate(rule, CATALOG, SOURCES, DS) == BitMap([2, 3])
    assert LABEL.calls == 3  # condition cache: mỗi condition tính một lần
    with pytest.raises(DslError):
        evaluate(segment_pb2.Rule(operator=Ops.SUB, children=[x]), CATALOG, SOURCES, DS)


def test_result_is_a_copy():
    src = FakeSource(bitmaps={(A7, 1): [1]})
    out = evaluate(leaf(cond("mutex_event", ["a"])), CATALOG, {"mutex_event": src}, DS)
    out.add(99)
    assert evaluate(leaf(cond("mutex_event", ["a"])), CATALOG, {"mutex_event": src}, DS) == BitMap([1])



def test_extended_tags_resolved_via_tag_dict():
    d = TagDict()
    oa1, oa2 = d.encode("oa_1"), d.encode("oa_2")
    catalog = {
        "oa": AttributeSpec(9, "oa", Kind.NOT_MUTEX_EVENT, {}, RANGES, attribute_type=AttributeType.EXTENDED),
        "gift": AttributeSpec(10, "gift", Kind.PARTIAL_VALUE_BY_TAG, {}, RANGES, attribute_type=AttributeType.EXTENDED),
    }
    sources = {
        "oa": FakeSource(bitmaps={(A7, oa1): [1, 2], (A7, oa2): [2]}, tag_dict=d),
        "gift": FakeSource(sums={(A7, oa1): {1: 500}}, tag_dict=d),
    }
    assert evaluate_condition(cond("oa", ["oa_1"]), catalog, sources, DS) == BitMap([1, 2])
    assert evaluate_condition(cond("oa", ["oa_1", "oa_2"], "AND"), catalog, sources, DS) == BitMap([2])
    # chuỗi chưa có trong tag_dict → rỗng, không lỗi; OR vẫn giữ phần còn lại, AND thành rỗng
    assert evaluate_condition(cond("oa", ["oa_unknown"]), catalog, sources, DS) == BitMap()
    assert evaluate_condition(cond("oa", ["oa_1", "oa_unknown"], "OR"), catalog, sources, DS) == BitMap([1, 2])
    assert evaluate_condition(cond("oa", ["oa_1", "oa_unknown"], "AND"), catalog, sources, DS) == BitMap()
    assert evaluate_condition(cond("gift", ["oa_1"], vr=GTE_500), catalog, sources, DS) == BitMap([1])
    assert evaluate_condition(cond("gift", ["nope"], vr=GTE_500), catalog, sources, DS) == BitMap()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
