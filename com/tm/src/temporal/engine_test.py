"""Unit test engine: EXTENDED usage-driven khi usage tăng giữa chừng (CLAUDE.md §3.6).

Property test giữ usage cố định; ở đây usage tăng giữa tháng qua query on-demand → ngày sau planner phải
materialize tag mới, kể cả với LAST_MONTH đang CARRY_FORWARD.
"""

import sys

import pytest
from pyroaring import BitMap

from com.tm.src.temporal.engine import AttributeEngine, Silver
from com.tm.src.temporal.model import (
    AttributeSpec,
    AttributeType,
    DateRange,
    Kind,
    ModelError,
    TagDict,
    TagEvent,
    parse_day,
    ts_ms_of,
)
from com.tm.src.temporal.planner import RangeMode, RangeStep, plan
from com.tm.src.temporal.ranges import window_of
from com.tm.src.temporal.reference import Reference

START = parse_day("2026-08-01")
RANGES = frozenset({DateRange.A7, DateRange.LAST_MONTH})


def setup():
    attr = AttributeSpec(1, "oa_follow", Kind.NOT_MUTEX_EVENT, {}, RANGES, attribute_type=AttributeType.EXTENDED)
    tags = TagDict()
    oa1, oa2 = tags.encode("oa_1"), tags.encode("oa_2")
    events = [
        TagEvent("e1", 1, ts_ms_of(parse_day("2026-08-10")), frozenset({oa1})),
        TagEvent("e2", 2, ts_ms_of(parse_day("2026-08-20")), frozenset({oa2})),
        TagEvent("e3", 3, ts_ms_of(parse_day("2026-08-25")), frozenset({oa1, oa2})),
    ]
    silver = Silver(attr.kind)
    silver.add_tag_events(events)
    engine = AttributeEngine(attr, silver, history_start=START, universe_of=lambda _: BitMap(range(10)), tag_dict=tags)
    return attr, engine, Reference(attr, tag_events=events, tag_dict=tags), oa1, oa2


def test_usage_growth_is_materialized_next_day_even_when_carrying_forward():
    attr, engine, ref, oa1, oa2 = setup()
    engine.usage[DateRange.LAST_MONTH].add(oa1)
    lm = window_of(DateRange.LAST_MONTH, parse_day("2026-09-10"))
    want = ref.members(lm.l, lm.r)
    assert want == {oa1: BitMap([1, 3]), oa2: BitMap([2, 3])}

    for day in range(START, parse_day("2026-09-05") + 1):
        steps = plan(day, attr, history_start=START, usage=engine.usage_snapshot())
        engine.run(steps)
        if day == parse_day("2026-09-03"):
            # segment mới dùng oa_2 với LAST_MONTH → on-demand + ghi usage
            assert engine.tag_bitmap(DateRange.LAST_MONTH, day, oa2) == want[oa2]
            assert engine.usage[DateRange.LAST_MONTH] == {oa1, oa2}
        if day == parse_day("2026-09-04"):
            (step,) = [s for s in steps if isinstance(s, RangeStep) and s.date_range == DateRange.LAST_MONTH]
            assert step.mode is RangeMode.CARRY_FORWARD and step.tags == (oa1, oa2)

    out = engine.query(DateRange.LAST_MONTH, parse_day("2026-09-05"))
    assert out.scope == {oa1, oa2}
    assert out.tags == want  # tag mới phải có mặt dù LAST_MONTH đang đóng băng


def test_tag_dict_required_only_for_extended():
    attr, engine, *_ = setup()
    std = AttributeSpec(2, "std", Kind.NOT_MUTEX_EVENT, {"a": 1}, RANGES)
    with pytest.raises(ModelError):
        AttributeEngine(attr, Silver(attr.kind), history_start=START, universe_of=lambda _: BitMap())
    with pytest.raises(ModelError):
        AttributeEngine(std, Silver(std.kind), history_start=START, universe_of=lambda _: BitMap(), tag_dict=TagDict())
    assert engine.extended_tag_id("oa_2") == 2 and engine.extended_tag_id("nope") is None


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
