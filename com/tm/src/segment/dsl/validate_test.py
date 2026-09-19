"""DSL validate matrix (CLAUDE.md §11 "Test bắt buộc"): dataType × dateRange × tagOp × valueRange × tags.

Kỳ vọng viết tay theo bảng §3.5 / §6.1 (không suy ra từ code).
"""

import itertools
import sys
from decimal import Decimal

import pytest

from com.tm.proto.vision.segment.v1 import segment_pb2
from com.tm.src.segment.dsl.validate import DslError, validate_condition, validate_rule
from com.tm.src.temporal.model import AttributeSpec, DateRange, Kind, ValueRange, parse_day

DS = parse_day("2026-09-15")
SUPPORTED = frozenset({DateRange.A1, DateRange.A7, DateRange.A30})

CATALOG = {
    k.value.lower(): AttributeSpec(
        attr_id=i + 1,
        name=k.value.lower(),
        kind=k,
        tags={"x": 1, "y": 2},
        supported_date_ranges=SUPPORTED,
        value_ranges={1: ValueRange(to_value=Decimal(10)), 2: ValueRange(from_value=Decimal(10))}
        if k is Kind.PARTIAL_VALUE
        else {},
    )
    for i, k in enumerate(Kind)
}

# (có tags, tagOp, có valueRange) → hợp lệ?  — theo §3.5 dòng "DSL validate" + §6.1
NO, ONE, TWO = (), ("x",), ("x", "y")
OR, AND, UNSET = "OR", "AND", None
RULES = {
    # MUTEX: bắt buộc tags; cấm tagOp=AND (luôn rỗng); cấm valueRange
    "MUTEX": {
        (NO, UNSET, False): False, (NO, OR, False): False, (NO, AND, False): False,
        (ONE, UNSET, False): True, (ONE, OR, False): True, (ONE, AND, False): False,
        (TWO, UNSET, False): True, (TWO, OR, False): True, (TWO, AND, False): False,
        (NO, UNSET, True): False, (NO, OR, True): False, (NO, AND, True): False,
        (ONE, UNSET, True): False, (ONE, OR, True): False, (ONE, AND, True): False,
        (TWO, UNSET, True): False, (TWO, OR, True): False, (TWO, AND, True): False,
    },
    # NOT_MUTEX: bắt buộc tags; cho tagOp=AND; cấm valueRange
    "NOT_MUTEX": {
        (NO, UNSET, False): False, (NO, OR, False): False, (NO, AND, False): False,
        (ONE, UNSET, False): True, (ONE, OR, False): True, (ONE, AND, False): True,
        (TWO, UNSET, False): True, (TWO, OR, False): True, (TWO, AND, False): True,
        (NO, UNSET, True): False, (NO, OR, True): False, (NO, AND, True): False,
        (ONE, UNSET, True): False, (ONE, OR, True): False, (ONE, AND, True): False,
        (TWO, UNSET, True): False, (TWO, OR, True): False, (TWO, AND, True): False,
    },
    # PARTIAL_VALUE: tags hoặc valueRange ad-hoc (ít nhất một).
    # TODO(verify): có cả tags lẫn valueRange → chưa định nghĩa, hiện từ chối.
    "PARTIAL_VALUE": {
        (NO, UNSET, False): False, (NO, OR, False): False, (NO, AND, False): False,
        (ONE, UNSET, False): True, (ONE, OR, False): True, (ONE, AND, False): True,
        (TWO, UNSET, False): True, (TWO, OR, False): True, (TWO, AND, False): True,
        (NO, UNSET, True): True, (NO, OR, True): True, (NO, AND, True): True,
        (ONE, UNSET, True): False, (ONE, OR, True): False, (ONE, AND, True): False,
        (TWO, UNSET, True): False, (TWO, OR, True): False, (TWO, AND, True): False,
    },
    # PARTIAL_VALUE_BY_TAG: bắt buộc tags + valueRange; tagOp OR/AND đều được
    "PARTIAL_VALUE_BY_TAG": {
        (NO, UNSET, False): False, (NO, OR, False): False, (NO, AND, False): False,
        (ONE, UNSET, False): False, (ONE, OR, False): False, (ONE, AND, False): False,
        (TWO, UNSET, False): False, (TWO, OR, False): False, (TWO, AND, False): False,
        (NO, UNSET, True): False, (NO, OR, True): False, (NO, AND, True): False,
        (ONE, UNSET, True): True, (ONE, OR, True): True, (ONE, AND, True): True,
        (TWO, UNSET, True): True, (TWO, OR, True): True, (TWO, AND, True): True,
    },
}  # fmt: skip


def data_type(kind: Kind) -> str:
    match kind:
        case Kind.MUTEX_EVENT | Kind.MUTEX_STATE:
            return "MUTEX"
        case Kind.NOT_MUTEX_EVENT | Kind.NOT_MUTEX_STATE:
            return "NOT_MUTEX"
        case Kind.PARTIAL_VALUE:
            return "PARTIAL_VALUE"
        case Kind.PARTIAL_VALUE_BY_TAG:
            return "PARTIAL_VALUE_BY_TAG"


# window → hợp lệ? (A180 không nằm trong supportedDateRanges; custom giới hạn ds − 399 … ds)
WINDOWS = {
    "A7": (dict(date_range=DateRange.A7), True),
    "A180_unsupported": (dict(date_range=DateRange.A180), False),
    "custom_ok": (dict(custom_date_range=segment_pb2.CustomDateRange(from_date="2025-08-12", to_date="2026-09-15")), True),
    "custom_too_old": (dict(custom_date_range=segment_pb2.CustomDateRange(from_date="2025-08-11", to_date="2026-09-15")), False),
    "custom_future": (dict(custom_date_range=segment_pb2.CustomDateRange(from_date="2026-09-01", to_date="2026-09-16")), False),
    "custom_reversed": (dict(custom_date_range=segment_pb2.CustomDateRange(from_date="2026-09-10", to_date="2026-09-01")), False),
    "custom_bad_date": (dict(custom_date_range=segment_pb2.CustomDateRange(from_date="2026-13-01", to_date="2026-09-01")), False),
    "none": (dict(), False),
    "unspecified": (dict(date_range=DateRange.DATE_RANGE_UNSPECIFIED), False),
}

MATRIX = [
    (kind, wname, tags, op, has_vr)
    for kind in Kind
    for wname in WINDOWS
    for (tags, op, has_vr) in RULES[data_type(kind)]
]


def test_rules_table_complete():
    combos = set(itertools.product([NO, ONE, TWO], [UNSET, OR, AND], [False, True]))
    for dt, table in RULES.items():
        assert set(table) == combos, dt


def condition(kind: Kind, window: dict, tags, op, has_vr) -> segment_pb2.Condition:
    c = segment_pb2.Condition(attr=kind.value.lower(), tags=list(tags), **window)
    if op is not None:
        c.tag_op = segment_pb2.Condition.TagOp.Value(op)
    if has_vr:
        c.value_range.from_value = "500000"
        c.value_range.from_inclusive = True
    return c


@pytest.mark.parametrize(
    "kind,wname,tags,op,has_vr",
    MATRIX,
    ids=[f"{k.value}-{w}-{len(t)}tags-{o}-{'vr' if v else 'novr'}" for k, w, t, o, v in MATRIX],
)
def test_matrix(kind, wname, tags, op, has_vr):
    window, window_ok = WINDOWS[wname]
    ok = window_ok and RULES[data_type(kind)][(tags, op, has_vr)]
    c = condition(kind, window, tags, op, has_vr)
    if ok:
        assert validate_condition(c, CATALOG, DS) is CATALOG[kind.value.lower()]
    else:
        with pytest.raises(DslError):
            validate_condition(c, CATALOG, DS)


@pytest.mark.parametrize(
    "cond",
    [
        segment_pb2.Condition(attr="unknown", tags=["x"], date_range=DateRange.A7),
        segment_pb2.Condition(attr="not_mutex_event", tags=["z"], date_range=DateRange.A7),  # tag lạ
        segment_pb2.Condition(attr="not_mutex_event", tags=["x", "x"], date_range=DateRange.A7),
        segment_pb2.Condition(attr="partial_value", date_range=DateRange.A7, value_range={}),  # range rỗng
        segment_pb2.Condition(
            attr="partial_value", date_range=DateRange.A7, value_range={"from_value": "abc"}
        ),
        segment_pb2.Condition(
            attr="partial_value", date_range=DateRange.A7, value_range={"from_value": "5", "to_value": "1"}
        ),
    ],
)
def test_invalid_conditions(cond):
    with pytest.raises(DslError):
        validate_condition(cond, CATALOG, DS)


def leaf(**kw):
    return segment_pb2.Rule(condition=segment_pb2.Condition(attr="not_mutex_event", tags=["x"], date_range=DateRange.A7, **kw))


def test_rule_tree():
    ops = segment_pb2.Rule.Operator
    validate_rule(segment_pb2.Rule(operator=ops.SUB, children=[leaf(), leaf()]), CATALOG, DS)
    validate_rule(segment_pb2.Rule(operator=ops.AND, children=[leaf()]), CATALOG, DS)
    bad = [
        segment_pb2.Rule(operator=ops.SUB, children=[leaf()]),
        segment_pb2.Rule(operator=ops.OR, children=[]),
        segment_pb2.Rule(children=[leaf()]),
        segment_pb2.Rule(operator=ops.AND, children=[leaf()], condition=leaf().condition),
        segment_pb2.Rule(  # lỗi ở node lá sâu (NOT_MUTEX thiếu tags)
            operator=ops.AND,
            children=[
                segment_pb2.Rule(
                    operator=ops.OR,
                    children=[segment_pb2.Rule(condition=segment_pb2.Condition(attr="not_mutex_event", date_range=DateRange.A7))],
                )
            ],
        ),
    ]
    for rule in bad:
        with pytest.raises(DslError):
            validate_rule(rule, CATALOG, DS)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
