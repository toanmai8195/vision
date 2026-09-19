"""Evaluator thuần của Segment DSL: `AND` / `OR` / `SUB`, `tagOp`, `valueRange` (CLAUDE.md §6.1, §3.2).

Nguồn bitmap/SUM trừu tượng qua `ConditionSource` — engine (bản tối ưu) và reference (ngây thơ) đều cài đặt,
segment-builder (Go, P5) port cùng logic và dùng chung golden `seg_1001` / `seg_1002`.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from functools import reduce
from typing import Protocol

from pyroaring import BitMap

from com.tm.src.segment.dsl.validate import (
    Catalog,
    Condition,
    Operator,
    Rule,
    TagOp,
    validate_condition,
    validate_rule,
    value_range_of,
    window_ref,
)
from com.tm.src.temporal.model import ANY_TAG, Kind, users_in_range
from com.tm.src.temporal.ranges import WindowRef


class ConditionSource(Protocol):
    def tag_bitmap(self, ref: WindowRef, ds: int, tag_id: int) -> BitMap: ...

    def pv_sums(self, ref: WindowRef, ds: int, tag_id: int) -> dict[int, Decimal]: ...


def evaluate_condition(cond: Condition, catalog: Catalog, sources: Mapping[str, ConditionSource], ds: int) -> BitMap:
    attr = validate_condition(cond, catalog, ds)
    src = sources[attr.name]
    ref = window_ref(cond)
    vr = value_range_of(cond)
    tag_ids = [attr.tag_id(n) for n in cond.tags]

    match attr.kind:
        case Kind.MUTEX_EVENT | Kind.MUTEX_STATE | Kind.NOT_MUTEX_EVENT | Kind.NOT_MUTEX_STATE:
            per_tag = [src.tag_bitmap(ref, ds, t) for t in tag_ids]
        case Kind.PARTIAL_VALUE:
            if vr is not None:  # ad-hoc: SUM toàn attribute (tag 0) ∈ valueRange
                return users_in_range(src.pv_sums(ref, ds, ANY_TAG), vr)
            per_tag = [src.tag_bitmap(ref, ds, t) for t in tag_ids]
        case Kind.PARTIAL_VALUE_BY_TAG:
            # SUM riêng từng tag, không cộng gộp giữa các tag (§3.2).
            per_tag = [users_in_range(src.pv_sums(ref, ds, t), vr) for t in tag_ids]

    if cond.tag_op == TagOp.AND:
        return reduce(lambda a, b: a & b, per_tag)
    return BitMap.union(BitMap(), *per_tag)


def evaluate(rule: Rule, catalog: Catalog, sources: Mapping[str, ConditionSource], ds: int) -> BitMap:
    validate_rule(rule, catalog, ds)
    cache: dict[bytes, BitMap] = {}

    def go(node: Rule) -> BitMap:
        if node.HasField("condition"):
            key = node.condition.SerializeToString(deterministic=True)
            if key not in cache:
                cache[key] = evaluate_condition(node.condition, catalog, sources, ds)
            return cache[key]
        children = [go(c) for c in node.children]
        match node.operator:
            case Operator.AND:
                return reduce(lambda a, b: a & b, children)
            case Operator.OR:
                return BitMap.union(BitMap(), *children)
            case Operator.SUB:
                return children[0] - BitMap.union(BitMap(), *children[1:])
            case other:
                raise ValueError(f"unknown operator {other}")

    return BitMap(go(rule))
