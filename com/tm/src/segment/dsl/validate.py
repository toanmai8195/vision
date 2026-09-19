"""Validate Segment DSL theo catalog (CLAUDE.md §6.1, ma trận §3.5 dòng "DSL validate").

| dataType | tags | tagOp=AND | valueRange |
|---|---|---|---|
| MUTEX | bắt buộc | cấm (luôn rỗng) | cấm |
| NOT_MUTEX | bắt buộc | cho phép | cấm |
| PARTIAL_VALUE | `tags` hoặc `valueRange` (ít nhất một) | cho phép | tuỳ chọn (ad-hoc) |
| PARTIAL_VALUE_BY_TAG | bắt buộc | cho phép | bắt buộc |

`dateRange` phải thuộc `supportedDateRanges`; `customDateRange`: `from ≤ to ≤ ds`, `from ≥ ds − 399`.
Cách tính suy ra từ `dataType` của attribute — DSL không có field `mode`.
"""

from __future__ import annotations

from collections.abc import Mapping

from com.tm.proto.vision.segment.v1 import segment_pb2
from com.tm.src.temporal.model import AttributeSpec, DateRange, Kind, ModelError, ValueRange, parse_day
from com.tm.src.temporal.ranges import CustomRange, WindowRef, custom_window

Rule = segment_pb2.Rule
Condition = segment_pb2.Condition
TagOp = segment_pb2.Condition.TagOp
Operator = segment_pb2.Rule.Operator

Catalog = Mapping[str, AttributeSpec]
"""attribute name → spec."""


class DslError(ValueError):
    def __init__(self, path: str, msg: str):
        super().__init__(f"{path}: {msg}")
        self.path = path


def window_ref(cond: Condition, path: str = "condition") -> WindowRef:
    match cond.WhichOneof("window"):
        case "date_range":
            if cond.date_range == DateRange.DATE_RANGE_UNSPECIFIED:
                raise DslError(path, "date_range is unspecified")
            return cond.date_range
        case "custom_date_range":
            try:
                return CustomRange(parse_day(cond.custom_date_range.from_date), parse_day(cond.custom_date_range.to_date))
            except ValueError as e:
                raise DslError(path, f"invalid custom_date_range: {e}") from None
        case None:
            raise DslError(path, "date_range or custom_date_range is required")
        case other:
            raise DslError(path, f"unknown window {other}")


def value_range_of(cond: Condition, path: str = "condition") -> ValueRange | None:
    if not cond.HasField("value_range"):
        return None
    try:
        return ValueRange.from_proto(cond.value_range)
    except ModelError as e:
        raise DslError(path, f"invalid value_range: {e}") from None


def validate_condition(cond: Condition, catalog: Catalog, ds: int | None = None, path: str = "condition") -> AttributeSpec:
    attr = catalog.get(cond.attr)
    if attr is None:
        raise DslError(path, f"unknown attribute {cond.attr!r}")

    ref = window_ref(cond, path)
    if isinstance(ref, CustomRange):
        if ref.from_day > ref.to_day:
            raise DslError(path, "custom_date_range from_date > to_date")
        if ds is not None:
            try:
                custom_window(ref, ds)
            except ModelError as e:
                raise DslError(path, str(e)) from None
    elif ref not in attr.supported_date_ranges:
        raise DslError(path, f"date_range {DateRange.Name(ref)} not in supported_date_ranges of {attr.name}")

    if len(set(cond.tags)) != len(cond.tags):
        raise DslError(path, "duplicate tag")
    for name in cond.tags:
        if name not in attr.tags:
            raise DslError(path, f"unknown tag {name!r} of {attr.name}")
    if cond.tag_op not in (TagOp.TAG_OP_UNSPECIFIED, TagOp.OR, TagOp.AND):
        raise DslError(path, f"unknown tag_op {cond.tag_op}")

    has_tags = len(cond.tags) > 0
    is_and = cond.tag_op == TagOp.AND
    vr = value_range_of(cond, path)
    match attr.kind:
        case Kind.MUTEX_EVENT | Kind.MUTEX_STATE:
            if not has_tags:
                raise DslError(path, "MUTEX condition requires tags")
            if is_and:
                raise DslError(path, "tag_op AND on MUTEX is always empty")
            if vr is not None:
                raise DslError(path, "value_range is not allowed on MUTEX")
        case Kind.NOT_MUTEX_EVENT | Kind.NOT_MUTEX_STATE:
            if not has_tags:
                raise DslError(path, "NOT_MUTEX condition requires tags")
            if vr is not None:
                raise DslError(path, "value_range is not allowed on NOT_MUTEX")
        case Kind.PARTIAL_VALUE:
            if not has_tags and vr is None:
                raise DslError(path, "PARTIAL_VALUE condition requires tags or value_range")
            if has_tags and vr is not None:
                # TODO(verify): CLAUDE.md §3.5 cho phép "tags hoặc valueRange (ít nhất một)" nhưng chưa định nghĩa
                # nghĩa khi có cả hai (giao? hợp?). Từ chối tường minh cho tới khi chốt semantics.
                raise DslError(path, "PARTIAL_VALUE with both tags and value_range is not supported yet")
        case Kind.PARTIAL_VALUE_BY_TAG:
            if not has_tags:
                raise DslError(path, "PARTIAL_VALUE_BY_TAG condition requires tags")
            if vr is None:
                raise DslError(path, "PARTIAL_VALUE_BY_TAG condition requires value_range")
    return attr


def validate_rule(rule: Rule, catalog: Catalog, ds: int | None = None, path: str = "rule") -> None:
    is_leaf = rule.HasField("condition")
    if is_leaf:
        if rule.operator != Operator.OPERATOR_UNSPECIFIED or rule.children:
            raise DslError(path, "a node has either condition or operator + children")
        validate_condition(rule.condition, catalog, ds, f"{path}.condition")
        return
    match rule.operator:
        case Operator.AND | Operator.OR:
            if not rule.children:
                raise DslError(path, f"{Operator.Name(rule.operator)} needs at least 1 child")
        case Operator.SUB:
            if len(rule.children) < 2:
                raise DslError(path, "SUB needs at least 2 children")
        case Operator.OPERATOR_UNSPECIFIED:
            raise DslError(path, "operator is unspecified")
        case other:
            raise DslError(path, f"unknown operator {other}")
    for i, child in enumerate(rule.children):
        validate_rule(child, catalog, ds, f"{path}.children[{i}]")
