"""Luật hợp lệ của attribute / tag trong catalog (CLAUDE.md §3.1–§3.6).

Cùng luật được enforce ở Postgres bằng CHECK constraint
(com/tm/src/sql/postgres/migrations/V1__meta_schema.sql, V3__agg_func_attribute_type.sql).
"""

from com.tm.proto.vision.catalog.v1 import catalog_pb2

DataType = catalog_pb2.DataType
FeedMode = catalog_pb2.FeedMode
AggFunc = catalog_pb2.AggFunc
AttributeType = catalog_pb2.AttributeType

SUPPORTED_AGG_FUNCS = (AggFunc.SUM, AggFunc.COUNT, AggFunc.MIN, AggFunc.MAX)


class CatalogError(ValueError):
    """Attribute hoặc tag vi phạm luật catalog."""


def validate_attribute(attr: catalog_pb2.Attribute) -> None:
    """Raise CatalogError nếu attribute không hợp lệ."""
    if not attr.name:
        raise CatalogError("attribute name is required")
    if attr.attr_group_id <= 0:
        raise CatalogError(f"{attr.name}: attr_group_id must be positive")
    if not attr.supported_date_ranges:
        raise CatalogError(f"{attr.name}: supported_date_ranges must not be empty")

    # Rẽ nhánh exhaustive theo 4 loại dữ liệu — không có nhánh default ngầm.
    dt = attr.data_type
    if dt in (DataType.MUTEX, DataType.NOT_MUTEX):
        if attr.feed_mode not in (FeedMode.EVENT, FeedMode.STATE):
            raise CatalogError(f"{attr.name}: {DataType.Name(dt)} requires feed_mode EVENT or STATE")
    elif dt in (DataType.PARTIAL_VALUE, DataType.PARTIAL_VALUE_BY_TAG):
        if attr.feed_mode != FeedMode.EVENT:
            raise CatalogError(f"{attr.name}: {DataType.Name(dt)} only supports feed_mode EVENT")
    elif dt == DataType.DATA_TYPE_UNSPECIFIED:
        raise CatalogError(f"{attr.name}: data_type is unspecified")
    else:
        raise CatalogError(f"{attr.name}: unsupported data_type {dt}")

    # aggFunc (§3.2.1): chỉ PARTIAL_VALUE(_BY_TAG); UNSPECIFIED = SUM.
    if dt in (DataType.PARTIAL_VALUE, DataType.PARTIAL_VALUE_BY_TAG):
        if attr.agg_func not in (AggFunc.AGG_FUNC_UNSPECIFIED, *SUPPORTED_AGG_FUNCS):
            raise CatalogError(f"{attr.name}: unsupported agg_func {attr.agg_func}")
    elif attr.agg_func != AggFunc.AGG_FUNC_UNSPECIFIED:
        raise CatalogError(f"{attr.name}: agg_func is only allowed for PARTIAL_VALUE(_BY_TAG)")

    # attributeType (§3.6): EXTENDED chỉ cho NOT_MUTEX và PARTIAL_VALUE_BY_TAG.
    at = attr.attribute_type
    if at == AttributeType.EXTENDED:
        if dt not in (DataType.NOT_MUTEX, DataType.PARTIAL_VALUE_BY_TAG):
            # TODO(verify): MUTEX EXTENDED cần "tag mới nhất" dạng cột theo user (CLAUDE.md §3.5).
            raise CatalogError(f"{attr.name}: EXTENDED is only supported for NOT_MUTEX and PARTIAL_VALUE_BY_TAG")
    elif at not in (AttributeType.ATTRIBUTE_TYPE_UNSPECIFIED, AttributeType.STANDARD):
        raise CatalogError(f"{attr.name}: unsupported attribute_type {at}")


def validate_tag(attr: catalog_pb2.Attribute, tag: catalog_pb2.Tag) -> None:
    """Raise CatalogError nếu tag không hợp lệ với attribute của nó."""
    if attr.attribute_type == AttributeType.EXTENDED:
        raise CatalogError(f"tag {tag.name}: EXTENDED attribute has no catalog tags (silver.tag_dict)")
    if tag.attr_id != attr.id:
        raise CatalogError(f"tag {tag.name}: attr_id {tag.attr_id} != {attr.id}")
    if tag.id < 1:
        raise CatalogError(f"tag {tag.name}: id must be >= 1 (0 is reserved for __any__)")
    if not tag.name:
        raise CatalogError("tag name is required")

    has_range = tag.HasField("value_range")
    dt = attr.data_type
    if dt == DataType.PARTIAL_VALUE:
        if not has_range:
            raise CatalogError(f"tag {tag.name}: PARTIAL_VALUE tag requires value_range")
        vr = tag.value_range
        if not vr.HasField("from_value") and not vr.HasField("to_value"):
            raise CatalogError(f"tag {tag.name}: value_range needs from_value or to_value")
    elif dt in (DataType.MUTEX, DataType.NOT_MUTEX, DataType.PARTIAL_VALUE_BY_TAG):
        if has_range:
            raise CatalogError(f"tag {tag.name}: value_range is only allowed for PARTIAL_VALUE")
    elif dt == DataType.DATA_TYPE_UNSPECIFIED:
        raise CatalogError(f"tag {tag.name}: attribute data_type is unspecified")
    else:
        raise CatalogError(f"tag {tag.name}: unsupported data_type {dt}")
