"""Luật hợp lệ của attribute / tag trong catalog (CLAUDE.md §3.1–§3.4).

Cùng luật được enforce ở Postgres bằng CHECK constraint
(com/tm/src/sql/postgres/migrations/V1__meta_schema.sql).
"""

from com.tm.proto.vision.catalog.v1 import catalog_pb2

DataType = catalog_pb2.DataType
FeedMode = catalog_pb2.FeedMode


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


def validate_tag(attr: catalog_pb2.Attribute, tag: catalog_pb2.Tag) -> None:
    """Raise CatalogError nếu tag không hợp lệ với attribute của nó."""
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
