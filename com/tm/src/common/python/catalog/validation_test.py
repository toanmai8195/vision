"""Test luật catalog với 6 attribute ví dụ (com/tm/docs/data-flow-examples.md)."""

import sys

import pytest

from com.tm.proto.vision.catalog.v1 import catalog_pb2
from com.tm.src.common.python.catalog.validation import (
    CatalogError,
    validate_attribute,
    validate_tag,
)

DT = catalog_pb2.DataType
FM = catalog_pb2.FeedMode
DR = catalog_pb2.DateRange

ALL_RANGES = [DR.A1, DR.A7, DR.A30, DR.ALWAYS_ACTIVE]


def attr(id_, name, dt, fm, group=1):
    return catalog_pb2.Attribute(
        id=id_, name=name, data_type=dt, feed_mode=fm, attr_group_id=group, supported_date_ranges=ALL_RANGES
    )


EXAMPLE_ATTRIBUTES = [
    attr(101, "txn_category", DT.NOT_MUTEX, FM.EVENT),
    attr(102, "txn_amount", DT.PARTIAL_VALUE, FM.EVENT),
    attr(103, "txn_amount_by_category", DT.PARTIAL_VALUE_BY_TAG, FM.EVENT),
    attr(201, "user_city", DT.MUTEX, FM.STATE, group=2),
    attr(202, "product_holding", DT.NOT_MUTEX, FM.STATE, group=2),
    attr(301, "churn_score_band", DT.MUTEX, FM.EVENT, group=3),
]


def test_examples_cover_all_four_data_types():
    covered = {a.data_type for a in EXAMPLE_ATTRIBUTES}
    assert covered == {DT.MUTEX, DT.NOT_MUTEX, DT.PARTIAL_VALUE, DT.PARTIAL_VALUE_BY_TAG}
    # MUTEX/NOT_MUTEX có cả EVENT và STATE
    for dt in (DT.MUTEX, DT.NOT_MUTEX):
        assert {a.feed_mode for a in EXAMPLE_ATTRIBUTES if a.data_type == dt} == {FM.EVENT, FM.STATE}


@pytest.mark.parametrize("a", EXAMPLE_ATTRIBUTES, ids=lambda a: a.name)
def test_example_attributes_valid(a):
    validate_attribute(a)


@pytest.mark.parametrize(
    "a",
    [
        attr(1, "unspecified", DT.DATA_TYPE_UNSPECIFIED, FM.EVENT),
        attr(2, "mutex_no_feed", DT.MUTEX, FM.FEED_MODE_UNSPECIFIED),
        attr(3, "not_mutex_no_feed", DT.NOT_MUTEX, FM.FEED_MODE_UNSPECIFIED),
        attr(4, "pv_state", DT.PARTIAL_VALUE, FM.STATE),
        attr(5, "pvbt_state", DT.PARTIAL_VALUE_BY_TAG, FM.STATE),
        attr(6, "no_group", DT.MUTEX, FM.EVENT, group=0),
    ],
    ids=lambda a: a.name,
)
def test_invalid_attributes(a):
    with pytest.raises(CatalogError):
        validate_attribute(a)


def vr(from_value=None, to_value=None):
    r = catalog_pb2.ValueRange(from_inclusive=True, to_inclusive=False)
    if from_value is not None:
        r.from_value = from_value
    if to_value is not None:
        r.to_value = to_value
    return r


@pytest.mark.parametrize(
    "attr_idx,tag,ok",
    [
        (3, catalog_pb2.Tag(attr_id=201, id=1, name="hcm"), True),  # MUTEX
        (0, catalog_pb2.Tag(attr_id=101, id=1, name="fnb"), True),  # NOT_MUTEX
        (1, catalog_pb2.Tag(attr_id=102, id=1, name="lt_500k", value_range=vr("0", "500000")), True),  # PV
        (1, catalog_pb2.Tag(attr_id=102, id=3, name="gte_2m", value_range=vr(from_value="2000000")), True),
        (2, catalog_pb2.Tag(attr_id=103, id=1, name="fnb"), True),  # PV_BY_TAG
        (1, catalog_pb2.Tag(attr_id=102, id=1, name="no_range"), False),
        (1, catalog_pb2.Tag(attr_id=102, id=1, name="empty_range", value_range=vr()), False),
        (2, catalog_pb2.Tag(attr_id=103, id=1, name="fnb", value_range=vr("0")), False),
        (3, catalog_pb2.Tag(attr_id=201, id=1, name="hcm", value_range=vr("0")), False),
        (3, catalog_pb2.Tag(attr_id=201, id=0, name="__any__"), False),
        (3, catalog_pb2.Tag(attr_id=999, id=1, name="wrong_attr"), False),
    ],
)
def test_tags(attr_idx, tag, ok):
    a = EXAMPLE_ATTRIBUTES[attr_idx]
    if ok:
        validate_tag(a, tag)
    else:
        with pytest.raises(CatalogError):
            validate_tag(a, tag)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
