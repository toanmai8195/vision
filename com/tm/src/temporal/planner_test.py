"""Unit test planner: thứ tự bước, bước theo từng loại, checkpoint, late data, LAST_MONTH đóng băng."""

import sys
from decimal import Decimal

import pytest

from com.tm.src.temporal.model import AttributeSpec, DateRange, Kind, ValueRange, parse_day
from com.tm.src.temporal.planner import (
    BlockSource,
    BlockStep,
    CheckpointStep,
    DailyStep,
    DailyTable,
    DqCheck,
    DqStep,
    FoldStep,
    LatestKind,
    PlanError,
    RangeMode,
    RangeStep,
    plan,
    plan_backfill,
)

DS = parse_day("2026-09-15")  # thứ Ba
SUNDAY = parse_day("2026-09-13")
START = parse_day("2026-01-01")
RANGES = frozenset({DateRange.A1, DateRange.A30, DateRange.A7, DateRange.LAST_MONTH, DateRange.ALWAYS_ACTIVE})


def attr(kind: Kind) -> AttributeSpec:
    vr = {1: ValueRange(from_value=Decimal(0))} if kind is Kind.PARTIAL_VALUE else {}
    return AttributeSpec(1, "a", kind, {"t": 1}, RANGES, vr)


EXPECTED = {
    Kind.MUTEX_EVENT: (DailyTable.TAG_DAILY, BlockSource.ADD_ANY, LatestKind.LATEST),
    Kind.NOT_MUTEX_EVENT: (DailyTable.TAG_DAILY, BlockSource.SIG, LatestKind.POS),
    Kind.MUTEX_STATE: (DailyTable.TAG_STATE_DELTA, BlockSource.REMOVED, LatestKind.STATE),
    Kind.NOT_MUTEX_STATE: (DailyTable.TAG_STATE_DELTA, BlockSource.REMOVED, LatestKind.STATE),
    Kind.PARTIAL_VALUE: (DailyTable.PV_DAILY, BlockSource.PV, None),
    Kind.PARTIAL_VALUE_BY_TAG: (DailyTable.PV_DAILY, BlockSource.PV, None),
}

ORDER = [DailyStep, BlockStep, FoldStep, CheckpointStep, RangeStep, DqStep]


def test_expected_covers_all_kinds():
    assert set(EXPECTED) == set(Kind)


@pytest.mark.parametrize("kind", list(Kind), ids=lambda k: k.value)
@pytest.mark.parametrize("ds", [DS, SUNDAY])
def test_plan_per_kind(kind, ds):
    table, source, latest = EXPECTED[kind]
    steps = plan(ds, attr(kind), history_start=START)
    # thứ tự: daily → block → fold → checkpoint → range → DQ
    assert [ORDER.index(type(s)) for s in steps] == sorted(ORDER.index(type(s)) for s in steps)
    assert [s for s in steps if isinstance(s, DailyStep)] == [DailyStep(ds, table)]
    (block,) = [s for s in steps if isinstance(s, BlockStep)]
    assert block.source is source
    folds = [s for s in steps if isinstance(s, FoldStep)]
    checkpoints = [s for s in steps if isinstance(s, CheckpointStep)]
    if latest is None:
        assert not folds and not checkpoints
    else:
        assert folds == [FoldStep(ds, latest)]
        assert checkpoints == ([CheckpointStep(ds, latest)] if ds == SUNDAY else [])
    ranges = [s.date_range for s in steps if isinstance(s, RangeStep)]
    assert ranges == [DateRange.A1, DateRange.A7, DateRange.A30, DateRange.LAST_MONTH, DateRange.ALWAYS_ACTIVE]
    checks = {s.check for s in steps if isinstance(s, DqStep)}
    assert DqCheck.UIDX_IN_UNIVERSE in checks
    match kind:
        case Kind.MUTEX_EVENT:
            assert {DqCheck.MUTEX_ADD_SUM, DqCheck.MUTEX_WINDOW_DISJOINT, DqCheck.WINDOW_MONOTONIC} <= checks
        case Kind.NOT_MUTEX_EVENT:
            assert {DqCheck.NOT_MUTEX_ADD_DEL_DISJOINT, DqCheck.WINDOW_MONOTONIC} <= checks
        case Kind.MUTEX_STATE:
            assert {DqCheck.STATE_CONSISTENCY, DqCheck.MUTEX_WINDOW_DISJOINT} <= checks
        case Kind.NOT_MUTEX_STATE:
            assert DqCheck.STATE_CONSISTENCY in checks
        case Kind.PARTIAL_VALUE:
            assert DqCheck.PV_TOTAL in checks
        case Kind.PARTIAL_VALUE_BY_TAG:
            assert DqCheck.PV_TOTAL_BY_TAG in checks


def test_blocks_for_ds():
    (block,) = [s for s in plan(DS, attr(Kind.MUTEX_EVENT), history_start=START) if isinstance(s, BlockStep)]
    # e(09-15) = 20711 → (20711+1) chia hết cho 2, 4, 8
    assert block.blocks == ((1, 20710), (2, 20708), (3, 20704))


@pytest.mark.parametrize("kind", [Kind.MUTEX_STATE, Kind.NOT_MUTEX_STATE])
def test_state_bootstrap_uses_snapshot(kind):
    steps = plan(START, attr(kind), history_start=START)
    assert steps[0] == DailyStep(START, DailyTable.TAG_STATE_SNAPSHOT)
    late = plan(START + 2, attr(kind), history_start=START, reprocess_from=START)
    assert [s for s in late if isinstance(s, DailyStep)] == [
        DailyStep(START, DailyTable.TAG_STATE_SNAPSHOT),
        DailyStep(START + 1, DailyTable.TAG_STATE_DELTA),
        DailyStep(START + 2, DailyTable.TAG_STATE_DELTA),
    ]


@pytest.mark.parametrize("kind", list(Kind), ids=lambda k: k.value)
def test_late_data_reprocess(kind):
    steps = plan(DS, attr(kind), history_start=START, reprocess_from=DS - 3)
    assert [s.day for s in steps if isinstance(s, DailyStep)] == [DS - 3, DS - 2, DS - 1, DS]
    (block,) = [s for s in steps if isinstance(s, BlockStep)]
    # mọi block chứa một ngày trong [ds−3, ds] và đã đóng, theo (k, s) → con trước cha
    assert list(block.blocks) == sorted(block.blocks)
    assert (0, DS - 3) not in block.blocks and (1, DS - 3) in block.blocks and (3, DS - 7) in block.blocks
    folds = [s.day for s in steps if isinstance(s, FoldStep)]
    assert folds == ([DS - 3, DS - 2, DS - 1, DS] if EXPECTED[kind][2] else [])
    if EXPECTED[kind][2]:
        assert [s.day for s in steps if isinstance(s, CheckpointStep)] == [SUNDAY]
    with pytest.raises(PlanError):
        plan(DS, attr(kind), history_start=START, reprocess_from=DS - 4)


def test_last_month_frozen():
    a = attr(Kind.MUTEX_EVENT)

    def mode(ds, **kw):
        (s,) = [s for s in plan(ds, a, history_start=START, **kw) if isinstance(s, RangeStep) and s.date_range == DateRange.LAST_MONTH]
        return s.mode

    assert mode(parse_day("2026-09-01")) is RangeMode.COMPUTE
    assert mode(parse_day("2026-09-02")) is RangeMode.CARRY_FORWARD
    # late data rơi vào tháng trước → tính lại
    assert mode(parse_day("2026-09-02"), reprocess_from=parse_day("2026-08-31")) is RangeMode.COMPUTE
    assert mode(parse_day("2026-09-04"), reprocess_from=parse_day("2026-09-01")) is RangeMode.CARRY_FORWARD


def test_plan_errors():
    a = attr(Kind.MUTEX_EVENT)
    with pytest.raises(PlanError):
        plan(START - 1, a, history_start=START)
    with pytest.raises(PlanError):
        plan(DS, a, history_start=START, reprocess_from=DS + 1)


def test_plan_backfill():
    a = attr(Kind.NOT_MUTEX_EVENT)
    steps = plan_backfill(DS - 9, DS, a, history_start=START)
    assert [s.day for s in steps if isinstance(s, DailyStep)] == list(range(DS - 9, DS + 1))
    assert steps == [s for d in range(DS - 9, DS + 1) for s in plan(d, a, history_start=START)]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
