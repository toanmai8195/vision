"""Planner (khung P1): `(ds, attribute)` → danh sách bước theo thứ tự (CLAUDE.md §6 Temporal + Range).

Thứ tự: daily → block → LATEST/POS/STATE → checkpoint (Chủ nhật) → range → DQ.
P1 chỉ sinh bước (chưa sinh SQL); `engine.AttributeEngine` chạy các bước này trong bộ nhớ, nên property test
kiểm luôn cả planner. P4 sinh SQL StarRocks cho cùng các bước.

Late data (≤ 3 ngày): `reprocess_from = d` → làm lại daily `d..ds`, build lại mọi block chứa các ngày đó,
fold lại LATEST/STATE từ `d` tới `ds`. Cũ hơn 3 ngày → `plan_backfill`.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass

from com.tm.src.temporal.blocks import Block, blocks_closed_by
from com.tm.src.temporal.latest import is_checkpoint_day
from com.tm.src.temporal.model import AttributeSpec, DateRange, Kind, ModelError, to_date
from com.tm.src.temporal.ranges import ALL_DATE_RANGES, LAST_N, window_of

LATE_DATA_MAX_DAYS = 3


class PlanError(ModelError):
    pass


class DailyTable(enum.Enum):
    TAG_DAILY = "tag_daily"  # ADD / DEL (EVENT)
    TAG_STATE_DELTA = "tag_state_delta"  # ADDED / REMOVED (STATE)
    TAG_STATE_SNAPSHOT = "tag_state_snapshot"  # bootstrap STATE: ADDED = STATE(ds)
    PV_DAILY = "pv_daily"


class BlockSource(enum.Enum):
    ADD_ANY = "ADD(d,0)"  # MUTEX EVENT
    SIG = "SIG(d,t)"  # NOT_MUTEX EVENT
    PV = "pv_daily"  # PARTIAL_VALUE, PARTIAL_VALUE_BY_TAG
    REMOVED = "REMOVED(d,t)"  # STATE — CLAUDE.md §4.2 "chỉ cho custom range lịch sử"


class LatestKind(enum.Enum):
    LATEST = "LATEST"  # MUTEX EVENT
    POS = "POS"  # NOT_MUTEX EVENT
    STATE = "STATE"  # STATE


class RangeMode(enum.Enum):
    COMPUTE = "COMPUTE"
    CARRY_FORWARD = "CARRY_FORWARD"  # LAST_MONTH đóng băng: copy bản của ds − 1


class DqCheck(enum.Enum):
    """CLAUDE.md §9 (chỉ các check BLOCK tính được từ dữ liệu của attribute)."""

    MUTEX_ADD_SUM = "Σ cnt(ADD(d,t)) == cnt(ADD(d,0))"
    NOT_MUTEX_ADD_DEL_DISJOINT = "ADD(d,t) ∩ DEL(d,t) = ∅"
    MUTEX_WINDOW_DISJOINT = "tag rời nhau trong mỗi window"
    WINDOW_MONOTONIC = "cnt(A7) ≤ cnt(A30) ≤ … ≤ cnt(ALWAYS_ACTIVE)"
    STATE_CONSISTENCY = "STATE(d) == (STATE(d−1) − REMOVED) ∪ ADDED"
    PV_TOTAL = "Σ pv_daily(d) == Σ silver(d)"
    PV_TOTAL_BY_TAG = "Σ pv_daily(d, tag) == Σ silver(d, tag)"
    UIDX_IN_UNIVERSE = "uidx ⊆ UNIVERSE(ds)"


@dataclass(frozen=True)
class DailyStep:
    day: int
    table: DailyTable


@dataclass(frozen=True)
class BlockStep:
    source: BlockSource
    blocks: tuple[Block, ...]
    """Theo thứ tự k tăng dần."""


@dataclass(frozen=True)
class FoldStep:
    day: int
    latest: LatestKind


@dataclass(frozen=True)
class CheckpointStep:
    day: int
    latest: LatestKind


@dataclass(frozen=True)
class RangeStep:
    ds: int
    date_range: int
    mode: RangeMode


@dataclass(frozen=True)
class DqStep:
    ds: int
    check: DqCheck
    days: tuple[int, ...]
    """Các ngày daily vừa được (re)compute — check theo ngày chạy trên các ngày này."""


Step = DailyStep | BlockStep | FoldStep | CheckpointStep | RangeStep | DqStep


@dataclass(frozen=True)
class _Profile:
    daily: DailyTable
    block_source: BlockSource | None
    latest: LatestKind | None
    dq: tuple[DqCheck, ...]


def _profile(kind: Kind) -> _Profile:
    match kind:
        case Kind.MUTEX_EVENT:
            return _Profile(
                DailyTable.TAG_DAILY,
                BlockSource.ADD_ANY,
                LatestKind.LATEST,
                (DqCheck.MUTEX_ADD_SUM, DqCheck.MUTEX_WINDOW_DISJOINT, DqCheck.WINDOW_MONOTONIC),
            )
        case Kind.NOT_MUTEX_EVENT:
            return _Profile(
                DailyTable.TAG_DAILY,
                BlockSource.SIG,
                LatestKind.POS,
                (DqCheck.NOT_MUTEX_ADD_DEL_DISJOINT, DqCheck.WINDOW_MONOTONIC),
            )
        case Kind.MUTEX_STATE:
            return _Profile(
                DailyTable.TAG_STATE_DELTA,
                BlockSource.REMOVED,
                LatestKind.STATE,
                (DqCheck.STATE_CONSISTENCY, DqCheck.MUTEX_WINDOW_DISJOINT),
            )
        case Kind.NOT_MUTEX_STATE:
            return _Profile(
                DailyTable.TAG_STATE_DELTA,
                BlockSource.REMOVED,
                LatestKind.STATE,
                (DqCheck.STATE_CONSISTENCY,),
            )
        case Kind.PARTIAL_VALUE:
            return _Profile(DailyTable.PV_DAILY, BlockSource.PV, None, (DqCheck.PV_TOTAL,))
        case Kind.PARTIAL_VALUE_BY_TAG:
            return _Profile(DailyTable.PV_DAILY, BlockSource.PV, None, (DqCheck.PV_TOTAL_BY_TAG,))


def _range_order(dr: int) -> tuple[int, int]:
    return (0, LAST_N[dr]) if dr in LAST_N else (1, ALL_DATE_RANGES.index(dr))


def plan(ds: int, attr: AttributeSpec, *, history_start: int, reprocess_from: int | None = None) -> list[Step]:
    """Các bước cho ngày `ds` của một attribute.

    - `history_start`: ngày đầu lịch sử của attribute; ngày đó STATE nạp snapshot thay vì delta.
    - `reprocess_from = d` (`ds − 3 ≤ d < ds`): silver của các ngày `d..ds−1` đổi (late data).
    """
    p = _profile(attr.kind)
    if ds < history_start:
        raise PlanError(f"ds {to_date(ds)} < history_start {to_date(history_start)}")
    first = ds if reprocess_from is None else max(reprocess_from, history_start)
    if first > ds:
        raise PlanError(f"reprocess_from {to_date(first)} > ds {to_date(ds)}")
    if ds - first > LATE_DATA_MAX_DAYS:
        raise PlanError(f"late data older than {LATE_DATA_MAX_DAYS} days ({to_date(first)}) → use plan_backfill")
    days = tuple(range(first, ds + 1))

    steps: list[Step] = []
    for d in days:
        table = p.daily
        if d == history_start and table is DailyTable.TAG_STATE_DELTA:
            table = DailyTable.TAG_STATE_SNAPSHOT
        steps.append(DailyStep(d, table))

    if p.block_source is not None:
        # Block chứa ngày d và đã đóng tại ds ⇔ block kết thúc trong [d, ds] ⇔ ⋃ closed_by(d'), d' ∈ [d, ds].
        # Sắp theo (k, s): block con luôn được build trước block cha.
        blocks = sorted(b for d in days for b in blocks_closed_by(d))
        steps.append(BlockStep(p.block_source, tuple(blocks)))

    if p.latest is not None:
        for d in days:
            steps.append(FoldStep(d, p.latest))
        for d in days:
            if is_checkpoint_day(d):
                steps.append(CheckpointStep(d, p.latest))

    first_of_month = to_date(ds).day == 1
    for dr in sorted(attr.supported_date_ranges, key=_range_order):
        mode = RangeMode.COMPUTE
        if dr == DateRange.LAST_MONTH and not first_of_month:
            # Đóng băng từ ngày 1; chỉ tính lại khi late data rơi vào tháng trước.
            if first > window_of(DateRange.LAST_MONTH, ds).r:
                mode = RangeMode.CARRY_FORWARD
        steps.append(RangeStep(ds, dr, mode))

    for check in (*p.dq, DqCheck.UIDX_IN_UNIVERSE):
        steps.append(DqStep(ds, check, days))
    return steps


def plan_backfill(from_day: int, to_day: int, attr: AttributeSpec, *, history_start: int) -> list[Step]:
    """Chạy tuần tự từng ngày (CLAUDE.md §7 `vision_backfill`).

    `to_day` phải là `ds` mới nhất đã chạy: LATEST/STATE của mọi ngày sau `from_day` phải được fold lại.
    """
    if from_day > to_day:
        raise PlanError("from_day > to_day")
    steps: list[Step] = []
    for d in range(from_day, to_day + 1):
        steps.extend(plan(d, attr, history_start=history_start))
    return steps
