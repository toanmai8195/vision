"""Chạy plan của `planner` trong bộ nhớ — mô hình tối ưu (daily → block → LATEST → range) của một attribute.

Đây là "implementation tối ưu" mà property test so với `reference` (CLAUDE.md §4.4). Bảng StarRocks ở P4
có đúng các bước này; engine giữ chúng trong dict:
`daily` ~ `gold.tag_daily`/`gold.pv_daily`, `blocks` ~ `gold.tag_block`/`gold.pv_block`,
`latest` ~ `gold.tag_latest` + `gold.tag_state_checkpoint`, `ranges` ~ `gold.tag_range_bitmap`/`gold.pv_range_value`,
`usage` ~ `meta.condition_usage` (EXTENDED, §3.6), `tag_dict` ~ `silver.tag_dict`.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from decimal import Decimal

from pyroaring import BitMap

from com.tm.src.temporal.blocks import BlockStore
from com.tm.src.temporal.latest import LatestStore, fold_state
from com.tm.src.temporal.model import (
    ANY_TAG,
    AttributeSpec,
    DateRange,
    Kind,
    LabelDaily,
    ModelError,
    StateDaily,
    StateInterval,
    TagDict,
    TagEvent,
    TagSets,
    TagSums,
    ValueEvent,
    aggregator,
    event_value,
    merge_values,
    reduce_mutex_day,
    reduce_not_mutex_day,
    reduce_pv_day,
    state_delta,
    to_date,
    union_sets,
    validate_intervals,
    validate_tag_event,
    validate_value_event,
)
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
    RangeMode,
    RangeStep,
    Step,
)
from com.tm.src.temporal.ranges import LAST_N, Window, WindowRef, label_window, pv_tag_bitmaps, resolve_window, window_of

KEEP_RANGE_DAYS = 7


class DqError(RuntimeError):
    """Invariant DQ mức BLOCK bị vi phạm (CLAUDE.md §9)."""


# --------------------------------------------------------------------------- silver (input)


class Silver:
    """Silver của một attribute *như đã biết tại thời điểm chạy* (late data được thêm dần)."""

    def __init__(self, kind: Kind):
        self.kind = kind
        self._tag_events: dict[int, list[TagEvent]] = defaultdict(list)
        self._value_events: dict[int, list[ValueEvent]] = defaultdict(list)
        self._intervals: list[StateInterval] = []

    def add_tag_events(self, events: Iterable[TagEvent]) -> None:
        for ev in events:
            validate_tag_event(self.kind, ev)
            self._tag_events[ev.ds].append(ev)

    def add_value_events(self, events: Iterable[ValueEvent]) -> None:
        for ev in events:
            validate_value_event(self.kind, ev)
            self._value_events[ev.ds].append(ev)

    def set_intervals(self, intervals: Iterable[StateInterval]) -> None:
        intervals = list(intervals)
        validate_intervals(self.kind, intervals)
        self._intervals = intervals

    def tag_events(self, day: int) -> list[TagEvent]:
        return self._tag_events.get(day, [])

    def value_events(self, day: int) -> list[ValueEvent]:
        return self._value_events.get(day, [])

    def intervals(self) -> list[StateInterval]:
        return self._intervals


# --------------------------------------------------------------------------- output


@dataclass
class RangeOutput:
    """Một `(ds, date_range)`: `tags` ~ `tag_range_bitmap`, `sums` ~ `pv_range_value` (chỉ loại PARTIAL_VALUE*).

    `scope`: None = mọi tag; EXTENDED = tập tag đã materialize (tag ngoài scope phải tính on-demand).
    """

    tags: TagSets = field(default_factory=dict)
    sums: TagSums | None = None
    scope: frozenset[int] | None = None

    def covers(self, tag_id: int) -> bool:
        return self.scope is None or tag_id in self.scope

    def restrict(self, scope: frozenset[int]) -> RangeOutput:
        return RangeOutput(
            tags={t: bm for t, bm in self.tags.items() if t in scope},
            sums=None if self.sums is None else {t: m for t, m in self.sums.items() if t in scope},
            scope=scope,
        )


def _or_tagged(a: TagSets, b: TagSets) -> TagSets:
    return union_sets(a, b)


def _block_store(attr: AttributeSpec) -> BlockStore:
    match attr.kind:
        case Kind.MUTEX_EVENT | Kind.NOT_MUTEX_EVENT | Kind.MUTEX_STATE | Kind.NOT_MUTEX_STATE:
            return BlockStore(_or_tagged, dict, lambda v: not v)
        case Kind.PARTIAL_VALUE | Kind.PARTIAL_VALUE_BY_TAG:
            agg = attr.agg_func
            return BlockStore(lambda a, b: merge_values(a, b, agg), dict, lambda v: not v)


_EXPECTED: dict[Kind, tuple[set[DailyTable], BlockSource | None, LatestKind | None]] = {
    Kind.MUTEX_EVENT: ({DailyTable.TAG_DAILY}, BlockSource.ADD_ANY, LatestKind.LATEST),
    Kind.NOT_MUTEX_EVENT: ({DailyTable.TAG_DAILY}, BlockSource.SIG, LatestKind.POS),
    Kind.MUTEX_STATE: ({DailyTable.TAG_STATE_DELTA, DailyTable.TAG_STATE_SNAPSHOT}, BlockSource.REMOVED, LatestKind.STATE),
    Kind.NOT_MUTEX_STATE: (
        {DailyTable.TAG_STATE_DELTA, DailyTable.TAG_STATE_SNAPSHOT},
        BlockSource.REMOVED,
        LatestKind.STATE,
    ),
    Kind.PARTIAL_VALUE: ({DailyTable.PV_DAILY}, BlockSource.PV, None),
    Kind.PARTIAL_VALUE_BY_TAG: ({DailyTable.PV_DAILY}, BlockSource.PV, None),
}


class AttributeEngine:
    def __init__(
        self,
        attr: AttributeSpec,
        silver: Silver,
        *,
        history_start: int,
        universe_of: Callable[[int], BitMap],
        tag_dict: TagDict | None = None,
    ):
        if silver.kind is not attr.kind:
            raise ModelError(f"silver kind {silver.kind} != attribute kind {attr.kind}")
        if attr.is_extended != (tag_dict is not None):
            raise ModelError(f"{attr.name}: tag_dict is required for EXTENDED and only for EXTENDED")
        self.tag_dict = tag_dict
        # ~ meta.condition_usage: date_range → tag_id được segment dùng (chỉ EXTENDED)
        self.usage: dict[int, set[int]] = defaultdict(set)
        self._tag_dict_seen: dict[str, int] = {}
        self.attr = attr
        self.kind = attr.kind
        self.silver = silver
        self.history_start = history_start
        self._universe_of = universe_of
        self.daily: dict[int, LabelDaily | StateDaily | TagSums] = {}
        self.blocks: BlockStore = _block_store(attr)
        self.latest: LatestStore | None = LatestStore(attr.kind, self._daily_of) if attr.kind.is_label else None
        self.ranges: dict[int, dict[int, RangeOutput]] = {}
        self.ds: int | None = None

    # ------------------------------------------------------------------ execute

    def run(self, steps: Iterable[Step]) -> None:
        for step in steps:
            self.execute(step)

    def execute(self, step: Step) -> None:
        tables, block_source, latest_kind = _EXPECTED[self.kind]
        match step:
            case DailyStep(day=day, table=table):
                if table not in tables:
                    raise ModelError(f"{self.kind.value}: unexpected daily table {table}")
                self._daily_step(day, table)
            case BlockStep(source=source, blocks=blocks):
                if source is not block_source:
                    raise ModelError(f"{self.kind.value}: unexpected block source {source}")
                for b in blocks:
                    self.blocks.build(b)
            case FoldStep(day=day, latest=lk):
                if lk is not latest_kind or self.latest is None:
                    raise ModelError(f"{self.kind.value}: unexpected fold {lk}")
                self.latest.fold(day)
            case CheckpointStep(day=day, latest=lk):
                if lk is not latest_kind or self.latest is None:
                    raise ModelError(f"{self.kind.value}: unexpected checkpoint {lk}")
                self.latest.checkpoint(day)
            case RangeStep(ds=ds, date_range=dr, mode=mode, tags=tags):
                if (tags is not None) != self.attr.is_extended:
                    raise ModelError(f"{self.attr.name}: range scope must be set exactly for EXTENDED")
                self._range_step(ds, dr, mode, None if tags is None else frozenset(tags))
            case DqStep(ds=ds, check=check, days=days):
                self._dq_step(ds, check, days)
            case _:
                raise ModelError(f"unknown step {step!r}")

    def _daily_of(self, day: int):
        v = self.daily.get(day)
        if v is not None:
            return v
        match self.kind:
            case Kind.MUTEX_EVENT | Kind.NOT_MUTEX_EVENT:
                return LabelDaily()
            case Kind.MUTEX_STATE | Kind.NOT_MUTEX_STATE:
                return StateDaily()
            case Kind.PARTIAL_VALUE | Kind.PARTIAL_VALUE_BY_TAG:
                return {}

    def _daily_step(self, day: int, table: DailyTable) -> None:
        # Ghi đè nguyên ngày (~ DELETE (ds, attr_id) + INSERT) → idempotent khi chạy lại.
        match self.kind:
            case Kind.MUTEX_EVENT:
                daily = reduce_mutex_day(self.silver.tag_events(day))
                block_value = {ANY_TAG: daily.add[ANY_TAG]} if ANY_TAG in daily.add else {}
            case Kind.NOT_MUTEX_EVENT:
                daily = reduce_not_mutex_day(self.silver.tag_events(day))
                block_value = daily.sig()
            case Kind.MUTEX_STATE | Kind.NOT_MUTEX_STATE:
                snapshot = table is DailyTable.TAG_STATE_SNAPSHOT
                daily = state_delta(self.silver.intervals(), day, snapshot=snapshot)
                block_value = dict(daily.removed)
            case Kind.PARTIAL_VALUE | Kind.PARTIAL_VALUE_BY_TAG:
                daily = reduce_pv_day(self.kind, self.silver.value_events(day), self.attr.agg_func)
                block_value = daily
        self.daily[day] = daily
        self.blocks.set_day(day, block_value)

    # ------------------------------------------------------------------ range

    def compute(self, w: Window, scope: frozenset[int] | None = None) -> RangeOutput:
        """Kết quả của window bất kỳ (precompute hoặc custom on-demand); `scope` giới hạn tag (EXTENDED)."""
        out = self._compute(w, scope)
        return out if scope is None else out.restrict(scope)

    def _compute(self, w: Window, scope: frozenset[int] | None) -> RangeOutput:
        clamped = w.clamp(self.history_start)
        match self.kind:
            case Kind.MUTEX_EVENT | Kind.NOT_MUTEX_EVENT:
                if clamped is None:
                    return RangeOutput()
                latest_r = self.latest.state_at(w.r)
                if scope is not None:  # chỉ đụng tag trong scope — chi phí tỉ lệ tag được dùng
                    latest_r = {t: bm for t, bm in latest_r.items() if t in scope}
                window_blocks = None if w.always_active else self.blocks.window(clamped.l, clamped.r)
                return RangeOutput(tags=label_window(self.kind, latest_r, window_blocks))
            case Kind.MUTEX_STATE | Kind.NOT_MUTEX_STATE:
                if clamped is None:
                    return RangeOutput()
                return RangeOutput(tags=label_window(self.kind, self.latest.state_at(w.r), None))
            case Kind.PARTIAL_VALUE | Kind.PARTIAL_VALUE_BY_TAG:
                if clamped is None:
                    return RangeOutput(sums={})
                sums = self.blocks.window(clamped.l, clamped.r)
                return RangeOutput(tags=pv_tag_bitmaps(self.kind, sums, self.attr.value_ranges), sums=sums)

    def _range_step(self, ds: int, dr: int, mode: RangeMode, scope: frozenset[int] | None) -> None:
        if dr not in self.attr.supported_date_ranges:
            raise ModelError(f"{self.attr.name}: {dr} not in supported_date_ranges")
        out = None
        if mode is RangeMode.CARRY_FORWARD:
            prev = self.ranges.get(ds - 1, {}).get(dr)
            # Không có bản ds−1 (engine mới khởi động giữa tháng) hoặc scope EXTENDED mở rộng → tính lại.
            if prev is not None and (scope is None or (prev.scope is not None and scope <= prev.scope)):
                out = prev if scope is None else prev.restrict(scope)
        if out is None:
            out = self.compute(window_of(dr, ds), scope)
        self.ranges.setdefault(ds, {})[dr] = out
        if self.ds is None or ds > self.ds:
            self.ds = ds
        for d in [d for d in self.ranges if d <= self.ds - KEEP_RANGE_DAYS]:
            del self.ranges[d]

    def query(self, ref: WindowRef, ds: int) -> RangeOutput:
        """Date range chuẩn → bản đã materialize; custom range → tính on-demand."""
        if isinstance(ref, int):
            try:
                return self.ranges[ds][ref]
            except KeyError:
                raise ModelError(f"{self.attr.name}: range {ref} not materialized for {to_date(ds)}") from None
        return self.compute(resolve_window(ref, ds))

    def query_tag(self, ref: WindowRef, ds: int, tag_id: int) -> RangeOutput:
        """Như `query` nhưng tag EXTENDED chưa materialize → tính on-demand và ghi usage (§3.6)."""
        out = self.query(ref, ds)
        if out.covers(tag_id):
            return out
        if isinstance(ref, int):
            self.usage[ref].add(tag_id)  # ~ ghi meta.condition_usage → ngày sau precompute
        return self.compute(resolve_window(ref, ds), frozenset({tag_id}))

    def usage_snapshot(self) -> dict[int, frozenset[int]]:
        return {dr: frozenset(tags) for dr, tags in self.usage.items()}

    # ConditionSource (segment evaluator)
    def extended_tag_id(self, tag_string: str) -> int | None:
        if self.tag_dict is None:
            raise ModelError(f"{self.attr.name}: not an EXTENDED attribute")
        return self.tag_dict.lookup(tag_string)

    def tag_bitmap(self, ref: WindowRef, ds: int, tag_id: int) -> BitMap:
        return self.query_tag(ref, ds, tag_id).tags.get(tag_id, BitMap())

    def pv_sums(self, ref: WindowRef, ds: int, tag_id: int) -> dict[int, Decimal]:
        sums = self.query_tag(ref, ds, tag_id).sums
        if sums is None:
            raise ModelError(f"{self.attr.name}: {self.kind.value} has no partial value")
        return sums.get(tag_id, {})

    # ------------------------------------------------------------------ DQ (§9)

    def _dq_step(self, ds: int, check: DqCheck, days: tuple[int, ...]) -> None:
        name = f"{self.attr.name} {to_date(ds)} {check.name}"
        match check:
            case DqCheck.MUTEX_ADD_SUM:
                for d in days:
                    daily = self._daily_of(d)
                    total = sum(len(bm) for t, bm in daily.add.items() if t != ANY_TAG)
                    if total != len(daily.add.get(ANY_TAG, BitMap())):
                        raise DqError(f"{name}: day {to_date(d)}")
            case DqCheck.NOT_MUTEX_ADD_DEL_DISJOINT:
                for d in days:
                    daily = self._daily_of(d)
                    for t, bm in daily.add.items():
                        if bm & daily.dele.get(t, BitMap()):
                            raise DqError(f"{name}: day {to_date(d)} tag {t}")
            case DqCheck.MUTEX_WINDOW_DISJOINT:
                for dr, out in self.ranges.get(ds, {}).items():
                    if sum(len(bm) for bm in out.tags.values()) != len(BitMap.union(BitMap(), *out.tags.values())):
                        raise DqError(f"{name}: range {dr}")
            case DqCheck.WINDOW_MONOTONIC:
                outs = self.ranges.get(ds, {})
                chain = sorted((dr for dr in outs if dr in LAST_N), key=LAST_N.__getitem__)
                chain += [dr for dr in outs if dr == DateRange.ALWAYS_ACTIVE]
                for a, b in zip(chain, chain[1:]):
                    tags = set(outs[a].tags) | set(outs[b].tags)
                    for t in (t for t in tags if outs[a].covers(t) and outs[b].covers(t)):
                        if len(outs[a].tags.get(t, BitMap())) > len(outs[b].tags.get(t, BitMap())):
                            raise DqError(f"{name}: tag {t} {a} > {b}")
            case DqCheck.STATE_CONSISTENCY:
                for d in days:
                    prev = self.latest.state_at(d - 1) if d > self.history_start else {}
                    if fold_state(prev, self._daily_of(d)) != self.latest.state_at(d):
                        raise DqError(f"{name}: day {to_date(d)}")
            case DqCheck.PV_TOTAL | DqCheck.PV_TOTAL_BY_TAG:
                # AGG theo aggFunc: SUM/COUNT so tổng; MIN/MAX so min/max (§9).
                op = aggregator(self.attr.agg_func)
                for d in days:
                    silver: dict[int, Decimal] = {}
                    for ev in self.silver.value_events(d):
                        v = event_value(self.attr.agg_func, ev.value)
                        silver[ev.tag] = op(silver[ev.tag], v) if ev.tag in silver else v
                    gold: dict[int, Decimal] = {}
                    for t, m in self._daily_of(d).items():
                        for v in m.values():
                            gold[t] = op(gold[t], v) if t in gold else v
                    if check is DqCheck.PV_TOTAL:
                        ok = _fold(op, silver.values()) == _fold(op, gold.values())
                    else:
                        ok = silver == gold
                    if not ok:
                        raise DqError(f"{name}: day {to_date(d)}")
            case DqCheck.TAG_DICT_APPEND_ONLY:
                snapshot = self.tag_dict.snapshot()
                if any(snapshot.get(k) != v for k, v in self._tag_dict_seen.items()):
                    raise DqError(f"{name}: tag_dict mapping changed or removed")
                self._tag_dict_seen = snapshot
            case DqCheck.UIDX_IN_UNIVERSE:
                universe = self._universe_of(ds)
                for d in days:
                    for bm in self._daily_uidx(d):
                        if not bm.issubset(universe):
                            raise DqError(f"{name}: day {to_date(d)} uidx {sorted(bm - universe)[:5]}")
            case _:
                raise ModelError(f"unknown DQ check {check}")

    def _daily_uidx(self, day: int) -> Iterable[BitMap]:
        daily = self._daily_of(day)
        match daily:
            case LabelDaily():
                return [*daily.add.values(), *daily.dele.values()]
            case StateDaily():
                return [*daily.added.values(), *daily.removed.values()]
            case dict():
                return [BitMap(m.keys()) for m in daily.values()]
        raise ModelError(f"unknown daily {daily!r}")


def _fold(op: Callable[[Decimal, Decimal], Decimal], values: Iterable[Decimal]) -> Decimal | None:
    acc = None
    for v in values:
        acc = v if acc is None else op(acc, v)
    return acc
