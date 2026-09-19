"""Reference implementation — cách tính ngây thơ đúng định nghĩa CLAUDE.md §3.2 / §3.4.

Duyệt toàn bộ event trong window, lấy signal gần nhất theo `(event_ts, event_id)`, SUM. Không dùng
daily/block/LATEST. Mọi implementation tối ưu (engine, SQL P4) phải khớp với module này (§4.4).
Chỉ dùng cho test: độ phức tạp O(#event × #window).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal

from pyroaring import BitMap

from com.tm.src.temporal.model import (
    ANY_TAG,
    DAY_S,
    AttributeSpec,
    Kind,
    ModelError,
    StateInterval,
    TagEvent,
    TagSets,
    TagSums,
    ValueEvent,
    drop_empty,
    ds_of_ts_ms,
    ts_ms_of,
    users_in_range,
    validate_intervals,
    validate_tag_event,
    validate_value_event,
)
from com.tm.src.temporal.ranges import WindowRef, resolve_window


@dataclass(frozen=True)
class _Signal:
    key: tuple[int, str]  # (event_ts, event_id)
    day: int
    uidx: int
    tag: int
    is_add: bool


def _signals_of(events: Iterable[TagEvent]) -> list[_Signal]:
    out = []
    for ev in events:
        for t in ev.tags_add:
            out.append(_Signal(ev.order_key, ev.ds, ev.uidx, t, True))
        for t in ev.tags_remove:
            out.append(_Signal(ev.order_key, ev.ds, ev.uidx, t, False))
    return out


def _state_signals(intervals: list[StateInterval], l: int, r: int) -> list[_Signal]:
    """STATE (§3.4): giá trị đang giữ được coi là ADD lại mỗi ngày (cuối ngày); mất giá trị = REMOVE (đầu ngày)."""
    out = []
    held = {(iv.uidx, iv.tag, d) for iv in intervals for d in range(max(l, iv.valid_from), (r if iv.valid_to is None else min(r, iv.valid_to - 1)) + 1)}
    for uidx, tag, d in held:
        out.append(_Signal((ts_ms_of(d, DAY_S - 1), "~state"), d, uidx, tag, True))
    for iv in intervals:
        d = iv.valid_to
        if d is not None and l <= d <= r and (iv.uidx, iv.tag, d) not in held:
            out.append(_Signal((ts_ms_of(d, 0), "~state"), d, iv.uidx, iv.tag, False))
    return out


def _mutex(signals: Iterable[_Signal]) -> TagSets:
    """User ∈ t ⇔ ADD gần nhất trong window là t và không có REMOVE t sau lần ADD đó."""
    by_user: dict[int, list[_Signal]] = defaultdict(list)
    for s in signals:
        by_user[s.uidx].append(s)
    out: TagSets = defaultdict(BitMap)
    for uidx, sigs in by_user.items():
        adds = [s for s in sigs if s.is_add]
        if not adds:
            continue
        last = max(adds, key=lambda s: s.key)
        if not any(not s.is_add and s.tag == last.tag and s.key > last.key for s in sigs):
            out[last.tag].add(uidx)
    return dict(out)


def _not_mutex(signals: Iterable[_Signal]) -> TagSets:
    """User ∈ t ⇔ signal gần nhất của t trong window là ADD."""
    last: dict[tuple[int, int], _Signal] = {}
    for s in signals:
        k = (s.uidx, s.tag)
        if k not in last or s.key > last[k].key:
            last[k] = s
    out: TagSets = defaultdict(BitMap)
    for (uidx, tag), s in last.items():
        if s.is_add:
            out[tag].add(uidx)
    return dict(out)


class Reference:
    def __init__(
        self,
        attr: AttributeSpec,
        *,
        tag_events: Iterable[TagEvent] = (),
        value_events: Iterable[ValueEvent] = (),
        intervals: Iterable[StateInterval] = (),
    ):
        self.attr = attr
        self.kind = attr.kind
        tag_events, value_events, intervals = list(tag_events), list(value_events), list(intervals)
        for ev in tag_events:
            validate_tag_event(self.kind, ev)
        for ev in value_events:
            validate_value_event(self.kind, ev)
        if intervals:
            validate_intervals(self.kind, intervals)
        self._signals = _signals_of(tag_events)
        self._values = value_events
        self._intervals = intervals

    def _bounds(self, ref: WindowRef, ds: int) -> tuple[int, int]:
        w = resolve_window(ref, ds)
        if not w.always_active:
            return w.l, w.r
        days = [s.day for s in self._signals] + [ev.ds for ev in self._values] + [iv.valid_from for iv in self._intervals]
        return (min(days, default=w.r), w.r)

    def members(self, l: int, r: int) -> TagSets:
        """Tag bitmap của window `[l, r]` (MUTEX / NOT_MUTEX / bucket PARTIAL_VALUE)."""
        match self.kind:
            case Kind.MUTEX_EVENT:
                return _mutex(s for s in self._signals if l <= s.day <= r)
            case Kind.NOT_MUTEX_EVENT:
                return _not_mutex(s for s in self._signals if l <= s.day <= r)
            case Kind.MUTEX_STATE:
                return _mutex(_state_signals(self._intervals, l, r))
            case Kind.NOT_MUTEX_STATE:
                return _not_mutex(_state_signals(self._intervals, l, r))
            case Kind.PARTIAL_VALUE:
                by_user = self.sums(l, r).get(ANY_TAG, {})
                return drop_empty({t: users_in_range(by_user, vr) for t, vr in self.attr.value_ranges.items()})
            case Kind.PARTIAL_VALUE_BY_TAG:
                return {}

    def sums(self, l: int, r: int) -> TagSums:
        """SUM value trong `[l, r]` theo (tag, uidx) — user không có event thì không có mặt."""
        match self.kind:
            case Kind.PARTIAL_VALUE | Kind.PARTIAL_VALUE_BY_TAG:
                out: TagSums = {}
                for ev in self._values:
                    if l <= ds_of_ts_ms(ev.ts_ms) <= r:
                        m = out.setdefault(ev.tag, {})
                        m[ev.uidx] = m.get(ev.uidx, Decimal(0)) + ev.value
                return out
            case Kind.MUTEX_EVENT | Kind.MUTEX_STATE | Kind.NOT_MUTEX_EVENT | Kind.NOT_MUTEX_STATE:
                raise ModelError(f"{self.kind.value} has no partial value")

    # ConditionSource (segment evaluator)
    def tag_bitmap(self, ref: WindowRef, ds: int, tag_id: int) -> BitMap:
        return self.members(*self._bounds(ref, ds)).get(tag_id, BitMap())

    def pv_sums(self, ref: WindowRef, ds: int, tag_id: int) -> dict[int, Decimal]:
        return self.sums(*self._bounds(ref, ds)).get(tag_id, {})
