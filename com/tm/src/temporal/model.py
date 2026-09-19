"""Mô hình dữ liệu thuần + reduce trong ngày (L3) — CLAUDE.md §3, §4.1.

- Ngày = `e(ds)`: số ngày từ 1970-01-01 (int). `ds` suy ra từ `event_ts` theo Asia/Ho_Chi_Minh.
- Tập user = `pyroaring.BitMap` chứa `uidx`.
- Tag = `tag_id` (int ≥ 1); `tag_id = 0` là pseudo-tag `__any__` (ADD(d,0) của MUTEX, tag của PARTIAL_VALUE).
"""

from __future__ import annotations

import datetime as dt
import enum
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from pyroaring import BitMap

from com.tm.proto.vision.catalog.v1 import catalog_pb2

DataType = catalog_pb2.DataType
FeedMode = catalog_pb2.FeedMode
DateRange = catalog_pb2.DateRange

ANY_TAG = 0
EPOCH = dt.date(1970, 1, 1)
ICT_OFFSET_S = 7 * 3600
DAY_S = 86400

TagSets = dict[int, BitMap]
"""tag_id → tập uidx."""

TagSums = dict[int, dict[int, Decimal]]
"""tag_id → (uidx → SUM value). User có mặt ⇔ có ít nhất một event trong phạm vi (kể cả khi SUM = 0)."""


class ModelError(ValueError):
    """Dữ liệu đầu vào vi phạm luật của mô hình."""


# --------------------------------------------------------------------------- ngày


def epoch_day(d: dt.date) -> int:
    return (d - EPOCH).days


def to_date(e: int) -> dt.date:
    return EPOCH + dt.timedelta(days=e)


def parse_day(s: str) -> int:
    return epoch_day(dt.date.fromisoformat(s))


def ds_of_ts_ms(ts_ms: int) -> int:
    """`ds` (epoch day) của một timestamp UTC theo giờ Asia/Ho_Chi_Minh (UTC+7, không DST)."""
    return (ts_ms // 1000 + ICT_OFFSET_S) // DAY_S


def ts_ms_of(day: int, second_of_day: int = 0) -> int:
    """Timestamp UTC (ms) của giây `second_of_day` (giờ ICT) trong ngày `day`."""
    if not 0 <= second_of_day < DAY_S:
        raise ModelError(f"second_of_day out of range: {second_of_day}")
    return (day * DAY_S - ICT_OFFSET_S + second_of_day) * 1000


def parse_ts_ms(s: str) -> int:
    """ISO-8601 có timezone → epoch millis."""
    t = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    if t.tzinfo is None:
        raise ModelError(f"timestamp without timezone: {s}")
    return int(t.timestamp() * 1000)


# --------------------------------------------------------------------------- loại dữ liệu


class Kind(enum.Enum):
    """(dataType, feedMode) — đơn vị rẽ nhánh exhaustive ở mọi layer (CLAUDE.md §11)."""

    MUTEX_EVENT = "MUTEX_EVENT"
    MUTEX_STATE = "MUTEX_STATE"
    NOT_MUTEX_EVENT = "NOT_MUTEX_EVENT"
    NOT_MUTEX_STATE = "NOT_MUTEX_STATE"
    PARTIAL_VALUE = "PARTIAL_VALUE"
    PARTIAL_VALUE_BY_TAG = "PARTIAL_VALUE_BY_TAG"

    @property
    def is_label(self) -> bool:
        return self in LABEL_KINDS

    @property
    def is_state(self) -> bool:
        return self in (Kind.MUTEX_STATE, Kind.NOT_MUTEX_STATE)

    @property
    def is_mutex(self) -> bool:
        return self in (Kind.MUTEX_EVENT, Kind.MUTEX_STATE)


LABEL_KINDS = frozenset({Kind.MUTEX_EVENT, Kind.MUTEX_STATE, Kind.NOT_MUTEX_EVENT, Kind.NOT_MUTEX_STATE})
VALUE_KINDS = frozenset({Kind.PARTIAL_VALUE, Kind.PARTIAL_VALUE_BY_TAG})


def kind_of(data_type: int, feed_mode: int) -> Kind:
    match (data_type, feed_mode):
        case (DataType.MUTEX, FeedMode.EVENT):
            return Kind.MUTEX_EVENT
        case (DataType.MUTEX, FeedMode.STATE):
            return Kind.MUTEX_STATE
        case (DataType.NOT_MUTEX, FeedMode.EVENT):
            return Kind.NOT_MUTEX_EVENT
        case (DataType.NOT_MUTEX, FeedMode.STATE):
            return Kind.NOT_MUTEX_STATE
        case (DataType.PARTIAL_VALUE, FeedMode.EVENT):
            return Kind.PARTIAL_VALUE
        case (DataType.PARTIAL_VALUE_BY_TAG, FeedMode.EVENT):
            return Kind.PARTIAL_VALUE_BY_TAG
        case _:
            raise ModelError(
                f"unsupported (data_type, feed_mode) = ({DataType.Name(data_type)}, {FeedMode.Name(feed_mode)})"
            )


# --------------------------------------------------------------------------- value range


def parse_decimal(s: str) -> Decimal:
    try:
        v = Decimal(s)
    except InvalidOperation as e:
        raise ModelError(f"invalid decimal: {s!r}") from e
    if not v.is_finite():
        raise ModelError(f"decimal must be finite: {s!r}")
    return v


@dataclass(frozen=True)
class ValueRange:
    """Khoảng giá trị; `None` = vô hạn (CLAUDE.md §3.1)."""

    from_value: Decimal | None = None
    from_inclusive: bool = True
    to_value: Decimal | None = None
    to_inclusive: bool = False

    def __post_init__(self) -> None:
        if self.from_value is None and self.to_value is None:
            raise ModelError("value_range needs from_value or to_value")
        if self.from_value is not None and self.to_value is not None:
            if self.from_value > self.to_value:
                raise ModelError(f"value_range from {self.from_value} > to {self.to_value}")
            if self.from_value == self.to_value and not (self.from_inclusive and self.to_inclusive):
                raise ModelError(f"value_range [{self.from_value}, {self.to_value}] is empty")

    def contains(self, x: Decimal) -> bool:
        if self.from_value is not None:
            if x < self.from_value or (x == self.from_value and not self.from_inclusive):
                return False
        if self.to_value is not None:
            if x > self.to_value or (x == self.to_value and not self.to_inclusive):
                return False
        return True

    @classmethod
    def from_proto(cls, vr: catalog_pb2.ValueRange) -> ValueRange:
        return cls(
            from_value=parse_decimal(vr.from_value) if vr.HasField("from_value") else None,
            from_inclusive=vr.from_inclusive,
            to_value=parse_decimal(vr.to_value) if vr.HasField("to_value") else None,
            to_inclusive=vr.to_inclusive,
        )


# --------------------------------------------------------------------------- attribute


@dataclass(frozen=True)
class AttributeSpec:
    """Phần metadata của attribute mà temporal/segment cần (catalog.v1.Attribute + Tag)."""

    attr_id: int
    name: str
    kind: Kind
    tags: Mapping[str, int]
    """tag name → tag_id (≥ 1)."""
    supported_date_ranges: frozenset[int]
    value_ranges: Mapping[int, ValueRange] = field(default_factory=dict)
    """Chỉ PARTIAL_VALUE: tag_id → bucket định sẵn."""

    def __post_init__(self) -> None:
        if any(t < 1 for t in self.tags.values()):
            raise ModelError(f"{self.name}: tag_id must be >= 1")
        if not self.supported_date_ranges or DateRange.DATE_RANGE_UNSPECIFIED in self.supported_date_ranges:
            raise ModelError(f"{self.name}: invalid supported_date_ranges")
        match self.kind:
            case Kind.PARTIAL_VALUE:
                if set(self.value_ranges) != set(self.tags.values()):
                    raise ModelError(f"{self.name}: every PARTIAL_VALUE tag needs a value_range")
            case (
                Kind.MUTEX_EVENT
                | Kind.MUTEX_STATE
                | Kind.NOT_MUTEX_EVENT
                | Kind.NOT_MUTEX_STATE
                | Kind.PARTIAL_VALUE_BY_TAG
            ):
                if self.value_ranges:
                    raise ModelError(f"{self.name}: value_range is only allowed for PARTIAL_VALUE tags")

    @property
    def tag_ids(self) -> list[int]:
        return sorted(self.tags.values())

    def tag_id(self, name: str) -> int:
        try:
            return self.tags[name]
        except KeyError:
            raise ModelError(f"{self.name}: unknown tag {name!r}") from None

    @classmethod
    def from_proto(cls, attr: catalog_pb2.Attribute, tags: Iterable[catalog_pb2.Tag]) -> AttributeSpec:
        tags = list(tags)
        return cls(
            attr_id=attr.id,
            name=attr.name,
            kind=kind_of(attr.data_type, attr.feed_mode),
            tags={t.name: t.id for t in tags},
            supported_date_ranges=frozenset(attr.supported_date_ranges),
            value_ranges={t.id: ValueRange.from_proto(t.value_range) for t in tags if t.HasField("value_range")},
        )


# --------------------------------------------------------------------------- input (silver)


@dataclass(frozen=True)
class TagEvent:
    """MUTEX / NOT_MUTEX, feedMode EVENT: mỗi tag là một signal ADD / REMOVE."""

    event_id: str
    uidx: int
    ts_ms: int
    tags_add: frozenset[int] = frozenset()
    tags_remove: frozenset[int] = frozenset()

    @property
    def ds(self) -> int:
        return ds_of_ts_ms(self.ts_ms)

    @property
    def order_key(self) -> tuple[int, str]:
        """Thứ tự trong ngày: (event_ts, event_id)."""
        return (self.ts_ms, self.event_id)


@dataclass(frozen=True)
class ValueEvent:
    """PARTIAL_VALUE (`tag = 0`) / PARTIAL_VALUE_BY_TAG (`tag ≥ 1`)."""

    event_id: str
    uidx: int
    ts_ms: int
    tag: int
    value: Decimal

    @property
    def ds(self) -> int:
        return ds_of_ts_ms(self.ts_ms)


@dataclass(frozen=True)
class StateInterval:
    """Một version SCD2 (feedMode STATE): user giữ tag trong `[valid_from, valid_to)`; `None` = hiện tại."""

    uidx: int
    tag: int
    valid_from: int
    valid_to: int | None = None

    def holds(self, day: int) -> bool:
        return self.valid_from <= day and (self.valid_to is None or day < self.valid_to)


def validate_tag_event(kind: Kind, ev: TagEvent) -> None:
    match kind:
        case Kind.MUTEX_EVENT | Kind.NOT_MUTEX_EVENT:
            if not ev.tags_add and not ev.tags_remove:
                raise ModelError(f"{ev.event_id}: event has no tag")
            if ev.tags_add & ev.tags_remove:
                # Thứ tự ADD/REMOVE của cùng một tag trong một event không xác định → từ chối.
                raise ModelError(f"{ev.event_id}: same tag in tags_add and tags_remove")
            if kind is Kind.MUTEX_EVENT and len(ev.tags_add) > 1:
                raise ModelError(f"{ev.event_id}: MUTEX event adds more than one tag")
            if any(t < 1 for t in ev.tags_add | ev.tags_remove):
                raise ModelError(f"{ev.event_id}: tag_id must be >= 1")
        case Kind.MUTEX_STATE | Kind.NOT_MUTEX_STATE | Kind.PARTIAL_VALUE | Kind.PARTIAL_VALUE_BY_TAG:
            raise ModelError(f"{ev.event_id}: TagEvent is not an input of {kind.value}")


def validate_value_event(kind: Kind, ev: ValueEvent) -> None:
    match kind:
        case Kind.PARTIAL_VALUE:
            if ev.tag != ANY_TAG:
                raise ModelError(f"{ev.event_id}: PARTIAL_VALUE event must have tag 0")
        case Kind.PARTIAL_VALUE_BY_TAG:
            if ev.tag < 1:
                raise ModelError(f"{ev.event_id}: PARTIAL_VALUE_BY_TAG event needs tag >= 1")
        case Kind.MUTEX_EVENT | Kind.MUTEX_STATE | Kind.NOT_MUTEX_EVENT | Kind.NOT_MUTEX_STATE:
            raise ModelError(f"{ev.event_id}: ValueEvent is not an input of {kind.value}")


def validate_intervals(kind: Kind, intervals: Iterable[StateInterval]) -> None:
    """SCD2 hợp lệ: không chồng lấn theo (uidx, tag); MUTEX: không chồng lấn theo uidx (≤ 1 tag/ngày)."""
    match kind:
        case Kind.MUTEX_STATE:
            key = lambda iv: iv.uidx  # noqa: E731
        case Kind.NOT_MUTEX_STATE:
            key = lambda iv: (iv.uidx, iv.tag)  # noqa: E731
        case Kind.MUTEX_EVENT | Kind.NOT_MUTEX_EVENT | Kind.PARTIAL_VALUE | Kind.PARTIAL_VALUE_BY_TAG:
            raise ModelError(f"StateInterval is not an input of {kind.value}")
    groups: dict[object, list[StateInterval]] = defaultdict(list)
    for iv in intervals:
        if iv.tag < 1:
            raise ModelError(f"interval {iv}: tag_id must be >= 1")
        if iv.valid_to is not None and iv.valid_to <= iv.valid_from:
            raise ModelError(f"interval {iv}: valid_to must be > valid_from")
        groups[key(iv)].append(iv)
    for ivs in groups.values():
        ivs.sort(key=lambda iv: iv.valid_from)
        for a, b in zip(ivs, ivs[1:]):
            if a.valid_to is None or a.valid_to > b.valid_from:
                raise ModelError(f"overlapping SCD2 intervals: {a} / {b}")


# --------------------------------------------------------------------------- L3 output


@dataclass
class LabelDaily:
    """`gold.tag_daily` của một ngày (MUTEX / NOT_MUTEX, EVENT).

    MUTEX: `add[0]` = ADD(d,0) = ⋃ ADD(d,t). NOT_MUTEX: `sig(t)` = ADD(d,t) ∪ DEL(d,t).
    """

    add: TagSets = field(default_factory=dict)
    dele: TagSets = field(default_factory=dict)

    def sig(self) -> TagSets:
        out: TagSets = {}
        for t in self.add.keys() | self.dele.keys():
            if t != ANY_TAG:
                out[t] = self.add.get(t, BitMap()) | self.dele.get(t, BitMap())
        return out

    def is_empty(self) -> bool:
        return not self.add and not self.dele


@dataclass
class StateDaily:
    """Delta STATE của một ngày: ADDED(d,t), REMOVED(d,t). Ngày bootstrap: ADDED = STATE(d), REMOVED = ∅."""

    added: TagSets = field(default_factory=dict)
    removed: TagSets = field(default_factory=dict)

    def is_empty(self) -> bool:
        return not self.added and not self.removed


def _put(sets: TagSets, tag: int, uidx: int) -> None:
    bm = sets.get(tag)
    if bm is None:
        sets[tag] = bm = BitMap()
    bm.add(uidx)


def _by_user(events: Iterable[TagEvent]) -> dict[int, list[TagEvent]]:
    out: dict[int, list[TagEvent]] = defaultdict(list)
    for ev in events:
        out[ev.uidx].append(ev)
    for evs in out.values():
        evs.sort(key=lambda e: e.order_key)
    return out


def reduce_mutex_day(events: Iterable[TagEvent]) -> LabelDaily:
    """§4.1 MUTEX: ADD(d,t) = ADD cuối ngày là t; DEL(d,t) = REMOVE t sau ADD t cuối cùng (hoặc không ADD t)."""
    out = LabelDaily()
    for uidx, evs in _by_user(events).items():
        last_add_tag: int | None = None
        last_add_pos: dict[int, int] = {}
        last_remove_pos: dict[int, int] = {}
        for pos, ev in enumerate(evs):
            for t in ev.tags_remove:
                last_remove_pos[t] = pos
            for t in ev.tags_add:
                last_add_tag = t
                last_add_pos[t] = pos
        if last_add_tag is not None:
            _put(out.add, last_add_tag, uidx)
            _put(out.add, ANY_TAG, uidx)
        for t, rpos in last_remove_pos.items():
            if rpos > last_add_pos.get(t, -1):
                _put(out.dele, t, uidx)
    return out


def reduce_not_mutex_day(events: Iterable[TagEvent]) -> LabelDaily:
    """§4.1 NOT_MUTEX: theo từng tag, signal cuối ngày — ADD → ADD(d,t), REMOVE → DEL(d,t)."""
    out = LabelDaily()
    for uidx, evs in _by_user(events).items():
        last: dict[int, bool] = {}
        for ev in evs:
            for t in ev.tags_remove:
                last[t] = False
            for t in ev.tags_add:
                last[t] = True
        for t, is_add in last.items():
            _put(out.add if is_add else out.dele, t, uidx)
    return out


def held_on(intervals: Iterable[StateInterval], day: int) -> TagSets:
    out: TagSets = {}
    for iv in intervals:
        if iv.holds(day):
            _put(out, iv.tag, iv.uidx)
    return out


def state_delta(intervals: Iterable[StateInterval], day: int, *, snapshot: bool = False) -> StateDaily:
    """ADDED(d,t) = giữ ở d, không giữ ở d−1; REMOVED(d,t) = ngược lại (CLAUDE.md §3.4).

    Hai version liền nhau cùng tag (valid_to = valid_from = d) triệt tiêu nhau — đúng với
    `STATE(d) = (STATE(d−1) − REMOVED) ∪ ADDED`. `snapshot=True` (ngày bootstrap, STATE(d−1) coi là ∅): ADDED = STATE(d).
    """
    intervals = list(intervals)
    cur = held_on(intervals, day)
    if snapshot:
        return StateDaily(added=cur)
    prev = held_on(intervals, day - 1)
    out = StateDaily()
    for t in cur.keys() | prev.keys():
        c, p = cur.get(t, BitMap()), prev.get(t, BitMap())
        if a := c - p:
            out.added[t] = a
        if r := p - c:
            out.removed[t] = r
    return out


def reduce_pv_day(kind: Kind, events: Iterable[ValueEvent]) -> TagSums:
    """`gold.pv_daily`: SUM theo (tag_id, uidx); PARTIAL_VALUE luôn tag 0."""
    out: TagSums = {}
    for ev in events:
        validate_value_event(kind, ev)
        by_user = out.setdefault(ev.tag, {})
        by_user[ev.uidx] = by_user.get(ev.uidx, Decimal(0)) + ev.value
    return out


# --------------------------------------------------------------------------- phép trên TagSets / TagSums


def copy_sets(s: Mapping[int, BitMap]) -> TagSets:
    return {t: BitMap(bm) for t, bm in s.items()}


def union_sets(a: Mapping[int, BitMap], b: Mapping[int, BitMap]) -> TagSets:
    out = copy_sets(a)
    for t, bm in b.items():
        out[t] = out[t] | bm if t in out else BitMap(bm)
    return out


def add_sums(a: Mapping[int, Mapping[int, Decimal]], b: Mapping[int, Mapping[int, Decimal]]) -> TagSums:
    out: TagSums = {t: dict(m) for t, m in a.items()}
    for t, m in b.items():
        acc = out.setdefault(t, {})
        for u, v in m.items():
            acc[u] = acc.get(u, Decimal(0)) + v
    return out


def drop_empty(s: Mapping[int, BitMap]) -> TagSets:
    return {t: bm for t, bm in s.items() if bm}


def users_in_range(sums: Mapping[int, Decimal], vr: ValueRange) -> BitMap:
    return BitMap(u for u, v in sums.items() if vr.contains(v))
