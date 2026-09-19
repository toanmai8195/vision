"""Property test (CLAUDE.md §4.4): engine tối ưu == reference ngây thơ.

Mỗi case sinh ngẫu nhiên: ≤ 400 ngày lịch sử, vài user/tag, ADD/REMOVE xen kẽ (trùng timestamp → phân định
bằng event_id), late data ≤ 3 ngày (reprocess). Pipeline chạy từng ngày qua planner → engine (DQ BLOCK bật ở
3 ngày cuối), rồi so mọi date range chuẩn + custom range tại `ds` với reference.

Tổ hợp chọn qua env (mỗi tổ hợp một Bazel target): `PROPERTY_KIND`, `PROPERTY_AGG` (PARTIAL_VALUE*, mặc định SUM),
`PROPERTY_ATTR_TYPE` (STANDARD | EXTENDED); số case qua `PROPERTY_MAX_EXAMPLES`.

EXTENDED: tag lấy từ pool lớn hơn, `usage` ban đầu ngẫu nhiên; tag trong usage phải được materialize (scope đúng),
tag ngoài usage tính on-demand — cả hai đều phải == reference.
"""

from __future__ import annotations

import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from pyroaring import BitMap

from com.tm.src.temporal.model import (
    ANY_TAG,
    DAY_S,
    AggFunc,
    AttributeSpec,
    AttributeType,
    Kind,
    StateInterval,
    TagDict,
    TagEvent,
    ValueEvent,
    ValueRange,
    ds_of_ts_ms,
    parse_day,
    to_date,
    ts_ms_of,
)
from com.tm.src.temporal.planner import LATE_DATA_MAX_DAYS
from com.tm.src.temporal.ranges import ALL_DATE_RANGES, CUSTOM_MAX_LOOKBACK_DAYS, CustomRange, resolve_window
from com.tm.src.temporal.reference import Reference
from com.tm.src.temporal.testing import arrival_feed, intervals_feed, run_pipeline

KIND = Kind(os.environ.get("PROPERTY_KIND", "MUTEX_EVENT"))
AGG = AggFunc.Value(os.environ.get("PROPERTY_AGG", "SUM")) if not KIND.is_label else AggFunc.AGG_FUNC_UNSPECIFIED
ATTR_TYPE = AttributeType.Value(os.environ.get("PROPERTY_ATTR_TYPE", "STANDARD"))
EXTENDED = ATTR_TYPE == AttributeType.EXTENDED
MAX_EXAMPLES = int(os.environ.get("PROPERTY_MAX_EXAMPLES", "10000"))
MAX_DAYS = 400
TAGS = tuple(range(1, 11)) if EXTENDED else (1, 2, 3)  # EXTENDED: pool tag lớn hơn
# 2025-11-20: lịch sử cắt qua ranh giới tháng/năm, Chủ nhật, và block 2^k.
BASE = parse_day("2025-11-20")

settings.register_profile(
    "ci",
    max_examples=MAX_EXAMPLES,
    deadline=None,
    derandomize=True,
    database=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
settings.load_profile("ci")


def attribute(kind: Kind) -> AttributeSpec:
    value_ranges = {}
    if kind is Kind.PARTIAL_VALUE and AGG == AggFunc.COUNT:
        value_ranges = {
            1: ValueRange(Decimal(1), True, Decimal(1), True),  # đúng 1 event
            2: ValueRange(Decimal(1), False, Decimal(3), True),  # (1, 3]
            3: ValueRange(Decimal(3), False),  # > 3
        }
    elif kind is Kind.PARTIAL_VALUE:
        value_ranges = {
            1: ValueRange(to_value=Decimal(0), to_inclusive=False),  # < 0 (hoàn tiền)
            2: ValueRange(Decimal(0), True, Decimal(500), False),  # [0, 500) — chứa 0
            3: ValueRange(Decimal(500), True, Decimal(2000), True),  # [500, 2000]
            4: ValueRange(Decimal(2000), False),  # (2000, ∞)
        }
    tags = {} if EXTENDED else {f"t{t}": t for t in (value_ranges or TAGS)}
    return AttributeSpec(
        attr_id=1,
        name="prop",
        kind=kind,
        tags=tags,
        supported_date_ranges=frozenset(ALL_DATE_RANGES),
        value_ranges=value_ranges,
        agg_func=AGG,
        attribute_type=ATTR_TYPE,
    )


def tag_dict() -> TagDict | None:
    """EXTENDED: tag_string "t<i>" → tag_id i (cấp theo thứ tự như L2)."""
    if not EXTENDED:
        return None
    d = TagDict()
    for t in TAGS:
        assert d.encode(f"t{t}") == t
    return d


ATTR = attribute(KIND)


@dataclass(frozen=True)
class Scenario:
    ds: int
    arrivals: tuple  # (arrival_day, item)
    customs: tuple[CustomRange, ...]
    usage: tuple = ()  # EXTENDED: ((date_range, frozenset(tag)), …) ~ meta.condition_usage ban đầu

    def __repr__(self) -> str:  # dễ đọc khi hypothesis in ca fail
        rows = "\n  ".join(f"arrive {to_date(a)}: {item}" for a, item in self.arrivals)
        return f"Scenario(ds={to_date(self.ds)}, customs={self.customs}\n  {rows})"


SECONDS = st.one_of(st.sampled_from([0, 3600, 43200, DAY_S - 1]), st.integers(0, DAY_S - 1))
LAG = st.sampled_from([0, 0, 0, 1, 2, 3])


@st.composite
def scenarios(draw) -> Scenario:
    span = draw(st.integers(1, MAX_DAYS))
    ds = BASE + span - 1
    n_users = draw(st.integers(1, 4))
    n = draw(st.integers(0, 24))
    # Dồn một phần event về cuối lịch sử để các window ngắn có dữ liệu.
    day = st.one_of(st.integers(BASE, ds), st.integers(max(BASE, ds - 10), ds))
    arrivals = []
    for i in range(n):
        d = draw(day)
        ts = ts_ms_of(d, draw(SECONDS))
        uidx = draw(st.integers(1, n_users))
        event_id = f"e{draw(st.integers(0, 99)):02d}-{i}"  # prefix ngẫu nhiên: thứ tự event_id ≠ thứ tự sinh
        match KIND:
            case Kind.MUTEX_EVENT | Kind.NOT_MUTEX_EVENT:
                max_add = 1 if KIND is Kind.MUTEX_EVENT else 3
                add = draw(st.frozensets(st.sampled_from(TAGS), max_size=max_add))
                remove = draw(st.frozensets(st.sampled_from(TAGS), max_size=2)) - add
                if not add and not remove:
                    add = frozenset({TAGS[0]})
                item = TagEvent(event_id, uidx, ts, add, remove)
            case Kind.MUTEX_STATE:
                item = StateChange(event_id, uidx, ts, draw(st.sampled_from([None, *TAGS])), True)
            case Kind.NOT_MUTEX_STATE:
                item = StateChange(event_id, uidx, ts, draw(st.sampled_from(TAGS)), draw(st.booleans()))
            case Kind.PARTIAL_VALUE | Kind.PARTIAL_VALUE_BY_TAG:
                tag = ANY_TAG if KIND is Kind.PARTIAL_VALUE else draw(st.sampled_from(TAGS))
                value = Decimal(draw(st.integers(-300, 1500))).scaleb(draw(st.sampled_from([0, 0, -6])))
                item = ValueEvent(event_id, uidx, ts, tag, value)
        arrivals.append((min(d + draw(LAG), ds), item))

    lo = max(BASE - 5, ds - (CUSTOM_MAX_LOOKBACK_DAYS - 1))
    customs = []
    for _ in range(draw(st.integers(0, 3))):
        a, b = sorted((draw(st.integers(lo, ds)), draw(st.integers(lo, ds))))
        customs.append(CustomRange(a, b))
    usage = ()
    if EXTENDED:
        drs = draw(st.lists(st.sampled_from(ALL_DATE_RANGES), unique=True, max_size=len(ALL_DATE_RANGES)))
        usage = tuple((dr, draw(st.frozensets(st.sampled_from(TAGS), max_size=4))) for dr in drs)
    return Scenario(ds, tuple(arrivals), tuple(customs), usage)


# --------------------------------------------------------------------------- STATE: change stream → SCD2


@dataclass(frozen=True)
class StateChange:
    """Một bản ghi CDC: MUTEX — `tag` là giá trị mới (None = mất giá trị); NOT_MUTEX — bật/tắt `tag`."""

    event_id: str
    uidx: int
    ts_ms: int
    tag: int | None
    on: bool

    @property
    def ds(self) -> int:
        return ds_of_ts_ms(self.ts_ms)


def intervals_of(changes: list[StateChange]) -> list[StateInterval]:
    """SCD2 theo ngày: giá trị của ngày = sau bản ghi cuối ngày (theo `(ts, event_id)`)."""
    by_key: dict[tuple, dict[int, StateChange]] = defaultdict(dict)
    for c in sorted(changes, key=lambda c: (c.ts_ms, c.event_id)):
        key = c.uidx if KIND is Kind.MUTEX_STATE else (c.uidx, c.tag)
        by_key[key][c.ds] = c  # bản ghi cuối ngày thắng
    out = []
    for key, per_day in by_key.items():
        cur, start = None, None  # MUTEX: tag đang giữ; NOT_MUTEX: tag nếu đang bật
        for d in sorted(per_day):
            c = per_day[d]
            new = c.tag if KIND is Kind.MUTEX_STATE else (c.tag if c.on else None)
            if new != cur:
                if cur is not None:
                    out.append(StateInterval(c.uidx, cur, start, d))
                cur, start = new, d
        if cur is not None:
            uidx = key if KIND is Kind.MUTEX_STATE else key[0]
            out.append(StateInterval(uidx, cur, start, None))
    return out


# --------------------------------------------------------------------------- test


def run(sc: Scenario):
    universe = BitMap(range(1, 5))
    if KIND.is_state:
        by_arrival: dict[int, list[StateChange]] = defaultdict(list)
        for a, c in sc.arrivals:
            by_arrival[a].append(c)
        known: list[StateChange] = []
        cache: list[StateInterval] = []

        def versions(day: int):
            nonlocal cache
            new = by_arrival.get(day, [])
            if new:
                known.extend(new)
                cache = intervals_of(known)
            late = [c.ds for c in new if c.ds < day]
            return cache, (min(late) if late else None)

        feed = intervals_feed(versions)
        ref = Reference(ATTR, intervals=intervals_of([c for _, c in sc.arrivals]), tag_dict=tag_dict())
    else:
        tag_events = [(a, e) for a, e in sc.arrivals if isinstance(e, TagEvent)]
        value_events = [(a, e) for a, e in sc.arrivals if isinstance(e, ValueEvent)]
        feed = arrival_feed(tag_events, value_events)
        ref = Reference(
            ATTR, tag_events=[e for _, e in tag_events], value_events=[e for _, e in value_events], tag_dict=tag_dict()
        )

    engine = run_pipeline(
        ATTR,
        history_start=BASE,
        ds=sc.ds,
        feed=feed,
        universe_of=lambda _ds: universe,
        ranges_from=sc.ds - LATE_DATA_MAX_DAYS + 1,
        tag_dict=tag_dict(),
        initial_usage=dict(sc.usage) if EXTENDED else None,
    )
    return engine, ref


def ref_bounds(ref_window, ds):
    return (BASE if ref_window.always_active else ref_window.l), ref_window.r


EXAMPLES_RUN = 0


@given(scenarios())
def test_engine_matches_reference(sc: Scenario):
    global EXAMPLES_RUN
    EXAMPLES_RUN += 1
    engine, ref = run(sc)
    refs = [*ALL_DATE_RANGES, *sc.customs]
    usage = dict(sc.usage)
    for wref in refs:
        w = resolve_window(wref, sc.ds)
        l, r = ref_bounds(w, sc.ds)
        want_tags = ref.members(l, r)
        want_sums = None if KIND.is_label else ref.sums(l, r)
        got = engine.query(wref, sc.ds)
        if KIND.is_label:
            assert got.sums is None
        if EXTENDED and isinstance(wref, int):
            # Chỉ tag trong usage được materialize; tag khác tính on-demand qua ConditionSource.
            scope = usage.get(wref, frozenset())
            assert got.scope == scope, wref
            assert got.tags == {t: bm for t, bm in want_tags.items() if t in scope}, wref
            for t in TAGS:
                if KIND.is_label:
                    assert engine.tag_bitmap(wref, sc.ds, t) == want_tags.get(t, BitMap()), (wref, t)
                else:
                    assert engine.pv_sums(wref, sc.ds, t) == want_sums.get(t, {}), (wref, t)
            continue
        assert got.tags == want_tags, wref
        if not KIND.is_label:
            assert got.sums == want_sums, wref
        if KIND.is_mutex:
            # MUTEX: mỗi user ≤ 1 tag trong mọi window.
            assert sum(len(b) for b in got.tags.values()) == len(BitMap.union(BitMap(), *got.tags.values()))


def test_ran_enough_examples():
    """Done-criteria P1: ≥ 10K case / loại (chạy sau test chính trong cùng module)."""
    assert EXAMPLES_RUN >= MAX_EXAMPLES, EXAMPLES_RUN


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q", "-p", "no:cacheprovider"]))
