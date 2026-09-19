"""Property test (CLAUDE.md §4.4): engine tối ưu == reference ngây thơ.

Mỗi case sinh ngẫu nhiên: ≤ 400 ngày lịch sử, vài user/tag, ADD/REMOVE xen kẽ (trùng timestamp → phân định
bằng event_id), late data ≤ 3 ngày (reprocess). Pipeline chạy từng ngày qua planner → engine (DQ BLOCK bật ở
3 ngày cuối), rồi so mọi date range chuẩn + custom range tại `ds` với reference.

Loại chạy chọn qua env `PROPERTY_KIND` (mỗi loại một Bazel target); số case qua `PROPERTY_MAX_EXAMPLES`.
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
    AttributeSpec,
    Kind,
    StateInterval,
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
MAX_EXAMPLES = int(os.environ.get("PROPERTY_MAX_EXAMPLES", "10000"))
MAX_DAYS = 400
TAGS = (1, 2, 3)
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
    if kind is Kind.PARTIAL_VALUE:
        value_ranges = {
            1: ValueRange(to_value=Decimal(0), to_inclusive=False),  # < 0 (hoàn tiền)
            2: ValueRange(Decimal(0), True, Decimal(500), False),  # [0, 500) — chứa 0
            3: ValueRange(Decimal(500), True, Decimal(2000), True),  # [500, 2000]
            4: ValueRange(Decimal(2000), False),  # (2000, ∞)
        }
    tags = {f"t{t}": t for t in (value_ranges or TAGS)}
    return AttributeSpec(
        attr_id=1,
        name="prop",
        kind=kind,
        tags=tags,
        supported_date_ranges=frozenset(ALL_DATE_RANGES),
        value_ranges=value_ranges,
    )


ATTR = attribute(KIND)


@dataclass(frozen=True)
class Scenario:
    ds: int
    arrivals: tuple  # (arrival_day, item)
    customs: tuple[CustomRange, ...]

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
    return Scenario(ds, tuple(arrivals), tuple(customs))


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
        ref = Reference(ATTR, intervals=intervals_of([c for _, c in sc.arrivals]))
    else:
        tag_events = [(a, e) for a, e in sc.arrivals if isinstance(e, TagEvent)]
        value_events = [(a, e) for a, e in sc.arrivals if isinstance(e, ValueEvent)]
        feed = arrival_feed(tag_events, value_events)
        ref = Reference(ATTR, tag_events=[e for _, e in tag_events], value_events=[e for _, e in value_events])

    engine = run_pipeline(
        ATTR,
        history_start=BASE,
        ds=sc.ds,
        feed=feed,
        universe_of=lambda _ds: universe,
        ranges_from=sc.ds - LATE_DATA_MAX_DAYS + 1,
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
    for wref in refs:
        w = resolve_window(wref, sc.ds)
        l, r = ref_bounds(w, sc.ds)
        got = engine.query(wref, sc.ds)
        assert got.tags == ref.members(l, r), wref
        if KIND.is_label:
            assert got.sums is None
        else:
            assert got.sums == ref.sums(l, r), wref
        if KIND.is_mutex:
            # MUTEX: mỗi user ≤ 1 tag trong mọi window.
            assert sum(len(b) for b in got.tags.values()) == len(BitMap.union(BitMap(), *got.tags.values()))


def test_ran_enough_examples():
    """Done-criteria P1: ≥ 10K case / loại (chạy sau test chính trong cùng module)."""
    assert EXAMPLES_RUN >= MAX_EXAMPLES, EXAMPLES_RUN


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q", "-p", "no:cacheprovider"]))
