"""Date range → window `[l, r]` (CLAUDE.md §3.3).

| Code | Window as-of `ds` |
|---|---|
| `A1` | `[ds, ds]` |
| `A7` … `A180` | `[ds−N+1, ds]` |
| `IN_MONTH` | `[ngày 1 tháng(ds), ds]` |
| `LAST_MONTH` | tháng dương lịch trước (đóng băng từ ngày 1) |
| `ALWAYS_ACTIVE` | `[đầu lịch sử, ds]` |
| custom `{from, to}` | `[from, to]`, `to ≤ ds`, `from ≥ ds − 399` |
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

from pyroaring import BitMap

from com.tm.src.temporal.model import (
    ANY_TAG,
    DateRange,
    Kind,
    ModelError,
    TagSets,
    ValueRange,
    drop_empty,
    epoch_day,
    to_date,
    users_in_range,
)

CUSTOM_MAX_LOOKBACK_DAYS = 400

LAST_N: dict[int, int] = {
    DateRange.A1: 1,
    DateRange.A7: 7,
    DateRange.A15: 15,
    DateRange.A30: 30,
    DateRange.A60: 60,
    DateRange.A90: 90,
    DateRange.A120: 120,
    DateRange.A180: 180,
}

ALL_DATE_RANGES: tuple[int, ...] = (
    *LAST_N,
    DateRange.IN_MONTH,
    DateRange.LAST_MONTH,
    DateRange.ALWAYS_ACTIVE,
)

HISTORY_START = -(10**9)
"""`l` của ALWAYS_ACTIVE: không giới hạn (caller kẹp về ngày đầu dữ liệu nếu cần)."""


@dataclass(frozen=True)
class Window:
    l: int
    r: int
    always_active: bool = False

    def clamp(self, first_day: int) -> Window | None:
        """Bỏ phần trước ngày đầu dữ liệu; None nếu window rỗng."""
        l = max(self.l, first_day)
        return None if l > self.r else Window(l, self.r, self.always_active)


@dataclass(frozen=True)
class CustomRange:
    from_day: int
    to_day: int


WindowRef = int | CustomRange
"""`DateRange` (precompute) hoặc `CustomRange` (on-demand)."""


def date_range_name(dr: int) -> str:
    return DateRange.Name(dr)


def window_of(dr: int, ds: int) -> Window:
    if dr in LAST_N:
        return Window(ds - LAST_N[dr] + 1, ds)
    d = to_date(ds)
    match dr:
        case DateRange.IN_MONTH:
            return Window(epoch_day(d.replace(day=1)), ds)
        case DateRange.LAST_MONTH:
            last = d.replace(day=1) - dt.timedelta(days=1)
            return Window(epoch_day(last.replace(day=1)), epoch_day(last))
        case DateRange.ALWAYS_ACTIVE:
            return Window(HISTORY_START, ds, always_active=True)
        case _:
            raise ModelError(f"unsupported date range {dr}")


def custom_window(cr: CustomRange, ds: int) -> Window:
    if cr.from_day > cr.to_day:
        raise ModelError(f"custom range from {to_date(cr.from_day)} > to {to_date(cr.to_day)}")
    if cr.to_day > ds:
        raise ModelError(f"custom range to {to_date(cr.to_day)} > ds {to_date(ds)}")
    if cr.from_day < ds - (CUSTOM_MAX_LOOKBACK_DAYS - 1):
        raise ModelError(f"custom range from {to_date(cr.from_day)} < ds − {CUSTOM_MAX_LOOKBACK_DAYS - 1}")
    return Window(cr.from_day, cr.to_day)


def resolve_window(ref: WindowRef, ds: int) -> Window:
    if isinstance(ref, CustomRange):
        return custom_window(ref, ds)
    return window_of(ref, ds)


# --------------------------------------------------------------------------- công thức window theo loại (§4.3)


def label_window(kind: Kind, latest_r: Mapping[int, BitMap], window_blocks: Mapping[int, BitMap] | None) -> TagSets:
    """Tag bitmap của window `[l, r]` cho MUTEX / NOT_MUTEX.

    - MUTEX EVENT: `LATEST(r,t) ∩ SEEN[l,r]`, `SEEN = ⋃ ADD(·,0)` (`window_blocks[0]`).
    - NOT_MUTEX EVENT: `POS(r,t) ∩ ⋃_{[l,r]} SIG(·,t)`.
    - STATE: `STATE(r,t)` với mọi window.
    `window_blocks = None` ⇔ ALWAYS_ACTIVE (EVENT): trả thẳng LATEST/POS.
    """
    match kind:
        case Kind.MUTEX_EVENT:
            if window_blocks is None:
                return drop_empty(dict(latest_r))
            seen = window_blocks.get(ANY_TAG, BitMap())
            return drop_empty({t: bm & seen for t, bm in latest_r.items()})
        case Kind.NOT_MUTEX_EVENT:
            if window_blocks is None:
                return drop_empty(dict(latest_r))
            return drop_empty({t: bm & window_blocks[t] for t, bm in latest_r.items() if t in window_blocks})
        case Kind.MUTEX_STATE | Kind.NOT_MUTEX_STATE:
            return drop_empty(dict(latest_r))
        case Kind.PARTIAL_VALUE | Kind.PARTIAL_VALUE_BY_TAG:
            raise ModelError(f"{kind.value} is not a label kind")


def pv_tag_bitmaps(kind: Kind, sums: Mapping[int, Mapping[int, Decimal]], value_ranges: Mapping[int, ValueRange]) -> TagSets:
    """PARTIAL_VALUE: tag (bucket định sẵn) → user có SUM ∈ valueRange. PARTIAL_VALUE_BY_TAG: không có bucket."""
    match kind:
        case Kind.PARTIAL_VALUE:
            by_user = sums.get(ANY_TAG, {})
            return drop_empty({t: users_in_range(by_user, vr) for t, vr in value_ranges.items()})
        case Kind.PARTIAL_VALUE_BY_TAG:
            return {}
        case Kind.MUTEX_EVENT | Kind.MUTEX_STATE | Kind.NOT_MUTEX_EVENT | Kind.NOT_MUTEX_STATE:
            raise ModelError(f"{kind.value} is not a value kind")
