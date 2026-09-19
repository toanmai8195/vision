"""LATEST / POS / STATE theo ngày + checkpoint tuần (CLAUDE.md §4.3).

| Loại | Cập nhật mỗi ngày |
|---|---|
| MUTEX EVENT | `LATEST(d,t) = (ADD(d,t) − DEL(d,t)) ∪ (LATEST(d−1,t) − ADD(d,0) − DEL(d,t))` |
| NOT_MUTEX EVENT | `POS(d,t) = ADD(d,t) ∪ (POS(d−1,t) − DEL(d,t))` — chỉ tag có signal |
| STATE | `STATE(d,t) = (STATE(d−1,t) − REMOVED(d,t)) ∪ ADDED(d,t)` — chỉ tag có delta |

Lưu: 7 ngày gần nhất + checkpoint Chủ nhật. `state_at(r)` với `r` cũ = checkpoint tuần gần nhất + forward-fold ≤ 6 ngày.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

from pyroaring import BitMap

from com.tm.src.temporal.model import (
    ANY_TAG,
    Kind,
    LabelDaily,
    StateDaily,
    TagSets,
    to_date,
)

KEEP_DAYS = 7
SUNDAY = 6

EMPTY = BitMap()


def is_checkpoint_day(day: int) -> bool:
    return to_date(day).weekday() == SUNDAY


def fold_mutex(prev: Mapping[int, BitMap], daily: LabelDaily) -> TagSets:
    if daily.is_empty():
        return dict(prev)
    add_any = daily.add.get(ANY_TAG, EMPTY)
    out: TagSets = {}
    for t in (prev.keys() | daily.add.keys() | daily.dele.keys()) - {ANY_TAG}:
        dele = daily.dele.get(t, EMPTY)
        bm = (daily.add.get(t, EMPTY) - dele) | (prev.get(t, EMPTY) - add_any - dele)
        if bm:
            out[t] = bm
    return out


def _update_touched(prev: Mapping[int, BitMap], touched: set[int], new: Callable[[int], BitMap]) -> TagSets:
    """Chỉ tính lại tag có thay đổi trong ngày, tag khác giữ nguyên (chia sẻ bitmap, không sửa tại chỗ).

    Chi phí/ngày tỉ lệ số tag có signal — điều kiện để hỗ trợ tag EXTENDED (CLAUDE.md §3.6).
    """
    out: TagSets = dict(prev)
    for t in touched:
        bm = new(t)
        if bm:
            out[t] = bm
        else:
            out.pop(t, None)
    return out


def fold_not_mutex(prev: Mapping[int, BitMap], daily: LabelDaily) -> TagSets:
    return _update_touched(
        prev,
        daily.add.keys() | daily.dele.keys(),
        lambda t: daily.add.get(t, EMPTY) | (prev.get(t, EMPTY) - daily.dele.get(t, EMPTY)),
    )


def fold_state(prev: Mapping[int, BitMap], daily: StateDaily) -> TagSets:
    return _update_touched(
        prev,
        daily.added.keys() | daily.removed.keys(),
        lambda t: (prev.get(t, EMPTY) - daily.removed.get(t, EMPTY)) | daily.added.get(t, EMPTY),
    )


def fold_for(kind: Kind) -> Callable:
    match kind:
        case Kind.MUTEX_EVENT:
            return fold_mutex
        case Kind.NOT_MUTEX_EVENT:
            return fold_not_mutex
        case Kind.MUTEX_STATE | Kind.NOT_MUTEX_STATE:
            return fold_state
        case Kind.PARTIAL_VALUE | Kind.PARTIAL_VALUE_BY_TAG:
            raise ValueError(f"{kind.value} has no LATEST/POS/STATE")


class MissingStateError(RuntimeError):
    """Không dựng lại được LATEST/STATE tại ngày yêu cầu (thiếu checkpoint)."""


class LatestStore:
    """`gold.tag_latest` (7 ngày) + `gold.tag_state_checkpoint` (Chủ nhật) của một attribute.

    `daily_of(d)` trả về output L3 của ngày `d` (dùng cho forward-fold).
    Trước ngày đầu lịch sử (`first_day`) trạng thái là rỗng.
    """

    def __init__(self, kind: Kind, daily_of: Callable[[int], object]):
        self._fold = fold_for(kind)
        self._daily_of = daily_of
        self.first_day: int | None = None
        self.last_day: int | None = None
        self._recent: dict[int, TagSets] = {}
        self._checkpoints: dict[int, TagSets] = {}

    def fold(self, day: int) -> TagSets:
        """Tính trạng thái sau ngày `day` từ trạng thái ngày `day − 1` (ghi đè nếu đã có — reprocess)."""
        if self.first_day is None:
            self.first_day = day
        elif day < self.first_day:
            raise MissingStateError(f"day {day} is before history start {self.first_day}")
        elif self.last_day is not None and day > self.last_day + 1:
            raise MissingStateError(f"gap: last folded day {self.last_day}, got {day}")
        prev = self.state_at(day - 1) if day > self.first_day else {}
        cur = self._fold(prev, self._daily_of(day))
        self._recent[day] = cur
        if self.last_day is None or day > self.last_day:
            self.last_day = day
        for d in [d for d in self._recent if d <= self.last_day - KEEP_DAYS]:
            del self._recent[d]
        return cur

    def checkpoint(self, day: int) -> None:
        if not is_checkpoint_day(day):
            raise ValueError(f"{to_date(day)} is not a checkpoint day (Sunday)")
        self._checkpoints[day] = self._recent[day]

    def state_at(self, r: int) -> TagSets:
        if self.first_day is None or r < self.first_day:
            return {}
        if self.last_day is None or r > self.last_day:
            raise MissingStateError(f"day {r} not folded yet (last {self.last_day})")
        if r in self._recent:
            return self._recent[r]
        c = r - (to_date(r).weekday() - SUNDAY) % 7  # Chủ nhật gần nhất ≤ r
        if c < self.first_day:
            state: TagSets = {}
            start = self.first_day
        elif c in self._checkpoints:
            state = self._checkpoints[c]
            start = c + 1
        else:
            raise MissingStateError(f"missing checkpoint {to_date(c)} for {to_date(r)}")
        for d in range(start, r + 1):
            state = self._fold(state, self._daily_of(d))
        return state
