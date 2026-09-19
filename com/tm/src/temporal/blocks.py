"""Dyadic block (CLAUDE.md §4.2) — OR cho bitmap, SUM cho partial value.

Block `B(k, s)`: `s % 2^k == 0`, phủ ngày `[s, s + 2^k − 1]`, `k = 0..MAX_K`.
Logic greedy được port sang Go/Kotlin cho custom range và dùng chung golden test
(`testdata/golden/blocks.yaml`).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Generic, TypeVar

MAX_K = 8

Block = tuple[int, int]
"""(k, s)."""


def block_days(block: Block) -> tuple[int, int]:
    k, s = block
    return s, s + (1 << k) - 1


def blocks_closed_by(day: int) -> list[Block]:
    """Block k ≥ 1 build được khi ngày `day` đóng: `(e(day) + 1) % 2^k == 0`, theo thứ tự k tăng."""
    return [(k, day + 1 - (1 << k)) for k in range(1, MAX_K + 1) if (day + 1) % (1 << k) == 0]


def decompose(l: int, r: int) -> list[Block]:
    """Greedy trái → phải: block aligned lớn nhất nằm trọn trong `[l, r]`. Các block rời nhau."""
    if l > r:
        raise ValueError(f"empty window [{l}, {r}]")
    out = []
    while l <= r:
        k = MAX_K
        while l % (1 << k) != 0 or l + (1 << k) - 1 > r:
            k -= 1
        out.append((k, l))
        l += 1 << k
    return out


V = TypeVar("V")


class BlockStore(Generic[V]):
    """Lưu block của một nguồn (`ADD(·,0)`, `SIG(·,t)`, `pv_daily`, `REMOVED`); giá trị rỗng không lưu.

    `combine` phải kết hợp được và không sửa input (OR theo tag, SUM theo (tag, uidx)).
    """

    def __init__(self, combine: Callable[[V, V], V], empty: Callable[[], V], is_empty: Callable[[V], bool]):
        self._combine = combine
        self._empty = empty
        self._is_empty = is_empty
        self._blocks: dict[Block, V] = {}

    def _set(self, block: Block, value: V) -> None:
        if self._is_empty(value):
            self._blocks.pop(block, None)
        else:
            self._blocks[block] = value

    def get(self, block: Block) -> V:
        v = self._blocks.get(block)
        return self._empty() if v is None else v

    def set_day(self, day: int, value: V) -> None:
        """B(0, day) = giá trị của ngày (ghi đè — idempotent khi reprocess)."""
        self._set((0, day), value)

    def build(self, block: Block) -> None:
        """B(k,s) = B(k−1,s) ⊕ B(k−1,s+2^(k−1)). Caller build theo k tăng dần."""
        k, s = block
        if k < 1 or s % (1 << k) != 0:
            raise ValueError(f"invalid block {block}")
        half = 1 << (k - 1)
        a, b = self._blocks.get((k - 1, s)), self._blocks.get((k - 1, s + half))
        if a is None and b is None:
            self._blocks.pop(block, None)
        elif b is None:
            self._blocks[block] = a
        elif a is None:
            self._blocks[block] = b
        else:
            self._set(block, self._combine(a, b))

    def window(self, l: int, r: int) -> V:
        """⊕ các block của `decompose(l, r)`; chỉ dùng khi mọi block đã được build."""
        acc: V | None = None
        for block in decompose(l, r):
            v = self._blocks.get(block)
            if v is None:
                continue
            acc = v if acc is None else self._combine(acc, v)
        return self._empty() if acc is None else acc
