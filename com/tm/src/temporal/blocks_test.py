"""Unit test dyadic block (§4.2): phủ đúng window, rời nhau, aligned, số block, BlockStore."""

import random
import sys
from decimal import Decimal

import pytest

from com.tm.src.temporal.blocks import MAX_K, BlockStore, block_days, blocks_closed_by, decompose
from com.tm.src.temporal.model import DateRange, add_sums, parse_day
from com.tm.src.temporal.ranges import LAST_N, window_of

DS = parse_day("2026-09-15")


def test_decompose_partitions_window():
    for l in range(DS - 300, DS + 1):
        for r in (l, l + 1, l + 6, l + 29, l + 179, l + 399):
            blocks = decompose(l, r)
            days = [d for b in blocks for d in range(block_days(b)[0], block_days(b)[1] + 1)]
            assert days == list(range(l, r + 1))  # phủ đúng, rời nhau, trái → phải
            for k, s in blocks:
                assert 0 <= k <= MAX_K and s % (1 << k) == 0


def test_decompose_bounds():
    # CLAUDE.md §4.2: ≤ 6 block cho A180 (as-of 2026-09-15); cận trên tổng quát 2·log2(N).
    counts = {dr: len(decompose(window_of(dr, DS).l, DS)) for dr in LAST_N}
    assert counts[DateRange.A1] == 1
    assert counts[DateRange.A7] == 3
    assert counts[DateRange.A30] == 4
    assert counts[DateRange.A90] == 5
    assert counts[DateRange.A180] == 6
    for ds in range(DS - 800, DS + 1):
        for n in LAST_N.values():
            assert len(decompose(ds - n + 1, ds)) <= 2 * max(1, n.bit_length())


def test_decompose_rejects_empty():
    with pytest.raises(ValueError):
        decompose(DS, DS - 1)


def test_blocks_closed_by():
    for day in range(DS - 600, DS + 1):
        for k, s in blocks_closed_by(day):
            assert block_days((k, s))[1] == day and s % (1 << k) == 0 and 1 <= k <= MAX_K
        assert len(blocks_closed_by(day)) == min(MAX_K, ((day + 1) & -(day + 1)).bit_length() - 1)


def test_block_store_sum_matches_bruteforce():
    rnd = random.Random(7)
    lo, hi = DS - 520, DS
    daily = {d: {0: {rnd.randint(1, 5): Decimal(rnd.randint(-50, 50))}} for d in range(lo, hi + 1) if rnd.random() < 0.4}
    store = BlockStore(add_sums, dict, lambda v: not v)
    for d in range(lo, hi + 1):
        store.set_day(d, daily.get(d, {}))
        for b in blocks_closed_by(d):
            if b[1] >= lo:
                store.build(b)
    for _ in range(300):
        l = rnd.randint(lo, hi)
        r = rnd.randint(l, hi)
        want: dict = {}
        for d in range(l, r + 1):
            want = add_sums(want, daily.get(d, {}))
        assert store.window(l, r) == want, (l, r)


def test_build_rejects_unaligned():
    store = BlockStore(add_sums, dict, lambda v: not v)
    with pytest.raises(ValueError):
        store.build((2, 3))
    with pytest.raises(ValueError):
        store.build((0, 4))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
