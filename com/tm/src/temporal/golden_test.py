"""Golden test (CLAUDE.md §4.4): số liệu trong com/tm/docs/data-flow-examples.md và data-types.md.

Mỗi case chạy pipeline hằng ngày (planner → engine, có late data) và so với kỳ vọng; kết quả window
cũng được đối chiếu với reference (cách tính ngây thơ).
"""

import sys
from pathlib import Path

import pytest
from pyroaring import BitMap

from com.tm.src.segment.dsl.evaluator import evaluate, evaluate_condition
from com.tm.src.temporal.blocks import block_days, blocks_closed_by, decompose
from com.tm.src.temporal.model import ANY_TAG, DateRange, Kind, epoch_day, parse_day, to_date
from com.tm.src.temporal.ranges import CustomRange, window_of
from com.tm.src.temporal.reference import Reference
from com.tm.src.temporal.testing import (
    GoldenCase,
    as_sets,
    as_sums,
    load_cases,
    load_yaml,
    parse_condition,
    parse_rule,
)

GOLDEN = Path(__file__).parent / "testdata" / "golden"
CASE_FILES = ["s1_payment.yaml", "s2_cdc.yaml", "s3_churn.yaml", "data_types.yaml"]
CASES = [c for f in CASE_FILES for c in load_cases(GOLDEN / f)]


def reference_of(case: GoldenCase) -> Reference:
    return Reference(
        case.attr,
        tag_events=[ev for _, ev in case.tag_events],
        value_events=[ev for _, ev in case.value_events],
        intervals=case.intervals,
    )


def test_golden_covers_all_kinds():
    assert {c.attr.kind for c in CASES} == set(Kind), "golden phải có đủ 4 loại, MUTEX/NOT_MUTEX cả EVENT và STATE"


@pytest.fixture(scope="module")
def engines():
    return {c.name: c.run() for c in CASES}


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_case(case: GoldenCase, engines):
    engine = engines[case.name]
    ref = reference_of(case)
    exp = case.expected
    assert exp, f"{case.name}: no expectation"

    for row in exp.get("tag_daily", []):
        daily = engine.daily[parse_day(str(row["ds"]))]
        t = case.tag(row["tag"])
        assert daily.add.get(t, BitMap()) == BitMap(row["add"]), row
        if "del" in row:
            assert daily.dele.get(t, BitMap()) == BitMap(row["del"]), row
        if "sig" in row:
            assert daily.sig().get(t, BitMap()) == BitMap(row["sig"]), row

    for row in exp.get("state_daily", []):
        d = parse_day(str(row["ds"]))
        daily, t = engine.daily[d], case.tag(row["tag"])
        assert daily.added.get(t, BitMap()) == BitMap(row["added"]), row
        assert daily.removed.get(t, BitMap()) == BitMap(row["removed"]), row
        assert engine.latest.state_at(d).get(t, BitMap()) == BitMap(row["state"]), row

    if "pv_daily" in exp:
        by_day: dict[int, list] = {}
        for row in exp["pv_daily"]:
            by_day.setdefault(parse_day(str(row["ds"])), []).append(row)
        for d, rows in by_day.items():
            assert engine.daily[d] == as_sums(case, rows), to_date(d)
        assert set(engine.daily) >= set(by_day)
        assert {d for d, v in engine.daily.items() if v} == set(by_day)

    for row in exp.get("blocks", []):
        l, r = parse_day(str(row["from"])), parse_day(str(row["to"]))
        (block,) = decompose(l, r)
        assert block_days(block) == (l, r)
        got = {t: bm for t, bm in engine.blocks.get(block).items() if t != ANY_TAG}
        assert got == as_sets(case, row["tags"]), row

    for row in exp.get("latest", []):
        assert engine.latest.state_at(parse_day(str(row["ds"]))) == as_sets(case, row["tags"]), row

    for name, users in exp.get("seen", {}).items():
        w = window_of(DateRange.Value(name), case.ds)
        assert engine.blocks.window(w.l, w.r).get(ANY_TAG, BitMap()) == BitMap(users), name

    for name, tags in exp.get("ranges", {}).items():
        dr = DateRange.Value(name)
        want = as_sets(case, tags)
        assert engine.query(dr, case.ds).tags == want, name
        w = window_of(dr, case.ds)
        l = w.l if not w.always_active else case.history_start
        assert ref.members(l, w.r) == want, f"reference {name}"

    for name, rows in exp.get("pv_range_value", {}).items():
        dr = DateRange.Value(name)
        want = as_sums(case, rows)
        assert engine.query(dr, case.ds).sums == want, name
        w = window_of(dr, case.ds)
        assert ref.sums(w.l, w.r) == want, f"reference {name}"

    for row in exp.get("custom", []):
        cr = CustomRange(parse_day(str(row["from"])), parse_day(str(row["to"])))
        want = as_sets(case, row["tags"])
        assert engine.query(cr, case.ds).tags == want, row
        assert ref.members(cr.from_day, cr.to_day) == want, f"reference {row}"

    catalog = {case.attr.name: case.attr}
    for row in exp.get("conditions", []):
        cond = parse_condition(row["condition"])
        want = BitMap(row["result"])
        assert evaluate_condition(cond, catalog, {case.attr.name: engine}, case.ds) == want, row
        assert evaluate_condition(cond, catalog, {case.attr.name: ref}, case.ds) == want, f"reference {row}"


def test_segments(engines):
    doc = load_yaml(GOLDEN / "segments.yaml")
    ds = parse_day(str(doc["ds"]))
    by_name = {c.name: c for c in CASES}
    cases = {attr: by_name[case] for attr, case in doc["sources"].items()}
    catalog = {attr: c.attr for attr, c in cases.items()}
    sources = {attr: engines[c.name] for attr, c in cases.items()}
    ref_sources = {attr: reference_of(c) for attr, c in cases.items()}

    for seg in doc["segments"]:
        rule = parse_rule(seg["rule"])
        for step in seg["steps"]:
            node = rule
            for part in step["rule"].split("."):
                node = node.children[int(part.removeprefix("children[").removesuffix("]"))]
            assert evaluate(node, catalog, sources, ds) == BitMap(step["result"]), (seg["segment_id"], step)
        assert evaluate(rule, catalog, sources, ds) == BitMap(seg["result"]), seg["segment_id"]
        assert evaluate(rule, catalog, ref_sources, ds) == BitMap(seg["result"]), f"reference {seg['segment_id']}"


def test_blocks_golden():
    doc = load_yaml(GOLDEN / "blocks.yaml")
    for row in doc["epoch_days"]:
        assert epoch_day(row["date"]) == row["e"]

    def as_blocks(rows):
        return [(b["k"], parse_day(str(b["from"]))) for b in rows]

    for row in doc["closed_by"]:
        got = blocks_closed_by(parse_day(str(row["ds"])))
        assert got == as_blocks(row["blocks"]), row
        for b, want in zip(got, row["blocks"]):
            assert tuple(map(to_date, block_days(b))) == (want["from"], want["to"])

    for row in doc["decompose"]:
        got = decompose(parse_day(str(row["from"])), parse_day(str(row["to"])))
        if "blocks" in row:
            assert got == as_blocks(row["blocks"]), row["name"]
        if "count" in row:
            assert len(got) == row["count"], row["name"]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
