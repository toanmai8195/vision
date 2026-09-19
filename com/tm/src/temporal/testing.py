"""Helper cho test: mô phỏng pipeline hằng ngày (plan → engine) + nạp golden YAML.

Pipeline: với mỗi ngày chạy `D` từ `history_start` tới `ds`, silver nhận dữ liệu tới vào ngày `D`
(late data ≤ 3 ngày), planner sinh bước cho `D` (kèm `reprocess_from` nếu silver ngày cũ đổi), engine chạy.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml
from google.protobuf import json_format
from pyroaring import BitMap

from com.tm.proto.vision.segment.v1 import segment_pb2
from com.tm.src.temporal.engine import AttributeEngine, Silver
from com.tm.src.temporal.model import (
    ANY_TAG,
    AttributeSpec,
    DataType,
    DateRange,
    FeedMode,
    StateInterval,
    TagEvent,
    ValueEvent,
    ValueRange,
    kind_of,
    parse_day,
    parse_decimal,
    parse_ts_ms,
    ts_ms_of,
)
from com.tm.src.temporal.planner import DqStep, RangeStep, plan

Feed = Callable[[int, Silver], int | None]
"""`feed(D, silver)`: nạp dữ liệu tới vào ngày chạy `D`; trả về ngày cũ nhất (< D) có silver thay đổi."""


def run_pipeline(
    attr: AttributeSpec,
    *,
    history_start: int,
    ds: int,
    feed: Feed,
    universe_of: Callable[[int], BitMap],
    ranges_from: int | None = None,
) -> AttributeEngine:
    """Chạy plan từng ngày. `ranges_from`: bỏ bước range/DQ trước ngày này (tăng tốc property test)."""
    silver = Silver(attr.kind)
    engine = AttributeEngine(attr, silver, history_start=history_start, universe_of=universe_of)
    for day in range(history_start, ds + 1):
        reprocess_from = feed(day, silver)
        steps = plan(day, attr, history_start=history_start, reprocess_from=reprocess_from)
        if ranges_from is not None and day < ranges_from:
            steps = [s for s in steps if not isinstance(s, (RangeStep, DqStep))]
        engine.run(steps)
    return engine


def arrival_feed(tag_events: Iterable[tuple[int, TagEvent]] = (), value_events: Iterable[tuple[int, ValueEvent]] = ()) -> Feed:
    """Event tới silver vào ngày `arrival` (≥ ds của event)."""
    tag_by_day: dict[int, list[TagEvent]] = defaultdict(list)
    value_by_day: dict[int, list[ValueEvent]] = defaultdict(list)
    for arrival, ev in tag_events:
        tag_by_day[max(arrival, ev.ds)].append(ev)
    for arrival, ev in value_events:
        value_by_day[max(arrival, ev.ds)].append(ev)

    def feed(day: int, silver: Silver) -> int | None:
        tags, values = tag_by_day.get(day, []), value_by_day.get(day, [])
        silver.add_tag_events(tags)
        silver.add_value_events(values)
        late = [ev.ds for ev in [*tags, *values] if ev.ds < day]
        return min(late) if late else None

    return feed


def intervals_feed(versions: Callable[[int], tuple[list[StateInterval], int | None]]) -> Feed:
    """STATE: `versions(D)` = (SCD2 như biết tại D, ngày cũ nhất bị sửa muộn hoặc None)."""

    def feed(day: int, silver: Silver) -> int | None:
        intervals, changed_from = versions(day)
        silver.set_intervals(intervals)
        return changed_from if changed_from is not None and changed_from < day else None

    return feed


# --------------------------------------------------------------------------- golden YAML


@dataclass
class GoldenCase:
    name: str
    attr: AttributeSpec
    ds: int
    history_start: int
    universe: BitMap
    tag_events: list[tuple[int, TagEvent]] = field(default_factory=list)
    value_events: list[tuple[int, ValueEvent]] = field(default_factory=list)
    intervals: list[StateInterval] = field(default_factory=list)
    expected: dict[str, Any] = field(default_factory=dict)

    def tag(self, name: str) -> int:
        return ANY_TAG if name == "__any__" else self.attr.tag_id(name)

    def tag_name(self, tag_id: int) -> str:
        if tag_id == ANY_TAG:
            return "__any__"
        return next(n for n, t in self.attr.tags.items() if t == tag_id)

    def run(self) -> AttributeEngine:
        if self.attr.kind.is_state:
            feed = intervals_feed(lambda _day: (self.intervals, None))
        else:
            feed = arrival_feed(self.tag_events, self.value_events)
        return run_pipeline(
            self.attr, history_start=self.history_start, ds=self.ds, feed=feed, universe_of=lambda _ds: self.universe
        )


def _value_range(d: Mapping[str, Any]) -> ValueRange:
    return ValueRange(
        from_value=parse_decimal(str(d["from"])) if "from" in d else None,
        from_inclusive=d.get("from_inclusive", True),
        to_value=parse_decimal(str(d["to"])) if "to" in d else None,
        to_inclusive=d.get("to_inclusive", False),
    )


def _ts(raw: Mapping[str, Any]) -> int:
    if "ts" in raw:
        return parse_ts_ms(str(raw["ts"]))
    hh, mm = map(int, str(raw.get("at", "12:00")).split(":"))
    return ts_ms_of(parse_day(str(raw["day"])), hh * 3600 + mm * 60)


def _parse_case(raw: Mapping[str, Any]) -> GoldenCase:
    a = raw["attribute"]
    kind = kind_of(DataType.Value(a["data_type"]), FeedMode.Value(a.get("feed_mode", "EVENT")))
    tags: dict[str, int] = {}
    value_ranges: dict[int, ValueRange] = {}
    for name, spec in a["tags"].items():
        if isinstance(spec, int):
            tags[name] = spec
        else:
            tags[name] = spec["id"]
            value_ranges[spec["id"]] = _value_range(spec["value_range"])
    attr = AttributeSpec(
        attr_id=a["id"],
        name=a["name"],
        kind=kind,
        tags=tags,
        supported_date_ranges=frozenset(DateRange.Value(x) for x in a["supported_date_ranges"]),
        value_ranges=value_ranges,
    )
    case = GoldenCase(
        name=raw["name"],
        attr=attr,
        ds=parse_day(str(raw["ds"])),
        history_start=parse_day(str(raw["history_start"])),
        universe=BitMap(raw.get("universe", [])),
        expected=raw.get("expected", {}),
    )
    for i, e in enumerate(raw.get("tag_events", [])):
        ev = TagEvent(
            event_id=e.get("event_id", f"{case.name}-{i:04d}"),
            uidx=e["uidx"],
            ts_ms=_ts(e),
            tags_add=frozenset(case.tag(t) for t in e.get("add", [])),
            tags_remove=frozenset(case.tag(t) for t in e.get("remove", [])),
        )
        case.tag_events.append((parse_day(str(e["arrival"])) if "arrival" in e else ev.ds, ev))
    for i, e in enumerate(raw.get("value_events", [])):
        ev = ValueEvent(
            event_id=e.get("event_id", f"{case.name}-{i:04d}"),
            uidx=e["uidx"],
            ts_ms=_ts(e),
            tag=case.tag(e["tag"]) if "tag" in e else ANY_TAG,
            value=parse_decimal(str(e["value"])),
        )
        case.value_events.append((parse_day(str(e["arrival"])) if "arrival" in e else ev.ds, ev))
    for e in raw.get("intervals", []):
        case.intervals.append(
            StateInterval(
                uidx=e["uidx"],
                tag=case.tag(e["tag"]),
                valid_from=parse_day(str(e["from"])),
                valid_to=parse_day(str(e["to"])) if e.get("to") else None,
            )
        )
    return case


def load_cases(path: Path) -> list[GoldenCase]:
    with open(path) as f:
        doc = yaml.safe_load(f)
    return [_parse_case(c) for c in doc["cases"]]


def load_yaml(path: Path) -> Any:
    with open(path) as f:
        return yaml.safe_load(f)


def parse_condition(d: Mapping[str, Any]) -> segment_pb2.Condition:
    return json_format.ParseDict(d, segment_pb2.Condition())


def parse_rule(d: Mapping[str, Any]) -> segment_pb2.Rule:
    return json_format.ParseDict(d, segment_pb2.Rule())


def as_sets(case: GoldenCase, d: Mapping[str, Iterable[int]] | None) -> dict[int, BitMap]:
    """Tag name → list uidx (YAML) → tag_id → BitMap, bỏ tag rỗng."""
    return {case.tag(t): BitMap(v) for t, v in (d or {}).items() if v}


def as_sums(case: GoldenCase, rows: Iterable[Mapping[str, Any]]) -> dict[int, dict[int, Decimal]]:
    out: dict[int, dict[int, Decimal]] = {}
    for row in rows:
        tag = case.tag(row["tag"]) if "tag" in row else ANY_TAG
        out.setdefault(tag, {})[row["uidx"]] = parse_decimal(str(row["value"]))
    return out
