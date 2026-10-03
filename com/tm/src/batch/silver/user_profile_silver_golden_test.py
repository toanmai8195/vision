"""Golden silver của user_profile: envelope Debezium thô (data-flow-examples.md §2.1 L0) -> clean() -> SCD2 (+ uidx).
Khớp bảng L2 của §2.1. Chạy trong image Spark:
    docker run --rm --entrypoint python3 com.tm.spark.silver_user_profile:v1.0.0 -W ignore /app/com/tm/src/batch/silver/user_profile_silver_golden_test.py
"""

from __future__ import annotations

import datetime as dt
import json
import unittest

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from com.tm.src.batch.silver import user_dict as ud
from com.tm.src.batch.silver import user_profile_events as ev
from com.tm.src.batch.silver import user_profile_scd2 as sc

D = dt.date
OPEN = D(9999, 12, 31)

BRONZE_SCHEMA = T.StructType([
    T.StructField("topic", T.StringType()), T.StructField("kafka_partition", T.IntegerType()),
    T.StructField("kafka_offset", T.LongType()), T.StructField("msg_key", T.StringType()),
    T.StructField("payload", T.StringType()), T.StructField("ingest_ts", T.TimestampType()),
])

# L0 của §2.1: (partition, offset, user, envelope). birth_date 7444 = 1990-05-20, 11629 = 2001-11-03, 5523 = 1985-02-14.
def env(op, ts, user, city, birth, gender, before=None):
    after = None if op == "d" else {"user_id": user, "city_code": city, "birth_date": birth, "gender": gender}
    return json.dumps({"op": op, "source": {"table": "user_profile", "ts_ms": ts}, "before": before, "after": after, "ts_ms": ts})

U1001_HCM = {"user_id": "U1001", "city_code": "HCM", "birth_date": 7444, "gender": "F"}
GOLDEN_BRONZE = [
    (0, 1, "U1003", env("r", 1717210800000, "U1003", "HN", 5523, None)),             # 2024-06-01 03:00Z
    (1, 1, "U1001", env("r", 1736478000000, "U1001", "HCM", 7444, "F")),             # 2025-01-10 03:00Z
    (1, 41, "U1001", env("u", 1789441200000, "U1001", "HN", 7444, "F", U1001_HCM)),  # 2026-09-15 03:00Z = 10:00 ICT
    (0, 17, "U1002", env("c", 1789444800000, "U1002", "HCM", 11629, "M")),           # 2026-09-15 04:00Z
]
FIXTURE_DICT = [("U1001", 1), ("U1002", 2), ("U1003", 3), ("U1004", 4)]               # §0: dictionary golden

EXPECTED_SCD2 = [  # §2.1 L2 (sắp theo uidx, valid_from)
    (1, "U1001", "HCM", D(1990, 5, 20), "F", D(2025, 1, 10), D(2026, 9, 15), False),
    (1, "U1001", "HN", D(1990, 5, 20), "F", D(2026, 9, 15), OPEN, True),
    (2, "U1002", "HCM", D(2001, 11, 3), "M", D(2026, 9, 15), OPEN, True),
    (3, "U1003", "HN", D(1985, 2, 14), None, D(2024, 6, 1), OPEN, True),
]


class SilverGoldenTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("golden")
                     .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "1").getOrCreate())
        ingest = dt.datetime(2026, 9, 15, 5, 0, tzinfo=dt.timezone.utc)
        rows = [("vision.src.user_profile.v1", p, o, json.dumps({"user_id": u}), payload, ingest)
                for p, o, u, payload in GOLDEN_BRONZE]
        # message trùng vị trí Kafka (at-least-once) phải bị dedup, không sinh version thừa
        rows.append(("vision.src.user_profile.v1", 1, 41, json.dumps({"user_id": "U1001"}), GOLDEN_BRONZE[2][3], ingest))
        cls.events, cls.dlq = ev.clean(cls.spark.createDataFrame(rows, BRONZE_SCHEMA))
        cls.events.cache()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_clean_gives_four_events_and_empty_dlq(self):
        self.assertEqual(self.events.count(), 4)
        self.assertEqual(self.dlq.count(), 0)
        by_user = {(r.user_id, r.op): r.ds for r in self.events.collect()}
        self.assertEqual(by_user[("U1001", "u")], D(2026, 9, 15))        # 03:00Z = 10:00 ICT, cùng ngày
        self.assertEqual(by_user[("U1002", "c")], D(2026, 9, 15))
        self.assertEqual(by_user[("U1003", "r")], D(2024, 6, 1))

    def test_scd2_matches_data_flow_examples_l2(self):
        fixture = self.spark.createDataFrame(FIXTURE_DICT, "user_id string, uidx int")
        out = sc.with_uidx(sc.build_scd2(self.events, "2026-09-15"), fixture).collect()
        got = sorted((r.uidx, r.user_id, r.city_code, r.birth_date, r.gender, r.valid_from, r.valid_to, r.is_current)
                     for r in out)
        self.assertEqual(got, EXPECTED_SCD2)

    def test_fixture_dictionary_needs_no_new_entries(self):
        existing = self.spark.createDataFrame(FIXTURE_DICT, "user_id string, uidx int")
        self.assertEqual(ud.new_entries(ud.first_seen(self.events, "2026-09-15"), existing).count(), 0)

    def test_real_algorithm_assigns_by_first_appearance(self):
        empty = self.spark.createDataFrame([], "user_id string, uidx int")
        got = {r.user_id: r.uidx for r in ud.new_entries(ud.first_seen(self.events, "2026-09-15"), empty).collect()}
        self.assertEqual(got, {"U1003": 1, "U1001": 2, "U1002": 3})     # khác fixture §0: xem ghi chú ở data-flow-examples §2.1

    def test_rerun_gives_identical_result(self):
        fixture = self.spark.createDataFrame(FIXTURE_DICT, "user_id string, uidx int")
        a = sorted(map(tuple, sc.with_uidx(sc.build_scd2(self.events, "2026-09-15"), fixture).collect()))
        b = sorted(map(tuple, sc.with_uidx(sc.build_scd2(self.events, "2026-09-15"), fixture).collect()))
        self.assertEqual(a, b)


if __name__ == "__main__":
    unittest.main()
