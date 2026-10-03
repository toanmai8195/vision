"""Test build_scd2() với SparkSession local. Chạy trong image Spark:
    docker run --rm --entrypoint python3 com.tm.spark.silver_user_profile:v1.0.0 -W ignore /app/com/tm/src/batch/silver/user_profile_scd2_test.py
"""

from __future__ import annotations

import datetime as dt
import unittest

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from com.tm.src.batch.silver import user_profile_scd2 as m

EVENTS_SCHEMA = T.StructType([
    T.StructField("ds", T.DateType()),
    T.StructField("kafka_partition", T.IntegerType()),
    T.StructField("kafka_offset", T.LongType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("op", T.StringType()),
    T.StructField("source_ts_ms", T.LongType()),
    T.StructField("city_code", T.StringType()),
    T.StructField("birth_date", T.DateType()),
    T.StructField("gender", T.StringType()),
])

UTC = dt.timezone.utc
ICT = dt.timezone(dt.timedelta(hours=7))
D = dt.date
OPEN = D(9999, 12, 31)


def ts(y, mo, d, h=0, mi=0):
    """Mili-giây của một thời điểm theo giờ ICT, và ds (ngày ICT) tương ứng."""
    t = dt.datetime(y, mo, d, h, mi, tzinfo=ICT)
    return int(t.timestamp() * 1000), t.date()


class Scd2Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("test")
                     .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "1").getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def events(self, rows):
        """rows: (user, op, (y,m,d[,h,mi]), city, birth, gender) -> DataFrame events; offset theo thứ tự trong rows."""
        data = []
        for i, (user, op, when, city, birth, gender) in enumerate(rows):
            ms, ds = ts(*when)
            data.append((ds, 0, i, user, op, ms, city, birth, gender))
        return self.spark.createDataFrame(data, EVENTS_SCHEMA)

    def versions(self, rows, as_of=None):
        out = m.build_scd2(self.events(rows), as_of).collect()
        return sorted((r.user_id, r.valid_from, r.valid_to, r.city_code, r.birth_date, r.gender, r.is_current) for r in out)

    def test_golden_user_city_example(self):
        """data-flow-examples.md §2.1: U1001 đổi HCM->HN, U1002 mới, U1003 không đổi."""
        rows = [
            ("U1003", "r", (2024, 6, 1), "HN", D(1985, 2, 14), None),
            ("U1001", "r", (2025, 1, 10), "HCM", D(1990, 5, 20), "F"),
            ("U1001", "u", (2026, 9, 15, 10), "HN", D(1990, 5, 20), "F"),
            ("U1002", "c", (2026, 9, 15, 11), "HCM", D(2001, 11, 3), "M"),
        ]
        self.assertEqual(self.versions(rows), [
            ("U1001", D(2025, 1, 10), D(2026, 9, 15), "HCM", D(1990, 5, 20), "F", False),
            ("U1001", D(2026, 9, 15), OPEN, "HN", D(1990, 5, 20), "F", True),
            ("U1002", D(2026, 9, 15), OPEN, "HCM", D(2001, 11, 3), "M", True),
            ("U1003", D(2024, 6, 1), OPEN, "HN", D(1985, 2, 14), None, True),
        ])

    def test_same_day_changes_collapse_to_end_of_day(self):
        rows = [
            ("U1", "r", (2026, 9, 1), "HCM", None, None),
            ("U1", "u", (2026, 9, 5, 9), "DN", None, None),     # trạng thái trung gian, bị gộp
            ("U1", "u", (2026, 9, 5, 18), "CT", None, None),    # trạng thái cuối ngày
        ]
        self.assertEqual(self.versions(rows), [
            ("U1", D(2026, 9, 1), D(2026, 9, 5), "HCM", None, None, False),
            ("U1", D(2026, 9, 5), OPEN, "CT", None, None, True),
        ])

    def test_change_back_within_day_creates_no_version(self):
        rows = [
            ("U1", "r", (2026, 9, 1), "HCM", None, None),
            ("U1", "u", (2026, 9, 5, 9), "HN", None, None),
            ("U1", "u", (2026, 9, 5, 18), "HCM", None, None),   # cuối ngày về lại HCM: coi như không đổi
        ]
        self.assertEqual(self.versions(rows), [("U1", D(2026, 9, 1), OPEN, "HCM", None, None, True)])

    def test_unchanged_events_do_not_open_new_version(self):
        rows = [("U1", "r", (2026, 9, 1), "HN", None, "F"), ("U1", "u", (2026, 9, 3), "HN", None, "F"),
                ("U1", "u", (2026, 9, 4), "HN", None, "F")]
        self.assertEqual(self.versions(rows), [("U1", D(2026, 9, 1), OPEN, "HN", None, "F", True)])

    def test_gender_or_birth_date_change_opens_version(self):
        rows = [("U1", "r", (2026, 9, 1), "HN", D(1990, 1, 1), None), ("U1", "u", (2026, 9, 3), "HN", D(1990, 1, 1), "F"),
                ("U1", "u", (2026, 9, 6), "HN", D(1991, 1, 1), "F")]
        self.assertEqual([(v[1], v[2]) for v in self.versions(rows)],
                         [(D(2026, 9, 1), D(2026, 9, 3)), (D(2026, 9, 3), D(2026, 9, 6)), (D(2026, 9, 6), OPEN)])

    def test_clearing_city_is_a_version_not_a_delete(self):
        rows = [("U1", "r", (2026, 9, 1), "HN", None, None), ("U1", "u", (2026, 9, 4), None, None, None)]
        self.assertEqual(self.versions(rows), [
            ("U1", D(2026, 9, 1), D(2026, 9, 4), "HN", None, None, False),
            ("U1", D(2026, 9, 4), OPEN, None, None, None, True),
        ])

    def test_delete_closes_version_and_recreate_opens_new_one(self):
        rows = [("U1", "r", (2026, 9, 1), "HN", None, None), ("U1", "d", (2026, 9, 7), "HN", None, None),
                ("U1", "c", (2026, 9, 10), "DN", None, None)]
        self.assertEqual(self.versions(rows), [
            ("U1", D(2026, 9, 1), D(2026, 9, 7), "HN", None, None, False),     # đóng khi xoá
            ("U1", D(2026, 9, 10), OPEN, "DN", None, None, True),             # khoảng trống 09-07..09-10 giữ nguyên
        ])

    def test_deleted_user_has_no_current_version(self):
        rows = [("U1", "r", (2026, 9, 1), "HN", None, None), ("U1", "d", (2026, 9, 7), "HN", None, None)]
        out = self.versions(rows)
        self.assertEqual(out, [("U1", D(2026, 9, 1), D(2026, 9, 7), "HN", None, None, False)])

    def test_order_of_arrival_does_not_matter_late_data(self):
        rows = [("U1", "r", (2026, 9, 1), "HCM", None, None), ("U1", "u", (2026, 9, 5), "HN", None, None),
                ("U1", "u", (2026, 9, 3), "DN", None, None)]          # đến muộn: event ngày 03 vào sau ngày 05
        self.assertEqual([(v[1], v[2], v[3]) for v in self.versions(rows)], [
            (D(2026, 9, 1), D(2026, 9, 3), "HCM"), (D(2026, 9, 3), D(2026, 9, 5), "DN"), (D(2026, 9, 5), OPEN, "HN")])

    def test_as_of_ignores_later_events(self):
        rows = [("U1", "r", (2026, 9, 1), "HCM", None, None), ("U1", "u", (2026, 9, 5), "HN", None, None)]
        self.assertEqual(self.versions(rows, as_of="2026-09-04"), [("U1", D(2026, 9, 1), OPEN, "HCM", None, None, True)])

    def test_versions_of_a_user_are_contiguous_and_one_current(self):
        rows = [("U1", "r", (2026, 9, 1), "A", None, None), ("U1", "u", (2026, 9, 2), "B", None, None),
                ("U1", "u", (2026, 9, 3), "C", None, None), ("U2", "r", (2026, 9, 1), "A", None, None)]
        out = m.build_scd2(self.events(rows)).collect()
        u1 = sorted((r for r in out if r.user_id == "U1"), key=lambda r: r.valid_from)
        for a, b in zip(u1, u1[1:]):
            self.assertEqual(a.valid_to, b.valid_from)
        self.assertEqual(sum(1 for r in out if r.user_id == "U1" and r.is_current), 1)


if __name__ == "__main__":
    unittest.main()
