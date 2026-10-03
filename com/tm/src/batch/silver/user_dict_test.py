"""Test new_entries() / first_seen() với SparkSession local. Chạy trong image Spark:
    docker run --rm --entrypoint python3 com.tm.spark.silver_user_profile:v1.0.0 -W ignore /app/com/tm/src/batch/silver/user_dict_test.py
"""

from __future__ import annotations

import datetime as dt
import unittest

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from com.tm.src.batch.silver import user_dict as m

D = dt.date
USERS = T.StructType([T.StructField("user_id", T.StringType()), T.StructField("first_ts_ms", T.LongType()),
                      T.StructField("first_seen_ds", T.DateType())])
EXISTING = T.StructType([T.StructField("user_id", T.StringType()), T.StructField("uidx", T.IntegerType())])
EVENTS = T.StructType([T.StructField("ds", T.DateType()), T.StructField("user_id", T.StringType()),
                       T.StructField("source_ts_ms", T.LongType())])


class UserDictTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("test")
                     .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "1").getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def users(self, rows):
        return self.spark.createDataFrame([(u, t, D(2026, 9, 15)) for u, t in rows], USERS)

    def existing(self, rows):
        return self.spark.createDataFrame(rows, EXISTING)

    def entries(self, users, existing=()):
        out = m.new_entries(self.users(users), self.existing(list(existing))).collect()
        return {r.user_id: r.uidx for r in out}

    def test_dense_from_one_ordered_by_first_appearance(self):
        got = self.entries([("U3", 300), ("U1", 100), ("U2", 200)])
        self.assertEqual(got, {"U1": 1, "U2": 2, "U3": 3})

    def test_tie_on_timestamp_broken_by_user_id(self):
        self.assertEqual(self.entries([("B", 100), ("A", 100)]), {"A": 1, "B": 2})

    def test_arrival_order_does_not_change_assignment(self):
        rows = [("U1", 100), ("U2", 200), ("U3", 300), ("U4", 400)]
        self.assertEqual(self.entries(rows), self.entries(list(reversed(rows))))

    def test_existing_users_get_no_new_entry_and_new_users_continue_after_max(self):
        got = self.entries([("U1", 100), ("U2", 200), ("U9", 900)], existing=[("U1", 1), ("U2", 2)])
        self.assertEqual(got, {"U9": 3})

    def test_gaps_in_existing_are_never_reused(self):
        # dictionary có lỗ (uidx 2 đã từng cấp rồi mất): user mới vẫn tiếp sau max, không lấp lỗ.
        got = self.entries([("N", 500)], existing=[("A", 1), ("C", 3)])
        self.assertEqual(got, {"N": 4})

    def test_user_missing_from_events_keeps_its_entry_untouched(self):
        # user đã bị xoá ở nguồn (không còn trong users): hàm chỉ trả phần thêm, không bao giờ xoá/đổi mapping cũ.
        got = self.entries([("U2", 200)], existing=[("GONE", 1)])
        self.assertEqual(got, {"U2": 2})

    def test_rerun_is_idempotent(self):
        users = [("U1", 100), ("U2", 200)]
        first = self.entries(users)
        again = self.entries(users, existing=list(first.items()))
        self.assertEqual(again, {})

    def test_overflow_beyond_int_range_raises(self):
        with self.assertRaises(ValueError):                    # đã dùng tới uidx lớn nhất của cột INT
            m.new_entries(self.users([("X", 1)]), self.existing([("A", m.MAX_UIDX)]))
        # còn đúng 1 chỗ trống thì cấp được 1 user, nhưng 2 user thì tràn
        self.assertEqual(self.entries([("X", 1)], existing=[("A", m.MAX_UIDX - 1)]), {"X": m.MAX_UIDX})
        with self.assertRaises(ValueError):
            m.new_entries(self.users([("X", 1), ("Y", 2)]), self.existing([("A", m.MAX_UIDX - 1)]))

    def test_first_seen_takes_min_ts_and_min_ds_and_respects_as_of(self):
        ev = self.spark.createDataFrame([
            (D(2026, 9, 2), "U1", 2000), (D(2026, 9, 1), "U1", 1000), (D(2026, 9, 5), "U1", 5000),
            (D(2026, 9, 9), "U2", 9000)], EVENTS)
        got = {r.user_id: (r.first_ts_ms, r.first_seen_ds) for r in m.first_seen(ev, "2026-09-05").collect()}
        self.assertEqual(got, {"U1": (1000, D(2026, 9, 1))})   # U2 xuất hiện sau as_of nên chưa vào


if __name__ == "__main__":
    unittest.main()
