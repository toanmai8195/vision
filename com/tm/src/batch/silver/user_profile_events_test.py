"""Test clean() với SparkSession local (không cần Iceberg/Kafka). Chạy trong image Spark:
    docker run --rm --entrypoint python3 com.tm.spark.silver_user_profile:v1.0.0 /app/com/tm/src/batch/silver/user_profile_events_test.py
(unittest thuần: image Spark không có pytest; Python 3.8.)"""

from __future__ import annotations

import datetime as dt
import json
import unittest

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from com.tm.src.batch.silver import user_profile_events as m

BRONZE_SCHEMA = T.StructType([
    T.StructField("topic", T.StringType()),
    T.StructField("kafka_partition", T.IntegerType()),
    T.StructField("kafka_offset", T.LongType()),
    T.StructField("msg_key", T.StringType()),
    T.StructField("payload", T.StringType()),
    T.StructField("ingest_ts", T.TimestampType()),
])

UTC = dt.timezone.utc


def ms(y, mo, d, h=0, mi=0, s=0):
    return int(dt.datetime(y, mo, d, h, mi, s, tzinfo=UTC).timestamp() * 1000)


def envelope(op, user, city=None, birth=None, gender=None, ts=0, before=None, after=None):
    row = {"user_id": user, "city_code": city, "birth_date": birth, "gender": gender}
    body = {"op": op, "source": {"ts_ms": ts}, "ts_ms": ts + 5}
    body["before"] = row if before is None and op == "d" else before
    body["after"] = None if op == "d" else (row if after is None else after)
    return json.dumps(body)


class CleanTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("test")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def run_clean(self, rows):
        ingest = dt.datetime(2026, 9, 15, 3, 0, tzinfo=UTC)
        data = [("t", p, o, k, payload, ingest) for (p, o, k, payload) in rows]
        return m.clean(self.spark.createDataFrame(data, BRONZE_SCHEMA))

    def key(self, u):
        return json.dumps({"user_id": u})

    def test_valid_events_parsed_for_every_op(self):
        rows = [
            (0, 0, self.key("U1"), envelope("r", "U1", "HCM", 7444, "F", ms(2026, 9, 15, 3))),
            (0, 1, self.key("U1"), envelope("c", "U2", "HN", 11629, None, ms(2026, 9, 15, 4))),
            (0, 2, self.key("U1"), envelope("u", "U1", "HN", 7444, "F", ms(2026, 9, 15, 5))),
            (0, 3, self.key("U1"), envelope("d", "U1", "HN", 7444, "F", ms(2026, 9, 15, 6))),
        ]
        events, dlq = self.run_clean(rows)
        self.assertEqual(dlq.count(), 0)
        got = {r.kafka_offset: r for r in events.collect()}
        self.assertEqual({o: r.op for o, r in got.items()}, {0: "r", 1: "c", 2: "u", 3: "d"})
        self.assertEqual(got[0].birth_date, dt.date(1990, 5, 20))
        self.assertEqual(got[1].birth_date, dt.date(2001, 11, 3))
        self.assertIsNone(got[1].gender)
        # delete: trạng thái lấy từ `before`
        self.assertEqual((got[3].city_code, got[3].gender), ("HN", "F"))
        self.assertEqual(got[2].user_id, "U1")

    def test_dedup_by_kafka_position(self):
        e = envelope("c", "U1", "HCM", 7444, "F", ms(2026, 9, 15, 3))
        rows = [(0, 7, self.key("U1"), e), (0, 7, self.key("U1"), e),   # cùng vị trí: bản giao trùng
                (1, 7, self.key("U1"), e)]                               # partition khác: là message khác
        events, dlq = self.run_clean(rows)
        self.assertEqual(sorted((r.kafka_partition, r.kafka_offset) for r in events.collect()), [(0, 7), (1, 7)])
        self.assertEqual(dlq.count(), 0)

    def test_ds_uses_ict_not_utc(self):
        cases = {
            ms(2026, 9, 14, 16, 59, 59): dt.date(2026, 9, 14),   # 23:59:59 ICT
            ms(2026, 9, 14, 17, 0, 0): dt.date(2026, 9, 15),     # 00:00:00 ICT
            ms(2026, 9, 14, 18, 30, 0): dt.date(2026, 9, 15),    # 01:30 ICT (ví dụ e-9003)
        }
        rows = [(0, i, self.key("U1"), envelope("u", "U1", "HN", 7444, "F", ts)) for i, ts in enumerate(cases)]
        events, _ = self.run_clean(rows)
        by_ts = {r.source_ts_ms: r.ds for r in events.collect()}
        self.assertEqual(by_ts, cases)

    def test_bad_rows_go_to_dlq_with_reason(self):
        good_ts = ms(2026, 9, 15, 3)
        rows = [
            (0, 0, self.key("U1"), "not json"),
            (0, 1, self.key("U1"), json.dumps({"source": {"ts_ms": good_ts}})),                       # thiếu op
            (0, 2, self.key("U1"), json.dumps({"op": "x", "source": {"ts_ms": good_ts}, "after": {"user_id": "U1"}})),
            (0, 3, self.key("U1"), json.dumps({"op": "c", "after": {"user_id": "U1"}})),               # thiếu source.ts_ms
            (0, 4, None, json.dumps({"op": "c", "source": {"ts_ms": good_ts}, "after": {"city_code": "HN"}})),  # thiếu user_id
            (0, 5, self.key("U1"), json.dumps({"op": "c", "source": {"ts_ms": good_ts}, "after": None})),       # thiếu trạng thái
            (0, 6, self.key("U1"), None),                                                              # payload null
            (0, 7, self.key("U1"), envelope("c", "U1", "HCM", 7444, "F", good_ts)),                    # hợp lệ
        ]
        events, dlq = self.run_clean(rows)
        self.assertEqual([r.kafka_offset for r in events.collect()], [7])
        reasons = {r.kafka_offset: r.reason for r in dlq.collect()}
        self.assertEqual(reasons, {0: "invalid_json", 1: "unknown_op", 2: "unknown_op", 3: "missing_source_ts",
                                   4: "missing_user_id", 5: "missing_row_state", 6: "invalid_json"})
        # DLQ giữ nguyên payload gốc để điều tra; ds của DLQ = ngày ICT của ingest_ts (03:00Z = 10:00 ICT 09-15)
        self.assertEqual({r.ds for r in dlq.collect()}, {dt.date(2026, 9, 15)})
        self.assertEqual(next(r.payload for r in dlq.collect() if r.kafka_offset == 0), "not json")

    def test_user_id_falls_back_to_payload_when_key_missing(self):
        rows = [(0, 0, None, envelope("c", "U9", "HN", 7444, "F", ms(2026, 9, 15, 3)))]
        events, dlq = self.run_clean(rows)
        self.assertEqual([r.user_id for r in events.collect()], ["U9"])
        self.assertEqual(dlq.count(), 0)


if __name__ == "__main__":
    unittest.main()
