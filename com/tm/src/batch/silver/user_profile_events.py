"""Silver (L2), bước 1: bronze.user_profile_cdc_raw -> silver.user_profile_cdc_events (+ DLQ).

Input : Iceberg `vision.bronze.user_profile_cdc_raw` (envelope Debezium nguyên văn, bước 3).
Output: `vision.silver.user_profile_cdc_events` (sự kiện CDC sạch, phân vùng `ds`)
        `vision.silver.user_profile_cdc_dlq`    (dòng lỗi + lý do, phân vùng `ds` = ngày ICT của ingest_ts)

Cách làm
- Dedup theo vị trí Kafka (kafka_partition, kafka_offset): user_profile là bảng trạng thái qua CDC, không có
  `event_id`; mỗi message Kafka có vị trí duy nhất nên bản trùng do at-least-once (Flink restart) bị loại.
- `ds` = ngày theo Asia/Ho_Chi_Minh (ICT) của `source.ts_ms` (thời điểm thay đổi ở DB nguồn).
- Dòng lỗi vào DLQ với `reason`: invalid_json, unknown_op, missing_source_ts, missing_user_id, missing_row_state.
- Trạng thái của dòng sau sự kiện: `after`; với op=d là `before`. Debezium phát DATE (birth_date) là số ngày từ
  1970-01-01 nên đổi lại thành date.
- Idempotent: ghi theo `ds` bằng overwrite(ds = D) (một commit atomic, kể cả khi kết quả rỗng) -> chạy lại cùng
  `--ds` ra cùng kết quả. Event đến muộn: chạy lại `ds` cũ sẽ gom thêm (quét toàn bộ bronze).

Chạy (trong image Spark, compose profile `silver`):
    spark-submit --master 'local[*]' user_profile_events.py --ds 2026-09-15
Chú ý: image Spark dùng Python 3.8 nên file này tương thích 3.8.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from typing import Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

ICT = "Asia/Ho_Chi_Minh"
VALID_OPS = ["c", "u", "d", "r"]  # create, update, delete, snapshot (Debezium)

CATALOG = "vision"
BRONZE = "vision.bronze.user_profile_cdc_raw"
EVENTS = "vision.silver.user_profile_cdc_events"
DLQ = "vision.silver.user_profile_cdc_dlq"

EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  ds DATE, kafka_partition INT, kafka_offset BIGINT, user_id STRING, op STRING,
  source_ts_ms BIGINT, event_ts TIMESTAMP, city_code STRING, birth_date DATE, gender STRING, ingest_ts TIMESTAMP
) USING iceberg PARTITIONED BY (ds)
"""
DLQ_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  ds DATE, topic STRING, kafka_partition INT, kafka_offset BIGINT, msg_key STRING, payload STRING,
  reason STRING, ingest_ts TIMESTAMP
) USING iceberg PARTITIONED BY (ds)
"""


def _ict_date(ts: "F.Column") -> "F.Column":
    """Ngày (DATE) theo ICT của một timestamp (session timezone phải là UTC)."""
    return F.to_date(F.from_utc_timestamp(ts, ICT))


def clean(bronze: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Bronze -> (events sạch, DLQ), cả hai có cột `ds`, chưa lọc theo ngày. Thuần Spark, không đụng catalog.

    Cột bắt buộc của bronze: topic, kafka_partition, kafka_offset, msg_key, payload, ingest_ts.
    """
    # 1. Dedup theo vị trí Kafka (giữ bản ghi vào bronze sớm nhất).
    pos = Window.partitionBy("kafka_partition", "kafka_offset").orderBy(F.col("ingest_ts").asc_nulls_last())
    dedup = bronze.withColumn("_rn", F.row_number().over(pos)).filter("_rn = 1").drop("_rn")

    # 2. Trích trường từ payload (không dựa vào cột op/source_ts_ms Flink đã trích sẵn).
    payload = F.col("payload")
    op = F.get_json_object(payload, "$.op")
    source_ts_ms = F.get_json_object(payload, "$.source.ts_ms").cast("long")
    user_id = F.coalesce(
        F.get_json_object(F.col("msg_key"), "$.user_id"),
        F.get_json_object(payload, "$.after.user_id"),
        F.get_json_object(payload, "$.before.user_id"),
    )
    is_delete = op == F.lit("d")
    state_obj = F.when(is_delete, F.get_json_object(payload, "$.before")).otherwise(F.get_json_object(payload, "$.after"))

    def state(field: str) -> "F.Column":
        return F.when(is_delete, F.get_json_object(payload, "$.before." + field)).otherwise(
            F.get_json_object(payload, "$.after." + field)
        )

    parsed = dedup.select(
        "topic", "kafka_partition", "kafka_offset", "msg_key", "payload", "ingest_ts",
        op.alias("op"),
        source_ts_ms.alias("source_ts_ms"),
        user_id.alias("user_id"),
        state_obj.alias("_state"),
        state("city_code").alias("city_code"),
        state("birth_date").cast("int").alias("_birth_days"),
        state("gender").alias("gender"),
        F.get_json_object(payload, "$").alias("_json"),
    )

    # 3. Lý do lỗi (None = hợp lệ), kiểm tra theo thứ tự ưu tiên.
    reason = (
        F.when(F.col("_json").isNull(), "invalid_json")
        .when(F.col("op").isNull() | ~F.col("op").isin(VALID_OPS), "unknown_op")
        .when(F.col("source_ts_ms").isNull(), "missing_source_ts")
        .when(F.col("user_id").isNull(), "missing_user_id")
        .when(F.col("_state").isNull(), "missing_row_state")
    )
    classified = parsed.withColumn("reason", reason)

    event_ts = (F.col("source_ts_ms") / 1000).cast("timestamp")
    events = classified.filter(F.col("reason").isNull()).select(
        _ict_date(event_ts).alias("ds"),
        "kafka_partition", "kafka_offset", "user_id", "op", "source_ts_ms",
        event_ts.alias("event_ts"),
        "city_code",
        F.date_add(F.lit("1970-01-01").cast("date"), F.col("_birth_days")).alias("birth_date"),
        "gender", "ingest_ts",
    )
    dlq = classified.filter(F.col("reason").isNotNull()).select(
        _ict_date(F.col("ingest_ts")).alias("ds"),
        "topic", "kafka_partition", "kafka_offset", "msg_key", "payload", "reason", "ingest_ts",
    )
    return events, dlq


def _env(key: str, default: str) -> str:
    return os.environ.get(key) or default


def build_session(app_name: str = "silver_user_profile_events") -> SparkSession:
    """SparkSession với catalog Iceberg REST `vision` (cấu hình qua biến môi trường, như job Flink)."""
    c = "spark.sql.catalog." + CATALOG
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(c, "org.apache.iceberg.spark.SparkCatalog")
        .config(c + ".type", "rest")
        .config(c + ".uri", _env("ICEBERG_REST_URI", "http://iceberg-rest:8181"))
        .config(c + ".warehouse", _env("ICEBERG_WAREHOUSE", "s3://vision-warehouse/"))
        .config(c + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(c + ".s3.endpoint", _env("S3_ENDPOINT", "http://minio:9000"))
        .config(c + ".s3.path-style-access", "true")
        .config(c + ".s3.access-key-id", _env("S3_ACCESS_KEY", "vision"))
        .config(c + ".s3.secret-access-key", _env("S3_SECRET_KEY", "vision-secret"))
        .config(c + ".client.region", "us-east-1")
        .getOrCreate()
    )


def _today_ict() -> str:
    return (dt.datetime.utcnow() + dt.timedelta(hours=7)).date().isoformat()


def run(spark: SparkSession, ds: str) -> Tuple[int, int]:
    """Xử lý một `ds`: ghi đè phân vùng ds của events và DLQ. Trả về (số events, số DLQ) của ngày đó."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS vision.silver")
    spark.sql(EVENTS_DDL.format(table=EVENTS))
    spark.sql(DLQ_DDL.format(table=DLQ))

    events, dlq = clean(spark.table(BRONZE))
    day = F.lit(ds).cast("date")
    ev, dl = events.filter(F.col("ds") == day).cache(), dlq.filter(F.col("ds") == day).cache()
    n_ev, n_dl = ev.count(), dl.count()
    ev.writeTo(EVENTS).overwrite(F.col("ds") == day)
    dl.writeTo(DLQ).overwrite(F.col("ds") == day)
    return n_ev, n_dl


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ds", default=None, help="YYYY-MM-DD theo ICT (mặc định: hôm nay ICT)")
    args = p.parse_args(argv)
    ds = args.ds or _today_ict()
    try:
        dt.date.fromisoformat(ds)
    except ValueError:
        p.error("--ds phải có dạng YYYY-MM-DD")
    spark = build_session()
    try:
        n_ev, n_dl = run(spark, ds)
        print("silver user_profile ds=%s: events=%d dlq=%d" % (ds, n_ev, n_dl))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
