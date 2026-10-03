"""Silver (L2), bước 2: silver.user_profile_cdc_events -> silver.user_profile_scd2 (SCD2 mức ngày).

Input : `vision.silver.user_profile_cdc_events` (job user_profile_events.py).
Output: `vision.silver.user_profile_scd2`: user_id, city_code, birth_date, gender, valid_from, valid_to, is_current.

Quy tắc (xem docs/phases/step-04-silver.md)
- Mức NGÀY: trạng thái của user trong ngày `ds` = event cuối ngày theo (source_ts_ms, kafka_partition, kafka_offset);
  nhiều thay đổi cùng ngày chỉ cho ra một version (khớp cách daily tính ADDED/REMOVED).
- Đổi `city_code`/`birth_date`/`gender` => đóng version cũ, mở version mới. Khoảng nửa mở [valid_from, valid_to),
  version hiện tại có valid_to = 9999-12-31 và is_current = true.
- op=d: đóng version hiện tại (valid_to = ds xoá), không mở version mới; user quay lại thì mở version mới.
- Dựng lại TOÀN BỘ từ events có ds <= --ds mỗi lần chạy rồi ghi đè cả bảng: idempotent, tự đúng với event đến muộn.
  (Chạy ds cũ sau ds mới đưa bảng về trạng thái tới ds cũ.)
Python 3.8 (image Spark).
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from com.tm.src.batch.silver.user_profile_events import EVENTS, build_session

SCD2 = "vision.silver.user_profile_scd2"
OPEN_END = "9999-12-31"

SCD2_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  user_id STRING, city_code STRING, birth_date DATE, gender STRING,
  valid_from DATE, valid_to DATE, is_current BOOLEAN
) USING iceberg
"""


def build_scd2(events: DataFrame, as_of: Optional[str] = None) -> DataFrame:
    """Events sạch -> bảng SCD2 mức ngày. Thuần Spark, không đụng catalog.

    Cột bắt buộc của events: ds, kafka_partition, kafka_offset, user_id, op, source_ts_ms, city_code, birth_date, gender.
    `as_of` (YYYY-MM-DD): chỉ dùng events có ds <= as_of.
    """
    if as_of is not None:
        events = events.filter(F.col("ds") <= F.lit(as_of).cast("date"))

    # 1. Trạng thái cuối ngày của mỗi (user, ds): event cuối theo (source_ts_ms, partition, offset).
    last_in_day = Window.partitionBy("user_id", "ds").orderBy(
        F.col("source_ts_ms").desc(), F.col("kafka_partition").desc(), F.col("kafka_offset").desc())
    day_state = (events.withColumn("_rn", F.row_number().over(last_in_day)).filter("_rn = 1")
                 .select("user_id", "ds", (F.col("op") == "d").alias("deleted"), "city_code", "birth_date", "gender"))

    # 2. Chữ ký trạng thái (null-safe) để phát hiện thay đổi; bản ghi xoá có chữ ký riêng.
    null = F.lit("∅")
    sig = F.when(F.col("deleted"), F.lit("DELETED")).otherwise(F.concat_ws(
        "|", F.coalesce(F.col("city_code"), null), F.coalesce(F.col("birth_date").cast("string"), null),
        F.coalesce(F.col("gender"), null)))
    by_day = Window.partitionBy("user_id").orderBy("ds")
    marked = day_state.withColumn("_sig", sig).withColumn("_prev_sig", F.lag("_sig").over(by_day))

    # 3. Chỉ giữ ngày có thay đổi (điểm đổi); version k kéo dài tới điểm đổi kế tiếp.
    changes = marked.filter(F.col("_prev_sig").isNull() | (F.col("_sig") != F.col("_prev_sig")))
    nxt = F.lead("ds").over(by_day)
    versions = changes.withColumn("_next_ds", nxt).filter(~F.col("deleted"))
    open_end = F.lit(OPEN_END).cast("date")
    return versions.select(
        "user_id", "city_code", "birth_date", "gender",
        F.col("ds").alias("valid_from"),
        F.coalesce(F.col("_next_ds"), open_end).alias("valid_to"),
        F.col("_next_ds").isNull().alias("is_current"),
    )


def run(spark: SparkSession, ds: str) -> int:
    """Dựng lại SCD2 as-of `ds` và ghi đè cả bảng. Trả về số version."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS vision.silver")
    spark.sql(SCD2_DDL.format(table=SCD2))
    scd2 = build_scd2(spark.table(EVENTS), ds).cache()
    n = scd2.count()
    scd2.writeTo(SCD2).overwrite(F.lit(True))
    return n


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ds", default=None, help="YYYY-MM-DD theo ICT (mặc định: hôm nay ICT)")
    args = p.parse_args(argv)
    ds = args.ds or (dt.datetime.utcnow() + dt.timedelta(hours=7)).date().isoformat()
    try:
        dt.date.fromisoformat(ds)
    except ValueError:
        p.error("--ds phải có dạng YYYY-MM-DD")
    spark = build_session("silver_user_profile_scd2")
    try:
        print("silver user_profile_scd2 as-of ds=%s: versions=%d" % (ds, run(spark, ds)))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
