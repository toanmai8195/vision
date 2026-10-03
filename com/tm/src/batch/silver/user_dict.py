"""Silver (L2), bước 3: user_id -> uidx (dictionary append-only).

Input : `vision.silver.user_profile_cdc_events` (mọi user từng xuất hiện, as-of --ds).
Output: `vision.silver.user_dict (user_id STRING, uidx INT, first_seen_ds DATE)`.

Luật (CLAUDE.md §6)
- uidx là số nguyên dày bắt đầu từ 1; gán cho user CHƯA có trong dictionary theo thứ tự xuất hiện đầu tiên
  (source_ts_ms nhỏ nhất, hòa thì theo user_id) => dựng lại từ đầu ra cùng kết quả.
- CHỈ APPEND: không UPDATE/DELETE, không tái sử dụng uidx (user bị xoá ở OLTP vẫn giữ uidx).
- uidx lưu cột INT có dấu (cùng kiểu `uidx INT` của pv_daily) nên tối đa 2^31-1 (~2,1 tỷ, ≫ 100M thiết kế và < 2^32 của
  bitmap 32-bit): vượt thì lỗi tường minh, không cắt ngầm.
- Idempotent: chạy lại khi không có user mới thì không ghi gì.
- Một writer tại một thời điểm (Airflow max_active_runs=1). TODO(verify): row_number toàn cục dồn về 1 partition,
  nạp lần đầu hàng chục triệu user cần xem lại (bước 10 scale).
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

USER_DICT = "vision.silver.user_dict"
MAX_UIDX = 2 ** 31 - 1  # cột INT có dấu; bitmap32 cho phép tới 2^32-1 nhưng INT là giới hạn chặt hơn

USER_DICT_DDL = """
CREATE TABLE IF NOT EXISTS {table} (user_id STRING, uidx INT, first_seen_ds DATE) USING iceberg
"""


def first_seen(events: DataFrame, as_of: Optional[str] = None) -> DataFrame:
    """Mỗi user một dòng: (user_id, first_ts_ms, first_seen_ds), từ events có ds <= as_of."""
    if as_of is not None:
        events = events.filter(F.col("ds") <= F.lit(as_of).cast("date"))
    return events.groupBy("user_id").agg(F.min("source_ts_ms").alias("first_ts_ms"), F.min("ds").alias("first_seen_ds"))


def new_entries(users: DataFrame, existing: DataFrame) -> DataFrame:
    """Các dòng dictionary cần THÊM: user trong `users` (cột user_id, first_ts_ms, first_seen_ds) chưa có trong
    `existing` (cột user_id, uidx). uidx = max(existing) + thứ hạng theo (first_ts_ms, user_id). Rỗng nếu không có user mới.
    Ném ValueError nếu uidx vượt MAX_UIDX (2^31-1)."""
    fresh = users.join(existing.select("user_id"), "user_id", "left_anti")
    max_row = existing.agg(F.max("uidx").alias("m")).collect()[0]
    base = int(max_row["m"]) if max_row["m"] is not None else 0
    n = fresh.count()
    if base + n > MAX_UIDX:
        raise ValueError("uidx vượt %d (cột INT): base=%d, thêm %d user" % (MAX_UIDX, base, n))
    rank = F.row_number().over(Window.orderBy(F.col("first_ts_ms").asc(), F.col("user_id").asc()))
    return fresh.select("user_id", (rank + F.lit(base)).cast("int").alias("uidx"), "first_seen_ds")


def run(spark: SparkSession, ds: str) -> int:
    """Thêm user mới (as-of ds) vào dictionary. Trả về số user vừa thêm."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS vision.silver")
    spark.sql(USER_DICT_DDL.format(table=USER_DICT))
    entries = new_entries(first_seen(spark.table(EVENTS), ds), spark.table(USER_DICT)).cache()
    n = entries.count()
    if n:
        entries.writeTo(USER_DICT).append()
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
    spark = build_session("silver_user_dict")
    try:
        print("silver user_dict as-of ds=%s: thêm %d user" % (ds, run(spark, ds)))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
