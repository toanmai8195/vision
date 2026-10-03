# Bước 4 — Silver (L2)

| | Nội dung |
|---|---|
| **Input** | `bronze.user_profile_cdc_raw` |
| **Output** | Iceberg `silver.user_profile_cdc_events` (sự kiện CDC sạch) + `silver.user_profile_cdc_dlq`; `silver.user_profile_scd2`; `silver.user_dict` (`user_id → uidx`); `UNIVERSE(ds)`. (Sau này thêm `payment_txn`, `user_product_scd2`, `churn_score` khi mở rộng.) |

```
bronze ──Spark──▶ parse ──▶ dedup (partition, offset) ──▶ ds theo ICT ──▶ silver.user_profile_cdc_events ──▶ CDC→SCD2 ──▶ silver
                  └─ dòng lỗi ──▶ silver.user_profile_cdc_dlq                              user_id ──▶ uidx (append-only)
```

- Thêm **Spark** vào compose: PySpark `local[*]` trong **1 container**, chạy theo lô (`--ds YYYY-MM-DD`, như job Airflow sau này); chưa dựng cluster master/worker. Image build bằng Bazel (base `apache/spark:3.5.6-python3` pin digest + jar Iceberg), profile `silver` của compose.
- Job parse+làm sạch (`com/tm/src/batch/silver/user_profile_events.py`): `user_profile` là bảng trạng thái qua CDC, **không có `event_id`**, nên dedup theo **vị trí Kafka** `(kafka_partition, kafka_offset)` (duy nhất mỗi message, loại bản trùng do at-least-once). Dedup `event_id` làm khi có nguồn event (bước 11).
- `ds` = ngày ICT của `source.ts_ms` (thời điểm thay đổi ở DB nguồn). Dòng lỗi vào DLQ với `reason`: `invalid_json`, `unknown_op`, `missing_source_ts`, `missing_user_id`, `missing_row_state`; DLQ phân vùng theo ngày ICT của `ingest_ts`.
- Idempotent: ghi theo `ds` bằng `overwrite(ds = D)` (một commit atomic, kể cả khi kết quả rỗng) nên chạy lại cùng `ds` ra cùng kết quả. Late data: chạy lại `ds` cũ sẽ gom thêm event đến muộn (quét toàn bronze, retention 30 ngày).
- Chạy: `bazel run --config=linux-arm64 //com/tm/src/batch/silver:silver_user_profile_docker`, rồi `DS=2026-10-03 docker compose -f com/tm/docker/vision/docker-compose.yml --profile silver run --rm spark-silver-user-profile` (bỏ `DS` = hôm nay ICT). Xem kết quả bằng StarRocks (`ice.silver.user_profile_cdc_events`, profile `query`). Unit test (SparkSession local, 5 ca: mọi `op`, dedup vị trí Kafka, ranh giới ngày ICT, DLQ đủ lý do, `user_id` từ payload): `docker run --rm --entrypoint python3 com.tm.spark.silver_user_profile:v1.0.0 -W ignore /app/com/tm/src/batch/silver/user_profile_events_test.py`.
- Spark image dùng **Python 3.8** (khác 3.11 của repo): code PySpark viết tương thích 3.8.
- Debezium phát cột `DATE` (vd `birth_date`) dưới dạng số ngày từ 1970-01-01 → đổi lại thành date ở silver.
- `ds` = ngày theo Asia/Ho_Chi_Minh; thứ tự trong ngày theo `(event_ts, event_id)`.
- `uidx` số nguyên dày, chỉ append, không tái sử dụng.
- Idempotent: chạy lại cùng `ds` ra cùng kết quả. Late data ≤ 3 ngày tự reprocess.

Checklist: xem mục "Bước 4" trong `../checklist.md`.
