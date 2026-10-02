# Bước 3 — Bronze (L1, OLAP)

> Phạm vi hiện tại: chỉ nguồn S2a `user_profile` (xem `phases.md`).

| | Nội dung |
|---|---|
| **Input** | Postgres OLTP (bước 2): bảng `src.user_profile` |
| **Output** | Iceberg `bronze.user_profile_cdc_raw`: envelope Debezium nguyên văn + vị trí Kafka + thời điểm thay đổi + loại thao tác (c/u/d/r) |

```
OLTP ──Debezium──▶ Kafka vision.src.user_profile.v1 ──Flink Java (application mode)──▶ Iceberg bronze (MinIO)
```

- Ghi OLTP trước rồi ingest sang OLAP, **không ghi song song** (tránh lệch khi một bên lỗi).
- Job Flink Java: `com/tm/src/ingest/cdc/flink` (Java 17, DataStream, theo repo `ironman`). `KafkaSource` → `BronzeRecord` → `FlinkSink` (Iceberg REST catalog + S3FileIO). Checkpoint 10s ở `s3://vision-flink/checkpoints`; Iceberg chỉ commit khi checkpoint xong.
- Cột bronze: `topic, kafka_partition, kafka_offset, kafka_ts, msg_key, payload, op, source_ts_ms, cdc_ts_ms, ingest_ts, ingest_hour` (partition theo `ingest_hour`, UTC). Payload hỏng vẫn được giữ (cột trích sẵn null); tombstone bị bỏ.
- Build + chạy: `bazel run --config=linux-arm64 //com/tm/src/ingest/cdc/flink:bronze_ingest_docker` rồi `docker compose -f com/tm/docker/vision/docker-compose.yml up -d` (UI Flink http://localhost:8081).
- CDC giao at-least-once → trùng được xử lý ở silver. Bronze chưa làm sạch; retention 30 ngày.
- Catalog Iceberg dev dùng SQLite nên **chỉ chịu được ít sink commit song song**; thêm nhiều nguồn cùng lúc có thể gây `SQLITE_BUSY` và job restart (đã gặp với 6 sink). Khi mở rộng: đổi catalog sang Postgres hoặc kiểm tra lại độ ổn định.
- Thêm nguồn sau: thêm topic vào connector Debezium (`table.include.list`, `message.key.columns`), `kafka-init`, 1 dòng ở `IcebergBronze.TOPIC_TO_TABLE`. Churn score (file ML) đi qua PySpark loader riêng, thêm ở bước 14.

Checklist: xem mục "Bước 3" trong `../checklist.md`.
