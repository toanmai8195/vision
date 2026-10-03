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
- Xem bronze bằng SQL/DBeaver: `docker compose -f com/tm/docker/vision/docker-compose.yml --profile query up -d` (StarRocks allin1, image ~8GB; bước 5 dùng lại làm gold). DBeaver: kiểu **StarRocks**, host `localhost`, port `19030`, database `ice.bronze`, user `root`, không mật khẩu. Catalog `ice` do `starrocks-init` tạo. Ví dụ: `SELECT op, count(*) FROM ice.bronze.user_profile_cdc_raw GROUP BY op`. Lưu ý: StarRocks **cache metadata Iceberg** nên có thể thấy snapshot cũ (`REFRESH EXTERNAL TABLE ice.bronze.user_profile_cdc_raw`), và query đầu sau khi khởi động có thể timeout (chạy lại, đặt `SET query_timeout=120`).
- Kiểm tra tự động insert/update/delete → bronze: `com/tm/src/ingest/cdc/verify_bronze.sh` (cần stack + profile `query`; thoát 0 nếu bronze có đủ `c`, `u`, `d` đúng `before`/`after`).
- Nghiệm thu "bronze khớp OLTP": `com/tm/src/ingest/cdc/verify_bronze_matches_oltp.sh` tạo thay đổi (insert/đổi city/đổi giới tính/xoá city/xoá user) rồi so (a) số `(partition, offset)` khác nhau ở bronze == số message trong topic Kafka, (b) trạng thái cuối mỗi user suy từ bronze == `src.user_profile`. Đã thử cả ca Flink dừng (script FAIL) rồi chạy lại (khớp). Dừng generator live trước khi chạy.
- CDC giao at-least-once → trùng được xử lý ở silver. Bronze chưa làm sạch; retention 30 ngày.
- Catalog Iceberg dev (`iceberg-rest`) lưu metadata ở Postgres, database `iceberg_catalog` trong cùng instance `postgres-oltp` (DB riêng, tách khỏi schema `src` mà Debezium đọc). Image build từ `com/tm/docker/vision/iceberg-rest/Dockerfile` (fixture + driver Postgres, pin sha256). Dùng SQLite mặc định thì 6 sink commit song song gây `SQLITE_BUSY` và job restart liên tục; với Postgres đã thử 6 sink có tải: 0 restart, đủ bản ghi.
- Thêm nguồn sau: thêm topic vào connector Debezium (`table.include.list`, `message.key.columns`), `kafka-init`, 1 dòng ở `IcebergBronze.TOPIC_TO_TABLE`. Churn score (file ML) đi qua PySpark loader riêng, thêm ở bước 14.

Checklist: xem mục "Bước 3" trong `../checklist.md`.
