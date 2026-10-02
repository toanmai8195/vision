# Bước 3 — Bronze (L1, OLAP)

| | Nội dung |
|---|---|
| **Input** | Postgres OLTP (bước 2): bảng `src.user_profile` (nguồn duy nhất giai đoạn này) |
| **Output** | Iceberg `bronze.user_profile_cdc_raw`: bản ghi gốc + thời điểm thay đổi + loại thao tác (insert/update/delete) |

```
OLTP ──Debezium──▶ Kafka ──Flink Java──▶ Iceberg bronze (MinIO)
```

- Ghi OLTP trước rồi ingest sang OLAP, **không ghi song song** (tránh lệch khi một bên lỗi).
- Thêm vào compose: Kafka, Debezium, Flink, MinIO + Iceberg REST.
- CDC giao at-least-once → trùng được xử lý ở silver (dedup `event_id`).
- Thêm nguồn sau: thêm topic vào connector Debezium (`table.include.list`, `message.key.columns`), `kafka-init`, và 1 dòng vào `IcebergBronze.TOPIC_TO_TABLE`. Churn score (file ML) đi qua PySpark loader riêng, thêm ở "Mở rộng".
- Bronze chưa làm sạch; retention 30 ngày.

Checklist: xem mục "Bước 3" trong `../checklist.md`.
