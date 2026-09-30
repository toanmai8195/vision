# Bước 3 — Bronze (L1, OLAP)

| | Nội dung |
|---|---|
| **Input** | Postgres OLTP (bước 2); file churn score (ML) |
| **Output** | Iceberg `bronze.*_raw`: bản ghi gốc + thời điểm thay đổi + loại thao tác (insert/update/delete) |

```
OLTP ──Debezium──▶ Kafka ──Flink SQL──▶ Iceberg bronze (MinIO)
file ML ──PySpark loader (sensor _SUCCESS)──▶ Iceberg bronze
```

- Ghi OLTP trước rồi ingest sang OLAP, **không ghi song song** (tránh lệch khi một bên lỗi).
- Thêm vào compose: Kafka, Debezium, Flink, MinIO + Iceberg REST.
- CDC giao at-least-once → trùng được xử lý ở silver (dedup `event_id`).
- Bronze chưa làm sạch; retention 30 ngày.

Checklist: xem mục "Bước 3" trong `../checklist.md`.
