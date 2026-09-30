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

## Checklist
**Mục tiêu**: data từ OLTP đi vào Iceberg bronze, chưa làm sạch. Ghi OLTP trước, ingest sang OLAP (không ghi song song).
- [ ] Thêm Kafka, Debezium, Flink, MinIO + Iceberg REST vào compose
- [ ] CDC: Debezium đọc log OLTP → Kafka → Flink SQL → `bronze.*_raw` (Iceberg), giữ bản ghi gốc + thời điểm thay đổi + loại thao tác
- [ ] Bảng event (payment) và bảng trạng thái (profile, product) đều qua CDC; churn score (file) qua file loader
- [ ] Insert/update/delete ở OLTP sau đó đều xuất hiện trong bronze

**Done khi**: số bản ghi và nội dung bronze khớp OLTP, kể cả sau khi sửa/xoá.
