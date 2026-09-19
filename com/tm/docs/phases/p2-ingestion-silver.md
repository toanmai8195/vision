# P2 — Ingestion & Silver (L0–L2) ⬜

> Đưa dữ liệu của 3 source vào hệ thống và chuẩn hoá: raw → bronze (nguyên văn) → silver (sạch, có `uidx`, `ds` theo ICT, SCD2).
> Thiết kế gốc: `CLAUDE.md` §2, §6 (Ingestion, Standardize). Số liệu mẫu: `data-flow-examples.md` §1–§3.

## Input / Output

| | Nội dung | Nơi lưu | Dạng |
|---|---|---|---|
| **Input S1** | Event thanh toán (realtime) | Kafka `vision.src.payment_event.v1` (qua `event-collector`) | JSON / proto `DataEvent` |
| **Input S2** | CDC hồ sơ + sản phẩm | Kafka topic CDC (Debezium) | JSON `{op, before, after, ts_ms}` |
| **Input S3** | Điểm churn từ ML (batch ngày) | MinIO `s3://ds-output/churn_score/dt=…/*.parquet` + `_SUCCESS` | Parquet |
| **Output bronze** | Payload nguyên văn + offset / file | Iceberg `bronze.payment_event_raw`, `bronze.user_profile_cdc_raw`, `bronze.user_product_cdc_raw`, `bronze.churn_score_raw` | bảng Iceberg, 30 ngày |
| **Output silver** | Dữ liệu sạch | Iceberg `silver.payment_txn`, `silver.user_profile_scd2`, `silver.user_product_scd2`, `silver.churn_score` | bảng Iceberg, 400 ngày |
| **Output dictionary** | `user_id → uidx`, tag EXTENDED → `tag_id` | Iceberg `silver.user_dict`, `silver.tag_dict`; Redis; Postgres `meta.user_dict_rev` | bảng + cache |

Toàn bộ dữ liệu test sinh bằng **simulator** (Python), tham số `--users --days --attrs`, có duplicate, late data, REMOVE.

## Flow

```
S1 app / simulator ──HTTP/gRPC──▶ event-collector (Go) ──▶ Kafka ──Flink SQL──▶ bronze.payment_event_raw ──┐
S2 DB ──Debezium──▶ Kafka CDC ──────────────────────────────────Flink SQL──▶ bronze.*_cdc_raw ─────────────┤
S3 ML ──parquet──▶ MinIO ──sensor _SUCCESS──▶ PySpark file loader ─────────▶ bronze.churn_score_raw ───────┤
                                                                                                          ▼
                                   PySpark silver job (Airflow vision_silver_<source>, theo ds)
                         parse · dedup event_id · ds theo ICT · DLQ · map uidx · MERGE CDC → SCD2
                                                                                                          ▼
                    silver.payment_txn · silver.user_profile_scd2 · silver.user_product_scd2 · silver.churn_score
                    silver.user_dict (→ Redis, meta.user_dict_rev) · silver.tag_dict
```

## Các bước

### Bước 1 — Nhận event realtime: `event-collector` (Go)
- **Biến đổi**: HTTP/gRPC → validate theo proto `DataEvent` → Kafka, key = `user_id` (cùng user vào cùng partition, giữ thứ tự). Event sai schema → trả lỗi, không vào Kafka.
```json
{"event_id":"e-9001","user_id":"U1001","mcc":"5812","amount":55000,"status":"SUCCESS","event_ts":"2026-09-15T03:02:11Z"}
```

### Bước 2 — Kafka → bronze: Flink SQL
- **Biến đổi**: không parse nghiệp vụ; ghi **payload nguyên văn** + `topic, partition, offset, ingest_ts` vào Iceberg, partition `ingest_hour`, exactly-once (checkpoint). Duplicate delivery vẫn giữ 2 dòng ở bronze.

| topic | partition | offset | payload | ingest_ts |
|---|---|---|---|---|
| vision.src.payment_event.v1 | 3 | 88120 | `{"event_id":"e-9001",…}` | 2026-09-15T03:02:12Z |
| vision.src.payment_event.v1 | 3 | 88121 | `{"event_id":"e-9001",…}` | 2026-09-15T03:02:12Z |

### Bước 3 — File → bronze: PySpark file loader
- **Biến đổi**: Airflow sensor chờ `_SUCCESS` → đọc parquet → ghi `bronze.churn_score_raw` kèm tên file. Chạy lại cùng `dt` → ghi đè partition (idempotent).

### Bước 4 — bronze → silver: PySpark
| Việc | Biến đổi | Ví dụ |
|---|---|---|
| Parse | JSON/Debezium → cột có kiểu; `amount` → DECIMAL(27,6) | |
| Dedup | giữ 1 dòng / `event_id` | e-9001 × 2 → 1 dòng |
| `ds` theo ICT | `ds = date(event_ts + 7h)` | e-9003 `2026-09-14T18:30Z` → ds **09-15** |
| Late data | event cũ ghi vào partition `ds` của nó; đánh dấu ngày cần reprocess | e-9005 tới ngày 15, ds **09-14** |
| DLQ | dòng lỗi parse / thiếu field → bảng DLQ, không chặn job | |
| Map `uidx` | tra dictionary; user mới → cấp `uidx` tiếp theo | U1002 → 2 |
| CDC → SCD2 | Iceberg `MERGE`: đóng version cũ (`valid_to = ds`), mở version mới | U1001: HCM `[2025-01-10, 2026-09-15)` → HN `[2026-09-15, ∞)` |

Kết quả `silver.payment_txn`:

| event_id | uidx | mcc | amount | status | event_ts (UTC) | ds |
|---|---|---|---|---|---|---|
| e-9001 | 1 | 5812 | 55000 | SUCCESS | 2026-09-15 03:02:11 | 2026-09-15 |
| e-9003 | 2 | 5814 | 45000 | SUCCESS | 2026-09-14 18:30:00 | 2026-09-15 |
| e-9005 | 3 | 4900 | 350000 | SUCCESS | 2026-09-14 09:00:00 | 2026-09-14 |

### Bước 5 — Dictionary
- `silver.user_dict (user_id, uidx)`: append-only, không tái sử dụng `uidx` (bitmap chỉ chứa số nguyên dày).
- Sync sang Redis (API tra `user_id → uidx`) và `meta.user_dict_rev (uidx → user_id)`.
- `UNIVERSE(ds)` = bitmap mọi `uidx` hợp lệ tới ngày `ds` (DQ dùng ở P3).
- `silver.tag_dict (attr_id, tag_string, tag_id)` cho attribute EXTENDED, cũng append-only.

### Bước 6 — Orchestration: Airflow
- DAG `vision_silver_<source>` chạy 00:30 ICT (event thêm hourly), phát Dataset `silver.*@ds` để P3 bắt đầu. SLA silver 02:00.

## Công nghệ
Go (collector) · Kafka · Debezium format · Flink SQL · MinIO (S3) · Iceberg REST catalog · PySpark 3.5 · Redis · Postgres · Airflow 2.10.

## Kiểm tra
- Simulator sinh dữ liệu của `data-flow-examples.md` → silver khớp bảng L2 (dedup e-9001, e-9003 sang 09-15, e-9005 late).
- Chạy lại DAG cùng `ds` → kết quả y hệt.
- Silver giữ đủ cho 4 loại: `(event_ts, event_id)`, `value` DECIMAL, `tag`, SCD2, `tag_string`.
