# P3 — Daily layer (L3) ⬜

> Gom silver thành dữ liệu **theo ngày** trong StarRocks: nhãn → bitmap user theo `(ngày, attribute, tag)`, số → giá trị aggregate theo `(ngày, attribute, tag, user)`.
> Thiết kế gốc: `CLAUDE.md` §4.1, §5, §9. Chuẩn đối chiếu: `temporal/model.py` (P1/P1b).

## Input / Output

| | Nội dung | Nơi lưu | Dạng |
|---|---|---|---|
| **Input** | Silver của P2 | Iceberg `silver.*` (StarRocks đọc qua external catalog `iceberg_vision`) | bảng Iceberg |
| **Input** | Catalog: attribute, tag, tag rule (vd `mcc → tag`) | Postgres `meta.*`, mirror sang StarRocks `meta.*` | bảng |
| **Output** | `gold.tag_daily`: `ADD`, `DEL`, `SIG` (EVENT) / `ADDED`, `REMOVED` (STATE) | StarRocks, PRIMARY KEY, partition `ds`, 400 ngày | cột `BITMAP` |
| **Output** | `gold.pv_daily`: `value` = AGG theo `aggFunc` | StarRocks, PK `(ds, attr_id, tag_id, uidx)` | DECIMAL(27,6) |
| **Output** | Kết quả DQ | StarRocks `dq.result` | bảng |

## Flow

```
Airflow vision_gold_daily (trigger: Dataset silver.*@ds)
  └─ dynamic task mapping theo attrGroupId (1 group = 1 source table)
       └─ SQL template com/tm/src/sql/starrocks/daily/<attr_group>.sql  ({{ ds }})
            1 lần scan silver ──▶ nhiều attribute cùng lúc
            DELETE WHERE ds=? AND attr_id=?  →  INSERT  (idempotent)
                  ▼
       gold.tag_daily · gold.pv_daily ──▶ DQ (§9) ──▶ dq.result ──▶ phát Dataset gold.daily@ds
```

## Các bước

### Bước 1 — Áp tag rule
- Tag rule nằm trong catalog, không hard-code trong SQL. Ví dụ payment: chỉ `status = SUCCESS`; `5812, 5814 → fnb`, `4722 → travel`, `4900 → bill`. e-9004 (FAILED) bị loại.

### Bước 2 — Reduce trong ngày theo loại (SQL StarRocks)

| Loại | SQL làm gì | Ví dụ output |
|---|---|---|
| MUTEX EVENT | theo user: ADD cuối ngày (window function theo `(event_ts, event_id)`) → `ADD(d,t)`; REMOVE sau ADD t cuối → `DEL(d,t)`; `ADD(d,0) = ⋃ ADD(d,t)`; gom bằng `bitmap_union(to_bitmap(uidx))` | churn 09-12: `ADD(mid)={1}`, `ADD(high)={3}`, `ADD(0)={1,3}` |
| NOT_MUTEX EVENT | theo `(user, tag)`: signal cuối ngày → `ADD` hoặc `DEL`; `SIG = ADD ∪ DEL` | txn_category 09-15: fnb `{1,2}`, travel `{1}` |
| STATE | từ SCD2: `ADDED` = version mở ngày `ds`, `REMOVED` = version đóng ngày `ds` | city 09-15: hcm `ADDED {2}`, `REMOVED {1}` |
| PARTIAL_VALUE | AGG (`aggFunc`) theo `(ds, uidx)`, `tag_id = 0` | txn_amount 09-15: `1→1255000`, `2→45000` |
| PARTIAL_VALUE_BY_TAG | AGG theo `(ds, tag_id, uidx)` | 09-15: `(1,fnb)→55000`, `(1,travel)→1200000` |
| EXTENDED | `tag_string → tag_id` qua `silver.tag_dict` trước khi reduce | |

Ví dụ SQL (PARTIAL_VALUE + BY_TAG, một scan):
```sql
DELETE FROM gold.pv_daily WHERE ds = '2026-09-15' AND attr_id IN (102, 103);
INSERT INTO gold.pv_daily
SELECT ds, 102, 0, uidx, SUM(amount) FROM iceberg_vision.silver.payment_txn
WHERE ds = '2026-09-15' AND status = 'SUCCESS' GROUP BY ds, uidx
UNION ALL
SELECT t.ds, 103, r.tag_id, t.uidx, SUM(t.amount)
FROM iceberg_vision.silver.payment_txn t JOIN meta.tag_rule_mcc r ON t.mcc = r.mcc
WHERE t.ds = '2026-09-15' AND t.status = 'SUCCESS' GROUP BY t.ds, r.tag_id, t.uidx;
```

### Bước 3 — Ghi idempotent
- Theo `(ds, attr_id)`: `DELETE` rồi `INSERT` (PRIMARY KEY table có cột BITMAP). **Không** dùng AGGREGATE KEY + `BITMAP_UNION` (chạy lại sẽ union với dữ liệu cũ).

### Bước 4 — DQ (§9)
| Check | Loại |
|---|---|
| `Σ cnt(ADD(d,t)) == cnt(ADD(d,0))` | MUTEX |
| `ADD(d,t) ∩ DEL(d,t) = ∅` | NOT_MUTEX |
| AGG `pv_daily` == AGG silver (cả theo tag) | PARTIAL_VALUE(_BY_TAG) |
| `uidx ⊆ UNIVERSE(ds)`; `tag_dict` append-only | mọi loại / EXTENDED |

Fail mức BLOCK → DAG dừng, không phát Dataset cho P4.

## Công nghệ
StarRocks 3.5 (PK table + BITMAP, external catalog Iceberg) · SQL template Jinja `{{ ds }}` · Airflow (dynamic task mapping, pool `starrocks_etl`).

## Kiểm tra
- Output == `model.py` trên cùng input (golden + dataset ngẫu nhiên nhỏ), đủ 4 loại, mọi `aggFunc`, STANDARD + EXTENDED.
- Chạy lại → không đổi; DQ pass. SLA daily 03:30.
