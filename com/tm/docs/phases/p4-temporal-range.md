# P4 — Temporal & Range (L4–L5) ⬜

> Từ dữ liệu theo ngày, tính kết quả cho **mọi date range** mỗi ngày: user nào thuộc tag nào trong A1…A180, IN_MONTH, LAST_MONTH, ALWAYS_ACTIVE; giá trị PV của mỗi user trong từng window.
> Thiết kế gốc: `CLAUDE.md` §4.2–§4.3, §3.6, §7. Chuẩn đối chiếu: `planner.py` + `engine.py` + `reference.py` (P1/P1b).

## Input / Output

| | Nội dung | Nơi lưu | Dạng |
|---|---|---|---|
| **Input** | `gold.tag_daily`, `gold.pv_daily` (P3) | StarRocks | BITMAP / DECIMAL |
| **Input** | Catalog (`supportedDateRanges`, `aggFunc`, `attributeType`), `meta.condition_usage` (EXTENDED) | Postgres → StarRocks `meta.*` | bảng |
| **Trung gian** | `gold.tag_block`, `gold.pv_block` (dyadic block, 400 ngày) | StarRocks | BITMAP / DECIMAL |
| **Trung gian** | `gold.tag_latest` (LATEST / POS / STATE, 7 ngày), `gold.tag_state_checkpoint` (Chủ nhật, 400 ngày) | StarRocks | BITMAP |
| **Output** | `gold.tag_range_bitmap (ds, attr_id, tag_id, date_range, bm, cardinality)` | StarRocks, 7 ngày | BITMAP |
| **Output** | `gold.pv_range_value (ds, attr_id, date_range, tag_id, uidx, value)` | StarRocks, 2 ngày | DECIMAL |

## Flow

```
Airflow vision_gold_temporal (trigger: Dataset gold.daily@ds)
  planner.py (ds, attribute) ──▶ danh sách SQL theo thứ tự:
     1. block      : B(k,s) = B(k−1,s) ⊕ B(k−1,s+2^(k−1))   cho mọi k với (e(ds)+1) % 2^k == 0
     2. latest     : LATEST / POS / STATE(ds) = fold(ngày ds−1, daily ds)
     3. checkpoint : nếu ds là Chủ nhật → chép STATE/LATEST sang tag_state_checkpoint
     4. range      : mỗi date range trong supportedDateRanges (EXTENDED: chỉ tag được dùng)
     5. DQ
  ──▶ vision_dq ──▶ phát Dataset cho P5
vision_late_data : silver ds−1..ds−3 đổi → làm lại daily + block chứa ngày đó + fold lại tới ds
vision_backfill  : chạy tuần tự từng ngày from..to
```

## Các bước

Ví dụ as-of `ds = 2026-09-15` (`e = 20711`).

### Bước 1 — Dyadic block
- Ngày 09-15 đóng → build `B1[09-14..15]`, `B2[09-12..15]`, `B3[09-08..15]` (vì 20712 chia hết cho 2, 4, 8).
- Nguồn block: MUTEX → `ADD(·,0)`; NOT_MUTEX → `SIG(·,t)`; PV → `pv_daily` (⊕ = `+` / `min` / `max` theo `aggFunc`); STATE → `REMOVED`.

| Block | SIG fnb (101) | pv 102 (SUM) |
|---|---|---|
| B1[09-10..11] | {2} | 2→600000 |
| B2[09-12..15] | {1,2} | 1→1255000, 2→45000, 3→350000 |

### Bước 2 — LATEST / POS / STATE
| Loại | SQL mỗi ngày | Ví dụ |
|---|---|---|
| MUTEX EVENT | `LATEST(d,t) = (ADD−DEL) ∪ (LATEST(d−1,t) − ADD(d,0) − DEL)` | churn sau 09-15: mid {1,2}, high {3,4} |
| NOT_MUTEX EVENT | `POS(d,t) = ADD ∪ (POS(d−1,t) − DEL)` | txn_category: fnb {1,2}, travel {1}, bill {3} |
| STATE | `STATE(d,t) = (STATE(d−1,t) − REMOVED) ∪ ADDED` | city: hcm {2}, hn {1,3} |

### Bước 3 — Range
Window → ghép block (A7 = `B0[09-09] ⊕ B1[09-10..11] ⊕ B2[09-12..15]`), rồi:

| Loại | Công thức | Ghi vào | Ví dụ A7 |
|---|---|---|---|
| MUTEX EVENT | `LATEST(r,t) ∩ ⋃ ADD(·,0)` | `tag_range_bitmap` | churn: mid {1,2}, high {3} |
| NOT_MUTEX EVENT | `POS(r,t) ∩ ⋃ SIG(·,t)` | `tag_range_bitmap` | fnb {1,2}, bill {3} |
| STATE | `STATE(r,t)` | `tag_range_bitmap` | hn {1,3} |
| PARTIAL_VALUE | AGG block → so bucket của tag | `pv_range_value` + `tag_range_bitmap` | 1→1255000, 2→645000, 3→350000 → 500k_2m {1,2}, lt_500k {3} |
| PARTIAL_VALUE_BY_TAG | AGG block theo tag | `pv_range_value` | (2,fnb)→645000 |
| EXTENDED | như trên, chỉ tag trong `condition_usage` | như trên | |

- `ALWAYS_ACTIVE` = LATEST / POS trực tiếp; `IN_MONTH` / `ALWAYS_ACTIVE` cập nhật incremental; `LAST_MONTH` tính ngày 1 rồi đóng băng (tính lại nếu late data rơi vào tháng trước).
- Custom range: không precompute; API nội bộ tính on-demand (checkpoint + fold ≤ 6 ngày cho `r < ds`), cache.

### Bước 4 — DQ
Monotonic `cnt(A7) ≤ cnt(A30) ≤ … ≤ cnt(ALWAYS_ACTIVE)` (EVENT); MUTEX: tag rời nhau trong mỗi window; STATE consistency.

## Công nghệ
Python planner (sinh SQL) · StarRocks 3.5 (BITMAP: `bitmap_or`, `bitmap_and`, `bitmap_andnot`) · Airflow (`vision_gold_temporal`, `vision_dq`, `vision_late_data`, `vision_backfill`, `vision_maintenance`).

## Kiểm tra
- `tag_range_bitmap` / `pv_range_value` == reference P1/P1b cho golden + dataset ngẫu nhiên, từng loại × từng date range.
- Late data 3 ngày và backfill == chạy lại từ đầu; 30 ngày liên tiếp không lỗi. SLA range 05:00.
