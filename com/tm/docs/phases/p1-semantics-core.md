# P1 — Semantics core ✅

> Khoá đúng semantics 4 loại dữ liệu bằng Python thuần, chạy trong bộ nhớ. **Không đọc/ghi database nào**: đây là chuẩn để đối chiếu SQL ở P3–P5.
> Thiết kế gốc: `CLAUDE.md` §3–§4, §6.1. Code: `com/tm/src/temporal/`, `com/tm/src/segment/dsl/`.

## Input / Output

| | Nội dung | Nơi lưu | Dạng |
|---|---|---|---|
| **Input** | Event đã chuẩn hoá (giả lập silver): `TagEvent`, `ValueEvent`, `StateInterval` (SCD2); catalog `AttributeSpec`; DSL segment | YAML golden `com/tm/src/temporal/testdata/golden/*.yaml` hoặc dữ liệu sinh bởi hypothesis | Python object |
| **Output** | Kết quả mỗi layer trong bộ nhớ: daily, block, LATEST/POS/STATE, tag bitmap theo date range, giá trị PV theo window, bitmap segment | dict Python + `pyroaring.BitMap` | in-memory |
| **Output phụ** | Bộ test làm chuẩn: golden (số liệu doc) + property test (engine == reference) | `*_test.py`, `testdata/golden/` | Bazel test |

## Flow

Hai đường tính **độc lập** cho cùng input, rồi so với nhau:

```
                 ┌──────────── reference.py (ngây thơ: quét mọi event trong window) ─────────────┐
input (silver) ──┤                                                                               ├──▶ so sánh == ?
                 └── planner.py ──▶ engine.py: daily ─▶ block ─▶ LATEST/POS/STATE ─▶ range ──────┘
                                                                                      │
                                                        segment/dsl: validate ─▶ evaluate ─▶ bitmap segment
```

`testing.run_pipeline` mô phỏng pipeline chạy **từng ngày** từ `history_start` tới `ds`, silver nhận dữ liệu dần (late data ≤ 3 ngày).

## Các bước (đường tối ưu, cùng thứ tự với SQL P3–P4)

Ví dụ xuyên suốt: attribute 301 `churn_score_band` (MUTEX EVENT), as-of `ds = 2026-09-15`.

### Bước 1 — Silver (input) → `model.py`
- Mỗi event có `ds` suy ra từ `event_ts` theo ICT (`ds_of_ts_ms`), sắp theo `(event_ts, event_id)`.
- Validate input: MUTEX event ADD > 1 tag, event ADD và REMOVE cùng tag, SCD2 chồng lấn → lỗi.
```
TagEvent(uidx=2, ts=2026-09-15T13:00Z, add={mid})   TagEvent(uidx=1, day=09-12, add={mid}) …
```

### Bước 2 — Daily (L3): `reduce_*_day`, `state_delta`, `reduce_pv_day`
| Loại | Biến đổi | Ví dụ (ngày 09-12) |
|---|---|---|
| MUTEX EVENT | ADD cuối ngày → `ADD(d,t)`, `ADD(d,0)`; REMOVE sau ADD → `DEL(d,t)` | `ADD(mid)={1}`, `ADD(high)={3}`, `ADD(0)={1,3}` |
| NOT_MUTEX EVENT | signal cuối ngày từng tag → `ADD`/`DEL`, `SIG = ADD ∪ DEL` | |
| STATE | SCD2 → `ADDED`/`REMOVED` (ngày đầu: snapshot) | city 09-15: `ADDED(hcm)={2}`, `REMOVED(hcm)={1}` |
| PARTIAL_VALUE(_BY_TAG) | SUM theo `(tag, uidx)` | `pv_daily(09-15)`: `1→1255000`, `2→45000` |

### Bước 3 — Dyadic block (L4): `blocks.py`
- `B(k,s)` phủ `[s, s+2^k−1]`; ngày `d` đóng → build các block kết thúc tại `d`. OR cho bitmap, SUM cho PV.
- Window → ghép block rời nhau: A7 as-of 09-15 = `[09-09] [09-10..11] [09-12..15]`.

### Bước 4 — LATEST / POS / STATE (L4): `latest.py`
- Fold mỗi ngày; lưu 7 ngày gần nhất + checkpoint Chủ nhật; ngày cũ = checkpoint + fold ≤ 6 ngày.
```
LATEST sau 09-12: low {2}, mid {1}, high {3,4}   →   sau 09-15: mid {1,2}, high {3,4}
```

### Bước 5 — Range (L5): `ranges.py` + `engine.py`
| Loại | Công thức | Ví dụ A7 |
|---|---|---|
| MUTEX EVENT | `LATEST(r,t) ∩ SEEN[l,r]` | mid {1,2}, high {3} |
| NOT_MUTEX EVENT | `POS(r,t) ∩ SIG_window(t)` | txn_category: fnb {1,2}, travel {1}, bill {3} |
| STATE | `STATE(r,t)` | user_city: hcm {2}, hn {1,3} |
| PARTIAL_VALUE | SUM block → so bucket | txn_amount: lt_500k {3}, 500k_2m {1,2} |
| PARTIAL_VALUE_BY_TAG | SUM block theo tag (ngưỡng ở condition) | fnb: 1→55K, 2→645K |

Planner sinh thêm bước DQ (§9) và engine chạy luôn: fail → `DqError`.

### Bước 6 — Segment: `segment/dsl/`
- `validate.py`: kiểm condition theo `dataType` (ma trận 980 case).
- `evaluator.py`: AND / OR / SUB + tagOp + valueRange, cache theo condition.
```
seg_1001 = (churn mid|high A30 ∩ city hn A7) − txn fnb A7 = ({1,2,3,4} ∩ {1,3}) − {1,2} = {3}
seg_1002 = txn_amount A7 ≥ 1M ∪ fnb A7 ≥ 500K = {1} ∪ {2} = {1,2}
```

## Công nghệ
Python 3.11 · `pyroaring` (Roaring bitmap) · `hypothesis` (property test) · `pyyaml` (golden) · Bazel `py_test`.

## Kiểm tra
- `bazel test //com/tm/src/temporal:golden_test` — số liệu trong `data-flow-examples.md` + `data-types.md`.
- `bazel test //com/tm/src/temporal:all` — 6 property test × 10K case (4 loại × EVENT/STATE), có late data.
- Phạm vi: chỉ `aggFunc = SUM`, tag `STANDARD` → mở rộng ở P1b.
