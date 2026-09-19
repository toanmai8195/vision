# P1b — Mở rộng semantics: `aggFunc` + `EXTENDED` ✅

> Mở rộng thư viện P1 theo `CLAUDE.md` §3.2.1, §3.6. Vẫn là Python thuần trong bộ nhớ, cộng thêm thay đổi contract (proto) và catalog (Postgres).

## Input / Output

| | Nội dung | Nơi lưu | Dạng |
|---|---|---|---|
| **Input** | Như P1, thêm: event PV với `aggFunc` COUNT/MIN/MAX; event có tag là **chuỗi tự do** (EXTENDED) | golden YAML mới + hypothesis | Python object |
| **Output 1** | Contract mới: `AggFunc`, `AttributeType` trong `catalog.v1.Attribute` | `com/tm/proto/vision/catalog/v1/catalog.proto` | Protobuf |
| **Output 2** | Catalog mới: `meta.attribute.agg_func`, `meta.attribute.attribute_type`, bảng `meta.condition_usage` | Postgres (migration Flyway V3) | bảng |
| **Output 3** | Thư viện `temporal/` + `segment/dsl/` hỗ trợ tổ hợp mới; dictionary tag thuần | code Python | in-memory |

## Flow

```
event PV (value) ──reduce theo aggFunc──▶ pv_daily ──⊕ block (+ / min / max)──▶ pv window ──▶ so valueRange
event tag string ──tag_dict (append-only)──▶ tag_id ──▶ daily / block / POS như STANDARD
                                                        │
             segment dùng (attr, tag, window) ──▶ meta.condition_usage ──▶ planner chỉ sinh range cho tag này
```

## Các bước

### Bước 1 — `aggFunc` cho PARTIAL_VALUE(_BY_TAG)
Ví dụ user Chi: 01/09 1.5M · 10/09 300K · 15/09 200K · 15/09 100K.

| aggFunc | Daily 15/09 | A7 (09/09–15/09) | Condition ví dụ |
|---|---|---|---|
| SUM | 300K | 600K | tổng A7 ≥ 500K → ✅ |
| COUNT | 2 | 3 | số giao dịch A7 ≥ 3 → ✅ |
| MIN | 100K | 100K | giao dịch nhỏ nhất A7 < 150K → ✅ |
| MAX | 200K | 300K | giao dịch lớn nhất A30 ≥ 1M → ✅ (A30 = 1.5M) |

- Block ghép bằng `+` (SUM/COUNT) hoặc `min`/`max`. Late data: tính lại daily rồi build lại block, không dùng phép trừ.
- `AVG`, `DISTINCT_COUNT`, `FIRST/LAST` → lỗi tường minh.

### Bước 2 — Tag `EXTENDED`
Ví dụ `oa_follow` (NOT_MUTEX EVENT, EXTENDED): event `{uidx 1, add ["oa_12345"]}`.
1. `tag_dict`: `(attr oa_follow, "oa_12345") → tag_id 1`; chuỗi mới → cấp id tiếp theo, không tái sử dụng.
2. Daily / block / POS như STANDARD, nhưng chỉ đụng tag có signal trong ngày.
3. Range **usage-driven**: planner nhận tập tag trong `condition_usage`; tag không ai dùng → không tính range.
4. Segment dùng tag mới lần đầu → engine tính on-demand từ block + POS, rồi ghi `condition_usage`.
5. DSL: tag EXTENDED không kiểm catalog; chuỗi chưa có trong dict → bitmap rỗng.

Tổ hợp hợp lệ: NOT_MUTEX (EVENT/STATE) + EXTENDED, PARTIAL_VALUE_BY_TAG + EXTENDED. MUTEX/PARTIAL_VALUE + EXTENDED → lỗi catalog.

### Bước 3 — Contract & catalog
- Proto: `enum AggFunc {SUM, COUNT, MIN, MAX}`, `enum AttributeType {STANDARD, EXTENDED}`.
- Postgres V3: cột mới + CHECK constraint theo luật trên; bảng `meta.condition_usage(attr_id, tag_string, date_window, segment_id, last_used_at)`. V4 seed `txn_count` (COUNT), `txn_max` (MAX), `oa_follow`, `gift_value` (EXTENDED).
- `validation.py` + seed cập nhật.

### Bước 4 — Test
- Golden mới trong docs (`data-types.md` §5–§6, `data-flow-examples.md` §7) + `testdata/golden/p1b_aggfunc_extended.yaml`: Chi × COUNT/MIN/MAX, `oa_follow` (NOT_MUTEX EVENT EXTENDED), `voucher_holding` (NOT_MUTEX STATE EXTENDED), `gift_value` (PARTIAL_VALUE_BY_TAG EXTENDED), `seg_1003`.
- `engine_test.py`: usage tăng giữa tháng → LAST_MONTH carry-forward vẫn materialize tag mới.
- Property test thêm target: PV / PVBT × {COUNT, MIN, MAX}; NOT_MUTEX EXTENDED × {EVENT, STATE}; PVBT EXTENDED — ≥ 10K case mỗi target.

## Công nghệ
Python 3.11, pyroaring, hypothesis · Protobuf · Postgres + Flyway.

## Kiểm tra
Golden mới + toàn bộ target P1 cũ xanh; tổ hợp không hỗ trợ ra lỗi tường minh; `bazel test //...` xanh.
