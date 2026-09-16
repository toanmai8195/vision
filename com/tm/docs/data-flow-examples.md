# Data flow examples — dữ liệu qua từng layer

> Định nghĩa gốc ở `CLAUDE.md` §3–§5. Mọi con số ở đây là **golden test** (`com/tm/src/temporal/testdata/golden/`) —
> đổi semantics thì sửa cả hai. Các kết quả đã được đối chiếu với reference implementation (cách tính ngây thơ theo định nghĩa §3.2).

## 0. Bối cảnh

As-of `ds = 2026-09-15` (`e = 20711`). Ký hiệu bitmap `{1,2}` = tập `uidx`.

**Source & attribute**

| attr_id | name | Source | dataType | feedMode | Tags |
|---|---|---|---|---|---|
| 101 | `txn_category` | S1 payment (Kafka) | NOT_MUTEX | EVENT | fnb(1), travel(2), bill(3) |
| 102 | `txn_amount` | S1 | PARTIAL_VALUE | EVENT | lt_500k(1) `[0, 500K)`, 500k_2m(2) `[500K, 2M)`, gte_2m(3) `[2M, ∞)` |
| 103 | `txn_amount_by_category` | S1 | PARTIAL_VALUE_BY_TAG | EVENT | fnb(1), travel(2), bill(3) |
| 201 | `user_city` | S2 profile CDC | MUTEX | STATE | hcm(1), hn(2) |
| 202 | `product_holding` | S2 product CDC | NOT_MUTEX | STATE | paylater(1), insurance(2) |
| 301 | `churn_score_band` | S3 ML parquet | MUTEX | EVENT | low(1) `<0.3`, mid(2) `[0.3,0.7)`, high(3) `≥0.7` |

**Dictionary** (`silver.user_dict`, append-only)

| user_id | uidx |
|---|---|
| U1001 | 1 |
| U1002 | 2 |
| U1003 | 3 |
| U1004 | 4 |

---

## 1. S1 — payment_event (NOT_MUTEX + PARTIAL_VALUE + PARTIAL_VALUE_BY_TAG)

### L0 Source — topic `vision.src.payment_event.v1`
```json
{"event_id":"e-8001","user_id":"U1002","mcc":"5812","amount":600000,"status":"SUCCESS","event_ts":"2026-09-10T05:00:00Z"}   // đã xử lý ngày 09-10
{"event_id":"e-9001","user_id":"U1001","mcc":"5812","amount":55000,"status":"SUCCESS","event_ts":"2026-09-15T03:02:11Z"}
{"event_id":"e-9001","user_id":"U1001","mcc":"5812","amount":55000,"status":"SUCCESS","event_ts":"2026-09-15T03:02:11Z"}   // duplicate delivery
{"event_id":"e-9002","user_id":"U1001","mcc":"4722","amount":1200000,"status":"SUCCESS","event_ts":"2026-09-15T05:10:00Z"}
{"event_id":"e-9003","user_id":"U1002","mcc":"5814","amount":45000,"status":"SUCCESS","event_ts":"2026-09-14T18:30:00Z"}    // 01:30 ICT ngày 15
{"event_id":"e-9004","user_id":"U1003","mcc":"5812","amount":30000,"status":"FAILED","event_ts":"2026-09-15T08:00:00Z"}
{"event_id":"e-9005","user_id":"U1003","mcc":"4900","amount":350000,"status":"SUCCESS","event_ts":"2026-09-14T09:00:00Z"}   // tới muộn vào ngày 15
```

### L1 Bronze — `bronze.payment_event_raw` (Flink, partition `ingest_hour`, payload nguyên văn)

| topic | partition | offset | payload | ingest_ts |
|---|---|---|---|---|
| vision.src.payment_event.v1 | 3 | 88120 | `{"event_id":"e-9001",…}` | 2026-09-15T03:02:12Z |
| vision.src.payment_event.v1 | 3 | 88121 | `{"event_id":"e-9001",…}` | 2026-09-15T03:02:12Z |
| … | | | | |

### L2 Silver — `silver.payment_txn` (dedup `event_id`, `ds` theo ICT, map `uidx`)

| event_id | uidx | mcc | amount | status | event_ts (UTC) | ds |
|---|---|---|---|---|---|---|
| e-8001 | 2 | 5812 | 600000 | SUCCESS | 2026-09-10 05:00:00 | 2026-09-10 |
| e-9001 | 1 | 5812 | 55000 | SUCCESS | 2026-09-15 03:02:11 | 2026-09-15 |
| e-9002 | 1 | 4722 | 1200000 | SUCCESS | 2026-09-15 05:10:00 | 2026-09-15 |
| e-9003 | 2 | 5814 | 45000 | SUCCESS | 2026-09-14 18:30:00 | **2026-09-15** |
| e-9004 | 3 | 5812 | 30000 | FAILED | 2026-09-15 08:00:00 | 2026-09-15 |
| e-9005 | 3 | 4900 | 350000 | SUCCESS | 2026-09-14 09:00:00 | **2026-09-14** → late, reprocess ds 09-14 |

Tag rule (`meta.tag_rule_mcc`, chỉ `SUCCESS`): `5812,5814 → fnb`, `4722 → travel`, `4900 → bill`.

### L3 Daily

**101 `txn_category`** — mỗi giao dịch là signal ADD (không có REMOVE) → `gold.tag_daily`:

| ds | tag | ADD | DEL | SIG |
|---|---|---|---|---|
| 2026-09-10 | fnb | {2} | {} | {2} |
| 2026-09-14 | bill | {3} | {} | {3} |
| 2026-09-15 | fnb | {1,2} | {} | {1,2} |
| 2026-09-15 | travel | {1} | {} | {1} |

U1001 có 2 tag cùng ngày — hợp lệ vì NOT_MUTEX.

**102 / 103** — `gold.pv_daily` (SUM theo ngày):
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

| ds | attr | tag | uidx | value |
|---|---|---|---|---|
| 2026-09-10 | 102 | 0 | 2 | 600000 |
| 2026-09-10 | 103 | fnb | 2 | 600000 |
| 2026-09-14 | 102 | 0 | 3 | 350000 |
| 2026-09-14 | 103 | bill | 3 | 350000 |
| 2026-09-15 | 102 | 0 | 1 | 1255000 |
| 2026-09-15 | 102 | 0 | 2 | 45000 |
| 2026-09-15 | 103 | fnb | 1 | 55000 |
| 2026-09-15 | 103 | travel | 1 | 1200000 |
| 2026-09-15 | 103 | fnb | 2 | 45000 |

### L4 Temporal

Ngày 09-15 đóng → build `B1[09-14..15]`, `B2[09-12..15]`, `B3[09-08..15]` (vì `20712 % 8 == 0`).

A7 = `B0[09-09] ⊕ B1[09-10..11] ⊕ B2[09-12..15]`:

| Block | SIG fnb (101) | SIG bill (101) | pv 102 | pv 103 |
|---|---|---|---|---|
| B0[09-09] | {} | {} | – | – |
| B1[09-10..11] | {2} | {} | 2→600000 | (2,fnb)→600000 |
| B2[09-12..15] | {1,2} | {3} | 1→1255000, 2→45000, 3→350000 | (1,fnb)→55000, (1,travel)→1200000, (2,fnb)→45000, (3,bill)→350000 |

`POS(09-15)` (101, không có DEL): fnb `{1,2}`, travel `{1}`, bill `{3}`.

### L5 Range

**101** `tag_range_bitmap` = `POS(r,t) ∩ ⋃ SIG` trên window:

| date_range | fnb | travel | bill |
|---|---|---|---|
| A1 | {1,2} | {1} | {} |
| A7 | {1,2} | {1} | {3} |

**102** `pv_range_value` (A7) = SUM các block: `1→1255000`, `2→645000`, `3→350000`. Map theo `valueRange` của tag → `tag_range_bitmap`:

| date_range | lt_500k | 500k_2m | gte_2m |
|---|---|---|---|
| A1 | {2} | {1} | {} |
| A7 | {3} | {1,2} | {} |

**103** `pv_range_value` (A7): `(1,fnb)→55000`, `(1,travel)→1200000`, `(2,fnb)→645000`, `(3,bill)→350000`. Không có bucket định sẵn — `valueRange` nằm trong condition:
- `txn_amount_by_category`, tag `fnb`, A7, `valueRange ≥ 500000` → `{2}`
- `txn_amount`, A7, `valueRange ≥ 1000000` (ad-hoc) → `{1}`

---

## 2. S2 — CDC (STATE)

### 2.1 `user_city` (MUTEX, STATE)

**L0 Source** (Debezium):
```json
{"op":"u","source":{"table":"user_profile"},"before":{"user_id":"U1001","city_code":"HCM"},"after":{"user_id":"U1001","city_code":"HN"},"ts_ms":1789441200000}
{"op":"c","source":{"table":"user_profile"},"before":null,"after":{"user_id":"U1002","city_code":"HCM"},"ts_ms":1789444800000}
```
U1003 ở HN từ 2024, không có thay đổi.

**L1 Bronze** — `bronze.user_profile_cdc_raw`: payload nguyên văn + offset.

**L2 Silver** — `silver.user_profile_scd2` (Spark `MERGE`):

| uidx | city_code | valid_from | valid_to | is_current |
|---|---|---|---|---|
| 1 | HCM | 2025-01-10 | 2026-09-15 | false |
| 1 | HN | 2026-09-15 | 9999-12-31 | true |
| 2 | HCM | 2026-09-15 | 9999-12-31 | true |
| 3 | HN | 2024-06-01 | 9999-12-31 | true |

U1002 là user mới → cấp `uidx = 2`.

**L3 Daily** — `ADDED` = version có `valid_from = ds`, `REMOVED` = version có `valid_to = ds`:

| ds | tag | ADDED | REMOVED | STATE sau apply |
|---|---|---|---|---|
| 2026-09-14 | hcm | – | – | {1} |
| 2026-09-14 | hn | – | – | {3} |
| 2026-09-15 | hcm | {2} | {1} | **{2}** |
| 2026-09-15 | hn | {1} | {} | **{1,3}** |

**L4/L5** — STATE: mọi window kết thúc ở `ds` đều bằng `STATE(ds)`:

| date_range | hcm | hn |
|---|---|---|
| A1, A7, A30, …, ALWAYS_ACTIVE | {2} | {1,3} |

So sánh: nếu `user_city` là **EVENT** (chỉ có signal khi CDC đổi), A30 hn chỉ có `{1}` — U1003 bị mất vì lần ADD cuối từ 2024. Đó là lý do trạng thái hồ sơ dùng `STATE`.

Custom `[09-01, 09-10]`: `STATE(09-10)` = checkpoint Chủ nhật `09-06` + apply delta 09-07..09-10 → hcm `{1}`, hn `{3}`.

### 2.2 `product_holding` (NOT_MUTEX, STATE)

**L2 Silver** `silver.user_product_scd2`: trước 09-15 U1001 có {paylater, insurance}, U1002 có {paylater}. Ngày 09-15: U1001 đóng paylater, U1003 mua insurance.

**L3**:

| ds | tag | ADDED | REMOVED | STATE sau apply |
|---|---|---|---|---|
| 2026-09-14 | paylater | – | – | {1,2} |
| 2026-09-14 | insurance | – | – | {1} |
| 2026-09-15 | paylater | {} | {1} | **{2}** |
| 2026-09-15 | insurance | {3} | {} | **{1,3}** |

**L5** mọi window: paylater `{2}`, insurance `{1,3}` → U1001 vẫn có insurance (tag khác không bị ảnh hưởng — NOT_MUTEX).

---

## 3. S3 — churn_score (MUTEX, EVENT)

### L0 Source — `s3://ds-output/churn_score/dt=2026-09-15/part-00000.parquet`

| user_id | score | model_version | scored_at |
|---|---|---|---|
| U1002 | 0.41 | churn-v7 | 2026-09-15T13:00:00Z |

Model chỉ chấm user active gần đây → mỗi ngày một phần user. Mỗi row = signal ADD tag bucket.

### L2 Silver — `silver.churn_score` (partition `ds`): `(ds, uidx, score, model_version)`

### L3 Daily (`ADD`, không có DEL)

| ds | low | mid | high | ADD(·,0) |
|---|---|---|---|---|
| 2026-09-01 | {} | {} | {4} | {4} |
| 2026-09-09 | {2} | {} | {1} | {1,2} |
| 2026-09-12 | {} | {1} | {3} | {1,3} |
| 2026-09-15 | {} | {2} | {} | {2} |

### L4 Temporal — `LATEST(d,t) = (ADD − DEL) ∪ (LATEST(d−1,t) − ADD(d,0) − DEL)`

| sau ngày | low | mid | high |
|---|---|---|---|
| 09-01 | {} | {} | {4} |
| 09-09 | {2} | {} | {1,4} |
| 09-12 | {2} | {1} | {3,4} |
| 09-15 | **{}** | **{1,2}** | **{3,4}** |

`SEEN = ⋃ ADD(·,0)`: A1 `{2}` · A7 `{1,2,3}` · A30 `{1,2,3,4}`.

### L5 Range — `LATEST(r,t) ∩ SEEN`

| date_range | low | mid | high |
|---|---|---|---|
| A1 | {} | {2} | {} |
| A7 | {} | {1,2} | {3} |
| A30 | {} | {1,2} | {3,4} |
| ALWAYS_ACTIVE | {} | {1,2} | {3,4} |
| CUSTOM `[09-01, 09-10]` | {2} | {} | {1,4} |

Custom: `LATEST(09-10) = LATEST(09-09)` → low `{2}`, high `{1,4}`; `SEEN[09-01..09-10] = {1,2,4}`.

Mỗi window các tag rời nhau (MUTEX) ✔.

---

## 4. REMOVE trong MUTEX EVENT (ca biên)

Attribute giả định `preferred_channel` (MUTEX, EVENT), tag `a`, `b`:

| User | Event | ADD / DEL theo ngày | Kết quả A7 as-of 09-15 |
|---|---|---|---|
| X | 09-10 ADD a · 09-12 ADD b · 09-13 10:00 REMOVE b | 09-10 ADD a · 09-12 ADD b · 09-13 DEL b | **không thuộc tag nào** (ADD cuối là b, bị REMOVE sau) |
| Y | 09-14 08:00 REMOVE a · 09-14 09:00 ADD a | 09-14 ADD a, DEL a = {} (REMOVE trước ADD) | **a** |
| Z | 09-14 09:00 ADD a · 09-14 10:00 REMOVE a | 09-14 ADD a, DEL a = {Z} | **không thuộc tag nào** |

Đúng định nghĩa MUTEX (`CLAUDE.md` §3.2): lấy ADD gần nhất trong window; nếu tag đó có REMOVE sau thời điểm ADD → không thuộc tag nào.

---

## 5. L6 Segment

### seg_1001 — "Churn mid/high A30, ở HN, không mua F&B 7 ngày"
```json
{"operator": "SUB", "children": [
  {"operator": "AND", "children": [
    {"condition": {"attr": "churn_score_band", "tags": ["mid", "high"], "tagOp": "OR", "dateRange": "A30"}},
    {"condition": {"attr": "user_city", "tags": ["hn"], "dateRange": "A7"}}
  ]},
  {"condition": {"attr": "txn_category", "tags": ["fnb"], "dateRange": "A7"}}
]}
```

| Bước | Bitmap |
|---|---|
| c1 churn mid ∪ high (A30) | {1,2} ∪ {3,4} = {1,2,3,4} |
| c2 city hn (A7) | {1,3} |
| c3 txn fnb (A7) | {1,2} |
| `(c1 ∩ c2) − c3` | **{3}** |

### seg_1002 — "Chi tiêu lớn 7 ngày"
```json
{"operator": "OR", "children": [
  {"condition": {"attr": "txn_amount", "dateRange": "A7", "valueRange": {"fromValue": "1000000", "fromInclusive": true}}},
  {"condition": {"attr": "txn_amount_by_category", "tags": ["fnb"], "dateRange": "A7", "valueRange": {"fromValue": "500000", "fromInclusive": true}}}
]}
```

| Bước | Bitmap |
|---|---|
| c1 SUM amount A7 ≥ 1M | {1} |
| c2 SUM fnb A7 ≥ 500K | {2} |
| `c1 ∪ c2` | **{1,2}** |

### Output

| Nơi | seg_1001 | seg_1002 |
|---|---|---|
| `seg.segment_bitmap` | `(seg_1001, v12, 2026-09-15, {3}, 1)` | `(seg_1002, v3, 2026-09-15, {1,2}, 2)` |
| S3 | `s3://vision-segments/ds=2026-09-15/seg_1001/v12.roar` + `v12.manifest.json` | `…/seg_1002/v3.roar` |
| Postgres `meta.segment_version` | `BUILDING → PUBLISHED` | như trên |
| Kafka `vision.segment.published.v1` | `{"segmentId":"seg_1001","version":12,"count":1,"uri":"s3://…/v12.roar"}` | … |

---

## 6. L7 Activation

```http
GET /v1/segments/seg_1001/count
→ {"segmentId":"seg_1001","version":12,"asOfDs":"2026-09-15","count":1}

GET /v1/segments/seg_1002/users?limit=1000
→ {"segmentId":"seg_1002","version":3,"users":["U1001","U1002"],"nextCursor":null}

GET /v1/users/U1003/segments
→ {"userId":"U1003","segments":[{"segmentId":"seg_1001","version":12,"asOfDs":"2026-09-15"}]}

GET /v1/segments/seg_1001/contains/U1001
→ {"segmentId":"seg_1001","userId":"U1001","contains":false,"version":12}

POST /v1/segments/contains   {"userIds":["U1001","U1003"],"segmentIds":["seg_1001","seg_1002"]}
→ {"results":[{"userId":"U1001","segmentIds":["seg_1002"]},{"userId":"U1003","segmentIds":["seg_1001"]}]}

POST /v1/segments/seg_1002/exports
→ 202 {"exportId":"exp_77","status":"RUNNING"}
```
