# CLAUDE.md — Vision: hệ thống Segmentation (CDP)

> **Bản thiết kế + luật làm việc** cho repo `vision`. Code phải khớp định nghĩa ở đây.
> Đổi semantics (loại dữ liệu, range, DSL, API) → **sửa file này trước**, rồi code + golden test.
>
> **Phạm vi hiện tại (tối giản): 1 nguồn duy nhất — S2a `user_profile` → `user_city` (MUTEX, STATE).**
> Làm xong cả luồng (bronze → silver → … → activation) với nguồn này rồi mới mở rộng, theo thứ tự:
> (1) S1 payment → `NOT_MUTEX` rồi `PARTIAL_VALUE` / `PARTIAL_VALUE_BY_TAG` + các `aggFunc`; (2) S2b product → `NOT_MUTEX` `STATE`;
> (3) S3 churn score → `MUTEX` `EVENT`; (4) S4/S5 → `EXTENDED`.
> Các nguồn còn lại vẫn mô tả đầy đủ ở tài liệu (là đích cuối), nhưng **chưa** ingest/xử lý cho tới khi tới lượt.
>
> **⚠️ Đích cuối: hệ thống PHẢI xử lý đủ 4 loại dữ liệu: `MUTEX` · `NOT_MUTEX` · `PARTIAL_VALUE` · `PARTIAL_VALUE_BY_TAG`** ở mọi layer — xem §3.2, §3.5, luật §13.
> Hai chiều cấu hình trực giao với loại dữ liệu: **`aggFunc`** (hàm aggregate của `PARTIAL_VALUE(_BY_TAG)`, §3.2.1) và **`attributeType`** `STANDARD`/`EXTENDED` (tag khai báo sẵn hay chuỗi tự do cardinality cao, §3.6).
>
> Tài liệu chi tiết (đọc khi cần, không nạp mặc định):
> - `com/tm/docs/data-types.md` — giải thích trực quan 4 loại dữ liệu, ví dụ timeline, cách chọn loại cho attribute mới
> - `com/tm/docs/data-flow-examples.md` — dữ liệu qua từng layer, ví dụ input/output đầy đủ (= golden test)
> - `com/tm/docs/capacity.md` — ước lượng quy mô, chi phí, rủi ro
> - `com/tm/docs/phases.md` — tổng quan kế hoạch theo layer (bước 0–10)
> - `com/tm/docs/phases/step-NN-*.md` — flow dữ liệu từng bước: input/output, công nghệ
> - `com/tm/docs/checklist.md` — **checklist tổng hợp** mọi bước (task + tiêu chí done), nguồn theo dõi tiến độ; chạy bằng skill `/execute`

---

## 0. TL;DR

- **Luồng**: data source → ingestion → chuẩn hoá → **tag bitmap / partial value theo ngày** → temporal → **materialize mọi date range** → build **segment** → **activation API** (users by segment, segments by user, contains, count).
- **Quy mô thiết kế**: 100M user · 500 attribute · 500–1000 tag/attribute (≤ 500K tag) · mọi date range mỗi ngày · 5K segment rebuild/create mỗi ngày.
- **Ý tưởng lõi**:
  1. User → số nguyên dày `uidx` (global dictionary) → tập user = **Roaring bitmap**.
  2. `MUTEX`/`NOT_MUTEX` quy về bitmap theo ngày `ADD(d,t)`, `DEL(d,t)`; `PARTIAL_VALUE` / `PARTIAL_VALUE_BY_TAG` quy về aggregate theo ngày (`SUM` mặc định; `COUNT`/`MIN`/`MAX`).
  3. Window = ghép **dyadic block** (A180 ≤ 11 block, tổng quát ≤ 2·log2 N). Tag theo window = `LATEST(r,t) ∩ SEEN[l,r]` → chi phí/ngày tỉ lệ **số tag**, không tỉ lệ độ dài window.
  4. Tính theo dạng dữ liệu (§3.6): nhãn cardinality thấp → **precompute bitmap** mọi tag × date range; số → aggregate theo ngày/block, **so ngưỡng lúc build segment**; tag cardinality cao (`EXTENDED`) → **chỉ tính condition đang được segment dùng** (usage-driven).
- **Build**: Bazel 8 monorepo (cần tham khảo layout/macro → repo `/Users/toanmai/Documents/code/thor`), Python / Go / Kotlin.

---

## 1. Tech stack

| Thành phần | Vai trò | Ghi chú |
|---|---|---|
| **Bazel 8** (bzlmod) | Build/test/image toàn repo | tham khảo thor |
| **Python 3.11** | Airflow DAG, PySpark, temporal planner, seed generator, DQ | rules_python + `pip.parse` |
| **Go** | `segment-builder` | rules_go + gazelle, `RoaringBitmap/roaring/v2` |
| **Kotlin + Vert.x + Dagger2** | `segment-manager`, `activation-api` | JVM 21, Vert.x 5 coroutines, Dagger qua `java_plugin` |
| **Kafka** | Source event bus + domain event | *bổ sung* |
| **Debezium + Flink (Java, DataStream)** | OLTP (Postgres) → CDC → Kafka → Iceberg bronze; job Java 17 chạy application mode, tham khảo repo `ironman` (`com/tm/flink`) | *bổ sung* |
| **Spark (PySpark)** | Bronze → Silver (dedup, MERGE CDC, SCD2), backfill | *bổ sung* |
| **Iceberg** (REST catalog) + **MinIO/S3** | bronze, silver, archive | |
| **StarRocks 3.5.x** | Gold: tag bitmap, partial value, temporal, range, segment bitmap; đọc Iceberg qua external catalog | |
| **StarRocks bitmap** | Kiểu tập user | uidx < 2^32 (BITMAP32) |
| **Postgres** | Catalog attribute/tag, segment definition/version, build run; Airflow DB | *bổ sung* |
| **Redis** | Cache `user_id ↔ uidx` cho API | *bổ sung* |
| **Airflow 2.10.x** | Orchestration theo `ds` | tham khảo thor; nâng cấp 3.x sau |
| **Prometheus + Grafana** | Metrics, dashboard, alert | cấm label cardinality cao (§10) |
| **Protobuf** | Contract DSL/catalog/event cho Go/Kotlin/Python | |

---

## 2. Kiến trúc & layer

```
L0 SOURCE        L1 INGESTION       L2 STANDARDIZE       L3 DAILY             L4 TEMPORAL          L5 RANGE              L6 SEGMENT             L7 ACTIVATION
S1 payment (OLTP)→ CDC→Kafka→Flink(Java)→ Spark: dedup, ds,  → StarRocks:         → planner(Py)→SQL:   → tag_range_bitmap    → segment-builder(Go) → activation-api
S2 profile (OLTP)→ Iceberg bronze      →  SCD2, uidx dict   →  tag_daily (ADD/DEL)→  blocks, LATEST,    →  pv_range_value      →  DSL→bitmap, cache  →  (Kotlin/Vert.x)
S3 ML score file→ Spark file load  →  silver (Iceberg)  →  pv_daily (AGG)     →  STATE checkpoint   →  (mọi supported      →  S3 snapshot, Kafka →  mmap roaring
                  bronze (raw)                                                                         date range)
Cross-cutting: Postgres catalog · Airflow (theo ds) · DQ invariants · Prometheus/Grafana
```

| Layer | Storage | Bảng chính | Retention |
|---|---|---|---|
| L1 Bronze | Iceberg | `bronze.<source>_raw` | 30 ngày |
| L2 Silver | Iceberg | `silver.payment_txn`, `silver.user_profile_scd2`, `silver.churn_score`, `silver.user_dict`, `silver.tag_dict` (EXTENDED) | 400 ngày |
| L3 Daily | StarRocks | `gold.tag_daily`, `gold.pv_daily` | 400 ngày |
| L4 Temporal | StarRocks | `gold.tag_block`, `gold.pv_block`, `gold.tag_latest`, `gold.tag_state_checkpoint` | block 400 ngày; latest 7 ngày + checkpoint tuần 400 ngày |
| L5 Range | StarRocks | `gold.tag_range_bitmap`, `gold.pv_range_value` | 7 ngày / 2 ngày |
| L6 Segment | StarRocks + S3 + Postgres | `seg.segment_bitmap`, `s3://vision-segments/…`, `meta.segment_version` | 30 ngày / 7 version |
| L7 Serving | mmap file + Redis | — | version active + 1 bản trước |

**Thời gian**
- `ds` = ngày dữ liệu theo **Asia/Ho_Chi_Minh** (`YYYY-MM-DD`); timestamp lưu UTC; thứ tự sự kiện trong ngày theo `(event_ts, event_id)`.
- `e(ds)` = số ngày từ `1970-01-01` (`e(2026-09-15) = 20711`), dùng cho dyadic block.
- Pipeline chạy T+1. Late data ≤ 3 ngày tự reprocess; cũ hơn → DAG backfill.

---

## 3. Mô hình dữ liệu

### 3.1 Attribute & Tag

- `attr_id INT`, `tag_id INT ≥ 1` (duy nhất trong attribute). `tag_id = 0` dành cho pseudo-tag `__any__` (attribute-level).
- Cấu hình attribute (catalog):

| Field | Giá trị |
|---|---|
| `dataType` | `MUTEX` · `NOT_MUTEX` · `PARTIAL_VALUE` · `PARTIAL_VALUE_BY_TAG` |
| `feedMode` | `EVENT` · `STATE` (§3.4) — chỉ cho `MUTEX`/`NOT_MUTEX`; `PARTIAL_VALUE(_BY_TAG)` luôn `EVENT` |
| `supportedDateRanges` | tập date range được precompute (§3.3) |
| `attrGroupId` | nhóm theo source table — đơn vị chạy job |
| `aggFunc` | `SUM` (mặc định) · `COUNT` · `MIN` · `MAX` — chỉ cho `PARTIAL_VALUE(_BY_TAG)` (§3.2.1); loại khác phải để trống |
| `attributeType` | `STANDARD` (mặc định, tag khai báo trong catalog) · `EXTENDED` (tag là chuỗi tự do từ nguồn, sinh động) — §3.6 |

- Tag của `PARTIAL_VALUE` là **khoảng giá trị** `valueRange {fromValue, fromInclusive, toValue, toInclusive}` (bucket định sẵn; số dạng chuỗi thập phân, thiếu `fromValue` = −∞, thiếu `toValue` = +∞). Bucket chỉ là tiện ích: ngưỡng chủ yếu nằm **ad-hoc trong condition** của segment và được so lúc build.
- Tag của attribute `EXTENDED` (vd mã OA, mã quà, mã campaign): chuỗi tự do, **không** khai báo trong `meta.tag`; map sang `tag_id ≥ 1` qua dictionary `silver.tag_dict (attr_id, tag_string) → tag_id` append-only, không tái sử dụng (như `uidx`).
- Tag của `PARTIAL_VALUE_BY_TAG` là **nhóm** mà con số thuộc về (vd ngành hàng `fnb`, `travel`); ngưỡng `valueRange` nằm trong condition của segment.

### 3.2 Bốn loại dữ liệu

Tóm tắt trực quan (chi tiết + ví dụ timeline: `com/tm/docs/data-types.md`):
- **MUTEX** — mỗi lúc user có ≤ 1 tag (city, churn band). Trong window: lấy **ADD gần nhất của cả attribute**; tag đó bị REMOVE sau → không thuộc tag nào (không quay lại tag cũ).
- **NOT_MUTEX** — nhiều tag cùng lúc, mỗi tag độc lập (sản phẩm đang dùng). Trong window: **từng tag**, tín hiệu gần nhất là ADD → có tag.
- **PARTIAL_VALUE** — dữ liệu là **con số** (số tiền). Trong window: **AGG** (`aggFunc`, mặc định SUM) rồi so `valueRange`.
- **PARTIAL_VALUE_BY_TAG** — **con số gắn với một tag** (số tiền theo ngành hàng). Trong window: **AGG riêng từng tag** rồi so `valueRange`.

Input (proto `com/tm/proto/vision/event/v1`):
- `MUTEX` / `NOT_MUTEX`: `{event_id, user_id, attr, tags_add[], tags_remove[], event_ts}` → mỗi tag là signal `ADD` / `REMOVE`.
- `PARTIAL_VALUE`: `{event_id, user_id, attr, value, event_ts}`.
- `PARTIAL_VALUE_BY_TAG`: `{event_id, user_id, attr, tag, value, event_ts}`.

| Loại | User ∈ tag `t` trong window `[l, r]` khi… | Ví dụ |
|---|---|---|
| **MUTEX** | tag được **ADD gần nhất trong window** là `t`, và **không có REMOVE `t` sau** lần ADD đó | `user_city`, `churn_score_band` |
| **NOT_MUTEX** | xét riêng tag `t`: **signal gần nhất trong window** của `t` là `ADD` | `txn_category`, `product_holding` |
| **PARTIAL_VALUE** | `AGG(value trong window) ∈ valueRange` (của tag, hoặc ad-hoc trong condition) | `txn_amount`: tổng tiền A30 ≥ 1M |
| **PARTIAL_VALUE_BY_TAG** | `AGG(value của tag t trong window) ∈ valueRange` (condition bắt buộc có `valueRange`) | `txn_amount_by_category`: chi F&B A7 ≥ 500K |

Luật:
- MUTEX ⇒ tại mọi `r` và window, mỗi user thuộc **≤ 1 tag**. NOT_MUTEX: nhiều tag cùng lúc.
- PARTIAL_VALUE(_BY_TAG): user **không có event nào** (của tag đó) trong window thì không thuộc bất kỳ `valueRange` nào (kể cả range chứa 0). Muốn "không phát sinh" → dùng `SUB`.
- PARTIAL_VALUE(_BY_TAG) lưu `value` dạng **DECIMAL(27,6)** để cộng dồn/trừ không lệch.
- PARTIAL_VALUE_BY_TAG với nhiều tag + `tagOp=OR`: user thoả nếu **ít nhất một** tag có AGG ∈ `valueRange`; `tagOp=AND`: **mọi** tag đều thoả. (Không gộp giữa các tag.)

#### 3.2.1 Hàm aggregate (`aggFunc`) của PARTIAL_VALUE(_BY_TAG)

Tương ứng "computed trait" của các CDP phổ biến: `AGG(property) OVER window [WHERE dimension = tag]` — PARTIAL_VALUE là aggregate không dimension, PARTIAL_VALUE_BY_TAG là aggregate có dimension (tag).

| `aggFunc` | Daily `pv_daily` | ⊕ khi ghép block | Ví dụ |
|---|---|---|---|
| `SUM` (mặc định) | `SUM(value)` | `+` | tổng chi tiêu, tổng cashback |
| `COUNT` | số event (bỏ qua `value`) | `+` | số giao dịch, số lần nạp |
| `MIN` | `MIN(value)` | `min` | giao dịch nhỏ nhất |
| `MAX` | `MAX(value)` | `max` | giao dịch lớn nhất |

- Mỗi attribute **một** `aggFunc`. Cần cả tổng và số lần → 2 attribute cùng nguồn, cùng `attrGroupId` (vẫn 1 scan).
- Chỉ nhận hàm **ghép được qua block** (kết hợp, không cần phép trừ). Late data: daily tính lại từ silver, block build lại → đúng cho cả MIN/MAX.
- Ngoài phạm vi v1 (lỗi tường minh nếu khai báo): `AVG` (= SUM/COUNT trên 2 attribute, so ngưỡng trên tỉ số), `DISTINCT_COUNT` (không ghép được qua block, cần HLL/bitmap), `FIRST`/`LAST` value.
- **Không** dùng PARTIAL_VALUE cho giá trị trạng thái (số dư ví, hạn mức, credit score): cộng dồn là sai nghĩa → dùng MUTEX `STATE` với bucket.

### 3.3 Date range

| Code | Window as-of `ds` | Precompute |
|---|---|---|
| `A1` | `[ds, ds]` | ✅ |
| `A7` `A15` `A30` `A60` `A90` `A120` `A180` | `[ds−N+1, ds]` | ✅ |
| `IN_MONTH` | `[ngày 1 tháng(ds), ds]` | ✅ |
| `LAST_MONTH` | tháng dương lịch trước | ✅ (tính ngày 1, đóng băng) |
| `ALWAYS_ACTIVE` | `[đầu lịch sử, ds]` (= "most recent" toàn thời gian) | ✅ incremental |
| `customDateRange {fromDate, toDate}` | `[fromDate, toDate]`, `toDate ≤ ds`, `fromDate ≥ ds−399` | ❌ on-demand + cache |
| `A0` | intraday realtime | ❌ ngoài phạm vi v1 |

Chỉ precompute các range trong `supportedDateRanges` của attribute.

### 3.4 Feed mode (cho MUTEX / NOT_MUTEX)

| feedMode | Nguồn | Ý nghĩa | Encoding L3 |
|---|---|---|---|
| `EVENT` | stream / batch event (payment, ML score) | signal ADD/REMOVE tại thời điểm xảy ra | `ADD(d,t)`, `DEL(d,t)` hằng ngày |
| `STATE` | CDC / snapshot trạng thái (profile, product holding) | giá trị hiện tại **được coi là ADD lại mỗi ngày**; mất giá trị = REMOVE | `ADDED(d,t)`, `REMOVED(d,t)` (delta) + `STATE(d,t)` + checkpoint tuần |

Hệ quả `STATE`: với **mọi** window kết thúc ở `r`, kết quả = `STATE(r,t)`. Nếu nạp city như `EVENT` thì user đổi city từ 2 năm trước sẽ rơi khỏi A30 — sai kỳ vọng nghiệp vụ.

### 3.5 Ma trận bắt buộc: mỗi layer xử lý 4 loại thế nào

| Layer / thành phần | MUTEX | NOT_MUTEX | PARTIAL_VALUE | PARTIAL_VALUE_BY_TAG |
|---|---|---|---|---|
| L2 Silver | giữ `event_ts`, `event_id` để sắp thứ tự ADD/REMOVE | như MUTEX | giữ `value` DECIMAL, không làm tròn | như PARTIAL_VALUE + giữ `tag` |
| L3 Daily | `ADD(d,t)` = ADD cuối ngày; `DEL(d,t)`; `ADD(d,0)` | `ADD(d,t)`/`DEL(d,t)` theo signal cuối ngày **từng tag**; `SIG` | `gold.pv_daily` = AGG theo `(ds, uidx)`, `tag_id=0` | `gold.pv_daily` = AGG theo `(ds, tag_id, uidx)` |
| L4 Block | trên `ADD(·,0)` (theo attribute) | trên `SIG(·,t)` (theo tag) | AGG ⊕ trên `pv_daily` | AGG ⊕ trên `pv_daily` theo tag |
| L4 Latest | `LATEST` (ADD mới xoá tag khác) | `POS` (từng tag độc lập) | — | — |
| L5 Range | `LATEST ∩ SEEN` → `tag_range_bitmap` | `POS ∩ SIG_window` → `tag_range_bitmap` | `pv_range_value`; tag (`valueRange` định sẵn) → `tag_range_bitmap` | `pv_range_value` theo `(tag, uidx)`; không có bucket định sẵn |
| STATE feed | `STATE(r,t)`, ≤ 1 tag/user | `STATE(r,t)`, nhiều tag/user | không áp dụng | không áp dụng |
| DSL validate | cấm `tagOp=AND`; cấm `valueRange` | cho `tagOp=AND`; cấm `valueRange` | `tags` hoặc `valueRange` ad-hoc (ít nhất một) | **bắt buộc** `tags` + `valueRange` |
| segment-builder | lấy bitmap range | lấy bitmap range | tag → bitmap; ad-hoc `valueRange` / custom range → query `pv_range_value`/`pv_block` + cache | luôn query `pv_range_value`/`pv_block` theo tag + cache |
| DQ | tag rời nhau mỗi ngày & mỗi window | `ADD ∩ DEL = ∅` | AGG `pv_daily` = AGG silver | AGG theo tag = AGG silver theo tag |
| `EXTENDED` (§3.6) | ❌ v1 — lỗi tường minh khi khai báo (LATEST phải trừ `ADD(d,0)` khỏi **mọi** tag mỗi ngày → O(#tag) với tag không giới hạn). `TODO(verify)`: cần thì lưu "tag mới nhất" dạng cột theo user | POS chỉ cập nhật tag có signal trong ngày; range **usage-driven** | không áp dụng (tag là bucket, luôn `STANDARD`) | `pv_daily`/`pv_block` như STANDARD; range **usage-driven** |
| Golden test | churn, city | txn_category, product_holding | txn_amount (+ mỗi `aggFunc`) | txn_amount_by_category (+ mỗi `aggFunc`, EXTENDED) |

### 3.6 Chiến lược tính theo dạng dữ liệu & cardinality tag

`attributeType` trực giao với `dataType`: quyết định tag có khai báo sẵn không, từ đó quyết định **precompute hay tính theo nhu cầu**.

| Dạng | Precompute hằng ngày | Lúc build segment |
|---|---|---|
| Nhãn, `STANDARD` (MUTEX / NOT_MUTEX) | daily → block → LATEST/POS/STATE → `tag_range_bitmap` cho **mọi tag × `supportedDateRanges`** | lấy bitmap |
| Số, PARTIAL_VALUE | `pv_daily`, `pv_block`, `pv_range_value` (supportedDateRanges); bucket định sẵn → `tag_range_bitmap` | bucket → bitmap; `valueRange` ad-hoc → lọc `pv_range_value` |
| Số theo tag, PARTIAL_VALUE_BY_TAG `STANDARD` | `pv_daily`, `pv_block`, `pv_range_value` | lọc theo tag + `valueRange` |
| `EXTENDED` (NOT_MUTEX, PARTIAL_VALUE_BY_TAG) | chỉ daily + block (+ POS với NOT_MUTEX) — chi phí tỉ lệ **tag có hoạt động**, không tỉ lệ tổng số tag | **usage-driven**: range chỉ materialize cho `(attr, tag, window)` được segment `ACTIVE` tham chiếu (`meta.condition_usage`); tag dùng lần đầu → on-demand + cache; tag không ai dùng không tốn chi phí range |

- Ngưỡng (`valueRange`) luôn so **lúc build**; không precompute theo ngưỡng.
- `EXTENDED`: `tag_string` chưa có trong `silver.tag_dict` → bitmap rỗng (không lỗi). Giới hạn "500–1000 tag/attribute" (§0) **không** áp dụng cho EXTENDED; quy mô tính theo số tag được dùng.
- Tương đương cách core-service hiện tại xử lý attribute EXTENDED / PARTIAL_VALUE (task on-demand theo `tagConditionId`), nhưng vision precompute thêm `pv_range_value` để nhiều segment dùng chung.


---

## 4. Thuật toán temporal

### 4.1 L3 — reduce trong ngày (MUTEX / NOT_MUTEX, EVENT)

Sắp event của user theo `(event_ts, event_id)` trong ngày `d`:
- **MUTEX**: `ADD(d,t)` = user có **ADD cuối ngày** là `t`. `DEL(d,t)` = user có REMOVE `t` **sau** ADD `t` cuối cùng trong ngày (hoặc bất kỳ REMOVE `t` nếu hôm đó không ADD `t`). `ADD(d,0) = ⋃ ADD(d,t)`.
- **NOT_MUTEX**: theo từng tag, signal cuối ngày: `ADD(d,t)` nếu là ADD, `DEL(d,t)` nếu là REMOVE (rời nhau). `SIG(d,t) = ADD(d,t) ∪ DEL(d,t)`.

### 4.2 Dyadic block (OR cho bitmap, AGG ⊕ cho partial value)

- Block `B(k,s)`, `s % 2^k == 0`, phủ ngày `[s, s+2^k−1]`, `k = 0..8`. Build khi đủ ngày: ngày `ds` đóng → mọi `k` có `(e(ds)+1) % 2^k == 0`: `B(k,s) = B(k−1,s) ⊕ B(k−1,s+2^(k−1))` (⊕ = OR cho bitmap; `+` cho SUM/COUNT, `min`/`max` cho MIN/MAX). Khấu hao ~1 phép/tag/ngày.
- Window `[l, r]` → greedy trái→phải, block aligned lớn nhất nằm trọn trong window. **Các block rời nhau** nên dùng được cho cả SUM/COUNT (không đếm trùng ngày).
  ```
  while l <= r:
      k = max k: l % 2^k == 0 and l + 2^k - 1 <= r
      take B(k, l); l += 2^k
  ```
  `ds = 2026-09-15`: A7 → 3 block `[09-09] [09-10..11] [09-12..15]`; A30 → 4; A90 → 5; A180 → 6.
  Tổng quát (mọi `ds`): A7 luôn 3; A30 ≤ 8; A90 ≤ 10; A180 ≤ 11 (≤ 2·log2 N) — `blocks_test.py`.
- Block dựng trên: `ADD(d,0)` (MUTEX), `SIG(d,t)` (NOT_MUTEX), `pv_daily` (PARTIAL_VALUE, PARTIAL_VALUE_BY_TAG), `REMOVED(d,t)` (STATE — chỉ cho custom range lịch sử).

### 4.3 LATEST & công thức window

| Loại | Cập nhật mỗi ngày (O(#tag)) | Window `[l, r]` |
|---|---|---|
| MUTEX | `LATEST(d,t) = (ADD(d,t) − DEL(d,t)) ∪ (LATEST(d−1,t) − ADD(d,0) − DEL(d,t))` | `LATEST(r,t) ∩ ⋃_{[l,r]} ADD(·,0)` |
| NOT_MUTEX | `POS(d,t) = ADD(d,t) ∪ (POS(d−1,t) − DEL(d,t))` | `POS(r,t) ∩ ⋃_{[l,r]} SIG(·,t)` |
| MUTEX/NOT_MUTEX, STATE | `STATE(d,t) = (STATE(d−1,t) − REMOVED(d,t)) ∪ ADDED(d,t)` | `STATE(r,t)` |
| PARTIAL_VALUE | — | `AGG ⊕` các block của window, group by `uidx`, lọc `valueRange` |
| PARTIAL_VALUE_BY_TAG | — | `AGG ⊕` các block của window, group by `(tag_id, uidx)`, lọc `valueRange` |

Lý do đúng (MUTEX): "ADD gần nhất trong `[l,r]` là `t` và không bị REMOVE sau" ⇔ "ADD gần nhất tính đến `r` là `t`, không bị REMOVE sau, **và** lần ADD đó `≥ l`". Vế đầu là `LATEST(r,t)`, vế sau là `SEEN = ⋃ ADD(·,0)` trên window. NOT_MUTEX tương tự theo từng tag.

- `ALWAYS_ACTIVE` = `LATEST(r,t)` / `POS(r,t)` trực tiếp.
- `r < ds` (custom range): `LATEST(r)`/`STATE(r)` = checkpoint tuần gần nhất + forward-fold ≤ 6 ngày.
- `IN_MONTH`, `ALWAYS_ACTIVE` của phần SEEN/AGG có thể cập nhật incremental (chỉ cộng thêm ngày mới; late data → tính lại từ block); `LAST_MONTH` = snapshot `IN_MONTH` ngày cuối tháng trước.
- PARTIAL_VALUE: tag (`valueRange` định sẵn) → materialize vào `tag_range_bitmap`; condition có `valueRange` ad-hoc → builder query `pv_range_value` (hoặc AGG trên block nếu custom date range), cache theo condition key.
- PARTIAL_VALUE_BY_TAG: không có bucket định sẵn → mọi condition đều query `pv_range_value` (window chuẩn) hoặc AGG trên `pv_block` (custom range), filter `tag_id IN (…)`, cache theo condition key.
- `EXTENDED`: như trên nhưng `tag_range_bitmap` / `pv_range_value` chỉ tính cho tag trong `meta.condition_usage` (§3.6).

### 4.4 Reference implementation (bắt buộc)

- `com/tm/src/temporal/reference.py`: cài đặt **ngây thơ** đúng định nghĩa §3.2 (duyệt toàn bộ event trong window, lấy signal gần nhất theo timestamp, aggregate theo `aggFunc`) bằng `pyroaring`.
- Planner/SQL thật phải pass **property-based test** (hypothesis) so với reference: ≤ 400 ngày, đủ 4 loại, 2 feed mode, mọi `aggFunc`, `STANDARD`/`EXTENDED`, REMOVE xen kẽ, late data.
- Ví dụ trong `com/tm/docs/data-flow-examples.md` là **golden test** (`com/tm/src/temporal/testdata/golden/`).

---

## 5. StarRocks: bảng & luật

- **Dùng PRIMARY KEY table có cột `BITMAP`** (StarRocks 3.5): upsert thay nguyên row.
- **Không dùng AGGREGATE KEY + `BITMAP_UNION`** cho bảng kết quả: ghi lại sẽ union với dữ liệu cũ → user đáng lẽ bị loại vẫn còn.
- **Idempotency**: job ghi theo `(ds, attr_id)` = `DELETE WHERE ds=? AND attr_id=?` rồi `INSERT` (tránh row mồ côi khi tag biến mất lúc chạy lại). Consumer chỉ đọc sau khi DAG ngày đó đánh dấu xong.
- Partition theo `ds` (drop partition cho retention).

```sql
CREATE TABLE gold.tag_range_bitmap (
  ds          DATE        NOT NULL,
  attr_id     INT         NOT NULL,
  tag_id      INT         NOT NULL,
  date_range  VARCHAR(16) NOT NULL,   -- A1..A180, IN_MONTH, LAST_MONTH, ALWAYS_ACTIVE
  bm          BITMAP      NOT NULL,
  cardinality BIGINT      NOT NULL
) PRIMARY KEY (ds, attr_id, tag_id, date_range)
PARTITION BY date_trunc('day', ds)
DISTRIBUTED BY HASH (attr_id, tag_id);

CREATE TABLE gold.pv_daily (
  ds       DATE           NOT NULL,
  attr_id  INT            NOT NULL,
  tag_id   INT            NOT NULL,   -- 0 cho PARTIAL_VALUE; tag nhóm cho PARTIAL_VALUE_BY_TAG
  uidx     INT            NOT NULL,
  value    DECIMAL(27, 6) NOT NULL    -- AGG theo aggFunc của attribute (SUM/COUNT/MIN/MAX)
) PRIMARY KEY (ds, attr_id, tag_id, uidx)
PARTITION BY date_trunc('day', ds)
DISTRIBUTED BY HASH (attr_id, uidx);
```

**BITMAP wire format**: 1 byte marker + payload — `0 EMPTY`, `1 SINGLE32` (4 byte LE), `2 BITMAP32` (Roaring portable, giống `RoaringBitmap.serialize()`), `3 SINGLE64`, `4 BITMAP64`.
- Đọc: `bitmap_to_base64(bm)` → bỏ marker → Roaring.
- Ghi: StarRocks 3.5 **không có `bitmap_from_base64`** → dùng `bitmap_from_binary(unhex(?))`.
- `uidx` là 32-bit: codec **từ chối** BITMAP64/SINGLE64 > `0xFFFFFFFF` thay vì cắt ngầm.
- Codec Go (`common/go/bitmapcodec`) và Kotlin (`common/kotlin/bitmapcodec`) có golden bytes lấy từ StarRocks thật (`hex(bitmap_to_binary(bitmap_from_string('1,2,3')))`) — chạy lại khi nâng version StarRocks.

Metadata Postgres `meta`: `attribute` (gồm `agg_func`, `attribute_type`), `tag` (chỉ STANDARD), `tag_rule`, `segment`, `segment_version`, `build_run`, `condition_usage` (condition đang được segment ACTIVE dùng — nguồn cho tính usage-driven); đồng bộ sang StarRocks `meta.*` để join.

---

## 6. Thành phần

**Ingestion (L1)** — nguồn nghiệp vụ nằm ở **OLTP** (Postgres nguồn, tách khỏi Postgres `meta`). Ghi vào OLTP trước, **không** ghi song song OLAP (tránh dual-write lệch). Debezium đọc log OLTP → Kafka (key `user_id`) → job Flink Java (`com/tm/src/ingest/cdc/flink`, tham khảo `ironman`) → `bronze.*_raw`; bảng event (payment, insert-only) và bảng trạng thái (profile, product) đều đi đường CDC, giữ cả update/delete + thời điểm thay đổi. Giao at-least-once → dedup `event_id` ở silver. Churn score (file ML) đi đường riêng: PySpark file loader có sensor `_SUCCESS`. Seed generator (Python) sinh data vào OLTP, tham số `--users --days --attrs`.

**Standardize (L2)** — PySpark: parse, dedup `event_id`, `ds` theo ICT, DLQ, Iceberg `MERGE` CDC → SCD2. **Dictionary** `user_id → uidx` append-only, không tái sử dụng; sync Redis + `meta.user_dict_rev(uidx, user_id)`. `UNIVERSE(ds)` = bitmap user hợp lệ. Derived attribute (vd `spend_band`) tính ở đây, đi vào như `STATE`.

**Daily (L3)** — StarRocks SQL template `com/tm/src/sql/starrocks/daily/<attr_group>.sql` (`{{ ds }}`): 1 scan / source table sinh nhiều attribute; tag rule nằm ở catalog, không hard-code.

**Temporal + Range (L4, L5)** — `com/tm/src/temporal/planner.py` (pure function): `(ds, attribute metadata)` → danh sách SQL theo thứ tự: daily → block → LATEST/POS/STATE → checkpoint (Chủ nhật) → range → DQ. Logic greedy block được port sang Go/Kotlin cho custom range, **dùng chung golden test**.

**segment-manager** (Kotlin/Vert.x/Dagger2): CRUD, validate DSL theo catalog, `estimate` (compile → StarRocks `bitmap_count`), trigger rebuild, lịch sử version.

**segment-builder** (Go): nhận `build_run` → topo-sort (segment tham chiếu segment) → **condition cache** key `(ds, attr, sorted(tags), tagOp, dateRange|custom, valueRange)` → lấy bitmap → evaluate in-process → ghi S3 `.roar` + manifest, `seg.segment_bitmap`, Postgres `PUBLISHED` (transaction), Kafka `vision.segment.published.v1`. Retry idempotent theo `(segment_id, version)`. Fetch bitmap theo batch giới hạn theo **byte**, không theo số row. Condition `EXTENDED` chưa có range materialize → tính on-demand, ghi `meta.condition_usage` để các ngày sau precompute (§3.6).

**activation-api** (Kotlin/Vert.x/Dagger2):
- Segment `ONLINE`: mmap `.roar` bằng `ImmutableRoaringBitmap`, hot-swap `AtomicReference` khi có event publish.
- `count`: từ manifest. `contains`: `user_id → uidx` (Caffeine → Redis) → `contains`.
- `segments by user`: duyệt segment ONLINE (5K × ~100–200ns). Vượt ngưỡng → index theo chunk high-16-bit.
- `users by segment`: cursor `(version, last_uidx)`; segment lớn → export async (`unnest_bitmap` + `INSERT INTO FILES`).
- Segment `OFFLINE`: fallback StarRocks `bitmap_contains`/`bitmap_count`.
- Mọi response có `version` + `asOfDs`.

### 6.1 Segment DSL (proto `com/tm/proto/vision/segment/v1`)

```json
{
  "segmentId": "seg_1001",
  "rule": {"operator": "SUB", "children": [
    {"operator": "AND", "children": [
      {"condition": {"attr": "churn_score_band", "tags": ["mid", "high"], "tagOp": "OR", "dateRange": "A30"}},
      {"condition": {"attr": "user_city", "tags": ["hn"], "dateRange": "A7"}}
    ]},
    {"condition": {"attr": "txn_category", "tags": ["fnb"], "dateRange": "A7"}}
  ]},
  "schedule": {"type": "DAILY"},
  "serving": "ONLINE"
}
```
- `operator`: `AND` · `OR` · `SUB` (con đầu trừ hợp các con còn lại).
- `condition`: `attr`, `tags[]`, `tagOp` (`OR` mặc định | `AND`), `dateRange` hoặc `customDateRange {fromDate, toDate}`, `valueRange` (chỉ PARTIAL_VALUE, PARTIAL_VALUE_BY_TAG).
- Validate: tag không thuộc attribute → lỗi với `STANDARD`; với `EXTENDED` tag là chuỗi tự do, không kiểm catalog (chưa có trong `tag_dict` → rỗng). `tagOp=AND` trên MUTEX → lỗi (luôn rỗng); `valueRange` trên MUTEX/NOT_MUTEX → lỗi; PARTIAL_VALUE thiếu cả `tags` lẫn `valueRange` → lỗi; PARTIAL_VALUE_BY_TAG thiếu `tags` hoặc `valueRange` → lỗi; `dateRange ∉ supportedDateRanges` → lỗi.
- Cách tính được **suy ra từ `dataType` / `aggFunc` / `attributeType` của attribute**, DSL không có field `mode`.

### 6.2 Activation API

```
GET  /v1/segments/{segmentId}/count
GET  /v1/segments/{segmentId}/users?cursor=&limit=
GET  /v1/users/{userId}/segments
GET  /v1/segments/{segmentId}/contains/{userId}
POST /v1/segments/contains                  {"userIds":[…], "segmentIds":[…]}
POST /v1/segments/{segmentId}/exports       → 202 {"exportId", "status"}
```

---

## 7. Orchestration (Airflow)

| DAG | Trigger | Việc |
|---|---|---|
| `vision_silver_<source>` | daily 00:30 ICT (+ hourly cho event) | bronze → silver, dictionary |
| `vision_gold_daily` | Dataset `silver.*@ds` | dynamic task mapping theo `attrGroupId` |
| `vision_gold_temporal` | Dataset `gold.daily@ds` | block, LATEST/STATE, checkpoint, range |
| `vision_dq` | sau temporal | invariant §9; fail `BLOCK` → không publish |
| `vision_segment_rebuild` | sau DQ | tạo `build_run`, gọi segment-builder, deferrable sensor |
| `vision_late_data` | daily | reprocess ds−1..ds−3 nếu silver đổi (LATEST phải fold lại tới ds) |
| `vision_backfill` | manual `from,to,attr_group` | tuần tự theo ngày |
| `vision_maintenance` | daily | retention, Iceberg compaction/expire snapshot |

Luật: DAG mỏng (logic ở thư viện có test) · idempotent theo `(ds, attr_id)` · `max_active_runs=1` cho DAG phụ thuộc ngày trước · pool `starrocks_etl` · image qua macro `com_tm_airflow_image`.

**SLA (ICT)**: silver 02:00 · daily 03:30 · range 05:00 · DQ 05:15 · **5K segment published 07:00**.

---

## 8. Repo layout & Bazel 8

```
vision/
├── .bazelversion  .bazelrc  MODULE.bazel  BUILD.bazel     # tham khảo thor
├── go.mod (module com.tm/vision)  maven_install.json  requirements_lock.txt
├── tools/rules/com_tm_container.bzl    # com_tm_py_image, com_tm_airflow_image (tham khảo thor) + com_tm_go_image, com_tm_kt_image
├── third_party/dagger/BUILD.bazel
└── com/tm/
    ├── proto/vision/{segment,catalog,event}/v1/
    ├── src/
    │   ├── ingest/{oltp(seed+DDL),cdc(debezium,flink Java),loader(Py)}/
    │   ├── batch/{silver,dictionary}/            # PySpark
    │   ├── sql/starrocks/{ddl,daily,dq}/
    │   ├── temporal/                             # model, blocks, latest, ranges, planner, engine, reference, testdata/golden
    │   ├── segment/{manager(Kt),builder(Go),dsl(Py)}/   # dsl: validate + evaluator tham chiếu
    │   ├── activation/api/                       # Kotlin
    │   ├── common/{go,kotlin,python}/            # bitmapcodec, config, metrics, catalog client
    │   └── observability/{prometheus,grafana}/
    ├── dags/
    ├── docker/vision/docker-compose.yml
    └── docs/{data-types.md,data-flow-examples.md,capacity.md,phases.md}
```

**Version đã pin (bước 0)** — Bazel `8.7.0` · `rules_python 1.5.4` · `rules_oci 2.2.6` · `tar.bzl 0.3.0` · `platforms 0.0.11` · `bazel_skylib 1.7.1` · `protobuf 29.3` · `rules_proto 7.1.0` · `rules_go 0.53.0` (Go 1.24.1) · `gazelle 0.42.0` · `rules_java 8.14.0` · `rules_jvm_external 6.7` · `rules_kotlin 2.4.10` (Vert.x 5.1.x cần Kotlin ≥ 2.3) · Vert.x `5.1.8` · Dagger `2.60.1` · Micrometer `1.16.7`.
- Python deps: `pip.parse(hub_name="pypi", requirements_lock="//third_party/python:requirements_lock.txt")`. `TODO(verify)`: image Python hiện cài `requirements` lúc container start (tham khảo thor) — chuyển sang layer site-packages dựng sẵn khi có service Python cần dependency (bước 4).
- JVM deps: sửa `artifacts` trong `MODULE.bazel` → `bazel run @maven//:pin` (lock `maven_install.json`, `fail_if_repin_required`).
- Go: `gazelle:map_kind go_binary com_tm_go_image` → gazelle quản lý `go_library`/`go_test`, binary luôn qua macro image.
- Go deps: `go_deps.from_file(go_mod="//:go.mod")`; thêm module: `go get <module>@<version>` rồi `bazel run //:gazelle`.
- JVM artifact dự kiến thêm ở các bước sau: `vertx-mysql-client` (StarRocks), `vertx-pg-client`, `vertx-redis-client`.
- OCI base: `python_base`, `airflow_base` (tham khảo thor), distroless static (Go), temurin 21 JRE (Kotlin).

**Dagger2** (`third_party/dagger/BUILD.bazel`):
```starlark
java_plugin(
    name = "dagger_compiler",
    processor_class = "dagger.internal.codegen.ComponentProcessor",
    generates_api = True,
    deps = ["@maven//:com_google_dagger_dagger_compiler"],
)
java_library(
    name = "dagger",
    exported_plugins = [":dagger_compiler"],
    exports = ["@maven//:com_google_dagger_dagger", "@maven//:javax_inject_javax_inject"],
    visibility = ["//visibility:public"],
)
```

**Image**: `com.tm.{py,go,kt,airflow}.<name>:v1.0.0`; target `<name>`, `<name>_image`, `<name>_docker`.

**Lệnh**
```bash
bazel build //...
bazel test //...
bazel run //:gazelle
bazel run @maven//:pin
bazel run //com/tm/src/activation/api:activation_api_docker
bazel run --config=linux-amd64 //com/tm/src/segment/builder:segment_builder_docker
docker compose -f com/tm/docker/vision/docker-compose.yml up -d
```

**Local stack** (dựa compose thor): **thêm dần theo bước** (`com/tm/docs/phases.md`): Postgres OLTP (bước 2) → kafka, debezium, flink, minio, iceberg-rest (3) → spark (4) → starrocks 3.5 allin1, postgres meta (5) → redis (9) → airflow, prometheus, grafana, statsd-exporter, pushgateway (10). Seed OLTP ~10K user.

---

## 9. Data quality (lưu `dq.result`)

| Check | Áp dụng | Severity |
|---|---|---|
| `Σ cnt(ADD(d,t)) == cnt(ADD(d,0))` | MUTEX | BLOCK |
| `ADD(d,t) ∩ DEL(d,t) = ∅` | NOT_MUTEX | BLOCK |
| Σ cnt tag của một window == cnt(⋃ tag) (rời nhau) | MUTEX (EVENT + STATE) | BLOCK |
| `cnt(A7(t)) ≤ cnt(A30(t)) ≤ … ≤ cnt(ALWAYS_ACTIVE(t))` | MUTEX, NOT_MUTEX (EVENT) | BLOCK |
| `STATE(d) == (STATE(d−1) − REMOVED) ∪ ADDED` (sample) | STATE | BLOCK |
| AGG `pv_daily` theo ngày == AGG silver (SUM/COUNT: tổng; MIN/MAX: min/max) | PARTIAL_VALUE | BLOCK |
| AGG `pv_daily` theo `(ngày, tag)` == AGG silver theo tag | PARTIAL_VALUE_BY_TAG | BLOCK |
| `tag_dict` append-only: mapping cũ không đổi/xoá | EXTENDED | BLOCK |
| `uidx ⊆ UNIVERSE(ds)` | mọi loại | BLOCK |
| Drift `cnt(t)` vs 7 ngày trước > 30% | mọi loại | WARN |
| Freshness source trước SLA | source | WARN → BLOCK sau 2h |
| Segment count đổi > 50% so với version trước | segment | WARN (hold nếu `critical`) |

---

## 10. Observability

- Naming: `vision_<component>_<what>_<unit>`.
- **Cấm label cardinality cao**: không dùng `user_id`, `uidx`, `tag_id`, `segment_id` làm label Prometheus. Số liệu theo tag/segment → StarRocks (`dq.result`, `seg.build_stats`), Grafana đọc qua MySQL datasource. Label cho phép: `component`, `source`, `attr_group`, `date_range`, `data_type`, `agg_func`, `attribute_type`, `status`, `endpoint`.
- Expose: Go `client_golang` · Kotlin Micrometer Prometheus · Airflow StatsD exporter · Spark/batch Pushgateway · StarRocks/Flink/Kafka built-in.
- Dashboard (`com/tm/src/observability/grafana/`): pipeline freshness/SLA · temporal cost · DQ · segment build · activation API · StarRocks.
- Alert: `VisionRangeNotReady` (05:30) · `VisionDQBlocked` · `VisionSegmentBuildFailureRatio > 1%` · `VisionSegmentPublishLate` (07:00) · `VisionApiP99High` · `VisionCollectorKafkaErrors`.
- SLO API ONLINE: `contains`/`count` p99 < 10ms · `segments by user` p99 < 20ms · 99.9%.

---

## 11. Coding conventions

**Chung**: contract qua proto (không DTO viết tay trùng) · ID kỹ thuật là số · không log `user_id`/SĐT ở INFO; bitmap/file chỉ chứa `uidx` · config qua env/file · job batch idempotent.

**Go**: `cmd/` + `internal/`; `context.Context` xuyên suốt; `log/slog`; `fmt.Errorf("...: %w", err)`; table-driven test; không global mutable state.

**Kotlin/Vert.x/Dagger2**: JVM 21; `CoroutineVerticle`, **không block event loop**; một `@Component`/deployable, `@Module` theo concern; constructor injection; client dùng chung `@Singleton`; Router → Handler → Service → Repository.

**Python**: 3.11, type hints, `ruff` + `pytest`; PySpark job có `main(args)` test được với SparkSession local; DAG không import nặng ở top-level.

**SQL**: một file/bước; template `{{ ds }}`, `{{ attr_id }}`; header ghi input/output + cách idempotent.

**Test bắt buộc**: temporal property test + golden · bitmap codec golden bytes · DSL validate matrix (dataType × dateRange × tagOp × valueRange) · API integration test với `.roar` nhỏ.
Mọi test của daily/temporal/range/segment phải **parametrize theo đủ 4 loại** (MUTEX, NOT_MUTEX, PARTIAL_VALUE, PARTIAL_VALUE_BY_TAG); với MUTEX/NOT_MUTEX thì cả `EVENT` và `STATE`, có ca REMOVE; với PARTIAL_VALUE(_BY_TAG) thì mọi `aggFunc`; với NOT_MUTEX / PARTIAL_VALUE_BY_TAG thì cả `STANDARD` và `EXTENDED`.

**Rẽ nhánh theo loại**: dùng `switch`/`when`/`match` **exhaustive** trên `(dataType, feedMode, attributeType)` và `aggFunc`; nhánh không hỗ trợ → lỗi rõ ràng, **không** `default` rơi ngầm về một loại.

---

## 12. Roadmap

Tổng quan: `com/tm/docs/phases.md`; checklist tổng hợp: `com/tm/docs/checklist.md`. Đi từ bài toán nghiệp vụ → dữ liệu nguồn → mới khai báo attribute theo nhu cầu.

| Bước | Layer | Nội dung |
|---|---|---|
| 0 | Nền móng | Bazel 8, compose local |
| 1 | L0 | bài toán kỹ thuật + ví dụ (tài liệu) |
| 2 | L0 | seed OLTP: schema + data mẫu của các nguồn |
| 3 | L1 | ingest OLTP → bronze (OLAP, Iceberg) |
| 4 | L2 | Spark silver, dictionary `uidx` |
| 5 | L3 | nhu cầu attribute + catalog, reference, SQL `tag_daily` / `pv_daily` |
| 6 | L4 | block, LATEST/POS/STATE, checkpoint |
| 7 | L5 | `tag_range_bitmap`, `pv_range_value`, custom range |
| 8 | L6 | proto segment, codec, segment-manager, segment-builder |
| 9 | L7 | activation-api |
| 10 | Vận hành | DQ, Airflow, metrics, scale (làm dần từ bước 5) |

Mọi bước đụng dữ liệu chỉ **done** khi chạy đúng cả 4 loại.

---

## 13. Luật khi Claude làm việc trong repo này

1. **Đích cuối là đủ 4 loại dữ liệu** (giai đoạn hiện tại chỉ làm `MUTEX` `STATE` từ 1 nguồn, xem đầu file; các loại còn lại chưa làm thì ghi `TODO` + lỗi tường minh, không bỏ qua im lặng) — `MUTEX`, `NOT_MUTEX`, `PARTIAL_VALUE`, `PARTIAL_VALUE_BY_TAG`. Mọi thay đổi ở daily/temporal/range/DSL/segment-builder/activation phải:
   - đối chiếu ma trận §3.5 cho từng loại;
   - có test cho từng loại (MUTEX/NOT_MUTEX: cả EVENT và STATE, có REMOVE; PARTIAL_VALUE*: mọi `aggFunc`; NOT_MUTEX/PARTIAL_VALUE_BY_TAG: cả STANDARD và EXTENDED);
   - chỉ báo xong khi cả 4 loại chạy đúng. Nếu cố ý chưa hỗ trợ một loại → báo rõ cho user + `TODO` + lỗi tường minh trong code, không bỏ qua im lặng.
2. Đụng vào daily/temporal/segment → đọc §3–§4, `com/tm/docs/data-types.md` và `com/tm/docs/data-flow-examples.md` trước. Không "đơn giản hoá" thành quét mọi event trong window ở đường production.
3. Semantics mơ hồ → hỏi user, không tự suy diễn; định nghĩa trong file này là nguồn duy nhất.
4. Đổi semantics → cập nhật file này + docs + golden test cùng commit.
5. Attribute mới: khai báo đủ `dataType`, `feedMode`, `supportedDateRanges`, `attrGroupId` trong catalog; không viết SQL riêng ngoài template.
6. Chạy `bazel test` cho package bị ảnh hưởng trước khi báo xong; Go → `bazel run //:gazelle` sau khi đổi import.
7. Pin version (Bazel deps, maven, image digest); không `latest` ngoài compose local.
8. Không thêm label Prometheus cardinality cao.
9. Điều chưa xác minh → ghi `TODO(verify)`, không đoán.
10. Làm theo thứ tự bước (layer) trong `com/tm/docs/phases.md`; trước khi báo xong một bước, tự kiểm mục **Done khi** trong `com/tm/docs/checklist.md` và tick checkbox ở đó khi hoàn thành việc.
11. **Git**: xong **mỗi task** (mỗi dòng `- [ ]` trong `checklist.md`) → tick checkbox rồi tạo **1 commit** (chỉ file của task đó + `checklist.md`; message `step N: <task>`). Xong **cả phase** (mọi task + `Done khi` đã tick) → **push** branch hiện tại. Không push giữa phase, không gộp nhiều task vào một commit, không commit khi test của task chưa xanh.
