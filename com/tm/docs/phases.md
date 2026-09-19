# Kế hoạch triển khai theo phase

> Thiết kế gốc: `CLAUDE.md`. Semantics 4 loại dữ liệu: `com/tm/docs/data-types.md`. Số liệu golden: `com/tm/docs/data-flow-examples.md`.
>
> Nguyên tắc chung cho mọi phase:
> - Kết thúc phase = **Done khi** đạt hết + `bazel build //... && bazel test //...` xanh + commit.
> - Phase nào đụng dữ liệu phải chạy đúng **cả 4 loại**: `MUTEX` · `NOT_MUTEX` · `PARTIAL_VALUE` · `PARTIAL_VALUE_BY_TAG` (MUTEX/NOT_MUTEX: cả `EVENT` và `STATE`, có REMOVE).
> - Đổi semantics trong lúc làm → cập nhật `CLAUDE.md` + docs + golden test trước.
> - Service mới phải có `/metrics` + log có cấu trúc ngay từ đầu (không đợi P7).
> - Mỗi phase mở đầu bằng dòng **Tóm tắt**: phase làm gì · sau phase có dữ liệu/sản phẩm gì, nằm ở đâu.
> - Mỗi phase có doc chi tiết trong `com/tm/docs/phases/`: input/output (dữ liệu gì, lưu ở đâu), flow, từng bước dùng công nghệ gì và dữ liệu biến đổi ra sao (ví dụ theo `data-flow-examples.md`).

## Tổng quan

```
P0 Foundation ──┬──▶ P2 Ingestion & Silver ──▶ P3 Daily (L3) ──▶ P4 Temporal & Range (L4–L5) ──▶ P5 Segment ──▶ P6 Activation
                │                                                        ▲                                         │
                └──▶ P1 Semantics core ──▶ P1b aggFunc + EXTENDED ───────┘                                         ▼
                                                                                                 P7 Observability ──▶ P8 Scale & hardening
```

| Phase | Tên | Trạng thái | Phụ thuộc | Chạy song song được với |
|---|---|---|---|---|
| P0 | Foundation | ✅ xong | — | — |
| P1 | Semantics core (reference + planner) | ✅ xong | P0 | P2, P3 |
| P1b | Mở rộng semantics: `aggFunc` + `EXTENDED` | ✅ xong | P1 | P2, P3 |
| P2 | Ingestion & Silver | ⬜ | P0 | P1, P1b |
| P3 | Daily layer (L3) | ⬜ | P2 | P1b |
| P4 | Temporal & Range (L4–L5) | ⬜ | P1b, P3 | — |
| P5 | Segment (manager + builder) | ⬜ | P4 | codec + DSL validate làm sớm từ P1 |
| P6 | Activation API | ⬜ | P5 (có thể bắt đầu với file `.roar` giả sau khi có codec) | P7 |
| P7 | Observability | ⬜ | P2 trở đi (làm dần) | P6 |
| P8 | Scale & hardening | ⬜ | P6, P7 | — |

---

## P0 — Foundation

> **Tóm tắt:** P0 dựng nền móng repo. Sau P0 có: Bazel build/test được Go · Kotlin · Python; contract proto `event` / `catalog` / `segment`; schema Postgres `meta.*` với 6 attribute mẫu (đủ 4 loại); stack local (Kafka, Flink, MinIO + Iceberg, Spark, StarRocks, Postgres, Airflow, Redis, Prometheus, Grafana) chạy bằng docker compose — **chưa có dữ liệu nghiệp vụ**.
>
> 📄 Chi tiết flow dữ liệu (input → các bước → output, công nghệ từng bước): [`phases/p0-foundation.md`](phases/p0-foundation.md)

**Mục tiêu**: repo build được mọi ngôn ngữ, chạy được stack local, có contract dữ liệu.

**Việc cần làm**
- [x] `.bazelversion` (8.x), `.bazelrc`, `MODULE.bazel`, `BUILD.bazel` theo pandora; pin `rules_go`, `gazelle`, `rules_kotlin`, `rules_jvm_external`, `protobuf`.
- [x] `tools/rules/com_tm_container.bzl`: `com_tm_py_image`, `com_tm_airflow_image` (từ pandora) + `com_tm_go_image`, `com_tm_kt_image`.
- [x] `third_party/dagger`; service Kotlin "hello" dùng Vert.x + Dagger2 build ra image.
- [x] Service Go "hello" + gazelle; Python lib "hello" + `pip.parse`.
- [x] Proto `com/tm/proto/vision/`:
  - `event/v1`: event cho 4 loại (`tags_add/tags_remove` · `value` · `tag + value`).
  - `catalog/v1`: `Attribute{dataType, feedMode, supportedDateRanges, attrGroupId}`, `Tag{valueRange?}`.
  - `segment/v1`: DSL (`operator AND/OR/SUB`, `condition{attr, tags, tagOp, dateRange|customDateRange, valueRange}`).
- [x] Postgres schema `meta.*` (migration tool) + seed 6 attribute của examples (đủ 4 loại, cả EVENT/STATE).
- [x] `com/tm/docker/vision/docker-compose.yml`: kafka, flink, minio, iceberg-rest, spark, starrocks 3.5, postgres, airflow, redis, prometheus, grafana.

**Done khi**
- [x] `bazel build //... && bazel test //...` xanh; `bazel run …_docker` load được image Go/Kotlin/Python/Airflow.
- [x] `docker compose up` → mọi service healthy.
- [x] Catalog có attribute cho **cả 4 dataType**.

---

## P1 — Semantics core (pure logic)

> **Tóm tắt:** P1 khoá đúng semantics 4 loại dữ liệu bằng code Python thuần, chạy trong bộ nhớ. Sau P1 có: thư viện `com/tm/src/temporal/` (reference ngây thơ, model L3, dyadic block, LATEST/POS/STATE, date range, planner, engine) và `com/tm/src/segment/dsl/` (validate + evaluator), golden test + property test 10K case/loại — **không ghi vào database nào**; đây là chuẩn để đối chiếu SQL ở P3–P5.
>
> 📄 Chi tiết flow dữ liệu (input → các bước → output, công nghệ từng bước): [`phases/p1-semantics-core.md`](phases/p1-semantics-core.md)

**Mục tiêu**: khoá đúng semantics trước khi viết SQL. Không cần StarRocks/Spark.

**Việc cần làm**
- [x] `com/tm/src/temporal/reference.py`: cách tính ngây thơ theo định nghĩa `CLAUDE.md` §3.2 cho 4 loại (duyệt event trong window, signal gần nhất, SUM), bitmap bằng `pyroaring`.
- [x] `com/tm/src/temporal/model.py`: reduce ngày (§4.1) → `ADD/DEL/SIG`, `ADDED/REMOVED/STATE`, `pv_daily`.
- [x] `blocks.py`: dyadic block build + greedy decomposition (§4.2).
- [x] `latest.py`: `LATEST`, `POS`, `STATE`, checkpoint + forward-fold (§4.3).
- [x] `ranges.py`: A1…A180, IN_MONTH, LAST_MONTH, ALWAYS_ACTIVE, CUSTOM cho 4 loại.
- [x] `planner.py` (khung): `(ds, attribute)` → danh sách bước (chưa sinh SQL thật).
- [x] Golden test từ `data-flow-examples.md` (`testdata/golden/*.yaml`).
- [x] Property test (hypothesis): implementation tối ưu == reference; ≤ 400 ngày, REMOVE xen kẽ, late data, cả EVENT/STATE.
- [x] Segment evaluator thuần (AND/OR/SUB, tagOp, valueRange) + DSL validate matrix.

**Done khi**
- [x] Golden pass cho: churn & city (MUTEX), txn_category & product_holding (NOT_MUTEX), txn_amount (PARTIAL_VALUE), txn_amount_by_category (PARTIAL_VALUE_BY_TAG), ca biên REMOVE.
- [x] Property test pass ≥ 10K case/loại.
- [x] `seg_1001 = {3}`, `seg_1002 = {1,2}` tính bằng evaluator thuần.

**Đã làm** (code ở `com/tm/src/temporal/`, `com/tm/src/segment/dsl/`)
- `engine.py` chạy các bước của `planner.py` trong bộ nhớ (daily → block → LATEST/POS/STATE → checkpoint → range → DQ);
  property test so engine với `reference.py`, nên planner cũng được kiểm. `testing.py` mô phỏng pipeline hằng ngày có late data ≤ 3 ngày.
- Golden: `testdata/golden/{s1_payment,s2_cdc,s3_churn,data_types,segments,blocks}.yaml` (data-flow-examples + data-types).
- Property test: `bazel test //com/tm/src/temporal:property_test_<kind>` — 6 target (4 loại × EVENT/STATE), mỗi target 10K case.
- `TODO(verify)`: PARTIAL_VALUE có cả `tags` lẫn `valueRange` → hiện từ chối (chưa định nghĩa nghĩa).
- Phạm vi P1: chỉ `aggFunc = SUM` và tag `STANDARD`; phần mở rộng ở P1b.

---

## P1b — Mở rộng semantics: `aggFunc` + `EXTENDED` ✅

> **Tóm tắt:** P1b mở rộng thư viện P1 theo CLAUDE.md §3.2.1 và §3.6: PARTIAL_VALUE(_BY_TAG) có `aggFunc` COUNT/MIN/MAX ngoài SUM; tag `EXTENDED` (chuỗi tự do, cardinality cao) cho NOT_MUTEX và PARTIAL_VALUE_BY_TAG, tính range theo mức sử dụng. Sau P1b có: như P1 nhưng hỗ trợ đủ các tổ hợp mới; proto `catalog` + Postgres `meta.attribute` có `agg_func`, `attribute_type`, bảng mới `meta.condition_usage` — **vẫn chưa có dữ liệu thật**.
>
> 📄 Chi tiết flow dữ liệu (input → các bước → output, công nghệ từng bước): [`phases/p1b-aggfunc-extended.md`](phases/p1b-aggfunc-extended.md)

**Mục tiêu**: khoá semantics mới trước khi viết SQL (P3–P4).

**Việc cần làm**
- [x] Proto `catalog/v1`: enum `AggFunc {SUM, COUNT, MIN, MAX}`, `AttributeType {STANDARD, EXTENDED}`; field trong `Attribute`. Migration Postgres: `meta.attribute.agg_func`, `meta.attribute.attribute_type`, bảng `meta.condition_usage`. Luật catalog: `aggFunc` chỉ cho PARTIAL_VALUE*; `EXTENDED` chỉ cho NOT_MUTEX (EVENT/STATE) và PARTIAL_VALUE_BY_TAG — MUTEX/PARTIAL_VALUE + EXTENDED → lỗi tường minh.
- [x] `model.py`: `AttributeSpec` có `agg_func`, `attribute_type`; `reduce_pv_day` và phép ⊕ block theo `aggFunc`; dictionary `tag_string → tag_id` append-only (thuần, cho test).
- [x] `reference.py`: aggregate theo `aggFunc`; tag EXTENDED là chuỗi.
- [x] `planner.py` / `engine.py`: range của `EXTENDED` chỉ cho tag trong `condition_usage`; tag dùng lần đầu → on-demand.
- [x] DSL validate + evaluator: tag EXTENDED không kiểm catalog, tag chưa có trong dict → rỗng; matrix thêm chiều `aggFunc` × `attributeType`.
- [x] Docs: thêm ví dụ vào `data-types.md`, `data-flow-examples.md` (vd `txn_count` COUNT, `txn_max` MAX, `oa_follow` NOT_MUTEX EXTENDED, `gift_value` PARTIAL_VALUE_BY_TAG EXTENDED) = golden mới.
- [x] Property test thêm target: PARTIAL_VALUE(_BY_TAG) × {COUNT, MIN, MAX}; NOT_MUTEX EXTENDED (EVENT, STATE); PARTIAL_VALUE_BY_TAG EXTENDED.
- [x] Cập nhật `com/tm/src/common/python/catalog/validation.py` + seed Postgres.

**Đã làm**
- Proto `catalog/v1`: `AggFunc`, `AttributeType`, field `Attribute.agg_func` (8), `attribute_type` (9).
- Postgres: `V3__agg_func_attribute_type.sql` (cột + CHECK + trigger + `meta.condition_usage`), `V4__seed_p1b_examples.sql` (`txn_count`, `txn_max`, `oa_follow`, `gift_value`) — đã chạy thử V1→V4 trên DB tạm, 11 ca chấp nhận/từ chối đúng.
- `model.py`: `aggregator` / `merge_values` / `reduce_pv_day(agg)`, `TagDict`, `AttributeSpec.agg_func/attribute_type`; `latest.py`: POS/STATE chỉ cập nhật tag có thay đổi.
- `planner.py`: `usage` → `RangeStep.tags` (scope), DQ `TAG_DICT_APPEND_ONLY`; `engine.py`: range theo scope, `query_tag` on-demand + ghi `usage`, LAST_MONTH carry-forward tính lại khi scope mở rộng.
- DSL: tag EXTENDED không kiểm catalog, chuỗi chưa có trong `tag_dict` → rỗng; matrix thêm chiều `aggFunc` × `attributeType`.
- Golden `p1b_aggfunc_extended.yaml` + `seg_1003 = {1}`; `engine_test.py` (usage tăng giữa tháng).
- Property test: 15 target (6 của P1 + 9 mới), mỗi target 10K case.

**Done khi**
- [x] Golden mới pass (mỗi `aggFunc`, mỗi tổ hợp EXTENDED hợp lệ); tổ hợp không hỗ trợ ra lỗi tường minh.
- [x] Property test pass ≥ 10K case cho mỗi target mới; target P1 vẫn xanh.
- [x] `bazel build //... && bazel test //...` xanh.

---

## P2 — Ingestion & Silver (L0–L2)

> **Tóm tắt:** P2 hoàn thiện phần ingest dữ liệu. Sau P2 có: event thô trong Kafka và Iceberg `bronze.*_raw`; dữ liệu sạch (dedup, `ds` theo ICT, SCD2) trong Iceberg `silver.payment_txn`, `silver.user_profile_scd2`, `silver.user_product_scd2`, `silver.churn_score`; dictionary `silver.user_dict` (user → `uidx`) và `silver.tag_dict` (tag EXTENDED → `tag_id`), sync Redis + `meta.user_dict_rev`.
>
> 📄 Chi tiết flow dữ liệu (input → các bước → output, công nghệ từng bước): [`phases/p2-ingestion-silver.md`](phases/p2-ingestion-silver.md)

**Mục tiêu**: 3 source chảy vào Iceberg silver, có `uidx`.

**Việc cần làm**
- [ ] Simulator (Python): S1 payment (Kafka), S2 profile + product CDC (Debezium format), S3 churn parquet; tham số `--users --days --attrs`, có duplicate, late data, REMOVE.
- [ ] `event-collector` (Go): HTTP/gRPC → validate proto → Kafka.
- [ ] Flink SQL: Kafka → `bronze.*_raw` (exactly-once).
- [ ] PySpark file loader S3 → bronze (sensor `_SUCCESS`).
- [ ] PySpark silver: `payment_txn` (dedup, `ds` ICT, DLQ), `user_profile_scd2` + `user_product_scd2` (MERGE CDC), `churn_score`.
- [ ] Dictionary `user_id → uidx` append-only + sync Redis + `meta.user_dict_rev`; `UNIVERSE(ds)`.
- [ ] Dictionary `silver.tag_dict (attr_id, tag_string) → tag_id` append-only cho attribute `EXTENDED`.
- [ ] Airflow DAG `vision_silver_<source>`.

**Done khi**
- Chạy simulator với dữ liệu của examples → silver khớp bảng L2 trong `data-flow-examples.md` (gồm dedup e-9001, e-9003 sang ds 09-15, e-9005 late).
- Rerun DAG cùng `ds` cho kết quả y hệt (idempotent).
- Silver giữ đủ thông tin cho 4 loại: thứ tự `(event_ts, event_id)`, `value` DECIMAL, `tag` cho BY_TAG, SCD2 cho STATE, `tag_string` cho EXTENDED.

---

## P3 — Daily layer (L3)

> **Tóm tắt:** P3 chuyển silver thành dữ liệu theo ngày. Sau P3 có (StarRocks): `gold.tag_daily` — bitmap user theo `(ngày, attribute, tag)` cho ADD/DEL/SIG (EVENT) và ADDED/REMOVED (STATE); `gold.pv_daily` — giá trị aggregate (`aggFunc`) theo `(ngày, attribute, tag, user)`; kết quả DQ trong `dq.result`.
>
> 📄 Chi tiết flow dữ liệu (input → các bước → output, công nghệ từng bước): [`phases/p3-daily.md`](phases/p3-daily.md)

**Mục tiêu**: bảng daily trong StarRocks cho 4 loại.

**Việc cần làm**
- [ ] DDL StarRocks (PK table, partition `ds`): `gold.tag_daily`, `gold.pv_daily`, `meta.*` mirror.
- [ ] SQL template `sql/starrocks/daily/<attr_group>.sql`, 1 scan / source table:
  - MUTEX EVENT: `ADD(d,t)` (ADD cuối ngày), `DEL(d,t)`, `ADD(d,0)`.
  - NOT_MUTEX EVENT: `ADD/DEL` theo signal cuối ngày từng tag, `SIG`.
  - MUTEX/NOT_MUTEX STATE: `ADDED/REMOVED` từ SCD2.
  - PARTIAL_VALUE: AGG (`aggFunc`) `(ds, uidx)`; PARTIAL_VALUE_BY_TAG: AGG `(ds, tag, uidx)`.
  - EXTENDED: `tag_string → tag_id` qua `silver.tag_dict`.
- [ ] Ghi idempotent `DELETE (ds, attr_id)` + `INSERT`.
- [ ] DQ cơ bản (§9): mutex rời nhau, `ADD ∩ DEL = ∅`, AGG pv = AGG silver (cả theo tag), `uidx ⊆ UNIVERSE`, `tag_dict` append-only.
- [ ] Airflow `vision_gold_daily` (dynamic task mapping theo `attrGroupId`).

**Done khi**
- Output L3 == `model.py` của P1/P1b trên cùng input (golden + dataset ngẫu nhiên nhỏ) cho **cả 4 loại**, mọi `aggFunc`, STANDARD + EXTENDED.
- DQ pass; rerun không đổi kết quả.

---

## P4 — Temporal & Range (L4–L5)

> **Tóm tắt:** P4 tính kết quả theo date range mỗi ngày. Sau P4 có (StarRocks): trung gian `gold.tag_block` / `gold.pv_block`, `gold.tag_latest` + `gold.tag_state_checkpoint`; kết quả cuối `gold.tag_range_bitmap` (bitmap user theo tag × A1…A180, IN_MONTH, LAST_MONTH, ALWAYS_ACTIVE) và `gold.pv_range_value` (giá trị aggregate theo user × date range); custom range tính on-demand. Airflow chạy daily / late data / backfill.
>
> 📄 Chi tiết flow dữ liệu (input → các bước → output, công nghệ từng bước): [`phases/p4-temporal-range.md`](phases/p4-temporal-range.md)

**Mục tiêu**: mỗi ngày có đủ mọi date range cho mọi tag.

**Việc cần làm**
- [ ] DDL: `gold.tag_block`, `gold.pv_block`, `gold.tag_latest`, `gold.tag_state_checkpoint`, `gold.tag_range_bitmap`, `gold.pv_range_value`.
- [ ] Planner sinh SQL thật: block → LATEST/POS/STATE → checkpoint Chủ nhật → range → DQ.
  - MUTEX: `LATEST ∩ SEEN` (block trên `ADD(·,0)`).
  - NOT_MUTEX: `POS ∩ SIG_window` (block trên `SIG(·,t)`).
  - STATE: `STATE(r,t)`.
  - PARTIAL_VALUE: AGG block → `pv_range_value` → tag `valueRange` → `tag_range_bitmap`.
  - PARTIAL_VALUE_BY_TAG: AGG block theo tag → `pv_range_value`.
  - EXTENDED: range chỉ cho tag trong `meta.condition_usage` (usage-driven).
- [ ] Incremental IN_MONTH / LAST_MONTH / ALWAYS_ACTIVE.
- [ ] Custom range on-demand (API nội bộ cho builder/manager) + cache.
- [ ] DQ: monotonic window, mutex rời nhau theo window, STATE consistency.
- [ ] Airflow: `vision_gold_temporal`, `vision_dq`, `vision_late_data`, `vision_backfill`, `vision_maintenance`.

**Done khi**
- `tag_range_bitmap` / `pv_range_value` == reference (P1/P1b) cho golden + dataset ngẫu nhiên, **từng loại × từng date range**, mọi `aggFunc`, STANDARD + EXTENDED.
- Late data 3 ngày và backfill cho kết quả == chạy lại từ đầu.
- Chạy 30 ngày liên tiếp trên local không lỗi.

---

## P5 — Segment

> **Tóm tắt:** P5 build segment từ các condition. Sau P5 có: bitmap segment trong StarRocks `seg.segment_bitmap`; file `.roar` + manifest trên S3 `s3://vision-segments/ds=…/`; định nghĩa + version trong Postgres `meta.segment`, `meta.segment_version`, `meta.condition_usage`; event publish trên Kafka `vision.segment.published.v1`; API quản lý segment (segment-manager).
>
> 📄 Chi tiết flow dữ liệu (input → các bước → output, công nghệ từng bước): [`phases/p5-segment.md`](phases/p5-segment.md)

**Mục tiêu**: 5K segment/ngày build, version và publish được.

**Việc cần làm**
- [ ] Bitmap codec Go + Kotlin, golden bytes lấy từ StarRocks 3.5 thật.
- [ ] `segment-manager` (Kotlin/Vert.x/Dagger2): CRUD, validate DSL theo `dataType` (matrix P1), `estimate` (SQL `bitmap_count`), trigger rebuild, lịch sử version.
- [ ] `segment-builder` (Go):
  - topo-sort segment tham chiếu segment;
  - condition cache `(ds, attr, tags, tagOp, dateRange|custom, valueRange)`;
  - condition EXTENDED chưa materialize → on-demand + ghi `meta.condition_usage`;
  - MUTEX/NOT_MUTEX/PARTIAL_VALUE (tag định sẵn) → lấy bitmap; PARTIAL_VALUE ad-hoc & PARTIAL_VALUE_BY_TAG → query `pv_range_value`/`pv_block`;
  - evaluate AND/OR/SUB → S3 `.roar` + manifest → `seg.segment_bitmap` → Postgres `PUBLISHED` → Kafka `vision.segment.published.v1`;
  - retry idempotent, batch fetch giới hạn theo byte.
- [ ] Airflow `vision_segment_rebuild` (deferrable sensor).

**Done khi**
- `seg_1001 = {3}`, `seg_1002 = {1,2}`; mỗi loại dữ liệu có ít nhất 1 segment test (kể cả custom range và ad-hoc `valueRange`).
- Kết quả builder == evaluator thuần P1.
- Rebuild cùng ngày không tạo version trùng; lỗi giữa chừng không publish nửa vời.

---

## P6 — Activation API

> **Tóm tắt:** P6 phục vụ segment cho hệ thống khác. Sau P6 có: `activation-api` trả count / danh sách user / segment của user / contains với độ trễ ms (mmap `.roar`, cache `user_id ↔ uidx` trên Redis); export segment lớn ra file trên S3. Không tạo bảng dữ liệu mới.
>
> 📄 Chi tiết flow dữ liệu (input → các bước → output, công nghệ từng bước): [`phases/p6-activation-api.md`](phases/p6-activation-api.md)

**Mục tiêu**: phục vụ segment cho hệ thống khác.

**Việc cần làm**
- [ ] `activation-api` (Kotlin/Vert.x/Dagger2):
  - `GET /v1/segments/{id}/count`
  - `GET /v1/segments/{id}/users?cursor=&limit=` (cursor gắn version)
  - `GET /v1/users/{userId}/segments`
  - `GET /v1/segments/{id}/contains/{userId}`, `POST /v1/segments/contains`
  - `POST /v1/segments/{id}/exports` (async, `unnest_bitmap` + `INSERT INTO FILES`)
- [ ] Tải + mmap `.roar` (ONLINE), hot-swap theo Kafka event; fallback StarRocks cho OFFLINE.
- [ ] `user_id ↔ uidx`: Caffeine → Redis → StarRocks.
- [ ] Integration test với `.roar` nhỏ; load test local.

**Done khi**
- Response khớp mục L7 trong `data-flow-examples.md`.
- Publish version mới trong lúc đang phân trang không làm lệch kết quả.
- Local: `contains`/`count` p99 < 10ms, `segments by user` p99 < 20ms (với 5K segment giả).

---

## P7 — Observability

> **Tóm tắt:** P7 giúp biết hệ thống chạy đúng hạn, đúng dữ liệu. Sau P7 có: metrics trong Prometheus, dashboard Grafana (SLA pipeline, chi phí theo loại dữ liệu, DQ, segment build, API), alert; số liệu theo tag/segment trong StarRocks `dq.result`, `seg.build_stats`.
>
> 📄 Chi tiết flow dữ liệu (input → các bước → output, công nghệ từng bước): [`phases/p7-observability.md`](phases/p7-observability.md)

**Mục tiêu**: biết hệ thống có đúng hạn, đúng dữ liệu không.

**Việc cần làm**
- [ ] Metrics chuẩn `vision_<component>_<what>_<unit>` cho mọi service/job (label có `data_type`, không label cardinality cao).
- [ ] StatsD exporter (Airflow), Pushgateway (Spark/batch), scrape StarRocks/Flink/Kafka.
- [ ] Grafana dashboard: pipeline SLA, temporal cost **theo data_type**, DQ, segment build, activation API, StarRocks.
- [ ] Grafana đọc `dq.result`, `seg.build_stats` (MySQL datasource) cho số liệu theo tag/segment.
- [ ] Alert: `VisionRangeNotReady`, `VisionDQBlocked`, `VisionSegmentBuildFailureRatio`, `VisionSegmentPublishLate`, `VisionApiP99High`, `VisionCollectorKafkaErrors`.

**Done khi**
- Cố ý làm hỏng 1 DQ / chậm 1 DAG / lỗi API → alert tương ứng bắn.
- Dashboard nhìn được chi phí và độ trễ tách theo 4 loại dữ liệu.

---

## P8 — Scale & hardening

> **Tóm tắt:** P8 đưa hệ thống lên quy mô thiết kế. Sau P8 có: số đo thật ở 100M user / 500 attribute / 5K segment trong `capacity.md`; cấu hình StarRocks đã tune; quyết định tối ưu PARTIAL_VALUE và EXTENDED; runbook vận hành (backfill, rollback, late data, đổi định nghĩa tag).
>
> 📄 Chi tiết flow dữ liệu (input → các bước → output, công nghệ từng bước): [`phases/p8-scale-hardening.md`](phases/p8-scale-hardening.md)

**Mục tiêu**: đạt quy mô thiết kế và vận hành được.

**Việc cần làm**
- [ ] Synthetic data: 100M user, 500 attribute (phân bổ 4 loại theo `capacity.md`), 500–1000 tag/attr (STANDARD) + vài attribute EXTENDED hàng trăm nghìn tag, 400 ngày, 5K segment.
- [ ] Benchmark từng bước L3→L6; đo storage thực tế.
- [ ] Quyết định tối ưu PARTIAL_VALUE / PARTIAL_VALUE_BY_TAG: chỉ `supportedDateRanges` → rolling incremental → BSI (chỉ khi cần).
- [ ] Tune StarRocks: bucket, partition, tablet, spill, resource group.
- [ ] Runbook: backfill, rollback version segment, reprocess late data, thay đổi định nghĩa tag.
- [ ] Cập nhật `capacity.md` bằng số đo thật.

**Done khi**
- Đạt SLA: range xong 05:00, **5K segment published 07:00**.
- Đạt SLO API ở tải mục tiêu.
- Không loại dữ liệu nào vượt ngân sách thời gian riêng của nó.
