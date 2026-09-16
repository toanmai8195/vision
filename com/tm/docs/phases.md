# Kế hoạch triển khai theo phase

> Thiết kế gốc: `CLAUDE.md`. Semantics 4 loại dữ liệu: `com/tm/docs/data-types.md`. Số liệu golden: `com/tm/docs/data-flow-examples.md`.
>
> Nguyên tắc chung cho mọi phase:
> - Kết thúc phase = **Done khi** đạt hết + `bazel build //... && bazel test //...` xanh + commit.
> - Phase nào đụng dữ liệu phải chạy đúng **cả 4 loại**: `MUTEX` · `NOT_MUTEX` · `PARTIAL_VALUE` · `PARTIAL_VALUE_BY_TAG` (MUTEX/NOT_MUTEX: cả `EVENT` và `STATE`, có REMOVE).
> - Đổi semantics trong lúc làm → cập nhật `CLAUDE.md` + docs + golden test trước.
> - Service mới phải có `/metrics` + log có cấu trúc ngay từ đầu (không đợi P7).

## Tổng quan

```
P0 Foundation ──┬──▶ P2 Ingestion & Silver ──▶ P3 Daily (L3) ──▶ P4 Temporal & Range (L4–L5) ──▶ P5 Segment ──▶ P6 Activation
                │                                                        ▲                                         │
                └──▶ P1 Semantics core (pure, không cần infra) ──────────┘                                         ▼
                                                                                                 P7 Observability ──▶ P8 Scale & hardening
```

| Phase | Tên | Phụ thuộc | Chạy song song được với |
|---|---|---|---|
| P0 | Foundation | — | — |
| P1 | Semantics core (reference + planner) | P0 | P2, P3 |
| P2 | Ingestion & Silver | P0 | P1 |
| P3 | Daily layer (L3) | P2 | P1 |
| P4 | Temporal & Range (L4–L5) | P1, P3 | — |
| P5 | Segment (manager + builder) | P4 | codec + DSL validate làm sớm từ P1 |
| P6 | Activation API | P5 (có thể bắt đầu với file `.roar` giả sau khi có codec) | P7 |
| P7 | Observability | P2 trở đi (làm dần) | P6 |
| P8 | Scale & hardening | P6, P7 | — |

---

## P0 — Foundation

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

**Mục tiêu**: khoá đúng semantics trước khi viết SQL. Không cần StarRocks/Spark.

**Việc cần làm**
- [ ] `com/tm/src/temporal/reference.py`: cách tính ngây thơ theo định nghĩa `CLAUDE.md` §3.2 cho 4 loại (duyệt event trong window, signal gần nhất, SUM), bitmap bằng `pyroaring`.
- [ ] `com/tm/src/temporal/model.py`: reduce ngày (§4.1) → `ADD/DEL/SIG`, `ADDED/REMOVED/STATE`, `pv_daily`.
- [ ] `blocks.py`: dyadic block build + greedy decomposition (§4.2).
- [ ] `latest.py`: `LATEST`, `POS`, `STATE`, checkpoint + forward-fold (§4.3).
- [ ] `ranges.py`: A1…A180, IN_MONTH, LAST_MONTH, ALWAYS_ACTIVE, CUSTOM cho 4 loại.
- [ ] `planner.py` (khung): `(ds, attribute)` → danh sách bước (chưa sinh SQL thật).
- [ ] Golden test từ `data-flow-examples.md` (`testdata/golden/*.yaml`).
- [ ] Property test (hypothesis): implementation tối ưu == reference; ≤ 400 ngày, REMOVE xen kẽ, late data, cả EVENT/STATE.
- [ ] Segment evaluator thuần (AND/OR/SUB, tagOp, valueRange) + DSL validate matrix.

**Done khi**
- Golden pass cho: churn & city (MUTEX), txn_category & product_holding (NOT_MUTEX), txn_amount (PARTIAL_VALUE), txn_amount_by_category (PARTIAL_VALUE_BY_TAG), ca biên REMOVE.
- Property test pass ≥ 10K case/loại.
- `seg_1001 = {3}`, `seg_1002 = {1,2}` tính bằng evaluator thuần.

---

## P2 — Ingestion & Silver (L0–L2)

**Mục tiêu**: 3 source chảy vào Iceberg silver, có `uidx`.

**Việc cần làm**
- [ ] Simulator (Python): S1 payment (Kafka), S2 profile + product CDC (Debezium format), S3 churn parquet; tham số `--users --days --attrs`, có duplicate, late data, REMOVE.
- [ ] `event-collector` (Go): HTTP/gRPC → validate proto → Kafka.
- [ ] Flink SQL: Kafka → `bronze.*_raw` (exactly-once).
- [ ] PySpark file loader S3 → bronze (sensor `_SUCCESS`).
- [ ] PySpark silver: `payment_txn` (dedup, `ds` ICT, DLQ), `user_profile_scd2` + `user_product_scd2` (MERGE CDC), `churn_score`.
- [ ] Dictionary `user_id → uidx` append-only + sync Redis + `meta.user_dict_rev`; `UNIVERSE(ds)`.
- [ ] Airflow DAG `vision_silver_<source>`.

**Done khi**
- Chạy simulator với dữ liệu của examples → silver khớp bảng L2 trong `data-flow-examples.md` (gồm dedup e-9001, e-9003 sang ds 09-15, e-9005 late).
- Rerun DAG cùng `ds` cho kết quả y hệt (idempotent).
- Silver giữ đủ thông tin cho 4 loại: thứ tự `(event_ts, event_id)`, `value` DECIMAL, `tag` cho BY_TAG, SCD2 cho STATE.

---

## P3 — Daily layer (L3)

**Mục tiêu**: bảng daily trong StarRocks cho 4 loại.

**Việc cần làm**
- [ ] DDL StarRocks (PK table, partition `ds`): `gold.tag_daily`, `gold.pv_daily`, `meta.*` mirror.
- [ ] SQL template `sql/starrocks/daily/<attr_group>.sql`, 1 scan / source table:
  - MUTEX EVENT: `ADD(d,t)` (ADD cuối ngày), `DEL(d,t)`, `ADD(d,0)`.
  - NOT_MUTEX EVENT: `ADD/DEL` theo signal cuối ngày từng tag, `SIG`.
  - MUTEX/NOT_MUTEX STATE: `ADDED/REMOVED` từ SCD2.
  - PARTIAL_VALUE: SUM `(ds, uidx)`; PARTIAL_VALUE_BY_TAG: SUM `(ds, tag, uidx)`.
- [ ] Ghi idempotent `DELETE (ds, attr_id)` + `INSERT`.
- [ ] DQ cơ bản (§9): mutex rời nhau, `ADD ∩ DEL = ∅`, tổng pv = tổng silver (cả theo tag), `uidx ⊆ UNIVERSE`.
- [ ] Airflow `vision_gold_daily` (dynamic task mapping theo `attrGroupId`).

**Done khi**
- Output L3 == `model.py` của P1 trên cùng input (golden + dataset ngẫu nhiên nhỏ) cho **cả 4 loại**.
- DQ pass; rerun không đổi kết quả.

---

## P4 — Temporal & Range (L4–L5)

**Mục tiêu**: mỗi ngày có đủ mọi date range cho mọi tag.

**Việc cần làm**
- [ ] DDL: `gold.tag_block`, `gold.pv_block`, `gold.tag_latest`, `gold.tag_state_checkpoint`, `gold.tag_range_bitmap`, `gold.pv_range_value`.
- [ ] Planner sinh SQL thật: block → LATEST/POS/STATE → checkpoint Chủ nhật → range → DQ.
  - MUTEX: `LATEST ∩ SEEN` (block trên `ADD(·,0)`).
  - NOT_MUTEX: `POS ∩ SIG_window` (block trên `SIG(·,t)`).
  - STATE: `STATE(r,t)`.
  - PARTIAL_VALUE: SUM block → `pv_range_value` → tag `valueRange` → `tag_range_bitmap`.
  - PARTIAL_VALUE_BY_TAG: SUM block theo tag → `pv_range_value`.
- [ ] Incremental IN_MONTH / LAST_MONTH / ALWAYS_ACTIVE.
- [ ] Custom range on-demand (API nội bộ cho builder/manager) + cache.
- [ ] DQ: monotonic window, mutex rời nhau theo window, STATE consistency.
- [ ] Airflow: `vision_gold_temporal`, `vision_dq`, `vision_late_data`, `vision_backfill`, `vision_maintenance`.

**Done khi**
- `tag_range_bitmap` / `pv_range_value` == reference (P1) cho golden + dataset ngẫu nhiên, **từng loại × từng date range**.
- Late data 3 ngày và backfill cho kết quả == chạy lại từ đầu.
- Chạy 30 ngày liên tiếp trên local không lỗi.

---

## P5 — Segment

**Mục tiêu**: 5K segment/ngày build, version và publish được.

**Việc cần làm**
- [ ] Bitmap codec Go + Kotlin, golden bytes lấy từ StarRocks 3.5 thật.
- [ ] `segment-manager` (Kotlin/Vert.x/Dagger2): CRUD, validate DSL theo `dataType` (matrix P1), `estimate` (SQL `bitmap_count`), trigger rebuild, lịch sử version.
- [ ] `segment-builder` (Go):
  - topo-sort segment tham chiếu segment;
  - condition cache `(ds, attr, tags, tagOp, dateRange|custom, valueRange)`;
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

**Mục tiêu**: đạt quy mô thiết kế và vận hành được.

**Việc cần làm**
- [ ] Synthetic data: 100M user, 500 attribute (phân bổ 4 loại theo `capacity.md`), 500–1000 tag/attr, 400 ngày, 5K segment.
- [ ] Benchmark từng bước L3→L6; đo storage thực tế.
- [ ] Quyết định tối ưu PARTIAL_VALUE / PARTIAL_VALUE_BY_TAG: chỉ `supportedDateRanges` → rolling incremental → BSI (chỉ khi cần).
- [ ] Tune StarRocks: bucket, partition, tablet, spill, resource group.
- [ ] Runbook: backfill, rollback version segment, reprocess late data, thay đổi định nghĩa tag.
- [ ] Cập nhật `capacity.md` bằng số đo thật.

**Done khi**
- Đạt SLA: range xong 05:00, **5K segment published 07:00**.
- Đạt SLO API ở tải mục tiêu.
- Không loại dữ liệu nào vượt ngân sách thời gian riêng của nó.
