# Checklist tổng hợp

Nguồn duy nhất để theo dõi tiến độ. Mỗi bước = 1 phase. Thiết kế + flow dữ liệu từng bước: `phases/step-NN-*.md`; tổng quan: `phases.md`.

Quy ước:
- Mỗi dòng `- [ ]` là **1 task** = 1 commit. Làm theo thứ tự từ trên xuống.
- Dòng `Done khi:` là tiêu chí nghiệm thu phase; tick khi đã kiểm chứng. Tất cả task + Done của phase xong → **push**.
- **Phạm vi hiện tại: 1 nguồn (S2a `user_profile`, `MUTEX` `STATE`).** Các bước từ 3 trở đi chỉ cần chạy đúng với nguồn/loại này; thêm nguồn và loại khác ở các bước 11–16 cuối file.
- Đích cuối: đủ 4 loại (MUTEX · NOT_MUTEX · PARTIAL_VALUE · PARTIAL_VALUE_BY_TAG).
- Skill `/execute` đọc file này để chọn task tiếp theo.

## Bước 0 — Nền móng  ([chi tiết](phases/step-00-foundation.md))
> Mục tiêu: có chỗ để viết code và chạy thử. Chưa có gì về nghiệp vụ.
- [x] Bazel 8 lõi: `.bazelversion`, `.bazelrc`, `MODULE.bazel`, `BUILD.bazel` + 1 `sh_test` mẫu (chưa cần ngôn ngữ nào)
- [x] File `com/tm/docker/vision/docker-compose.yml` rỗng khung; **service nào cần thì bước đó mới thêm** (service nào thêm ở bước nào: `phases/step-00-foundation.md`)
- [x] Done khi: `bazel test //...` xanh với `sh_test` mẫu.

## Bước 1 — Bài toán + ví dụ (L0)  ([chi tiết](phases/step-01-problem-examples.md))
> Mục tiêu: chốt bài toán trước khi đụng công nghệ. Chỉ viết tài liệu, chưa có code, chưa tạo attribute.
- [x] Bài toán kỹ thuật: phân khúc user để làm gì, vào/ra của hệ thống, ràng buộc (quy mô, SLA)
- [x] Ví dụ cụ thể: vài segment thực tế, đi qua dữ liệu thật từng bước (dùng `data-flow-examples.md` làm khung)
- [x] Các nguồn dữ liệu (payment, profile, churn score…): ai sinh ra, dạng event hay snapshot, ca biên (trùng, đến muộn, xoá/đổi giá trị)
- [x] Done khi: bạn đọc xong và đồng ý đó là bài toán cần giải.

## Bước 2 — Seed OLTP (L0)  ([chi tiết](phases/step-02-seed-oltp.md))
> Mục tiêu: có DB nguồn giống hệ thống thật, chứa data mẫu.
- [x] Thêm Postgres OLTP vào compose
- [x] Python trên Bazel: rules_python + `pip.parse`, macro `com_tm_py_image` (`tools/rules/com_tm_container.bzl`), 1 test mẫu xanh
- [x] Schema OLTP cho từng nguồn (bảng, cột, kiểu, khoá, cột thời gian) theo bài toán ở bước 1
- [x] Seed data khớp ví dụ bước 1, gồm cả ca biên (trùng, đến muộn, xoá/đổi giá trị)
- [x] Script seed chạy lại được; có cách sinh thêm data để test lớn hơn
- [x] Generator live (Go + image OCI, `com/tm/src/ingest/oltp/generator`): mỗi giây 1 thao tác lên `user_profile`, bật bằng profile `live` của compose
- [x] Done khi: query OLTP ra đúng data của ví dụ ở bước 1.

## Bước 3 — Bronze (L1, OLAP)  ([chi tiết](phases/step-03-bronze.md))
> Mục tiêu: data từ OLTP đi vào Iceberg bronze, chưa làm sạch. Ghi OLTP trước, ingest sang OLAP (không ghi song song).
- [x] Thêm Kafka, Debezium, Flink, MinIO + Iceberg REST vào compose
- [x] CDC: Debezium đọc log OLTP → Kafka → Flink Java (DataStream) → `bronze.user_profile_cdc_raw` (Iceberg), giữ bản ghi gốc + thời điểm thay đổi + loại thao tác
- [x] Bảng trạng thái `user_profile` qua CDC (nguồn duy nhất giai đoạn này; payment, product, churn score… thêm ở bước 11–16)
- [x] Insert/update/delete ở OLTP sau đó đều xuất hiện trong bronze (`com/tm/src/ingest/cdc/verify_bronze.sh`)
- [x] Done khi: số bản ghi và nội dung bronze khớp OLTP, kể cả sau khi sửa/xoá. (`com/tm/src/ingest/cdc/verify_bronze_matches_oltp.sh`)

## Bước 4 — Silver (L2)  ([chi tiết](phases/step-04-silver.md))
> Mục tiêu: dữ liệu sạch, user có `uidx`.
- [x] Spark (PySpark local, 1 container chạy theo lô): parse bronze CDC `user_profile`, dedup theo vị trí Kafka `(partition, offset)`, tính `ds` theo ICT, dòng lỗi vào DLQ → `silver.user_profile_cdc_events` + `silver.user_profile_cdc_dlq` (dedup `event_id` làm khi có nguồn event, bước 11)
- [x] CDC → SCD2 (profile): `silver.user_profile_cdc_events` → `silver.user_profile_scd2` theo ngày (product thêm ở bước 14)
- [x] Dictionary `user_id → uidx` (chỉ append, không tái sử dụng)
- [x] Done khi: chạy lại cùng `ds` ra cùng kết quả (idempotent); khớp `data-flow-examples.md`. (`user_profile_silver_golden_test.py`, `verify_silver_idempotent.sh`)

## Bước 5 — Daily (L3)  ([chi tiết](phases/step-05-daily.md))
> Mục tiêu: từ nhu cầu nghiệp vụ, khai báo attribute rồi rút gọn event thành trạng thái theo ngày.
- [ ] Nhu cầu tạo attribute: mỗi attribute trả lời câu hỏi nghiệp vụ nào, chọn `dataType` / `feedMode` / `aggFunc` ra sao (hướng dẫn chọn: `data-types.md`)
- [ ] Catalog Postgres `meta.*` + proto `catalog`; seed attribute theo nhu cầu vừa chốt (hiện chỉ `user_city` MUTEX `STATE`)
- [ ] `reference.py`: cách tính ngây thơ đúng §3.2, làm chuẩn so sánh cho các bước sau
- [ ] SQL `tag_daily` (`ADD/DEL`) và `pv_daily` (`SUM/COUNT/MIN/MAX`); `tag_dict` cho EXTENDED
- [ ] Ghi idempotent theo `(ds, attr_id)`
- [ ] Done khi: kết quả SQL khớp `reference.py` trên golden của `user_city` (§2.1); nhánh chưa làm báo lỗi tường minh.

## Bước 6 — Temporal (L4)  ([chi tiết](phases/step-06-temporal.md))
> Mục tiêu: gộp nhiều ngày mà không quét lại event.
- [ ] Dyadic block (`tag_block`, `pv_block`)
- [ ] `LATEST` (MUTEX), `POS` (NOT_MUTEX), `STATE` + checkpoint tuần
- [ ] Done khi: property test (hypothesis) — SQL == `reference.py` với ≤ 400 ngày, có REMOVE, late data, cho nhánh `STATE` (EVENT và các loại khác khi mở rộng).

## Bước 7 — Range (L5)  ([chi tiết](phases/step-07-range.md))
> Mục tiêu: có sẵn kết quả cho mọi date range (A1…A180, IN_MONTH, LAST_MONTH, ALWAYS_ACTIVE).
- [ ] `tag_range_bitmap` (nhãn và bucket)
- [ ] `pv_range_value` (số, lọc `valueRange` lúc build)
- [ ] EXTENDED: chỉ tính tag đang được segment dùng (usage-driven)
- [ ] Custom date range: tính on-demand + cache
- [ ] Done khi: mọi range của `user_city` khớp `reference.py`; các tag MUTEX rời nhau trong mỗi window.

## Bước 8 — Segment (L6)  ([chi tiết](phases/step-08-segment.md))
> Mục tiêu: từ DSL ra bitmap segment.
- [ ] Kotlin trên Bazel: rules_kotlin + `maven_install.json` + Dagger; macro `com_tm_kt_image`; 1 test mẫu xanh (Go đã dựng ở bước 2)
- [ ] Proto `segment` (DSL)
- [ ] Bitmap codec (Go + Kotlin), golden bytes lấy từ StarRocks thật
- [ ] `segment-manager` (Kotlin): CRUD, validate DSL, estimate
- [ ] `segment-builder` (Go): evaluate AND/OR/SUB, ghi S3 + Postgres, bắn Kafka
- [ ] Done khi: `seg_0001 = {1,3}` (theo golden §0.1) và build lại cùng version ra cùng kết quả.

## Bước 9 — Activation (L7)  ([chi tiết](phases/step-09-activation.md))
> Mục tiêu: tra cứu segment nhanh.
- [ ] `count`, `contains`, `users by segment`, `segments by user`
- [ ] Segment ONLINE: mmap `.roar`, hot-swap khi có version mới; OFFLINE: fallback StarRocks
- [ ] Done khi: `contains`/`count` p99 < 10ms trên dữ liệu local.

## Bước 10 — Vận hành  ([chi tiết](phases/step-10-operations.md))
- [ ] DQ: các invariant ở `CLAUDE.md` §9, fail BLOCK thì không publish
- [ ] Airflow: DAG theo từng layer + late data + backfill
- [ ] Metrics, dashboard, alert (không dùng label cardinality cao)
- [ ] Scale 100M user / 500 attribute / 5K segment; runbook
- [ ] Done khi: 5K segment publish trước 07:00 ICT trên dữ liệu synthetic.

## Bước 11 — MUTEX EVENT — S1 payment (`last_txn_category`)  ([chi tiết](phases/step-11-payment-mutex.md))
> Mục tiêu: `last_txn_category` (`MUTEX`, `EVENT`, `STANDARD`): ngành hàng của giao dịch `SUCCESS` gần nhất; chỉ xử lý thông tin MUTEX (`mcc`, `event_ts`, `status`), chưa dùng `amount` chạy qua mọi layer.
- [ ] Thêm `src.payment_event` vào OLTP (schema đủ cột, seed khớp §1 đủ 6 giao dịch, generator; bản cũ ở git `84ca0a2`) và CDC (connector, `kafka-init`, `TOPIC_TO_TABLE`); 2 sink bronze chạy ổn định (không restart)
- [ ] Silver `payment_txn` giữ **đủ cột** (`mcc`, `amount` DECIMAL(27,6), `status`) để các bước sau không phải ingest lại: dedup `event_id`, `ds` theo ICT, `uidx`, thứ tự `(event_ts, event_id)`; đến muộn tính lại `ds` cũ (≤ 3 ngày tự reprocess)
- [ ] Catalog `last_txn_category` (`MUTEX`, `EVENT`; tag rule `mcc → fnb/travel/bill`, chỉ `SUCCESS`); `reference.py` nhánh MUTEX `EVENT`. Dùng `amount`/NOT_MUTEX/PARTIAL_VALUE trên payment → lỗi tường minh (làm ở bước 12, 13)
- [ ] Daily: `ADD(d,t)` = ADD cuối ngày, `DEL(d,t)`, `ADD(d,0)`
- [ ] Temporal: block trên `ADD(·,0)` (theo attribute), `LATEST`; property test vs `reference.py` (payment không sinh REMOVE nên ca REMOVE dùng dữ liệu tổng hợp trong test)
- [ ] Range: `LATEST ∩ SEEN` → `tag_range_bitmap`; DQ `Σ cnt(ADD(d,t)) == cnt(ADD(d,0))`, tag rời nhau mỗi ngày và mỗi window
- [ ] Segment `seg_0002` (DSL SUB giữa `user_city` và `last_txn_category`) chạy qua builder và API
- [ ] Done khi: golden §1.0 khớp ở mọi layer; `seg_0002 = {3}`; property test xanh; ca `e-9001` (trùng), `e-9003` (ICT), `e-9004` (FAILED), `e-9005` (đến muộn) đúng.

## Bước 12 — NOT_MUTEX EVENT — S1 payment (`txn_category`)  ([chi tiết](phases/step-12-not-mutex.md))
> Mục tiêu: `txn_category` (`NOT_MUTEX`, `EVENT`, `STANDARD`; tag = ngành hàng theo MCC) chạy qua mọi layer.
- [ ] Catalog `txn_category` (`NOT_MUTEX`, `EVENT`); `reference.py` nhánh NOT_MUTEX `EVENT`
- [ ] Daily: `ADD/DEL/SIG` theo từng tag (một user nhiều tag cùng ngày)
- [ ] Temporal: block trên `SIG(·,t)`, `POS`; property test vs `reference.py` (có REMOVE, late data)
- [ ] Range: `POS ∩ SIG_window` → `tag_range_bitmap`; DSL cho `tagOp=AND`; segment-builder và API chạy được trên `txn_category`
- [ ] Done khi: golden `txn_category` (§1) khớp ở mọi layer; property test xanh; bước 11 không hồi quy.

## Bước 13 — PARTIAL_VALUE & PARTIAL_VALUE_BY_TAG + `aggFunc`  ([chi tiết](phases/step-13-partial-value.md))
> Mục tiêu: `txn_amount` (`PARTIAL_VALUE`) và `txn_amount_by_category` (`PARTIAL_VALUE_BY_TAG`), `aggFunc` `SUM`/`COUNT`/`MIN`/`MAX` (`STANDARD`) chạy qua mọi layer.
- [ ] Catalog `txn_amount`, `txn_amount_by_category` + `aggFunc` (+ lỗi tường minh cho `AVG`, `DISTINCT_COUNT`, `FIRST`/`LAST`); `reference.py` nhánh PV/BY_TAG mọi `aggFunc`
- [ ] Daily `pv_daily`: AGG theo `(ds, uidx)` và `(ds, tag_id, uidx)`; chỉ `SUCCESS`, không làm tròn
- [ ] Temporal `pv_block`: ghép block bằng ⊕ theo `aggFunc`; property test vs `reference.py`
- [ ] Range `pv_range_value`; bucket định sẵn → `tag_range_bitmap`; user không có event thì không thuộc `valueRange` nào (kể cả chứa 0)
- [ ] DSL: validate matrix `valueRange` (PV: `tags` hoặc `valueRange`; BY_TAG: bắt buộc cả hai; cấm trên MUTEX/NOT_MUTEX); builder query `pv_range_value`/`pv_block` (custom range) + condition cache; `tagOp` OR/AND cho BY_TAG
- [ ] Done khi: golden `txn_amount`, `txn_amount_by_category`, §7.1 (`aggFunc`) khớp; `seg_1002 = {1,2}`; bước 11–12 không hồi quy.

## Bước 14 — NOT_MUTEX STATE — S2b product holding  ([chi tiết](phases/step-14-product-state.md))
> Mục tiêu: `product_holding` (`NOT_MUTEX`, `STATE`) chạy qua mọi layer.
- [ ] Thêm `src.user_product` vào OLTP (schema, seed khớp §2.2, generator) và CDC (connector, topic, `TOPIC_TO_TABLE`)
- [ ] Silver `user_product_scd2` (mở sản phẩm = INSERT, đóng = DELETE)
- [ ] Catalog `product_holding` (`NOT_MUTEX`, `STATE`); `reference.py` nhánh NOT_MUTEX `STATE`
- [ ] Daily/temporal: `STATE(d,t)` theo từng tag (nhiều tag/user; DELETE chỉ gỡ tag đó) + checkpoint tuần; range = `STATE(ds)`
- [ ] Done khi: golden `product_holding` (§2.2) khớp, kể cả U1001 đóng `paylater` vẫn giữ `insurance`; property test xanh.

## Bước 15 — EXTENDED — S4/S5 (voucher, OA, app event)  ([chi tiết](phases/step-15-extended.md))
> Mục tiêu: attribute `EXTENDED` (tag là chuỗi tự do): `PARTIAL_VALUE_BY_TAG` (giá trị quà theo mã, số lần theo `event_name`, `COUNT`), `NOT_MUTEX` (follow OA, REMOVE khi unfollow) chạy qua mọi layer.
- [ ] Thêm 3 bảng `voucher_grant`, `oa_follow`, `app_event` vào OLTP (schema, seed, generator) và CDC; xác nhận bronze ổn định với 4+ sink
- [ ] Silver `tag_dict (attr_id, tag_string) → tag_id` append-only, không tái sử dụng; DQ `tag_dict` không đổi/xoá mapping cũ
- [ ] Catalog `attributeType=EXTENDED` (chỉ NOT_MUTEX, PARTIAL_VALUE_BY_TAG; MUTEX `EXTENDED` báo lỗi tường minh); `reference.py` nhánh EXTENDED
- [ ] Daily/temporal: chỉ tag có hoạt động; POS chỉ cập nhật tag có signal trong ngày
- [ ] Range usage-driven: `meta.condition_usage`, tag chưa có trong `tag_dict` → bitmap rỗng (không lỗi), tag dùng lần đầu on-demand + cache
- [ ] DSL: tag `EXTENDED` là chuỗi tự do, không kiểm catalog
- [ ] Done khi: golden §7.2, §7.3 khớp; `seg_1003 = {1}`; đủ 4 loại và cả `STANDARD`/`EXTENDED` chạy đúng ở mọi layer.

## Bước 16 — MUTEX EVENT từ file ML — S3 churn score  ([chi tiết](phases/step-16-churn-file.md))
> Mục tiêu: `churn_score_band` (`MUTEX`, `EVENT`, `STANDARD`; band low/mid/high), có REMOVE chạy qua mọi layer.
- [ ] Thêm file mẫu churn score (`churn_score_examples.csv`, bản cũ ở git `84ca0a2`); PySpark file loader có sensor `_SUCCESS` → `bronze.churn_score_raw`; chạy lại file `dt=<ds>` idempotent
- [ ] Silver `churn_score` (map `score → band`), model chỉ chấm một phần user mỗi ngày
- [ ] Catalog `churn_score_band` (`MUTEX`, `EVENT`); dùng lại đường MUTEX `EVENT` của bước 11, thêm ca REMOVE (golden §4)
- [ ] Done khi: golden §3, §4 (REMOVE, X/Y/Z) khớp; `seg_1001 = {3}` (churn + city + payment `txn_category`).
