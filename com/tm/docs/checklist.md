# Checklist tổng hợp

Nguồn duy nhất để theo dõi tiến độ. Mỗi bước = 1 phase. Thiết kế + flow dữ liệu từng bước: `phases/step-NN-*.md`; tổng quan: `phases.md`.

Quy ước:
- Mỗi dòng `- [ ]` là **1 task** = 1 commit. Làm theo thứ tự từ trên xuống.
- Dòng `Done khi:` là tiêu chí nghiệm thu phase; tick khi đã kiểm chứng. Tất cả task + Done của phase xong → **push**.
- Mọi bước đụng dữ liệu chỉ done khi chạy đúng cả 4 loại (MUTEX · NOT_MUTEX · PARTIAL_VALUE · PARTIAL_VALUE_BY_TAG).
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
- [ ] Seed data khớp ví dụ bước 1, gồm cả ca biên (trùng, đến muộn, xoá/đổi giá trị)
- [ ] Script seed chạy lại được; có cách sinh thêm data để test lớn hơn
- [ ] Done khi: query OLTP ra đúng data của ví dụ ở bước 1.

## Bước 3 — Bronze (L1, OLAP)  ([chi tiết](phases/step-03-bronze.md))
> Mục tiêu: data từ OLTP đi vào Iceberg bronze, chưa làm sạch. Ghi OLTP trước, ingest sang OLAP (không ghi song song).
- [ ] Thêm Kafka, Debezium, Flink, MinIO + Iceberg REST vào compose
- [ ] CDC: Debezium đọc log OLTP → Kafka → Flink SQL → `bronze.*_raw` (Iceberg), giữ bản ghi gốc + thời điểm thay đổi + loại thao tác
- [ ] Bảng event (payment) và bảng trạng thái (profile, product) đều qua CDC; churn score (file) qua file loader
- [ ] Insert/update/delete ở OLTP sau đó đều xuất hiện trong bronze
- [ ] Done khi: số bản ghi và nội dung bronze khớp OLTP, kể cả sau khi sửa/xoá.

## Bước 4 — Silver (L2)  ([chi tiết](phases/step-04-silver.md))
> Mục tiêu: dữ liệu sạch, user có `uidx`.
- [ ] Spark: dedup `event_id`, tính `ds` theo ICT, dòng lỗi vào DLQ
- [ ] CDC → SCD2 (profile, product)
- [ ] Dictionary `user_id → uidx` (chỉ append, không tái sử dụng)
- [ ] Done khi: chạy lại cùng `ds` ra cùng kết quả (idempotent); khớp `data-flow-examples.md`.

## Bước 5 — Daily (L3)  ([chi tiết](phases/step-05-daily.md))
> Mục tiêu: từ nhu cầu nghiệp vụ, khai báo attribute rồi rút gọn event thành trạng thái theo ngày.
- [ ] Nhu cầu tạo attribute: mỗi attribute trả lời câu hỏi nghiệp vụ nào, chọn `dataType` / `feedMode` / `aggFunc` ra sao (hướng dẫn chọn: `data-types.md`)
- [ ] Catalog Postgres `meta.*` + proto `catalog`; seed attribute theo nhu cầu vừa chốt (đủ 4 loại)
- [ ] `reference.py`: cách tính ngây thơ đúng §3.2, làm chuẩn so sánh cho các bước sau
- [ ] SQL `tag_daily` (`ADD/DEL`) và `pv_daily` (`SUM/COUNT/MIN/MAX`); `tag_dict` cho EXTENDED
- [ ] Ghi idempotent theo `(ds, attr_id)`
- [ ] Done khi: kết quả SQL khớp `reference.py` trên golden, cả 4 loại.

## Bước 6 — Temporal (L4)  ([chi tiết](phases/step-06-temporal.md))
> Mục tiêu: gộp nhiều ngày mà không quét lại event.
- [ ] Dyadic block (`tag_block`, `pv_block`)
- [ ] `LATEST` (MUTEX), `POS` (NOT_MUTEX), `STATE` + checkpoint tuần
- [ ] Done khi: property test (hypothesis) — SQL == `reference.py` với ≤ 400 ngày, có REMOVE, late data, EVENT và STATE.

## Bước 7 — Range (L5)  ([chi tiết](phases/step-07-range.md))
> Mục tiêu: có sẵn kết quả cho mọi date range (A1…A180, IN_MONTH, LAST_MONTH, ALWAYS_ACTIVE).
- [ ] `tag_range_bitmap` (nhãn và bucket)
- [ ] `pv_range_value` (số, lọc `valueRange` lúc build)
- [ ] EXTENDED: chỉ tính tag đang được segment dùng (usage-driven)
- [ ] Custom date range: tính on-demand + cache
- [ ] Done khi: mọi range khớp `reference.py`; MUTEX rời nhau trong mỗi window.

## Bước 8 — Segment (L6)  ([chi tiết](phases/step-08-segment.md))
> Mục tiêu: từ DSL ra bitmap segment.
- [ ] Go + Kotlin trên Bazel: rules_go + gazelle + `go.mod`; rules_kotlin + `maven_install.json` + Dagger; macro `com_tm_go_image`, `com_tm_kt_image`; mỗi ngôn ngữ 1 test mẫu xanh
- [ ] Proto `segment` (DSL)
- [ ] Bitmap codec (Go + Kotlin), golden bytes lấy từ StarRocks thật
- [ ] `segment-manager` (Kotlin): CRUD, validate DSL, estimate
- [ ] `segment-builder` (Go): evaluate AND/OR/SUB, ghi S3 + Postgres, bắn Kafka
- [ ] Done khi: `seg_1001 = {3}`, `seg_1002 = {1,2}` (theo golden) và build lại cùng version ra cùng kết quả.

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
