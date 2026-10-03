# Bước 8 — Segment (L6)

| | Nội dung |
|---|---|
| **Input** | range bitmap/value; DSL segment |
| **Output** | `.roar` trên S3 + manifest; `seg.segment_bitmap`; `meta.segment_version`; event Kafka `vision.segment.published.v1` |

```
DSL (AND/OR/SUB) ──validate theo catalog──▶ segment-manager (Kotlin)
build_run ──▶ segment-builder (Go): topo-sort ▶ condition cache ▶ lấy bitmap ▶ evaluate ▶ publish
```

- Go (rules_go, gazelle, macro `com_tm_go_image`) đã dựng từ bước 2; dựng Kotlin (rules_kotlin, Dagger) + macro `com_tm_kt_image` trước khi viết code.
- Bitmap codec Go + Kotlin, golden bytes lấy từ StarRocks thật.
- Validate DSL theo `dataType` (cấm `tagOp=AND` trên MUTEX, bắt buộc `valueRange` với BY_TAG…).
- Retry idempotent theo `(segment_id, version)`.
- Done: `seg_0001 = {1,3}` theo golden (sau mở rộng: `seg_1001 = {3}`, `seg_1002 = {1,2}`).
- Hiện chỉ DSL/condition trên `user_city` (+ `AND/OR/SUB` giữa các tag của nó); validate matrix mở rộng dần theo loại.

Checklist: xem mục "Bước 8" trong `../checklist.md`.
