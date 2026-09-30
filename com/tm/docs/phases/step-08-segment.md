# Bước 8 — Segment (L6)

| | Nội dung |
|---|---|
| **Input** | range bitmap/value; DSL segment |
| **Output** | `.roar` trên S3 + manifest; `seg.segment_bitmap`; `meta.segment_version`; event Kafka `vision.segment.published.v1` |

```
DSL (AND/OR/SUB) ──validate theo catalog──▶ segment-manager (Kotlin)
build_run ──▶ segment-builder (Go): topo-sort ▶ condition cache ▶ lấy bitmap ▶ evaluate ▶ publish
```

- Dựng Go (rules_go, gazelle) và Kotlin (rules_kotlin, Dagger) trên Bazel + macro `com_tm_go_image`, `com_tm_kt_image` trước khi viết code.
- Bitmap codec Go + Kotlin, golden bytes lấy từ StarRocks thật.
- Validate DSL theo `dataType` (cấm `tagOp=AND` trên MUTEX, bắt buộc `valueRange` với BY_TAG…).
- Retry idempotent theo `(segment_id, version)`.
- Done: `seg_1001 = {3}`, `seg_1002 = {1,2}` theo golden.

Checklist: xem mục "Bước 8" trong `../checklist.md`.
