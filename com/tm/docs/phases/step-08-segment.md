# Bước 8 — Segment (L6)

| | Nội dung |
|---|---|
| **Input** | range bitmap/value; DSL segment |
| **Output** | `.roar` trên S3 + manifest; `seg.segment_bitmap`; `meta.segment_version`; event Kafka `vision.segment.published.v1` |

```
DSL (AND/OR/SUB) ──validate theo catalog──▶ segment-manager (Kotlin)
build_run ──▶ segment-builder (Go): topo-sort ▶ condition cache ▶ lấy bitmap ▶ evaluate ▶ publish
```

- Bitmap codec Go + Kotlin, golden bytes lấy từ StarRocks thật.
- Validate DSL theo `dataType` (cấm `tagOp=AND` trên MUTEX, bắt buộc `valueRange` với BY_TAG…).
- Retry idempotent theo `(segment_id, version)`.
- Done: `seg_1001 = {3}`, `seg_1002 = {1,2}` theo golden.

## Checklist
**Mục tiêu**: từ DSL ra bitmap segment.
- [ ] Proto `segment` (DSL)
- [ ] Bitmap codec (Go + Kotlin), golden bytes lấy từ StarRocks thật
- [ ] `segment-manager` (Kotlin): CRUD, validate DSL, estimate
- [ ] `segment-builder` (Go): evaluate AND/OR/SUB, ghi S3 + Postgres, bắn Kafka

**Done khi**: `seg_1001 = {3}`, `seg_1002 = {1,2}` (theo golden) và build lại cùng version ra cùng kết quả.
