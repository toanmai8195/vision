# P5 — Segment ⬜

> Quản lý định nghĩa segment và build 5K segment mỗi ngày từ kết quả range của P4; version hoá và publish.
> Thiết kế gốc: `CLAUDE.md` §5 (codec), §6 (segment-manager, segment-builder), §6.1 (DSL). Chuẩn đối chiếu: `segment/dsl/evaluator.py` (P1).

## Input / Output

| | Nội dung | Nơi lưu | Dạng |
|---|---|---|---|
| **Input** | Định nghĩa segment (DSL) do người dùng tạo qua segment-manager | Postgres `meta.segment` | proto `segment.v1.Segment` (JSON) |
| **Input** | `gold.tag_range_bitmap`, `gold.pv_range_value`, `gold.pv_block` (custom range) | StarRocks | BITMAP / DECIMAL |
| **Output** | Bitmap segment theo ngày | StarRocks `seg.segment_bitmap` (30 ngày) | BITMAP |
| **Output** | File segment | S3 `s3://vision-segments/ds=…/<seg>/v<n>.roar` + `v<n>.manifest.json` | Roaring portable + JSON |
| **Output** | Version + trạng thái build | Postgres `meta.segment_version`, `meta.build_run`, `meta.condition_usage` | bảng |
| **Output** | Thông báo publish | Kafka `vision.segment.published.v1` | JSON |

## Flow

```
Người dùng ──HTTP──▶ segment-manager (Kotlin/Vert.x): CRUD · validate DSL theo catalog · estimate
                          │  ghi meta.segment, meta.condition_usage
Airflow vision_segment_rebuild (sau DQ) ──tạo build_run──▶ segment-builder (Go)
   1. topo-sort (segment tham chiếu segment)
   2. mỗi condition → condition cache key (ds, attr, tags, tagOp, window, valueRange)
        lấy bitmap / query giá trị từ StarRocks (fetch theo batch giới hạn byte)
   3. evaluate AND / OR / SUB trong bộ nhớ (roaring)
   4. ghi S3 .roar + manifest → seg.segment_bitmap → Postgres PUBLISHED (transaction) → Kafka
```

## Các bước

### Bước 1 — Tạo / sửa segment: segment-manager
- Validate theo ma trận `CLAUDE.md` §6.1 (vd `tagOp=AND` trên MUTEX → lỗi; PARTIAL_VALUE_BY_TAG thiếu `valueRange` → lỗi).
- `estimate`: dịch DSL sang SQL `bitmap_count` trên StarRocks để trả số user ước lượng ngay.
- Ghi condition vào `meta.condition_usage` (EXTENDED cần để P4 precompute).

### Bước 2 — Lấy dữ liệu cho từng condition: segment-builder
| Condition | Lấy từ đâu | Biến đổi |
|---|---|---|
| MUTEX / NOT_MUTEX, date range chuẩn | `tag_range_bitmap` | bitmap theo tag, gộp theo `tagOp` |
| PARTIAL_VALUE, tag bucket | `tag_range_bitmap` | như trên |
| PARTIAL_VALUE ad-hoc `valueRange` | `pv_range_value` | lọc `value ∈ range` → bitmap |
| PARTIAL_VALUE_BY_TAG | `pv_range_value` theo tag | lọc từng tag rồi gộp OR/AND |
| custom range | `pv_block` / block + checkpoint | tính on-demand, cache |
| EXTENDED chưa precompute | on-demand như custom | ghi `condition_usage` cho ngày sau |

BITMAP đọc bằng `bitmap_to_base64` → bỏ 1 byte marker → Roaring (codec Go/Kotlin, golden bytes lấy từ StarRocks thật).

### Bước 3 — Evaluate
```
seg_1001 = SUB( AND(churn mid|high A30, city hn A7), txn fnb A7 )
         = ({1,2,3,4} ∩ {1,3}) − {1,2} = {3}
seg_1002 = OR( txn_amount A7 ≥ 1M, txn_amount_by_category fnb A7 ≥ 500K ) = {1} ∪ {2} = {1,2}
```
Condition giống nhau giữa các segment chỉ tính một lần (condition cache).

### Bước 4 — Publish
| Nơi | seg_1001 |
|---|---|
| S3 | `s3://vision-segments/ds=2026-09-15/seg_1001/v12.roar` + `v12.manifest.json` (count, ds, version) |
| StarRocks | `seg.segment_bitmap (seg_1001, v12, 2026-09-15, {3}, 1)` |
| Postgres | `meta.segment_version`: `BUILDING → PUBLISHED` (transaction) |
| Kafka | `{"segmentId":"seg_1001","version":12,"count":1,"uri":"s3://…/v12.roar"}` |

Retry idempotent theo `(segment_id, version)`; lỗi giữa chừng → không publish nửa vời.

## Công nghệ
Kotlin + Vert.x 5 + Dagger2 (segment-manager) · Go + RoaringBitmap v2 (segment-builder) · StarRocks · MinIO/S3 · Postgres · Kafka · Airflow (deferrable sensor).

## Kiểm tra
- `seg_1001 = {3}`, `seg_1002 = {1,2}`; mỗi loại dữ liệu có ≥ 1 segment test (kể cả custom range, ad-hoc `valueRange`, EXTENDED).
- Kết quả builder == evaluator thuần P1 trên cùng input. SLA 5K segment published 07:00.
