# P6 — Activation API ⬜

> Cung cấp segment cho hệ thống khác (marketing, push, promotion) với độ trễ mili-giây.
> Thiết kế gốc: `CLAUDE.md` §6 (activation-api), §6.2, §10 (SLO).

## Input / Output

| | Nội dung | Nơi lưu | Dạng |
|---|---|---|---|
| **Input** | File segment đã publish | S3 `.roar` + manifest (P5) | Roaring portable |
| **Input** | Event publish | Kafka `vision.segment.published.v1` | JSON |
| **Input** | `user_id ↔ uidx` | Redis (P2), fallback StarRocks `meta.user_dict_rev` | key-value |
| **Input** | Segment `OFFLINE` | StarRocks `seg.segment_bitmap` | BITMAP |
| **Output** | Response HTTP (count, users, segments of user, contains) | — | JSON, luôn kèm `version`, `asOfDs` |
| **Output** | File export segment lớn | S3 (`INSERT INTO FILES`) | CSV / Parquet |

## Flow

```
Kafka segment.published ──▶ activation-api tải .roar từ S3 ──▶ mmap (ImmutableRoaringBitmap)
                                                              └─ hot-swap AtomicReference (version mới)
Client ──HTTP──▶ Router → Handler → Service
    user_id ──Caffeine → Redis → StarRocks──▶ uidx ──▶ contains / duyệt segment ONLINE
    segment OFFLINE ──▶ StarRocks bitmap_contains / bitmap_count
    export ──▶ StarRocks unnest_bitmap + INSERT INTO FILES ──▶ S3 (async, 202)
```

## Các bước

### Bước 1 — Nạp segment
- Nhận event publish → tải `.roar` → mmap. Giữ version active + 1 bản trước (để cursor đang phân trang không lệch).

### Bước 2 — Phục vụ request (ví dụ dữ liệu `data-flow-examples.md` §6)
| API | Biến đổi | Response |
|---|---|---|
| `GET /v1/segments/seg_1001/count` | đọc count từ manifest | `{"version":12,"asOfDs":"2026-09-15","count":1}` |
| `GET /v1/segments/seg_1002/users?limit=1000` | duyệt bitmap từ cursor `(version, last_uidx)` → `uidx → user_id` | `{"users":["U1001","U1002"],"nextCursor":null}` |
| `GET /v1/users/U1003/segments` | `U1003 → 3`; duyệt 5K segment ONLINE (`contains`, ~100–200ns/segment) | `{"segments":[{"segmentId":"seg_1001",…}]}` |
| `GET /v1/segments/seg_1001/contains/U1001` | `U1001 → 1`; `bitmap.contains(1)` | `{"contains":false,"version":12}` |
| `POST /v1/segments/contains` | ma trận users × segments | `{"results":[…]}` |
| `POST /v1/segments/seg_1002/exports` | job async trên StarRocks | `202 {"exportId":"exp_77","status":"RUNNING"}` |

### Bước 3 — Không log `user_id` ở INFO; metric không dùng label cardinality cao.

## Công nghệ
Kotlin + Vert.x 5 (coroutine, không block event loop) + Dagger2 · RoaringBitmap (`ImmutableRoaringBitmap`, mmap) · Caffeine · Redis · StarRocks · Kafka · S3.

## Kiểm tra
- Response khớp `data-flow-examples.md` §6; publish version mới giữa lúc phân trang không làm lệch kết quả.
- Local: `contains`/`count` p99 < 10ms, `segments by user` p99 < 20ms với 5K segment giả.
