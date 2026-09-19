# P8 — Scale & hardening ⬜

> Chạy toàn bộ pipeline P2–P6 ở quy mô thiết kế, đo, tối ưu và viết runbook.
> Thiết kế gốc: `CLAUDE.md` §0 (quy mô), §7 (SLA), §10 (SLO); `com/tm/docs/capacity.md`.

## Input / Output

| | Nội dung | Nơi lưu | Dạng |
|---|---|---|---|
| **Input** | Dữ liệu synthetic: 100M user, 500 attribute (phân bổ 4 loại theo `capacity.md`), 500–1000 tag/attribute STANDARD + vài attribute EXTENDED hàng trăm nghìn tag, 400 ngày, 5K segment | sinh bằng simulator → Kafka / MinIO → toàn pipeline | như P2 |
| **Output** | Số đo thật: thời gian từng bước L3→L6, dung lượng từng bảng, chi phí theo loại dữ liệu | `com/tm/docs/capacity.md`, dashboard P7 | tài liệu + metric |
| **Output** | Cấu hình đã tune | StarRocks (bucket, partition, tablet, spill, resource group), Airflow pool | config |
| **Output** | Runbook | `com/tm/docs/` | tài liệu |

## Flow

```
simulator (quy mô lớn) ──▶ P2 silver ──▶ P3 daily ──▶ P4 range ──▶ P5 segment ──▶ P6 API
            │                  đo mỗi bước (P7 metrics) ─────────────────────────────┘
            ▼
      so với SLA / SLO ──▶ tối ưu điểm nghẽn ──▶ đo lại ──▶ cập nhật capacity.md
```

## Các bước

### Bước 1 — Sinh dữ liệu quy mô lớn
Simulator chế độ scale; nạp 400 ngày lịch sử bằng `vision_backfill`.

### Bước 2 — Benchmark từng bước
| Bước | Đo |
|---|---|
| L3 daily | thời gian / attrGroup, dung lượng `tag_daily`, `pv_daily` |
| L4 block / latest | số phép bitmap / ngày, dung lượng block 400 ngày |
| L5 range | thời gian theo `data_type` × date range; kích thước `pv_range_value` |
| L6 segment | thời gian build 5K segment, tỉ lệ cache hit condition |
| L7 API | p99 ở tải mục tiêu |

### Bước 3 — Tối ưu (chỉ khi số đo cần)
- PARTIAL_VALUE(_BY_TAG): chỉ `supportedDateRanges` → rolling incremental → BSI (bit-sliced index).
- EXTENDED: kiểm chi phí usage-driven khi nhiều tag được dùng cùng lúc.
- StarRocks: bucket, partition, tablet, spill, resource group.

### Bước 4 — Runbook
Backfill · rollback version segment · reprocess late data · thay đổi định nghĩa tag.

## Công nghệ
Toàn bộ stack P2–P7.

## Kiểm tra
Range xong 05:00; **5K segment published 07:00**; đạt SLO API; không loại dữ liệu nào vượt ngân sách thời gian riêng của nó.
