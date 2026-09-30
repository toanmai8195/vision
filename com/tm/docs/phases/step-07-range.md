# Bước 7 — Range (L5)

| | Nội dung |
|---|---|
| **Input** | block, LATEST/POS/STATE |
| **Output** | `gold.tag_range_bitmap`, `gold.pv_range_value` cho mọi date range trong `supportedDateRanges` |

```
MUTEX/NOT_MUTEX : LATEST/POS ∩ SEEN(window) ──▶ tag_range_bitmap
PARTIAL_VALUE*  : AGG ⊕ block ──▶ pv_range_value   (so valueRange lúc build)
EXTENDED        : chỉ tag đang được segment dùng (usage-driven)
custom range    : on-demand + cache
```

- Range: A1…A180, IN_MONTH, LAST_MONTH, ALWAYS_ACTIVE (incremental).
- Ngưỡng `valueRange` luôn so lúc build, không precompute.
- Done: mọi range khớp `reference.py`; MUTEX rời nhau trong mỗi window.

## Checklist
**Mục tiêu**: có sẵn kết quả cho mọi date range (A1…A180, IN_MONTH, LAST_MONTH, ALWAYS_ACTIVE).
- [ ] `tag_range_bitmap` (nhãn và bucket)
- [ ] `pv_range_value` (số, lọc `valueRange` lúc build)
- [ ] EXTENDED: chỉ tính tag đang được segment dùng (usage-driven)
- [ ] Custom date range: tính on-demand + cache

**Done khi**: mọi range khớp `reference.py`; MUTEX rời nhau trong mỗi window.
