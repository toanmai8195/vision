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

Checklist: xem mục "Bước 7" trong `../checklist.md`.
